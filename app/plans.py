"""Plan version registry and entitlement transform between versions."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from .db import Database
from .models import PlanVersion, UserEntitlement


class PlanNotFoundError(KeyError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _row_to_plan(row) -> PlanVersion:
    raw_map = json.loads(row["migration_map"])
    return PlanVersion(
        plan_id=row["plan_id"],
        version=row["version"],
        entitlements={k: int(v) for k, v in json.loads(row["entitlements"]).items()},
        migration_map={k: (v[0], float(v[1])) for k, v in raw_map.items()},
    )


class PlanRepository:
    def __init__(self, db: Database) -> None:
        self._db = db

    def create_version(
        self,
        plan_id: str,
        version: int,
        entitlements: dict[str, int],
        migration_map: dict[str, tuple[str, float]] | None = None,
    ) -> PlanVersion:
        plan = PlanVersion(
            plan_id=plan_id,
            version=version,
            entitlements=dict(entitlements),
            migration_map=dict(migration_map or {}),
        )
        with self._db.transaction() as conn:
            conn.execute(
                "INSERT INTO plan_versions (plan_id, version, entitlements,"
                " migration_map, created_at) VALUES (?, ?, ?, ?, ?)",
                (
                    plan.plan_id,
                    plan.version,
                    json.dumps(plan.entitlements),
                    json.dumps({k: [v[0], v[1]] for k, v in plan.migration_map.items()}),
                    _now(),
                ),
            )
        return plan

    def get(self, plan_id: str, version: int) -> PlanVersion:
        with self._db.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM plan_versions WHERE plan_id = ? AND version = ?",
                (plan_id, version),
            ).fetchone()
        if row is None:
            raise PlanNotFoundError(f"{plan_id}@v{version}")
        return _row_to_plan(row)

    def latest_version(self, plan_id: str) -> PlanVersion:
        with self._db.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM plan_versions WHERE plan_id = ?"
                " ORDER BY version DESC LIMIT 1",
                (plan_id,),
            ).fetchone()
        if row is None:
            raise PlanNotFoundError(plan_id)
        return _row_to_plan(row)

    def list_versions(self, plan_id: str) -> list[PlanVersion]:
        with self._db.transaction() as conn:
            rows = conn.execute(
                "SELECT * FROM plan_versions WHERE plan_id = ? ORDER BY version",
                (plan_id,),
            ).fetchall()
        return [_row_to_plan(r) for r in rows]


def transform_entitlement(
    source: UserEntitlement,
    from_plan: PlanVersion,
    to_plan: PlanVersion,
) -> UserEntitlement:
    """Map a user's quota snapshot from one plan version to the next.

    Rules:
    - fields present in from_plan.migration_map carry over scaled by ratio
      (floored to int, never negative);
    - fields only in the new version get the new plan's default;
    - fields removed in the new version are dropped.
    """
    carried: dict[str, int] = {}
    for new_field, (old_field, ratio) in from_plan.migration_map.items():
        if new_field not in to_plan.entitlements:
            continue
        if old_field in source.entitlements:
            carried[new_field] = max(0, int(source.entitlements[old_field] * ratio))

    new_entitlements: dict[str, int] = {}
    for field, default in to_plan.entitlements.items():
        if field in carried:
            new_entitlements[field] = carried[field]
        elif field in source.entitlements:
            # same field name in both versions, no explicit mapping: keep value
            new_entitlements[field] = source.entitlements[field]
        else:
            new_entitlements[field] = default

    return UserEntitlement(
        user_id=source.user_id,
        plan_id=to_plan.plan_id,
        plan_version=to_plan.version,
        entitlements=new_entitlements,
    )
