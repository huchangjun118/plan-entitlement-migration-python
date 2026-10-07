"""User entitlement reads and normal (non-migration) writes."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from .db import Database
from .models import UserEntitlement


class EntitlementNotFoundError(KeyError):
    pass


class VersionConflictError(RuntimeError):
    """The row changed version between read and write (concurrent update)."""


class InsufficientQuotaError(RuntimeError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _row_to_entitlement(row) -> UserEntitlement:
    return UserEntitlement(
        user_id=row["user_id"],
        plan_id=row["plan_id"],
        plan_version=row["plan_version"],
        entitlements={k: int(v) for k, v in json.loads(row["entitlements"]).items()},
    )


class EntitlementService:
    def __init__(self, db: Database) -> None:
        self._db = db

    def get(self, user_id: str) -> UserEntitlement:
        """Consistent single-version snapshot of plan + quota.

        plan_id, plan_version and entitlements live in one row updated only
        inside transactions, so a read can never observe a field-level mix
        of two plan versions.
        """
        with self._db.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM user_entitlements WHERE user_id = ?", (user_id,)
            ).fetchone()
        if row is None:
            raise EntitlementNotFoundError(user_id)
        return _row_to_entitlement(row)

    def assign(self, user_id: str, plan_id: str, plan_version: int,
               entitlements: dict[str, int]) -> UserEntitlement:
        snap = UserEntitlement(user_id, plan_id, plan_version, dict(entitlements))
        with self._db.transaction() as conn:
            conn.execute(
                "INSERT INTO user_entitlements"
                " (user_id, plan_id, plan_version, entitlements, updated_at)"
                " VALUES (?, ?, ?, ?, ?)"
                " ON CONFLICT(user_id) DO UPDATE SET"
                " plan_id=excluded.plan_id, plan_version=excluded.plan_version,"
                " entitlements=excluded.entitlements, updated_at=excluded.updated_at",
                (user_id, plan_id, plan_version,
                 json.dumps(snap.entitlements), _now()),
            )
        return snap

    def consume(self, user_id: str, field: str, amount: int) -> UserEntitlement:
        """Decrement quota atomically; safe to run concurrently with migration.

        The read-modify-write happens inside one transaction, and the UPDATE
        is guarded by the plan_version read at the start, so a concurrent
        migration that bumps the version turns this into a VersionConflict
        instead of silently writing old-version fields onto a new-version row.
        """
        if amount < 0:
            raise ValueError("amount must be >= 0")
        with self._db.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM user_entitlements WHERE user_id = ?", (user_id,)
            ).fetchone()
            if row is None:
                raise EntitlementNotFoundError(user_id)
            snap = _row_to_entitlement(row)
            if field not in snap.entitlements:
                raise EntitlementNotFoundError(f"{user_id}:{field}")
            if snap.entitlements[field] < amount:
                raise InsufficientQuotaError(
                    f"{user_id}:{field} have={snap.entitlements[field]} want={amount}")
            snap.entitlements[field] -= amount
            cur = conn.execute(
                "UPDATE user_entitlements SET entitlements = ?, updated_at = ?"
                " WHERE user_id = ? AND plan_version = ?",
                (json.dumps(snap.entitlements), _now(), user_id, snap.plan_version),
            )
            if cur.rowcount != 1:
                raise VersionConflictError(user_id)
        return snap
