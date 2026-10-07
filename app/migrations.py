"""Online migration engine: batched, resumable, cancellable, rollbackable."""

from __future__ import annotations

import json
import threading
import time
import uuid
from collections.abc import Callable
from datetime import datetime, timezone

from .audit import AuditLog
from .db import Database
from .models import (
    Migration,
    MigrationConfig,
    MigrationStatus,
    UserEntitlement,
)
from .plans import PlanRepository, transform_entitlement


class MigrationNotFoundError(KeyError):
    pass


class MigrationStateError(RuntimeError):
    """Illegal state transition (e.g. rollback of a running migration)."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


_ACTIVE_STATUSES = (
    MigrationStatus.PENDING.value,
    MigrationStatus.RUNNING.value,
    MigrationStatus.CANCELLING.value,
    MigrationStatus.ROLLING_BACK.value,
)


def _ent_to_snap(e: UserEntitlement) -> dict:
    return {
        "user_id": e.user_id,
        "plan_id": e.plan_id,
        "plan_version": e.plan_version,
        "entitlements": e.entitlements,
    }


def _snap_to_ent(s: dict) -> UserEntitlement:
    return UserEntitlement(
        user_id=s["user_id"],
        plan_id=s["plan_id"],
        plan_version=s["plan_version"],
        entitlements={k: int(v) for k, v in s["entitlements"].items()},
    )


class MigrationEngine:
    """Drives plan-version migrations for user entitlement rows.

    Guarantees:
    - each user row is migrated in its own transaction together with the
      migration record and the checkpoint, so an interruption never leaves
      partial writes and resume never re-applies or skips;
    - a row is only rewritten while it still carries from_version, so
      concurrent normal writes never get mixed with migrated fields;
    - rollback replays migration records in reverse inside per-user
      transactions and is mutually exclusive with forward migration.
    """

    def __init__(self, db: Database, plans: PlanRepository, audit: AuditLog) -> None:
        self._db = db
        self._plans = plans
        self._audit = audit
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # persistence helpers
    # ------------------------------------------------------------------
    def _row_to_migration(self, row) -> Migration:
        cfg = json.loads(row["config"])
        return Migration(
            migration_id=row["migration_id"],
            plan_id=row["plan_id"],
            from_version=row["from_version"],
            to_version=row["to_version"],
            status=MigrationStatus(row["status"]),
            checkpoint=row["checkpoint"],
            config=MigrationConfig(**cfg),
            created_by=row["created_by"],
            error=row["error"],
        )

    def _load(self, migration_id: str) -> Migration:
        with self._db.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM migrations WHERE migration_id = ?", (migration_id,)
            ).fetchone()
        if row is None:
            raise MigrationNotFoundError(migration_id)
        return self._row_to_migration(row)

    def _set_status(self, conn, migration_id: str, status: MigrationStatus,
                    error: str | None = None) -> None:
        conn.execute(
            "UPDATE migrations SET status = ?, error = ?, updated_at = ?"
            " WHERE migration_id = ?",
            (status.value, error, _now(), migration_id),
        )

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    def start(self, plan_id: str, from_version: int, to_version: int,
              config: MigrationConfig, actor: str) -> Migration:
        from_plan = self._plans.get(plan_id, from_version)  # raises if missing
        self._plans.get(plan_id, to_version)
        if from_version == to_version:
            raise MigrationStateError("from_version == to_version")
        if config.batch_size <= 0:
            raise MigrationStateError("batch_size must be > 0")

        migration_id = uuid.uuid4().hex
        with self._lock, self._db.transaction() as conn:
            active = conn.execute(
                f"SELECT COUNT(*) AS n FROM migrations WHERE plan_id = ?"
                f" AND status IN ({','.join('?' * len(_ACTIVE_STATUSES))})",
                (plan_id, *_ACTIVE_STATUSES),
            ).fetchone()
            if active["n"] > 0:
                raise MigrationStateError(
                    f"plan {plan_id} already has an active migration")
            conn.execute(
                "INSERT INTO migrations (migration_id, plan_id, from_version,"
                " to_version, status, checkpoint, config, created_by, created_at,"
                " updated_at) VALUES (?, ?, ?, ?, ?, '', ?, ?, ?, ?)",
                (migration_id, plan_id, from_version, to_version,
                 MigrationStatus.PENDING.value,
                 json.dumps({
                     "batch_size": config.batch_size,
                     "rate_limit_per_sec": config.rate_limit_per_sec,
                     "batch_timeout_sec": config.batch_timeout_sec,
                 }),
                 actor, _now(), _now()),
            )
        self._audit.record(actor, "migration.start", migration_id, True, {
            "plan_id": plan_id, "from": from_plan.version, "to": to_version,
        })
        return self._load(migration_id)

    def get(self, migration_id: str) -> Migration:
        return self._load(migration_id)

    # ------------------------------------------------------------------
    # forward migration
    # ------------------------------------------------------------------
    def run(self, migration_id: str,
            batch_hook: Callable[[int], None] | None = None,
            config: MigrationConfig | None = None) -> Migration:
        """Process batches until done, cancelled or failed.

        batch_hook (test seam) is invoked with the batch index before each
        batch; raising from it simulates a crash between batches.
        config, when given, replaces the stored config (e.g. relaxing a
        timeout before resuming a failed migration).
        """
        migration = self._load(migration_id)
        with self._lock:
            if config is not None:
                with self._db.transaction() as conn:
                    conn.execute(
                        "UPDATE migrations SET config = ?, updated_at = ?"
                        " WHERE migration_id = ?",
                        (json.dumps({
                            "batch_size": config.batch_size,
                            "rate_limit_per_sec": config.rate_limit_per_sec,
                            "batch_timeout_sec": config.batch_timeout_sec,
                        }), _now(), migration_id))
            if migration.status in (MigrationStatus.PENDING,
                                    MigrationStatus.CANCELLED,
                                    MigrationStatus.FAILED):
                with self._db.transaction() as conn:
                    self._set_status(conn, migration_id, MigrationStatus.RUNNING)
            migration = self._load(migration_id)
            if migration.status != MigrationStatus.RUNNING:
                raise MigrationStateError(
                    f"cannot run migration in status {migration.status.value}")

        batch_index = 0
        try:
            while True:
                if batch_hook is not None:
                    batch_hook(batch_index)

                migration = self._load(migration_id)
                if migration.status == MigrationStatus.CANCELLING:
                    with self._db.transaction() as conn:
                        self._set_status(conn, migration_id,
                                         MigrationStatus.CANCELLED)
                    break
                if migration.status != MigrationStatus.RUNNING:
                    break

                started = time.monotonic()
                migrated = self.run_one_batch(migration_id)
                elapsed = time.monotonic() - started
                if elapsed > migration.config.batch_timeout_sec:
                    with self._db.transaction() as conn:
                        self._set_status(conn, migration_id, MigrationStatus.FAILED,
                                         f"batch timeout: {elapsed:.3f}s > "
                                         f"{migration.config.batch_timeout_sec}s")
                    break
                if migrated == 0:
                    with self._db.transaction() as conn:
                        self._set_status(conn, migration_id,
                                         MigrationStatus.COMPLETED)
                    break
                batch_index += 1

                rate = migration.config.rate_limit_per_sec
                if rate > 0:
                    # throttle: cap users/sec so slow batches cannot starve
                    # online requests of the write lock
                    target = migrated / rate
                    if target > elapsed:
                        time.sleep(target - elapsed)
        except Exception as exc:
            with self._db.transaction() as conn:
                self._set_status(conn, migration_id, MigrationStatus.FAILED,
                                 f"{type(exc).__name__}: {exc}")
            if batch_hook is None:
                raise
        return self._load(migration_id)

    def run_one_batch(self, migration_id: str) -> int:
        """Migrate at most config.batch_size users; returns rows migrated."""
        migration = self._load(migration_id)
        from_plan = self._plans.get(migration.plan_id, migration.from_version)
        to_plan = self._plans.get(migration.plan_id, migration.to_version)

        with self._db.transaction() as conn:
            rows = conn.execute(
                "SELECT * FROM user_entitlements WHERE plan_id = ?"
                " AND plan_version = ? AND user_id > ?"
                " ORDER BY user_id LIMIT ?",
                (migration.plan_id, migration.from_version,
                 migration.checkpoint, migration.config.batch_size),
            ).fetchall()

        migrated = 0
        for row in rows:
            source = UserEntitlement(
                user_id=row["user_id"],
                plan_id=row["plan_id"],
                plan_version=row["plan_version"],
                entitlements={k: int(v) for k, v in
                              json.loads(row["entitlements"]).items()},
            )
            target = transform_entitlement(source, from_plan, to_plan)
            if self._migrate_one(migration, source, target):
                migrated += 1
        return migrated

    def _migrate_one(self, migration: Migration, source: UserEntitlement,
                     target: UserEntitlement) -> bool:
        """Atomically migrate one user row, record it and move the checkpoint.

        The conditional UPDATE (plan_version = from_version) races safely
        with concurrent normal writes: if the row changed underneath us we
        simply retry/skip, never mixing fields of two versions.
        """
        with self._db.transaction() as conn:
            cur = conn.execute(
                "UPDATE user_entitlements SET plan_version = ?, entitlements = ?,"
                " updated_at = ? WHERE user_id = ? AND plan_version = ?",
                (target.plan_version, json.dumps(target.entitlements), _now(),
                 source.user_id, migration.from_version),
            )
            if cur.rowcount == 1:
                conn.execute(
                    "INSERT INTO migration_records (migration_id, user_id,"
                    " before_snapshot, after_snapshot, rolled_back, applied_at)"
                    " VALUES (?, ?, ?, ?, 0, ?)",
                    (migration.migration_id, source.user_id,
                     json.dumps(_ent_to_snap(source)),
                     json.dumps(_ent_to_snap(target)), _now()),
                )
            # checkpoint always advances so a concurrently reassigned row
            # cannot wedge the scan
            conn.execute(
                "UPDATE migrations SET checkpoint = ?, updated_at = ?"
                " WHERE migration_id = ?",
                (source.user_id, _now(), migration.migration_id),
            )
            return cur.rowcount == 1

    # ------------------------------------------------------------------
    # cancel / rollback
    # ------------------------------------------------------------------
    def cancel(self, migration_id: str, actor: str) -> Migration:
        """Cooperative cancel: takes effect at the next batch boundary."""
        with self._lock:
            migration = self._load(migration_id)
            if migration.status == MigrationStatus.PENDING:
                with self._db.transaction() as conn:
                    self._set_status(conn, migration_id, MigrationStatus.CANCELLED)
            elif migration.status == MigrationStatus.RUNNING:
                with self._db.transaction() as conn:
                    self._set_status(conn, migration_id, MigrationStatus.CANCELLING)
            else:
                raise MigrationStateError(
                    f"cannot cancel migration in status {migration.status.value}")
        self._audit.record(actor, "migration.cancel", migration_id, True)
        return self._load(migration_id)

    def rollback(self, migration_id: str, actor: str) -> Migration:
        """Restore all pre-migration snapshots recorded so far.

        Allowed only from a terminal forward state (completed/failed/
        cancelled); concurrent forward runs are excluded by the
        ROLLING_BACK status which also blocks new migrations on the plan,
        making the outcome deterministic.
        """
        with self._lock:
            migration = self._load(migration_id)
            if migration.status not in (MigrationStatus.COMPLETED,
                                        MigrationStatus.FAILED,
                                        MigrationStatus.CANCELLED):
                raise MigrationStateError(
                    f"cannot rollback migration in status"
                    f" {migration.status.value}; cancel it first")
            with self._db.transaction() as conn:
                self._set_status(conn, migration_id, MigrationStatus.ROLLING_BACK)

        try:
            while True:
                with self._db.transaction() as conn:
                    row = conn.execute(
                        "SELECT * FROM migration_records WHERE migration_id = ?"
                        " AND rolled_back = 0 ORDER BY user_id DESC LIMIT 1",
                        (migration_id,),
                    ).fetchone()
                if row is None:
                    break
                self._rollback_one(migration_id, row)
        except Exception:
            with self._db.transaction() as conn:
                self._set_status(conn, migration_id, MigrationStatus.FAILED,
                                 "rollback failed")
            raise

        with self._db.transaction() as conn:
            self._set_status(conn, migration_id, MigrationStatus.ROLLED_BACK)
        self._audit.record(actor, "migration.rollback", migration_id, True)
        return self._load(migration_id)

    def _rollback_one(self, migration_id: str, record_row) -> None:
        before = record_row["before_snapshot"]
        user_id = record_row["user_id"]
        with self._db.transaction() as conn:
            if before is None:
                # user did not exist before migration: remove the row
                conn.execute("DELETE FROM user_entitlements WHERE user_id = ?",
                             (user_id,))
            else:
                snap = json.loads(before)
                conn.execute(
                    "UPDATE user_entitlements SET plan_id = ?, plan_version = ?,"
                    " entitlements = ?, updated_at = ? WHERE user_id = ?",
                    (snap["plan_id"], snap["plan_version"],
                     json.dumps(snap["entitlements"]), _now(), user_id),
                )
            conn.execute(
                "UPDATE migration_records SET rolled_back = 1"
                " WHERE migration_id = ? AND user_id = ?",
                (migration_id, user_id),
            )
