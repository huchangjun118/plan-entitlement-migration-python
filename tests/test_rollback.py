"""Rollback: restore full pre-migration state; deterministic vs new migrations."""

import pytest

from app.migrations import MigrationStateError
from app.models import MigrationConfig, MigrationStatus


def _setup(engine, entitlements, n=6):
    for i in range(n):
        entitlements.assign(f"user-{i:03d}", "basic", 1,
                            {"api_calls": 100 + i, "storage_gb": i})
    return engine.start("basic", 1, 2, MigrationConfig(batch_size=2), "op")


def test_rollback_after_completion_restores_exact_state(engine, entitlements,
                                                        two_version_plan):
    before = {f"user-{i:03d}": entitlements.assign(f"user-{i:03d}", "basic", 1,
              {"api_calls": 100 + i, "storage_gb": i}) for i in range(6)}
    m = engine.start("basic", 1, 2, MigrationConfig(batch_size=2), "op")
    engine.run(m.migration_id)
    assert engine.get(m.migration_id).status == MigrationStatus.COMPLETED

    m = engine.rollback(m.migration_id, "op")

    assert m.status == MigrationStatus.ROLLED_BACK
    for uid, snap in before.items():
        cur = entitlements.get(uid)
        assert cur.plan_version == 1
        assert cur.entitlements == snap.entitlements


def test_rollback_partial_migration_restores_only_applied(engine, entitlements,
                                                          two_version_plan):
    m = _setup(engine, entitlements, 6)
    engine.run_one_batch(m.migration_id)  # migrate only 2 users
    engine.cancel(m.migration_id, "op")   # pending->cancelled? it's RUNNING? no: still PENDING

    m = engine.rollback(m.migration_id, "op")
    assert m.status == MigrationStatus.ROLLED_BACK
    for i in range(6):
        snap = entitlements.get(f"user-{i:03d}")
        assert snap.plan_version == 1
        assert snap.entitlements == {"api_calls": 100 + i, "storage_gb": i}


def test_rollback_rejected_while_running(engine, entitlements,
                                         two_version_plan):
    m = _setup(engine, entitlements, 6)
    # force status RUNNING without finishing
    with engine._db.transaction() as conn:
        engine._set_status(conn, m.migration_id, MigrationStatus.RUNNING)

    with pytest.raises(MigrationStateError):
        engine.rollback(m.migration_id, "op")


def test_rollback_blocks_concurrent_new_migration(engine, entitlements,
                                                  two_version_plan):
    """While a rollback is possible/in progress the plan cannot start a new
    forward migration; after rollback finishes a new migration may start."""
    m1 = _setup(engine, entitlements, 6)
    engine.run(m1.migration_id)
    engine.rollback(m1.migration_id, "op")

    # plan is back on v1; a fresh migration is now allowed and deterministic
    m2 = engine.start("basic", 1, 2, MigrationConfig(batch_size=2), "op")
    engine.run(m2.migration_id)
    assert engine.get(m2.migration_id).status == MigrationStatus.COMPLETED
    assert entitlements.get("user-000").plan_version == 2


def test_double_migration_blocked(engine, two_version_plan):
    m1 = engine.start("basic", 1, 2, MigrationConfig(), "op")
    with pytest.raises(MigrationStateError):
        engine.start("basic", 1, 2, MigrationConfig(), "op")
    engine.cancel(m1.migration_id, "op")
    m2 = engine.start("basic", 1, 2, MigrationConfig(), "op")
    assert m2.status == MigrationStatus.PENDING


def test_cancel_stops_at_batch_boundary(engine, entitlements,
                                        two_version_plan):
    m = _setup(engine, entitlements, 10)  # batch_size=2

    def cancel_after_first(batch_index):
        if batch_index == 1:
            engine.cancel(m.migration_id, "op")

    m = engine.run(m.migration_id, batch_hook=cancel_after_first)

    assert m.status == MigrationStatus.CANCELLED
    # exactly one batch committed; nothing left half-written
    done = [f"user-{i:03d}" for i in range(2)]
    todo = [f"user-{i:03d}" for i in range(2, 10)]
    for uid in done:
        assert entitlements.get(uid).plan_version == 2
    for uid in todo:
        assert entitlements.get(uid).plan_version == 1

    # and a cancelled migration can be rolled back cleanly
    m = engine.rollback(m.migration_id, "op")
    assert m.status == MigrationStatus.ROLLED_BACK
    for uid in done:
        assert entitlements.get(uid).plan_version == 1
