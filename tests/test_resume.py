"""Interruption & resume: restart continues from the last committed position."""

import pytest

from app.models import MigrationConfig, MigrationStatus


def test_crash_between_batches_then_resume(engine, entitlements,
                                           two_version_plan):
    quota = {"api_calls": 100, "storage_gb": 1}
    for i in range(10):
        entitlements.assign(f"user-{i:03d}", "basic", 1, dict(quota))

    m = engine.start("basic", 1, 2, MigrationConfig(batch_size=4), "op")

    # simulate a crash after the first batch commits
    def crash_after_first(batch_index):
        if batch_index == 1:
            raise RuntimeError("simulated crash")

    m = engine.run(m.migration_id, batch_hook=crash_after_first)
    assert m.status == MigrationStatus.FAILED
    assert m.checkpoint == "user-003"  # exactly one batch committed

    # resume: must continue from checkpoint, no re-apply, no skip
    m = engine.run(m.migration_id)
    assert m.status == MigrationStatus.COMPLETED

    for i in range(10):
        snap = entitlements.get(f"user-{i:03d}")
        assert snap.plan_version == 2
        assert snap.entitlements == {"requests": 200, "seats": 3}

    # every user recorded exactly once -> no duplicate application
    rows = engine._db._conn.execute(
        "SELECT user_id, COUNT(*) n FROM migration_records"
        " WHERE migration_id = ? GROUP BY user_id", (m.migration_id,)).fetchall()
    assert len(rows) == 10
    assert all(r["n"] == 1 for r in rows)


def test_no_partial_write_when_batch_fails_mid_user(engine, entitlements,
                                                    two_version_plan, db):
    for i in range(5):
        entitlements.assign(f"user-{i:03d}", "basic", 1,
                            {"api_calls": 50, "storage_gb": 1})

    m = engine.start("basic", 1, 2, MigrationConfig(batch_size=5), "op")

    # corrupt the transform target mid-batch by deleting plan v2 after start
    real_migrate_one = engine._migrate_one
    calls = {"n": 0}

    def failing_migrate_one(migration, source, target):
        calls["n"] += 1
        if calls["n"] == 3:
            raise RuntimeError("boom on third user")
        return real_migrate_one(migration, source, target)

    engine._migrate_one = failing_migrate_one
    with pytest.raises(RuntimeError):
        engine.run_one_batch(m.migration_id)

    # first two users committed with their records; third left untouched
    m = engine.get(m.migration_id)
    assert m.checkpoint == "user-001"
    assert entitlements.get("user-002").plan_version == 1
    assert entitlements.get("user-000").plan_version == 2
