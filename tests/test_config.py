"""Batching, rate limiting and timeout configuration semantics."""

import time

from app.models import MigrationConfig, MigrationStatus


def test_batch_size_controls_rows_per_batch(engine, entitlements,
                                            two_version_plan):
    for i in range(7):
        entitlements.assign(f"user-{i:03d}", "basic", 1,
                            {"api_calls": 10, "storage_gb": 1})
    m = engine.start("basic", 1, 2, MigrationConfig(batch_size=3), "op")

    assert engine.run_one_batch(m.migration_id) == 3
    assert engine.get(m.migration_id).checkpoint == "user-002"
    assert engine.run_one_batch(m.migration_id) == 3
    assert engine.run_one_batch(m.migration_id) == 1
    assert engine.run_one_batch(m.migration_id) == 0  # nothing left


def test_rate_limit_throttles_throughput(engine, entitlements,
                                         two_version_plan):
    for i in range(6):
        entitlements.assign(f"user-{i:03d}", "basic", 1,
                            {"api_calls": 10, "storage_gb": 1})
    # 3 users/batch at 20 users/sec -> each batch paced to >= 0.15s
    m = engine.start("basic", 1, 2,
                     MigrationConfig(batch_size=3, rate_limit_per_sec=20.0),
                     "op")

    started = time.monotonic()
    m = engine.run(m.migration_id)
    elapsed = time.monotonic() - started

    assert m.status == MigrationStatus.COMPLETED
    assert elapsed >= 0.25  # two paced batches


def test_batch_timeout_marks_failed_without_partial_state(engine, entitlements,
                                                          two_version_plan):
    for i in range(4):
        entitlements.assign(f"user-{i:03d}", "basic", 1,
                            {"api_calls": 10, "storage_gb": 1})
    m = engine.start("basic", 1, 2,
                     MigrationConfig(batch_size=2, batch_timeout_sec=0.0),
                     "op")

    m = engine.run(m.migration_id)

    assert m.status == MigrationStatus.FAILED
    assert "timeout" in (m.error or "")
    # the timed-out batch itself committed atomically; state is consistent
    # and resumable (with a relaxed timeout) rather than half-written
    m = engine.run(m.migration_id,
                   config=MigrationConfig(batch_size=2,
                                          batch_timeout_sec=30.0))
    assert m.status == MigrationStatus.COMPLETED
    for i in range(4):
        assert entitlements.get(f"user-{i:03d}").plan_version == 2
