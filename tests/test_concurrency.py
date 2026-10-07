"""Migration concurrent with normal reads/writes: reads stay self-consistent."""

import threading

from app.models import MigrationConfig, MigrationStatus

V1_FIELDS = {"api_calls", "storage_gb"}
V2_FIELDS = {"requests", "seats"}


def test_reads_never_see_mixed_versions(engine, entitlements,
                                        two_version_plan):
    for i in range(40):
        entitlements.assign(f"user-{i:03d}", "basic", 1,
                            {"api_calls": 1000, "storage_gb": 1})

    m = engine.start("basic", 1, 2, MigrationConfig(batch_size=3), "op")

    violations: list[str] = []
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            for i in range(40):
                snap = entitlements.get(f"user-{i:03d}")
                fields = set(snap.entitlements)
                if snap.plan_version == 1 and fields != V1_FIELDS:
                    violations.append(f"v1 row with fields {fields}")
                if snap.plan_version == 2 and fields != V2_FIELDS:
                    violations.append(f"v2 row with fields {fields}")

    def consumer():
        # normal online writes racing the migration
        while not stop.is_set():
            for i in range(40):
                try:
                    entitlements.consume(f"user-{i:03d}", "api_calls", 1)
                except Exception:
                    pass  # field gone after migration / conflicts are fine

    threads = [threading.Thread(target=reader) for _ in range(3)]
    threads.append(threading.Thread(target=consumer))
    for t in threads:
        t.start()

    m = engine.run(m.migration_id)
    stop.set()
    for t in threads:
        t.join()

    assert m.status == MigrationStatus.COMPLETED
    assert violations == []
    for i in range(40):
        assert entitlements.get(f"user-{i:03d}").plan_version == 2


def test_consume_during_migration_is_atomic(engine, entitlements,
                                            two_version_plan):
    """A consume that lands before the migration keeps its decrement after
    the version-conditional rewrite (no lost update, no mixed write)."""
    entitlements.assign("u1", "basic", 1, {"api_calls": 10, "storage_gb": 1})
    entitlements.consume("u1", "api_calls", 4)

    m = engine.start("basic", 1, 2, MigrationConfig(batch_size=10), "op")
    engine.run(m.migration_id)

    snap = entitlements.get("u1")
    assert snap.plan_version == 2
    # 6 remaining api_calls * ratio 2.0
    assert snap.entitlements["requests"] == 12
