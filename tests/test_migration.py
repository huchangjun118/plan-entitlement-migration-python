"""迁移引擎测试：断点续跑、并发读写自洽、回退、取消/超时。"""
import threading
import time

import pytest

from app.migration import MigrationEngine, MigrationError
from app.models import EntitlementRecord, MigrationStatus, PlanDefinition
from app.store import Store


@pytest.fixture()
def store():
    s = Store()
    s.put_plan(PlanDefinition("basic", 1, {"seats": 5, "storage_gb": 10}))
    s.put_plan(PlanDefinition("basic", 2, {"seats": 20, "storage_gb": 50, "ai_credits": 100}))
    yield s
    s.close()


@pytest.fixture()
def engine(store):
    return MigrationEngine(store)


def _seed_users(store, n, prefix="u"):
    for i in range(n):
        store.put_entitlement(EntitlementRecord(
            user_id=f"{prefix}{i:04d}", plan_id="basic", plan_version=1,
            quotas={"seats": 5, "storage_gb": 10},
            usage={"seats": 2, "storage_gb": 3}))


def test_full_migration(store, engine):
    _seed_users(store, 10)
    job = engine.create_job("basic", 2, "op", batch_size=3)
    assert job.from_version == 1
    job = engine.run(job.job_id, "op")
    assert job.status == MigrationStatus.COMPLETED
    assert job.checkpoint.processed == 10
    for i in range(10):
        rec = store.get_entitlement(f"u{i:04d}")
        assert rec.plan_version == 2
        assert rec.quotas == {"seats": 20, "storage_gb": 50, "ai_credits": 100}
        # usage 只保留新计划中仍存在的资源
        assert rec.usage == {"seats": 2, "storage_gb": 3}


def test_resume_after_crash(store, engine):
    """模拟中断：提交部分批次后丢弃 engine，用新 engine 从 checkpoint 续跑。"""
    _seed_users(store, 7)
    job = engine.create_job("basic", 2, "op", batch_size=2)
    # 手动提交一个批次后“崩溃”（不更新任务状态为完成）
    ids = store.list_entitlement_user_ids("basic", 1, "", 2)
    recs = [store.get_entitlement(uid) for uid in ids]
    store.save_snapshots(job.job_id, recs)
    migrated = [engine._migrate_record(r, 2) for r in recs]
    from app.models import MigrationCheckpoint
    store.commit_batch(job, migrated, MigrationCheckpoint(last_user_id=ids[-1], processed=2))

    # 重新执行：必须从 u0001 之后继续，不重复、不跳过
    engine2 = MigrationEngine(store)
    job = engine2.run(job.job_id, "op")
    assert job.status == MigrationStatus.COMPLETED
    assert job.checkpoint.processed == 7
    versions = {store.get_entitlement(f"u{i:04d}").plan_version for i in range(7)}
    assert versions == {2}
    # 断点续跑不重复应用：record_version 只增加一次
    assert store.get_entitlement("u0000").record_version == 1


def test_concurrent_reads_always_consistent(store, engine):
    """迁移进行中并发读取：每次读到的 plan_version 与 quotas 必须属于同一版本。"""
    _seed_users(store, 50)
    job = engine.create_job("basic", 2, "op", batch_size=1, rate_limit_per_sec=200)
    expected = {1: {"seats": 5, "storage_gb": 10},
                2: {"seats": 20, "storage_gb": 50, "ai_credits": 100}}
    violations = []
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            rec = store.get_entitlement("u0025")
            if rec is not None and rec.quotas != expected[rec.plan_version]:
                violations.append((rec.plan_version, dict(rec.quotas)))

    t = threading.Thread(target=reader)
    t.start()
    engine.run(job.job_id, "op")
    stop.set()
    t.join()
    assert violations == []


def test_concurrent_run_rejected(store, engine):
    _seed_users(store, 200)
    job = engine.create_job("basic", 2, "op", batch_size=1, rate_limit_per_sec=500)
    errors = []

    def run():
        try:
            engine.run(job.job_id, "op")
        except MigrationError as e:
            errors.append(str(e))

    t1, t2 = threading.Thread(target=run), threading.Thread(target=run)
    t1.start(); t2.start(); t1.join(); t2.join()
    final = store.get_job(job.job_id)
    assert final.status == MigrationStatus.COMPLETED
    assert final.checkpoint.processed == 200  # 恰好一次，不重复


def test_cancel_and_resume(store, engine):
    _seed_users(store, 10)
    job = engine.create_job("basic", 2, "op", batch_size=2)
    engine.cancel(job.job_id, "op")
    assert store.get_job(job.job_id).status == MigrationStatus.PAUSED
    # 取消后 run 立即停在断点；清除取消标记后可续跑完成
    job = engine.run(job.job_id, "op")
    assert job.status == MigrationStatus.PAUSED
    job.cancel_requested = False
    store.update_job_state(job)
    job = engine.run(job.job_id, "op")
    assert job.status == MigrationStatus.COMPLETED
    assert job.checkpoint.processed == 10


def test_batch_timeout_leaves_no_partial_write(store, engine):
    _seed_users(store, 10)
    job = engine.create_job("basic", 2, "op", batch_size=10, batch_timeout_sec=0)
    job = engine.run(job.job_id, "op")
    assert job.status == MigrationStatus.PAUSED
    assert job.error == "batch_timeout"
    # 超时批次未提交：所有用户仍是旧版本，无部分写入
    assert job.checkpoint.processed == 0
    assert all(store.get_entitlement(f"u{i:04d}").plan_version == 1 for i in range(10))
    # 恢复正常超时后可从断点续跑
    job.batch_timeout_sec = 30
    store.update_job_state(job)
    job = engine.run(job.job_id, "op")
    assert job.status == MigrationStatus.COMPLETED


def test_rollback_restores_exact_previous_state(store, engine):
    _seed_users(store, 6)
    before = {f"u{i:04d}": store.get_entitlement(f"u{i:04d}") for i in range(6)}
    job = engine.create_job("basic", 2, "op", batch_size=2)
    engine.run(job.job_id, "op")
    job = engine.rollback(job.job_id, "admin")
    assert job.status == MigrationStatus.ROLLED_BACK
    for uid, old in before.items():
        rec = store.get_entitlement(uid)
        assert rec.plan_version == old.plan_version
        assert rec.quotas == old.quotas
        assert rec.usage == old.usage


def test_rollback_partial_migration(store, engine):
    """迁移中断到一半时回退：已迁移的恢复，未迁移的保持原样。"""
    _seed_users(store, 5)
    job = engine.create_job("basic", 2, "op", batch_size=2)
    ids = store.list_entitlement_user_ids("basic", 1, "", 2)
    recs = [store.get_entitlement(uid) for uid in ids]
    store.save_snapshots(job.job_id, recs)
    migrated = [engine._migrate_record(r, 2) for r in recs]
    from app.models import MigrationCheckpoint
    store.commit_batch(job, migrated, MigrationCheckpoint(last_user_id=ids[-1], processed=2))
    store.get_job(job.job_id)  # 状态为 RUNNING 之外，直接置 PAUSED 模拟中断
    j = store.get_job(job.job_id)
    j.status = MigrationStatus.PAUSED
    store.update_job_state(j)

    engine.rollback(job.job_id, "admin")
    for i in range(5):
        rec = store.get_entitlement(f"u{i:04d}")
        assert rec.plan_version == 1
        assert rec.quotas == {"seats": 5, "storage_gb": 10}


def test_rollback_rejected_while_running(store, engine):
    _seed_users(store, 3)
    job = engine.create_job("basic", 2, "op")
    j = store.get_job(job.job_id)
    j.status = MigrationStatus.RUNNING
    store.update_job_state(j)
    with pytest.raises(MigrationError):
        engine.rollback(job.job_id, "admin")


def test_rollback_idempotent(store, engine):
    _seed_users(store, 3)
    job = engine.create_job("basic", 2, "op")
    engine.run(job.job_id, "op")
    engine.rollback(job.job_id, "admin")
    job = engine.rollback(job.job_id, "admin")  # 第二次回退无副作用
    assert job.status == MigrationStatus.ROLLED_BACK
    assert store.get_entitlement("u0000").plan_version == 1


def test_audit_trail(store, engine):
    _seed_users(store, 4)
    job = engine.create_job("basic", 2, "op", batch_size=2)
    engine.run(job.job_id, "op")
    engine.rollback(job.job_id, "admin")
    actions = [e.action.value for e in store.list_audit(job.job_id)]
    assert actions[0] == "migration_start"
    assert "migration_batch_commit" in actions
    assert "migration_complete" in actions
    assert actions[-2:] == ["rollback_start", "rollback_complete"]
    assert all(e.actor in ("op", "admin") for e in store.list_audit(job.job_id))


def test_rate_limit_slows_migration(store, engine):
    _seed_users(store, 4)
    job = engine.create_job("basic", 2, "op", batch_size=4, rate_limit_per_sec=50)
    start = time.monotonic()
    engine.run(job.job_id, "op")
    elapsed = time.monotonic() - start
    assert elapsed >= 4 / 50 * 0.8  # 限速生效（留 20% 余量）
