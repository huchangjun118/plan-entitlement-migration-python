"""跨进程重启的断点续跑：使用文件型 SQLite，模拟进程崩溃后重启恢复。"""
from app.migration import MigrationEngine
from app.models import EntitlementRecord, MigrationCheckpoint, MigrationStatus, PlanDefinition
from app.store import Store


def test_resume_across_restart(tmp_path):
    db = str(tmp_path / "state.db")
    store = Store(db)
    store.put_plan(PlanDefinition("basic", 1, {"seats": 5}))
    store.put_plan(PlanDefinition("basic", 2, {"seats": 20}))
    for i in range(6):
        store.put_entitlement(EntitlementRecord(
            user_id=f"u{i}", plan_id="basic", plan_version=1,
            quotas={"seats": 5}, usage={"seats": 1}))
    engine = MigrationEngine(store)
    job = engine.create_job("basic", 2, "op", batch_size=2)
    # 提交一个批次后模拟进程崩溃（直接关闭连接，不更新任务状态）
    ids = store.list_entitlement_user_ids("basic", 1, "", 2)
    recs = [store.get_entitlement(uid) for uid in ids]
    store.save_snapshots(job.job_id, recs)
    store.commit_batch(job, [engine._migrate_record(r, 2) for r in recs],
                       MigrationCheckpoint(last_user_id=ids[-1], processed=2))
    job_id = job.job_id
    store.close()

    # “重启”：新 Store + 新 Engine，从磁盘恢复
    store2 = Store(db)
    engine2 = MigrationEngine(store2)
    job = engine2.run(job_id, "op")
    assert job.status == MigrationStatus.COMPLETED
    assert job.checkpoint.processed == 6
    assert all(store2.get_entitlement(f"u{i}").plan_version == 2 for i in range(6))
    # 已迁移的用户未被重复应用
    assert store2.get_entitlement("u0").record_version == 1
    # 审计日志在重启后仍完整可追溯
    actions = [e.action.value for e in store2.list_audit(job_id)]
    assert "migration_start" in actions and "migration_complete" in actions
    store2.close()
