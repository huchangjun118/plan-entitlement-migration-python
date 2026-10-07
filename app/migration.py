"""迁移引擎：分批、断点续跑、限速、取消/超时、回退。

语义：
- 迁移以批次为单位提交，批次是原子事务（额度记录 + checkpoint 一起落库）。
- 任意时刻中断（进程崩溃、取消、批次超时）后重新 run()，都会从最近一次
  已提交的 checkpoint 继续，不重复应用、不跳过。
- 回退使用迁移前快照整体恢复；回退与同计划正在运行的迁移互斥，结果确定。
"""
from __future__ import annotations

import threading
import time

from .models import (
    AuditAction,
    EntitlementRecord,
    MigrationCheckpoint,
    MigrationJob,
    MigrationStatus,
    new_id,
)
from .store import Store


class MigrationError(Exception):
    pass


class MigrationEngine:
    def __init__(self, store: Store) -> None:
        self._store = store
        self._run_locks: dict[str, threading.Lock] = {}
        self._run_locks_guard = threading.Lock()

    # ---- 任务创建 ----

    def create_job(self, plan_id: str, to_version: int, actor: str,
                   batch_size: int = 50, rate_limit_per_sec: float = 0.0,
                   batch_timeout_sec: float = 30.0) -> MigrationJob:
        target = self._store.get_plan(plan_id, to_version)
        if target is None:
            raise MigrationError(f"计划 {plan_id} 不存在版本 {to_version}")
        # 源版本：目标版本之前最近的一个已定义版本
        from_version = None
        for v in range(to_version - 1, 0, -1):
            if self._store.get_plan(plan_id, v) is not None:
                from_version = v
                break
        if from_version is None:
            raise MigrationError(f"计划 {plan_id} 在版本 {to_version} 之前没有可迁移的源版本")
        if batch_size <= 0:
            raise MigrationError("batch_size 必须为正整数")
        job = MigrationJob(
            job_id=new_id(), plan_id=plan_id, from_version=from_version,
            to_version=to_version, batch_size=batch_size,
            rate_limit_per_sec=rate_limit_per_sec,
            batch_timeout_sec=batch_timeout_sec, created_by=actor)
        self._store.create_job(job)
        self._store.audit(AuditAction.MIGRATION_START, actor, job.job_id, {
            "plan_id": plan_id, "from_version": from_version, "to_version": to_version,
            "batch_size": batch_size, "rate_limit_per_sec": rate_limit_per_sec,
            "batch_timeout_sec": batch_timeout_sec})
        return job

    # ---- 单用户迁移变换 ----

    def _migrate_record(self, rec: EntitlementRecord, to_version: int) -> EntitlementRecord:
        """把一条旧版本记录整体变换为新版本记录（纯函数，不落库）。
        额度整体替换为新计划定义；usage 仅保留新计划中仍存在的资源项。"""
        plan = self._store.get_plan(rec.plan_id, to_version)
        if plan is None:
            raise MigrationError(f"计划 {rec.plan_id} 不存在版本 {to_version}")
        usage = {k: v for k, v in rec.usage.items() if k in plan.quotas}
        return EntitlementRecord(
            user_id=rec.user_id, plan_id=rec.plan_id, plan_version=to_version,
            quotas=dict(plan.quotas), usage=usage,
            record_version=rec.record_version + 1)

    # ---- 执行 / 断点续跑 ----

    def _job_lock(self, job_id: str) -> threading.Lock:
        with self._run_locks_guard:
            return self._run_locks.setdefault(job_id, threading.Lock())

    def run(self, job_id: str, actor: str) -> MigrationJob:
        """执行（或从断点恢复执行）迁移，直到完成/被取消/失败。
        同一任务的并发 run 会被拒绝，保证批次提交序列唯一。"""
        lock = self._job_lock(job_id)
        if not lock.acquire(blocking=False):
            raise MigrationError(f"任务 {job_id} 正在执行中")
        try:
            return self._run_locked(job_id, actor)
        finally:
            lock.release()

    def _run_locked(self, job_id: str, actor: str) -> MigrationJob:
        job = self._store.get_job(job_id)
        if job is None:
            raise MigrationError(f"任务 {job_id} 不存在")
        if job.status in (MigrationStatus.COMPLETED, MigrationStatus.ROLLED_BACK):
            return job  # 幂等：已完成/已回退的任务重跑无副作用
        job.status = MigrationStatus.RUNNING
        job.error = None
        self._store.update_job_state(job)
        self._store.audit(AuditAction.MIGRATION_RESUME, actor, job.job_id, {
            "resume_from": job.checkpoint.last_user_id,
            "processed": job.checkpoint.processed})

        interval = (1.0 / job.rate_limit_per_sec) if job.rate_limit_per_sec > 0 else 0.0
        while True:
            # 每批前重新加载，感知并发的 cancel 请求
            job = self._store.get_job(job_id)
            assert job is not None
            if job.cancel_requested:
                job.status = MigrationStatus.PAUSED
                self._store.update_job_state(job)
                self._store.audit(AuditAction.MIGRATION_PAUSE, actor, job.job_id, {
                    "reason": "cancel_requested", "processed": job.checkpoint.processed})
                return job

            user_ids = self._store.list_entitlement_user_ids(
                job.plan_id, job.from_version,
                after_user_id=job.checkpoint.last_user_id, limit=job.batch_size)
            if not user_ids:
                job.status = MigrationStatus.COMPLETED
                self._store.update_job_state(job)
                self._store.audit(AuditAction.MIGRATION_COMPLETE, actor, job.job_id, {
                    "processed": job.checkpoint.processed})
                return job

            started = time.monotonic()
            before: list[EntitlementRecord] = []
            after: list[EntitlementRecord] = []
            timed_out = False
            for uid in user_ids:
                if time.monotonic() - started > job.batch_timeout_sec:
                    timed_out = True
                    break
                rec = self._store.get_entitlement(uid)
                if rec is None or rec.plan_version != job.from_version:
                    continue  # 已被并发迁移走，跳过
                before.append(rec)
                after.append(self._migrate_record(rec, job.to_version))
                if interval > 0:
                    time.sleep(interval)

            if timed_out:
                # 本批次未提交，直接放弃：数据停留在上一个已提交 checkpoint，
                # 不会出现部分写入；任务转为 PAUSED 可从断点恢复。
                job = self._store.get_job(job_id)
                assert job is not None
                job.status = MigrationStatus.PAUSED
                job.error = "batch_timeout"
                self._store.update_job_state(job)
                self._store.audit(AuditAction.MIGRATION_PAUSE, actor, job.job_id, {
                    "reason": "batch_timeout", "processed": job.checkpoint.processed})
                return job

            if after:
                # 先存快照（迁移前完整状态），再原子提交本批次
                self._store.save_snapshots(job.job_id, before)
                checkpoint = MigrationCheckpoint(
                    last_user_id=user_ids[-1] if not before else before[-1].user_id,
                    processed=job.checkpoint.processed + len(after))
                # 游标必须前进到本批扫描到的最后一个 user_id，即使其中部分被跳过
                checkpoint.last_user_id = user_ids[-1]
                self._store.commit_batch(job, after, checkpoint)
                self._store.audit(AuditAction.MIGRATION_BATCH_COMMIT, actor, job.job_id, {
                    "batch_users": [r.user_id for r in after],
                    "processed": checkpoint.processed})
            else:
                # 整批都被并发迁走：仅推进游标，避免死循环
                checkpoint = MigrationCheckpoint(
                    last_user_id=user_ids[-1], processed=job.checkpoint.processed)
                job.checkpoint = checkpoint
                self._store.update_job_state(job)

    # ---- 取消 ----

    def cancel(self, job_id: str, actor: str) -> MigrationJob:
        job = self._store.get_job(job_id)
        if job is None:
            raise MigrationError(f"任务 {job_id} 不存在")
        if job.status in (MigrationStatus.COMPLETED, MigrationStatus.ROLLED_BACK):
            raise MigrationError(f"任务 {job_id} 已终结，无法取消")
        job.cancel_requested = True
        if job.status != MigrationStatus.RUNNING:
            job.status = MigrationStatus.PAUSED
        self._store.update_job_state(job)
        self._store.audit(AuditAction.MIGRATION_PAUSE, actor, job.job_id, {
            "reason": "cancel_requested"})
        return self._store.get_job(job_id)  # type: ignore[return-value]

    # ---- 回退 ----

    def rollback(self, job_id: str, actor: str) -> MigrationJob:
        """恢复到迁移前完整状态。与同一计划的运行中迁移互斥，结果确定。"""
        job = self._store.get_job(job_id)
        if job is None:
            raise MigrationError(f"任务 {job_id} 不存在")
        if job.status == MigrationStatus.RUNNING:
            raise MigrationError(f"任务 {job_id} 正在运行，先取消再回退")
        if job.status == MigrationStatus.ROLLED_BACK:
            return job  # 幂等
        lock = self._job_lock(job_id)
        with lock:
            self._store.audit(AuditAction.ROLLBACK_START, actor, job.job_id, {
                "processed": job.checkpoint.processed})
            restored = self._store.rollback(job.job_id)
            job.status = MigrationStatus.ROLLED_BACK
            job.error = None
            self._store.update_job_state(job)
            self._store.audit(AuditAction.ROLLBACK_COMPLETE, actor, job.job_id, {
                "restored": restored})
        return self._store.get_job(job_id)  # type: ignore[return-value]
