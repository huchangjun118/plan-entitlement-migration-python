"""持久化存储：SQLite 后端。

关键不变量：
- 用户额度记录（entitlement）作为整体一行存储，迁移时整行原子替换，
  读取永远得到自洽的单个版本，不会出现字段级混合。
- 每个批次在单个事务中提交：批次内全部用户的新记录 + checkpoint 一起落库，
  中断后不会出现部分写入。
- 迁移前对目标用户做快照（snapshot 表），回退时整体恢复。
"""
from __future__ import annotations

import json
import sqlite3
import threading

from .models import (
    AuditAction,
    AuditEntry,
    EntitlementRecord,
    MigrationCheckpoint,
    MigrationJob,
    MigrationStatus,
    PlanDefinition,
    new_id,
    now,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS plans (
    plan_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    quotas TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (plan_id, version)
);
CREATE TABLE IF NOT EXISTS entitlements (
    user_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL,
    plan_version INTEGER NOT NULL,
    quotas TEXT NOT NULL,
    usage TEXT NOT NULL,
    record_version INTEGER NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS migration_jobs (
    job_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL,
    from_version INTEGER NOT NULL,
    to_version INTEGER NOT NULL,
    status TEXT NOT NULL,
    batch_size INTEGER NOT NULL,
    rate_limit_per_sec REAL NOT NULL,
    batch_timeout_sec INTEGER NOT NULL,
    cancel_requested INTEGER NOT NULL,
    last_user_id TEXT NOT NULL,
    processed INTEGER NOT NULL,
    error TEXT,
    created_by TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS snapshots (
    job_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    record TEXT NOT NULL,          -- EntitlementRecord 的 JSON 序列化（迁移前完整状态）
    rolled_back INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (job_id, user_id)
);
CREATE TABLE IF NOT EXISTS audit_log (
    audit_id TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    actor TEXT NOT NULL,
    job_id TEXT,
    detail TEXT NOT NULL,
    at REAL NOT NULL
);
"""


def _rec_to_json(rec: EntitlementRecord) -> str:
    return json.dumps({
        "user_id": rec.user_id,
        "plan_id": rec.plan_id,
        "plan_version": rec.plan_version,
        "quotas": rec.quotas,
        "usage": rec.usage,
        "record_version": rec.record_version,
        "updated_at": rec.updated_at,
    })


def _rec_from_json(s: str) -> EntitlementRecord:
    d = json.loads(s)
    return EntitlementRecord(**d)


class Store:
    """线程安全的 SQLite 存储封装。所有多步写入都在单个事务中完成。"""

    def __init__(self, db_path: str = ":memory:") -> None:
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---- 计划定义 ----

    def put_plan(self, plan: PlanDefinition) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO plans(plan_id, version, quotas, description)"
                " VALUES (?,?,?,?)",
                (plan.plan_id, plan.version, json.dumps(plan.quotas), plan.description))

    def get_plan(self, plan_id: str, version: int) -> PlanDefinition | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM plans WHERE plan_id=? AND version=?",
                (plan_id, version)).fetchone()
        if row is None:
            return None
        return PlanDefinition(plan_id=row["plan_id"], version=row["version"],
                              quotas=json.loads(row["quotas"]),
                              description=row["description"])

    def latest_plan_version(self, plan_id: str) -> int | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT MAX(version) AS v FROM plans WHERE plan_id=?",
                (plan_id,)).fetchone()
        return row["v"] if row and row["v"] is not None else None

    # ---- 用户额度 ----

    @staticmethod
    def _row_to_rec(row: sqlite3.Row) -> EntitlementRecord:
        return EntitlementRecord(
            user_id=row["user_id"], plan_id=row["plan_id"],
            plan_version=row["plan_version"], quotas=json.loads(row["quotas"]),
            usage=json.loads(row["usage"]), record_version=row["record_version"],
            updated_at=row["updated_at"])

    def get_entitlement(self, user_id: str) -> EntitlementRecord | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM entitlements WHERE user_id=?", (user_id,)).fetchone()
        return self._row_to_rec(row) if row else None

    def put_entitlement(self, rec: EntitlementRecord) -> None:
        rec.updated_at = now()
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO entitlements"
                "(user_id, plan_id, plan_version, quotas, usage, record_version, updated_at)"
                " VALUES (?,?,?,?,?,?,?)",
                (rec.user_id, rec.plan_id, rec.plan_version, json.dumps(rec.quotas),
                 json.dumps(rec.usage), rec.record_version, rec.updated_at))

    def list_entitlement_user_ids(self, plan_id: str, plan_version: int,
                                  after_user_id: str, limit: int) -> list[str]:
        """按 user_id 升序返回指定计划版本、游标之后的用户，用于分批扫描。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT user_id FROM entitlements"
                " WHERE plan_id=? AND plan_version=? AND user_id>?"
                " ORDER BY user_id LIMIT ?",
                (plan_id, plan_version, after_user_id, limit)).fetchall()
        return [r["user_id"] for r in rows]

    # ---- 迁移任务 ----

    @staticmethod
    def _row_to_job(row: sqlite3.Row) -> MigrationJob:
        return MigrationJob(
            job_id=row["job_id"], plan_id=row["plan_id"],
            from_version=row["from_version"], to_version=row["to_version"],
            status=MigrationStatus(row["status"]), batch_size=row["batch_size"],
            rate_limit_per_sec=row["rate_limit_per_sec"],
            batch_timeout_sec=row["batch_timeout_sec"],
            cancel_requested=bool(row["cancel_requested"]),
            checkpoint=MigrationCheckpoint(last_user_id=row["last_user_id"],
                                           processed=row["processed"]),
            error=row["error"], created_by=row["created_by"],
            created_at=row["created_at"], updated_at=row["updated_at"])

    def create_job(self, job: MigrationJob) -> None:
        job.created_at = job.updated_at = now()
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO migration_jobs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (job.job_id, job.plan_id, job.from_version, job.to_version,
                 job.status.value, job.batch_size, job.rate_limit_per_sec,
                 job.batch_timeout_sec, int(job.cancel_requested),
                 job.checkpoint.last_user_id, job.checkpoint.processed,
                 job.error, job.created_by, job.created_at, job.updated_at))

    def get_job(self, job_id: str) -> MigrationJob | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM migration_jobs WHERE job_id=?", (job_id,)).fetchone()
        return self._row_to_job(row) if row else None

    def update_job_state(self, job: MigrationJob) -> None:
        job.updated_at = now()
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE migration_jobs SET status=?, cancel_requested=?,"
                " batch_size=?, rate_limit_per_sec=?, batch_timeout_sec=?,"
                " last_user_id=?, processed=?, error=?, updated_at=? WHERE job_id=?",
                (job.status.value, int(job.cancel_requested),
                 job.batch_size, job.rate_limit_per_sec, job.batch_timeout_sec,
                 job.checkpoint.last_user_id, job.checkpoint.processed,
                 job.error, job.updated_at, job.job_id))

    # ---- 批次原子提交 ----

    def commit_batch(self, job: MigrationJob,
                     migrated: list[EntitlementRecord],
                     checkpoint: MigrationCheckpoint) -> None:
        """单事务：写入本批全部用户的新额度记录 + 更新 checkpoint + 任务状态。
        中断只会发生在事务边界之外，因此不会留下部分写入。"""
        job.checkpoint = checkpoint
        job.updated_at = now()
        with self._lock, self._conn:
            for rec in migrated:
                rec.updated_at = job.updated_at
                self._conn.execute(
                    "INSERT OR REPLACE INTO entitlements"
                    "(user_id, plan_id, plan_version, quotas, usage, record_version, updated_at)"
                    " VALUES (?,?,?,?,?,?,?)",
                    (rec.user_id, rec.plan_id, rec.plan_version, json.dumps(rec.quotas),
                     json.dumps(rec.usage), rec.record_version, rec.updated_at))
            self._conn.execute(
                "UPDATE migration_jobs SET status=?, last_user_id=?, processed=?,"
                " error=?, updated_at=? WHERE job_id=?",
                (job.status.value, checkpoint.last_user_id, checkpoint.processed,
                 job.error, job.updated_at, job.job_id))

    # ---- 快照与回退 ----

    def save_snapshots(self, job_id: str, records: list[EntitlementRecord]) -> None:
        with self._lock, self._conn:
            for rec in records:
                self._conn.execute(
                    "INSERT OR IGNORE INTO snapshots(job_id, user_id, record) VALUES (?,?,?)",
                    (job_id, rec.user_id, _rec_to_json(rec)))

    def rollback(self, job_id: str) -> int:
        """单事务：把该任务快照过的用户恢复到迁移前状态，返回恢复条数。
        快照是迁移前的完整记录，整行恢复，不会留下混合状态。"""
        with self._lock, self._conn:
            rows = self._conn.execute(
                "SELECT user_id, record FROM snapshots WHERE job_id=? AND rolled_back=0",
                (job_id,)).fetchall()
            for row in rows:
                rec = _rec_from_json(row["record"])
                rec.record_version += 1
                rec.updated_at = now()
                self._conn.execute(
                    "INSERT OR REPLACE INTO entitlements"
                    "(user_id, plan_id, plan_version, quotas, usage, record_version, updated_at)"
                    " VALUES (?,?,?,?,?,?,?)",
                    (rec.user_id, rec.plan_id, rec.plan_version, json.dumps(rec.quotas),
                     json.dumps(rec.usage), rec.record_version, rec.updated_at))
                self._conn.execute(
                    "UPDATE snapshots SET rolled_back=1 WHERE job_id=? AND user_id=?",
                    (job_id, rec.user_id))
            return len(rows)

    # ---- 审计 ----

    def audit(self, action: AuditAction, actor: str, job_id: str | None,
              detail: dict) -> AuditEntry:
        entry = AuditEntry(audit_id=new_id(), action=action, actor=actor,
                           job_id=job_id, detail=detail, at=now())
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO audit_log VALUES (?,?,?,?,?,?)",
                (entry.audit_id, entry.action.value, entry.actor, entry.job_id,
                 json.dumps(entry.detail, ensure_ascii=False), entry.at))
        return entry

    def list_audit(self, job_id: str | None = None) -> list[AuditEntry]:
        with self._lock:
            if job_id is None:
                rows = self._conn.execute(
                    "SELECT * FROM audit_log ORDER BY at, audit_id").fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM audit_log WHERE job_id=? ORDER BY at, audit_id",
                    (job_id,)).fetchall()
        return [AuditEntry(audit_id=r["audit_id"], action=AuditAction(r["action"]),
                           actor=r["actor"], job_id=r["job_id"],
                           detail=json.loads(r["detail"]), at=r["at"]) for r in rows]
