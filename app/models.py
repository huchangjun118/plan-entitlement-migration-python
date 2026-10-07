"""领域模型：计划定义（版本化）、用户额度、迁移任务、审计记录。"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class MigrationStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"          # 被取消/中断，可从断点恢复
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


class AuditAction(str, Enum):
    MIGRATION_START = "migration_start"
    MIGRATION_BATCH_COMMIT = "migration_batch_commit"
    MIGRATION_PAUSE = "migration_pause"
    MIGRATION_RESUME = "migration_resume"
    MIGRATION_COMPLETE = "migration_complete"
    MIGRATION_FAIL = "migration_fail"
    ROLLBACK_START = "rollback_start"
    ROLLBACK_COMPLETE = "rollback_complete"
    PERMISSION_DENIED = "permission_denied"


@dataclass(frozen=True)
class PlanDefinition:
    """某个版本的计划定义。版本不可变，旧版本数据按其原版本解释。"""
    plan_id: str
    version: int
    quotas: dict[str, int]          # 资源名 -> 上限
    description: str = ""


@dataclass
class EntitlementRecord:
    """用户额度记录：计划与额度作为一个整体版本化存储，读写原子替换，
    保证任一时刻读到的都是自洽的单个版本，不会出现字段级混合。"""
    user_id: str
    plan_id: str
    plan_version: int
    quotas: dict[str, int]          # 生效的额度快照（与 plan_version 对应的解释结果）
    usage: dict[str, int] = field(default_factory=dict)
    record_version: int = 0         # 乐观锁 / 原子换版本序号
    updated_at: float = 0.0


@dataclass
class MigrationCheckpoint:
    """断点续跑位置：最后已提交的用户游标 + 已处理数量。"""
    last_user_id: str
    processed: int


@dataclass
class MigrationJob:
    job_id: str
    plan_id: str
    from_version: int
    to_version: int
    status: MigrationStatus = MigrationStatus.PENDING
    batch_size: int = 50
    rate_limit_per_sec: float = 0.0     # 0 表示不限速；>0 表示每秒最多处理的用户数
    batch_timeout_sec: float = 30.0     # 单批次超时，超时则该批次整体回滚
    cancel_requested: bool = False
    checkpoint: MigrationCheckpoint = field(
        default_factory=lambda: MigrationCheckpoint(last_user_id="", processed=0))
    error: str | None = None
    created_by: str = ""
    created_at: float = 0.0
    updated_at: float = 0.0


@dataclass(frozen=True)
class AuditEntry:
    audit_id: str
    action: AuditAction
    actor: str
    job_id: str | None
    detail: dict[str, Any]
    at: float


def new_id() -> str:
    return uuid.uuid4().hex


def now() -> float:
    return time.time()
