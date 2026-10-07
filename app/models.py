"""Core type definitions for plans, entitlements and migrations."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class MigrationStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLING_BACK = "rolling_back"
    ROLLED_BACK = "rolled_back"


@dataclass(frozen=True)
class PlanVersion:
    plan_id: str
    version: int
    # entitlement template: name -> default quota
    entitlements: dict[str, int]
    # mapping applied when migrating *from* this version to a newer one:
    # new_field -> (old_field, ratio); missing old fields fall back to defaults
    migration_map: dict[str, tuple[str, float]] = field(default_factory=dict)


@dataclass(frozen=True)
class UserEntitlement:
    """A user's plan binding and quota snapshot.

    plan_id/plan_version/entitlements are always read and written as one
    atomic unit so a reader never observes a field-level mix of versions.
    """

    user_id: str
    plan_id: str
    plan_version: int
    entitlements: dict[str, int]


@dataclass(frozen=True)
class MigrationConfig:
    batch_size: int = 50
    rate_limit_per_sec: float = 0.0  # 0 means unlimited
    batch_timeout_sec: float = 30.0


@dataclass
class Migration:
    migration_id: str
    plan_id: str
    from_version: int
    to_version: int
    status: MigrationStatus
    checkpoint: str  # last committed user_id; "" means not started
    config: MigrationConfig
    created_by: str
    error: str | None = None


@dataclass(frozen=True)
class MigrationRecord:
    """Per-user applied change; the source of truth for resume/rollback."""

    migration_id: str
    user_id: str
    before: UserEntitlement | None
    after: UserEntitlement
    rolled_back: bool = False


@dataclass(frozen=True)
class AuditEntry:
    actor: str
    action: str
    resource: str
    allowed: bool
    detail: dict[str, Any] = field(default_factory=dict)
