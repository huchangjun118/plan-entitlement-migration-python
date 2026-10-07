"""Actor/role based permission checks for migration operations."""

from __future__ import annotations

import enum


class Role(str, enum.Enum):
    ADMIN = "admin"                # full access
    MIGRATION_OPERATOR = "migration_operator"  # may run/cancel/rollback migrations
    READER = "reader"              # read-only


class PermissionDeniedError(PermissionError):
    pass


# action -> roles allowed to perform it
PERMISSIONS: dict[str, frozenset[Role]] = {
    "migration.start": frozenset({Role.ADMIN, Role.MIGRATION_OPERATOR}),
    "migration.cancel": frozenset({Role.ADMIN, Role.MIGRATION_OPERATOR}),
    "migration.rollback": frozenset({Role.ADMIN, Role.MIGRATION_OPERATOR}),
    "migration.resume": frozenset({Role.ADMIN, Role.MIGRATION_OPERATOR}),
    "plan.write": frozenset({Role.ADMIN}),
    "entitlement.read": frozenset({Role.ADMIN, Role.MIGRATION_OPERATOR, Role.READER}),
}


def check_permission(role: Role, action: str) -> bool:
    allowed = PERMISSIONS.get(action)
    return allowed is not None and role in allowed


def require_permission(role: Role, action: str) -> None:
    if not check_permission(role, action):
        raise PermissionDeniedError(f"role {role.value!r} may not perform {action!r}")
