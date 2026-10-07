"""权限校验：基于角色的简单令牌认证。"""
from __future__ import annotations

from enum import Enum


class Role(str, Enum):
    READER = "reader"          # 只读：查询计划/额度/任务状态
    OPERATOR = "operator"      # 可执行迁移、取消、恢复
    ADMIN = "admin"            # 可回退、管理计划定义


# 操作 -> 允许的角色集合
_ACTION_ROLES: dict[str, set[Role]] = {
    "read": {Role.READER, Role.OPERATOR, Role.ADMIN},
    "migrate": {Role.OPERATOR, Role.ADMIN},
    "cancel": {Role.OPERATOR, Role.ADMIN},
    "rollback": {Role.ADMIN},
    "manage_plans": {Role.ADMIN},
}


class PermissionDenied(Exception):
    def __init__(self, actor: str, action: str) -> None:
        super().__init__(f"actor {actor!r} 无权执行 {action!r}")
        self.actor = actor
        self.action = action


class Authenticator:
    """令牌 -> (actor, role) 的静态映射认证器。"""

    def __init__(self, tokens: dict[str, tuple[str, Role]] | None = None) -> None:
        self._tokens = dict(tokens or {})

    def add_token(self, token: str, actor: str, role: Role) -> None:
        self._tokens[token] = (actor, role)

    def authenticate(self, token: str) -> tuple[str, Role] | None:
        return self._tokens.get(token)

    def authorize(self, token: str, action: str) -> tuple[str, Role]:
        """校验通过返回 (actor, role)，否则抛 PermissionDenied。"""
        identity = self.authenticate(token)
        actor = identity[0] if identity else "<anonymous>"
        allowed = _ACTION_ROLES.get(action, set())
        if identity is None or identity[1] not in allowed:
            raise PermissionDenied(actor, action)
        return identity
