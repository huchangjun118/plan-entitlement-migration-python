"""Audit log for permission checks and migration lifecycle events."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from .db import Database
from .models import AuditEntry


class AuditLog:
    def __init__(self, db: Database) -> None:
        self._db = db

    def record(self, actor: str, action: str, resource: str,
               allowed: bool, detail: dict[str, Any] | None = None) -> None:
        with self._db.transaction() as conn:
            conn.execute(
                "INSERT INTO audit_log (actor, action, resource, allowed, detail, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (actor, action, resource, int(allowed),
                 json.dumps(detail or {}),
                 datetime.now(timezone.utc).isoformat()),
            )

    def list(self, limit: int = 100) -> list[AuditEntry]:
        with self._db.transaction() as conn:
            rows = conn.execute(
                "SELECT * FROM audit_log ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [
            AuditEntry(
                actor=r["actor"],
                action=r["action"],
                resource=r["resource"],
                allowed=bool(r["allowed"]),
                detail=json.loads(r["detail"]),
            )
            for r in rows
        ]
