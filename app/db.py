"""SQLite storage layer: schema and transactional helpers."""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager

SCHEMA = """
CREATE TABLE IF NOT EXISTS plan_versions (
    plan_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    entitlements TEXT NOT NULL,       -- JSON: name -> default quota
    migration_map TEXT NOT NULL,      -- JSON: new -> [old, ratio]
    created_at TEXT NOT NULL,
    PRIMARY KEY (plan_id, version)
);

CREATE TABLE IF NOT EXISTS user_entitlements (
    user_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL,
    plan_version INTEGER NOT NULL,
    entitlements TEXT NOT NULL,       -- JSON snapshot, atomic with version
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS migrations (
    migration_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL,
    from_version INTEGER NOT NULL,
    to_version INTEGER NOT NULL,
    status TEXT NOT NULL,
    checkpoint TEXT NOT NULL DEFAULT '',
    config TEXT NOT NULL,             -- JSON MigrationConfig
    created_by TEXT NOT NULL,
    error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS migration_records (
    migration_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    before_snapshot TEXT,             -- JSON UserEntitlement or NULL
    after_snapshot TEXT NOT NULL,     -- JSON UserEntitlement
    rolled_back INTEGER NOT NULL DEFAULT 0,
    applied_at TEXT NOT NULL,
    PRIMARY KEY (migration_id, user_id)
);

CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    resource TEXT NOT NULL,
    allowed INTEGER NOT NULL,
    detail TEXT NOT NULL,             -- JSON
    created_at TEXT NOT NULL
);
"""


class Database:
    """Thin wrapper over sqlite3 with transactional scope.

    A single connection is shared and guarded by an RLock so concurrent
    threads never interleave statements or commit each other's in-flight
    transactions. SQLite is single-writer anyway; the lock simply makes
    the semantics explicit and keeps transactions short by design.
    """

    def __init__(self, path: str = ":memory:") -> None:
        self._path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(SCHEMA)
        self._lock = threading.RLock()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Atomic unit of work: commit on success, rollback on error."""
        with self._lock:
            try:
                yield self._conn
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def close(self) -> None:
        self._conn.close()
