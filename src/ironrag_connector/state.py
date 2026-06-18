"""Persistent per-item cursor backed by SQLite.

Why persistent
==============

The connector's diff stage compares each source item's ``change_token``
(typically ``updated_at``) against the value stored at the last successful
push. If the value matches, the item is skipped without downloading
bytes. Keeping that cursor across restarts means a connector that restarts
nightly does not re-push every item every morning.

SQLite over JSON
================

JSON is fine until two writers touch the file at once (multi-worker
deployments, manual /sync/run hitting a sweep already in progress).
SQLite gives us atomic upsert at zero ops cost — single file, no daemon,
fsync per write — at the price of one extra dependency line in
``pyproject.toml`` (already part of the stdlib).

Schema
======

::

    CREATE TABLE cursor (
        kind TEXT NOT NULL,
        item_id TEXT NOT NULL,
        change_token TEXT,
        external_key TEXT NOT NULL,
        ironrag_document_id TEXT,
        ironrag_library_id TEXT,
        last_pushed_at TEXT NOT NULL,
        PRIMARY KEY (kind, item_id)
    );

The composite primary key matches the framework's identity model:
``(kind, item_id)`` is unique within a connector and small enough to
walk for the reaper pass.
"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


@dataclass(frozen=True)
class CursorRow:
    kind: str
    item_id: str
    change_token: str | None
    external_key: str
    ironrag_document_id: str | None
    ironrag_library_id: str | None
    last_pushed_at: str


class StateStore:
    """Thread-safe SQLite cursor store. One connection per process."""

    _SCHEMA = """
    CREATE TABLE IF NOT EXISTS cursor (
        kind TEXT NOT NULL,
        item_id TEXT NOT NULL,
        change_token TEXT,
        external_key TEXT NOT NULL,
        ironrag_document_id TEXT,
        ironrag_library_id TEXT,
        last_pushed_at TEXT NOT NULL,
        PRIMARY KEY (kind, item_id)
    );
    CREATE INDEX IF NOT EXISTS cursor_external_key_idx ON cursor(external_key);
    CREATE INDEX IF NOT EXISTS cursor_kind_idx ON cursor(kind);
    """

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._lock = threading.Lock()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            str(db_path),
            check_same_thread=False,
            isolation_level=None,
            timeout=30.0,
        )
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(self._SCHEMA)
        self._migrate()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def _cursor(self) -> Iterator[sqlite3.Cursor]:
        with self._lock:
            cur = self._conn.cursor()
            try:
                yield cur
            finally:
                cur.close()

    def get(self, kind: str, item_id: str) -> CursorRow | None:
        with self._cursor() as cur:
            row = cur.execute(
                "SELECT kind, item_id, change_token, external_key, "
                "ironrag_document_id, ironrag_library_id, last_pushed_at "
                "FROM cursor WHERE kind = ? AND item_id = ?",
                (kind, item_id),
            ).fetchone()
        if row is None:
            return None
        return CursorRow(*row)

    def get_by_external_key(self, external_key: str) -> CursorRow | None:
        with self._cursor() as cur:
            row = cur.execute(
                "SELECT kind, item_id, change_token, external_key, "
                "ironrag_document_id, ironrag_library_id, last_pushed_at "
                "FROM cursor WHERE external_key = ?",
                (external_key,),
            ).fetchone()
        if row is None:
            return None
        return CursorRow(*row)

    def upsert(
        self,
        *,
        kind: str,
        item_id: str,
        change_token: str | None,
        external_key: str,
        ironrag_document_id: str | None,
        ironrag_library_id: str | None = None,
    ) -> None:
        now = datetime.now(tz=UTC).isoformat()
        with self._cursor() as cur:
            cur.execute(
                """
                INSERT INTO cursor (
                    kind, item_id, change_token, external_key,
                    ironrag_document_id, ironrag_library_id, last_pushed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(kind, item_id) DO UPDATE SET
                    change_token = excluded.change_token,
                    external_key = excluded.external_key,
                    ironrag_document_id = COALESCE(
                        excluded.ironrag_document_id,
                        cursor.ironrag_document_id
                    ),
                    ironrag_library_id = COALESCE(
                        excluded.ironrag_library_id,
                        cursor.ironrag_library_id
                    ),
                    last_pushed_at = excluded.last_pushed_at
                """,
                (
                    kind,
                    item_id,
                    change_token,
                    external_key,
                    ironrag_document_id,
                    ironrag_library_id,
                    now,
                ),
            )

    def backfill_document_identity(
        self,
        *,
        kind: str,
        item_id: str,
        external_key: str,
        ironrag_document_id: str | None,
        ironrag_library_id: str | None,
    ) -> None:
        """Persist discovered IronRAG ownership without advancing source state."""
        with self._cursor() as cur:
            cur.execute(
                """
                UPDATE cursor
                SET
                    external_key = ?,
                    ironrag_document_id = COALESCE(?, ironrag_document_id),
                    ironrag_library_id = COALESCE(?, ironrag_library_id)
                WHERE kind = ? AND item_id = ?
                """,
                (
                    external_key,
                    ironrag_document_id,
                    ironrag_library_id,
                    kind,
                    item_id,
                ),
            )

    def delete(self, kind: str, item_id: str) -> None:
        with self._cursor() as cur:
            cur.execute(
                "DELETE FROM cursor WHERE kind = ? AND item_id = ?",
                (kind, item_id),
            )

    def items_of_kind(self, kind: str) -> list[CursorRow]:
        with self._cursor() as cur:
            rows = cur.execute(
                "SELECT kind, item_id, change_token, external_key, "
                "ironrag_document_id, ironrag_library_id, last_pushed_at "
                "FROM cursor WHERE kind = ?",
                (kind,),
            ).fetchall()
        return [CursorRow(*r) for r in rows]

    def _migrate(self) -> None:
        """Add columns introduced after early cursor databases were created."""
        with self._cursor() as cur:
            columns = {row[1] for row in cur.execute("PRAGMA table_info(cursor)")}
            if "ironrag_library_id" not in columns:
                cur.execute("ALTER TABLE cursor ADD COLUMN ironrag_library_id TEXT")
