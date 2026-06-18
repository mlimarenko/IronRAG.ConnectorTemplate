from __future__ import annotations

import sqlite3
from pathlib import Path

from ironrag_connector.state import StateStore


def test_upsert_then_get_roundtrip(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "s.sqlite")
    store.upsert(
        kind="page",
        item_id="42",
        change_token="2026-01-01T00:00:00Z",
        external_key="echo:page:42",
        ironrag_document_id="doc-1",
        ironrag_library_id="lib-1",
    )
    row = store.get("page", "42")
    assert row is not None
    assert row.external_key == "echo:page:42"
    assert row.ironrag_document_id == "doc-1"
    assert row.ironrag_library_id == "lib-1"
    assert row.change_token == "2026-01-01T00:00:00Z"
    store.close()


def test_upsert_preserves_document_id_when_omitted(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "s.sqlite")
    store.upsert(
        kind="page",
        item_id="42",
        change_token="t1",
        external_key="echo:page:42",
        ironrag_document_id="doc-1",
        ironrag_library_id="lib-1",
    )
    store.upsert(
        kind="page",
        item_id="42",
        change_token="t2",
        external_key="echo:page:42",
        ironrag_document_id=None,
    )
    row = store.get("page", "42")
    assert row is not None
    assert row.change_token == "t2"
    assert row.ironrag_document_id == "doc-1"
    assert row.ironrag_library_id == "lib-1"
    store.close()


def test_backfill_document_identity_preserves_change_token_and_timestamp(
    tmp_path: Path,
) -> None:
    store = StateStore(tmp_path / "s.sqlite")
    store.upsert(
        kind="page",
        item_id="42",
        change_token="t1",
        external_key="echo:page:42",
        ironrag_document_id=None,
    )
    before = store.get("page", "42")
    assert before is not None

    store.backfill_document_identity(
        kind="page",
        item_id="42",
        external_key="echo:page:42",
        ironrag_document_id="doc-1",
        ironrag_library_id="lib-1",
    )

    row = store.get("page", "42")
    assert row is not None
    assert row.change_token == "t1"
    assert row.last_pushed_at == before.last_pushed_at
    assert row.ironrag_document_id == "doc-1"
    assert row.ironrag_library_id == "lib-1"
    store.close()


def test_existing_database_is_migrated_with_library_column(tmp_path: Path) -> None:
    db = tmp_path / "s.sqlite"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE cursor (
            kind TEXT NOT NULL,
            item_id TEXT NOT NULL,
            change_token TEXT,
            external_key TEXT NOT NULL,
            ironrag_document_id TEXT,
            last_pushed_at TEXT NOT NULL,
            PRIMARY KEY (kind, item_id)
        );
        INSERT INTO cursor (
            kind, item_id, change_token, external_key,
            ironrag_document_id, last_pushed_at
        ) VALUES ('page', 'old', 't1', 'echo:page:old', 'doc-old', '2026-01-01T00:00:00Z');
        """
    )
    conn.close()

    store = StateStore(db)
    row = store.get("page", "old")
    assert row is not None
    assert row.ironrag_document_id == "doc-old"
    assert row.ironrag_library_id is None

    store.upsert(
        kind="page",
        item_id="old",
        change_token="t2",
        external_key="echo:page:old",
        ironrag_document_id="doc-new",
        ironrag_library_id="lib-new",
    )
    row = store.get("page", "old")
    assert row is not None
    assert row.ironrag_library_id == "lib-new"
    store.close()


def test_delete_removes_row(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "s.sqlite")
    store.upsert(
        kind="page",
        item_id="x",
        change_token="t",
        external_key="echo:page:x",
        ironrag_document_id="d",
    )
    store.delete("page", "x")
    assert store.get("page", "x") is None
    store.close()


def test_lookup_by_external_key(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "s.sqlite")
    store.upsert(
        kind="page",
        item_id="x",
        change_token="t",
        external_key="echo:page:x",
        ironrag_document_id="d",
    )
    row = store.get_by_external_key("echo:page:x")
    assert row is not None
    assert row.item_id == "x"
    store.close()


def test_items_of_kind_returns_only_matching_kind(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "s.sqlite")
    store.upsert(
        kind="page",
        item_id="a",
        change_token="t",
        external_key="echo:page:a",
        ironrag_document_id="d1",
    )
    store.upsert(
        kind="attachment",
        item_id="b",
        change_token="t",
        external_key="echo:attachment:b",
        ironrag_document_id="d2",
    )
    rows = store.items_of_kind("page")
    assert [r.item_id for r in rows] == ["a"]
    store.close()
