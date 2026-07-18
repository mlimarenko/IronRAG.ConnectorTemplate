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
        ironrag_library_id="lib-1",
    )
    row = store.get("page", "42")
    assert row is not None
    assert row.change_token == "t2"
    assert row.ironrag_document_id == "doc-1"
    assert row.ironrag_library_id == "lib-1"
    store.close()


def test_upsert_replaces_library_id_when_route_moves(tmp_path: Path) -> None:
    """Unlike ``ironrag_document_id`` (COALESCE-preserved when omitted),
    ``ironrag_library_id`` is a required, always-overwritten column -- a
    caller always knows the current route's library_id (routing resolves it
    before any IronRAG call), so there is no "preserve the old value" case
    to support for it."""
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
        ironrag_library_id="lib-2",
    )
    row = store.get("page", "42")
    assert row is not None
    assert row.ironrag_library_id == "lib-2"
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
        ironrag_library_id="lib-1",
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


def test_fresh_database_declares_library_id_not_null(tmp_path: Path) -> None:
    """The redesigned schema creates ``ironrag_library_id`` as ``NOT NULL``
    from the start -- there is no migration path for pre-redesign SQLite
    cursor files (plan S7.6): a connector upgrading onto this schema
    recreates its cursor via a full re-walk instead of altering an old
    table in place."""
    db = tmp_path / "s.sqlite"
    store = StateStore(db)
    store.close()

    conn = sqlite3.connect(db)
    try:
        columns = {
            row[1]: row[3]  # name -> notnull flag
            for row in conn.execute("PRAGMA table_info(cursor)")
        }
    finally:
        conn.close()
    assert columns["ironrag_library_id"] == 1


def test_delete_removes_row(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "s.sqlite")
    store.upsert(
        kind="page",
        item_id="x",
        change_token="t",
        external_key="echo:page:x",
        ironrag_document_id="d",
        ironrag_library_id="lib-1",
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
        ironrag_library_id="lib-1",
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
        ironrag_library_id="lib-1",
    )
    store.upsert(
        kind="attachment",
        item_id="b",
        change_token="t",
        external_key="echo:attachment:b",
        ironrag_document_id="d2",
        ironrag_library_id="lib-1",
    )
    rows = store.items_of_kind("page")
    assert [r.item_id for r in rows] == ["a"]
    store.close()
