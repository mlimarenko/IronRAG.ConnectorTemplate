from __future__ import annotations

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
    )
    row = store.get("page", "42")
    assert row is not None
    assert row.external_key == "echo:page:42"
    assert row.ironrag_document_id == "doc-1"
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
