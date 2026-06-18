from __future__ import annotations

from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import pytest
from echo_connector.adapter import EchoAdapter, EchoPage

from ironrag_connector.ironrag import IronRagError
from ironrag_connector.orchestrator import Orchestrator
from ironrag_connector.policy import (
    DeleteAction,
    DuplicateContentAction,
    PushPolicy,
    UpdateAction,
    UpsertAction,
)
from ironrag_connector.routing import (
    PolicyOverrides,
    Router,
    RoutingConfig,
)
from ironrag_connector.source import SourceItem, SourceItemRef
from ironrag_connector.state import StateStore
from ironrag_connector.sync import SyncManager

WS = UUID("00000000-0000-0000-0000-000000000099")
LIB = UUID("00000000-0000-0000-0000-000000000000")
LIB2 = UUID("00000000-0000-0000-0000-000000000002")
LIB3 = UUID("00000000-0000-0000-0000-000000000003")


class FakeIronRag:
    def __init__(self) -> None:
        self.documents: dict[tuple[UUID, str], dict[str, Any]] = {}
        self.uploads: list[dict[str, Any]] = []
        self.replaces: list[dict[str, Any]] = []
        self.deletes: list[str] = []
        self.duplicate_for_key: str | None = None
        self.replace_conflicts: set[str] = set()
        self.next_doc_id = 100
        self.find_calls = 0

    async def find_document_by_external_key(
        self, library_id: UUID, external_key: str
    ) -> dict[str, Any] | None:
        self.find_calls += 1
        return self.documents.get((library_id, external_key))

    async def get_document(self, document_id: str) -> dict[str, Any] | None:
        for (library_id, _key), doc in self.documents.items():
            if doc["id"] == document_id:
                return {**doc, "libraryId": str(library_id)}
        return None

    async def upload_document(
        self,
        *,
        library_id: UUID,
        external_key: str,
        file_bytes: bytes,
        file_name: str,
        mime_type: str,
        title: str | None,
        idempotency_key: str,
        document_hint: str | None = None,
        parent_external_key: str | None = None,
    ) -> dict[str, Any]:
        self.uploads.append(
            {
                "library_id": library_id,
                "external_key": external_key,
                "size": len(file_bytes),
                "idempotency_key": idempotency_key,
                "mime_type": mime_type,
                "document_hint": document_hint,
                "parent_external_key": parent_external_key,
            }
        )
        if self.duplicate_for_key == external_key:
            return {
                "document": {"id": "existing-uuid"},
                "duplicate_of_existing": True,
            }
        doc_id = f"doc-{self.next_doc_id}"
        self.next_doc_id += 1
        doc = {
            "id": doc_id,
            "externalKey": external_key,
            "title": title,
        }
        self.documents[(library_id, external_key)] = doc
        return {"document": doc}

    async def replace_document(
        self,
        *,
        document_id: str,
        file_bytes: bytes,
        file_name: str,
        mime_type: str,
        idempotency_key: str,
        document_hint: str | None = None,
    ) -> dict[str, Any]:
        if document_id in self.replace_conflicts:
            raise IronRagError(
                "IronRAG replace → 409: "
                '{"error":"conflict: document is still processing a previous mutation",'
                '"errorKind":"conflicting_mutation"}'
            )
        self.replaces.append(
            {
                "document_id": document_id,
                "size": len(file_bytes),
                "idempotency_key": idempotency_key,
                "document_hint": document_hint,
            }
        )
        return {"document": {"id": document_id}}

    async def delete_document(self, document_id: str, idempotency_key: str) -> None:
        self.deletes.append(document_id)
        for (lib, key), doc in list(self.documents.items()):
            if doc["id"] == document_id:
                self.documents.pop((lib, key))

    async def list_documents_by_external_key_prefix(
        self, library_id: UUID, prefix: str, *, page_size: int = 200
    ) -> list[tuple[str, str]]:
        return [
            (key, doc["id"])
            for (lib, key), doc in self.documents.items()
            if lib == library_id and key.startswith(prefix)
        ]


def _routing() -> RoutingConfig:
    return RoutingConfig.model_validate(
        {"default": {"workspace": str(WS), "library": str(LIB)}}
    )


def _routing_to(library_id: UUID) -> RoutingConfig:
    return RoutingConfig.model_validate(
        {"default": {"workspace": str(WS), "library": str(library_id)}}
    )


def _policies(default: PushPolicy | None = None) -> PolicyOverrides:
    return PolicyOverrides(default=default or PushPolicy(), by_kind={})


def _state(tmp_path: Path) -> StateStore:
    return StateStore(tmp_path / "s.sqlite")


@pytest.mark.asyncio
async def test_create_new_item(tmp_path: Path) -> None:
    adapter = EchoAdapter(
        {
            "1": EchoPage(
                item_id="1", title="One", body="hello", updated_at="t1"
            )
        }
    )
    state = _state(tmp_path)
    ironrag = FakeIronRag()
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=Router(_routing()),
        state=state,
        policies=_policies(),
    )
    ref = await _first(adapter.iter_items())
    out = await orchestrator.push_ref(ref)
    assert out.action == "created"
    assert out.ironrag_document_id == "doc-100"
    assert state.get("page", "1").change_token == "t1"


@pytest.mark.asyncio
async def test_document_hint_forwards_on_upload(tmp_path: Path) -> None:
    adapter = EchoAdapter({})
    state = _state(tmp_path)
    ironrag = FakeIronRag()
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=Router(_routing()),
        state=state,
        policies=_policies(),
    )
    item = SourceItem(
        ref=SourceItemRef(
            item_id="hinted",
            kind="page",
            external_key="echo:page:hinted",
            change_token="t1",
        ),
        payload=b"hello",
        mime_type="text/markdown",
        file_name="hinted.md",
        title="Hinted",
        document_hint="https://docs.example/hinted",
    )
    route = Router(_routing()).resolve(item.ref)

    out = await orchestrator.push_item(item, route, PushPolicy())

    assert out.action == "created"
    assert ironrag.uploads[0]["document_hint"] == "https://docs.example/hinted"


@pytest.mark.asyncio
async def test_document_hint_forwards_on_replace(tmp_path: Path) -> None:
    adapter = EchoAdapter({})
    state = _state(tmp_path)
    ironrag = FakeIronRag()
    ironrag.documents[(LIB, "echo:page:hinted")] = {
        "id": "doc-pre",
        "externalKey": "echo:page:hinted",
    }
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=Router(_routing()),
        state=state,
        policies=_policies(),
    )
    item = SourceItem(
        ref=SourceItemRef(
            item_id="hinted",
            kind="page",
            external_key="echo:page:hinted",
            change_token="t2",
        ),
        payload=b"hello again",
        mime_type="text/markdown",
        file_name="hinted.md",
        title="Hinted",
        document_hint="Canonical page label",
    )
    route = Router(_routing()).resolve(item.ref)

    out = await orchestrator.push_item(item, route, PushPolicy())

    assert out.action == "replaced"
    assert ironrag.replaces[0]["document_hint"] == "Canonical page label"


@pytest.mark.asyncio
async def test_unchanged_short_circuits_to_noop(tmp_path: Path) -> None:
    adapter = EchoAdapter(
        {"1": EchoPage(item_id="1", title="One", body="hello", updated_at="t1")}
    )
    state = _state(tmp_path)
    ironrag = FakeIronRag()
    ironrag.documents[(LIB, "echo:page:1")] = {
        "id": "doc-pre",
        "externalKey": "echo:page:1",
    }
    state.upsert(
        kind="page",
        item_id="1",
        change_token="t1",
        external_key="echo:page:1",
        ironrag_document_id="doc-pre",
        ironrag_library_id=str(LIB),
    )
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=Router(_routing()),
        state=state,
        policies=_policies(),
    )
    ref = await _first(adapter.iter_items())
    out = await orchestrator.push_ref(ref)
    assert out.action == "noop_unchanged"
    assert ironrag.uploads == []
    assert ironrag.replaces == []


@pytest.mark.asyncio
async def test_changed_item_replaces(tmp_path: Path) -> None:
    adapter = EchoAdapter(
        {"1": EchoPage(item_id="1", title="One", body="hello2", updated_at="t2")}
    )
    state = _state(tmp_path)
    ironrag = FakeIronRag()
    ironrag.documents[(LIB, "echo:page:1")] = {
        "id": "doc-pre",
        "externalKey": "echo:page:1",
    }
    state.upsert(
        kind="page",
        item_id="1",
        change_token="t1",
        external_key="echo:page:1",
        ironrag_document_id="doc-pre",
        ironrag_library_id=str(LIB),
    )
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=Router(_routing()),
        state=state,
        policies=_policies(),
    )
    ref = await _first(adapter.iter_items())
    out = await orchestrator.push_ref(ref)
    assert out.action == "replaced"
    assert ironrag.replaces and ironrag.replaces[0]["document_id"] == "doc-pre"
    assert state.get("page", "1").change_token == "t2"


@pytest.mark.asyncio
async def test_conflicting_replace_is_deferred_without_advancing_cursor(
    tmp_path: Path,
) -> None:
    adapter = EchoAdapter(
        {"1": EchoPage(item_id="1", title="One", body="hello2", updated_at="t2")}
    )
    state = _state(tmp_path)
    ironrag = FakeIronRag()
    ironrag.documents[(LIB, "echo:page:1")] = {
        "id": "doc-pre",
        "externalKey": "echo:page:1",
    }
    ironrag.replace_conflicts.add("doc-pre")
    state.upsert(
        kind="page",
        item_id="1",
        change_token="t1",
        external_key="echo:page:1",
        ironrag_document_id="doc-pre",
        ironrag_library_id=str(LIB),
    )
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=Router(_routing()),
        state=state,
        policies=_policies(),
    )

    ref = await _first(adapter.iter_items())
    out = await orchestrator.push_ref(ref)

    assert out.action == "skipped_changed"
    assert out.ironrag_document_id == "doc-pre"
    assert ironrag.uploads == []
    assert ironrag.replaces == []
    row = state.get("page", "1")
    assert row is not None
    assert row.change_token == "t1"


@pytest.mark.asyncio
async def test_legacy_cursor_same_route_conflict_preserves_old_token(
    tmp_path: Path,
) -> None:
    adapter = EchoAdapter(
        {"1": EchoPage(item_id="1", title="One", body="hello2", updated_at="t2")}
    )
    state = _state(tmp_path)
    ironrag = FakeIronRag()
    ironrag.documents[(LIB, "echo:page:1")] = {
        "id": "doc-pre",
        "externalKey": "echo:page:1",
    }
    ironrag.replace_conflicts.add("doc-pre")
    state.upsert(
        kind="page",
        item_id="1",
        change_token="t1",
        external_key="echo:page:1",
        ironrag_document_id="doc-pre",
        ironrag_library_id=None,
    )
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=Router(_routing()),
        state=state,
        policies=_policies(),
    )

    ref = await _first(adapter.iter_items())
    out = await orchestrator.push_ref(ref)

    assert out.action == "skipped_changed"
    assert out.ironrag_document_id == "doc-pre"
    assert ironrag.uploads == []
    assert ironrag.replaces == []
    row = state.get("page", "1")
    assert row is not None
    assert row.change_token == "t1"
    assert row.ironrag_document_id == "doc-pre"
    assert row.ironrag_library_id == str(LIB)


@pytest.mark.asyncio
async def test_on_changed_skip_does_not_replace(tmp_path: Path) -> None:
    adapter = EchoAdapter(
        {"1": EchoPage(item_id="1", title="One", body="hi", updated_at="t2")}
    )
    state = _state(tmp_path)
    ironrag = FakeIronRag()
    ironrag.documents[(LIB, "echo:page:1")] = {
        "id": "doc-pre",
        "externalKey": "echo:page:1",
    }
    policy = PushPolicy(on_changed=UpdateAction.SKIP)
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=Router(_routing()),
        state=state,
        policies=_policies(policy),
    )
    ref = await _first(adapter.iter_items())
    out = await orchestrator.push_ref(ref)
    assert out.action == "skipped_changed"
    assert ironrag.replaces == []


@pytest.mark.asyncio
async def test_on_new_skip_does_not_create(tmp_path: Path) -> None:
    adapter = EchoAdapter(
        {"1": EchoPage(item_id="1", title="One", body="hi", updated_at="t1")}
    )
    state = _state(tmp_path)
    ironrag = FakeIronRag()
    policy = PushPolicy(on_new=UpsertAction.SKIP)
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=Router(_routing()),
        state=state,
        policies=_policies(policy),
    )
    ref = await _first(adapter.iter_items())
    out = await orchestrator.push_ref(ref)
    assert out.action == "skipped_new"
    assert ironrag.uploads == []


@pytest.mark.asyncio
async def test_duplicate_content_skip(tmp_path: Path) -> None:
    adapter = EchoAdapter(
        {"1": EchoPage(item_id="1", title="One", body="hi", updated_at="t1")}
    )
    state = _state(tmp_path)
    ironrag = FakeIronRag()
    ironrag.duplicate_for_key = "echo:page:1"
    policy = PushPolicy(on_duplicate_content=DuplicateContentAction.SKIP)
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=Router(_routing()),
        state=state,
        policies=_policies(policy),
    )
    ref = await _first(adapter.iter_items())
    out = await orchestrator.push_ref(ref)
    assert out.action == "skipped_duplicate_content"
    assert out.ironrag_document_id == "existing-uuid"


@pytest.mark.asyncio
async def test_cursor_wins_over_server_find(tmp_path: Path) -> None:
    """Cursor with known document_id must short-circuit server lookup.

    Simulates a deployment where IronRAG's list endpoint does not expose
    externalKey: FakeIronRag.find_document_by_external_key returns None
    always. Without the cursor-wins fix the orchestrator would re-upload
    and trip a unique-violation server-side.
    """
    adapter = EchoAdapter(
        {"1": EchoPage(item_id="1", title="One", body="hi", updated_at="t2")}
    )
    state = _state(tmp_path)
    state.upsert(
        kind="page",
        item_id="1",
        change_token="t1",
        external_key="echo:page:1",
        ironrag_document_id="doc-pre",
        ironrag_library_id=str(LIB),
    )

    class BlindIronRag(FakeIronRag):
        async def find_document_by_external_key(self, library_id, external_key):
            return None  # IronRAG bug surrogate: list endpoint ignores filter

    ironrag = BlindIronRag()
    ironrag.documents[(LIB, "echo:page:1")] = {
        "id": "doc-pre",
        "externalKey": "echo:page:1",
    }
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=Router(_routing()),
        state=state,
        policies=_policies(),
    )
    ref = await _first(adapter.iter_items())
    out = await orchestrator.push_ref(ref)
    assert out.action == "replaced"
    assert ironrag.uploads == [], "must not upload — cursor knew doc_id"
    assert ironrag.replaces and ironrag.replaces[0]["document_id"] == "doc-pre"


@pytest.mark.asyncio
async def test_legacy_cursor_library_is_backfilled_before_trusting_doc_id(
    tmp_path: Path,
) -> None:
    adapter = EchoAdapter(
        {"1": EchoPage(item_id="1", title="One", body="hi", updated_at="t2")}
    )
    state = _state(tmp_path)
    state.upsert(
        kind="page",
        item_id="1",
        change_token="t1",
        external_key="echo:page:1",
        ironrag_document_id="doc-pre",
    )

    class BlindIronRag(FakeIronRag):
        async def find_document_by_external_key(self, library_id, external_key):
            return None

    ironrag = BlindIronRag()
    ironrag.documents[(LIB, "echo:page:1")] = {
        "id": "doc-pre",
        "externalKey": "echo:page:1",
    }
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=Router(_routing()),
        state=state,
        policies=_policies(),
    )

    ref = await _first(adapter.iter_items())
    out = await orchestrator.push_ref(ref)

    assert out.action == "replaced"
    assert ironrag.uploads == [], "must not upload with legacy cursor doc id"
    assert ironrag.replaces and ironrag.replaces[0]["document_id"] == "doc-pre"
    row = state.get("page", "1")
    assert row is not None
    assert row.ironrag_library_id == str(LIB)


@pytest.mark.asyncio
async def test_legacy_cursor_unknown_library_blocks_possible_duplicate_upload(
    tmp_path: Path,
) -> None:
    adapter = EchoAdapter(
        {"1": EchoPage(item_id="1", title="One", body="hi", updated_at="t2")}
    )
    state = _state(tmp_path)
    state.upsert(
        kind="page",
        item_id="1",
        change_token="t1",
        external_key="echo:page:1",
        ironrag_document_id="doc-pre",
    )

    class BlindIronRag(FakeIronRag):
        async def find_document_by_external_key(self, library_id, external_key):
            return None

        async def get_document(self, document_id: str) -> dict[str, Any] | None:
            return {"id": document_id, "externalKey": "echo:page:1"}

    ironrag = BlindIronRag()
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=Router(_routing()),
        state=state,
        policies=_policies(),
    )

    ref = await _first(adapter.iter_items())
    with pytest.raises(IronRagError, match="refusing to upload"):
        await orchestrator.push_ref(ref)

    assert ironrag.uploads == []


@pytest.mark.asyncio
async def test_legacy_cursor_lookup_timeout_blocks_possible_duplicate_upload(
    tmp_path: Path,
) -> None:
    adapter = EchoAdapter(
        {"1": EchoPage(item_id="1", title="One", body="hi", updated_at="t2")}
    )
    state = _state(tmp_path)
    state.upsert(
        kind="page",
        item_id="1",
        change_token="t1",
        external_key="echo:page:1",
        ironrag_document_id="doc-pre",
    )

    class BlindIronRag(FakeIronRag):
        async def find_document_by_external_key(self, library_id, external_key):
            return None

        async def get_document(self, document_id: str) -> dict[str, Any] | None:
            raise httpx.ReadTimeout("synthetic timeout")

    ironrag = BlindIronRag()
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=Router(_routing()),
        state=state,
        policies=_policies(),
        cursor_library_lookup_timeout_seconds=0.01,
    )

    ref = await _first(adapter.iter_items())
    with pytest.raises(IronRagError, match="refusing to upload"):
        await orchestrator.push_ref(ref)

    assert ironrag.uploads == []


@pytest.mark.asyncio
async def test_route_move_creates_new_target_and_reaps_old_target(tmp_path: Path) -> None:
    adapter = EchoAdapter(
        {"1": EchoPage(item_id="1", title="One", body="hello", updated_at="t1")}
    )
    state = _state(tmp_path)
    external_key = "echo:page:1"
    state.upsert(
        kind="page",
        item_id="1",
        change_token="t1",
        external_key=external_key,
        ironrag_document_id="doc-old",
        ironrag_library_id=str(LIB),
    )
    ironrag = FakeIronRag()
    ironrag.documents[(LIB, external_key)] = {
        "id": "doc-old",
        "externalKey": external_key,
    }
    router = Router(_routing_to(LIB2))
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=router,
        state=state,
        policies=_policies(),
    )
    manager = SyncManager(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        orchestrator=orchestrator,
        router=router,
        state=state,
        policies=_policies(),
        concurrency=1,
        interval_seconds=60,
    )

    report = await manager.run_once(reason="test")

    assert report.created == 1
    assert report.reaped == 1
    assert report.errors == 0
    assert (LIB, external_key) not in ironrag.documents
    assert (LIB2, external_key) in ironrag.documents
    row = state.get("page", "1")
    assert row is not None
    assert row.ironrag_document_id == "doc-100"
    assert row.ironrag_library_id == str(LIB2)


@pytest.mark.asyncio
async def test_route_move_from_legacy_cursor_reaps_old_target(tmp_path: Path) -> None:
    adapter = EchoAdapter(
        {"1": EchoPage(item_id="1", title="One", body="hello", updated_at="t1")}
    )
    state = _state(tmp_path)
    external_key = "echo:page:1"
    state.upsert(
        kind="page",
        item_id="1",
        change_token="t1",
        external_key=external_key,
        ironrag_document_id="doc-old",
    )
    ironrag = FakeIronRag()
    ironrag.documents[(LIB, external_key)] = {
        "id": "doc-old",
        "externalKey": external_key,
    }
    router = Router(_routing_to(LIB2))
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=router,
        state=state,
        policies=_policies(),
    )
    manager = SyncManager(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        orchestrator=orchestrator,
        router=router,
        state=state,
        policies=_policies(),
        concurrency=1,
        interval_seconds=60,
    )

    report = await manager.run_once(reason="test")

    assert report.created == 1
    assert report.reaped == 1
    assert report.errors == 0
    assert (LIB, external_key) not in ironrag.documents
    assert (LIB2, external_key) in ironrag.documents
    row = state.get("page", "1")
    assert row is not None
    assert row.ironrag_document_id == "doc-100"
    assert row.ironrag_library_id == str(LIB2)


@pytest.mark.asyncio
async def test_reap_respects_ignore_policy(tmp_path: Path) -> None:
    from ironrag_connector.source import SourceItemRef as Ref

    adapter = EchoAdapter({})
    state = _state(tmp_path)
    ironrag = FakeIronRag()
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=Router(_routing()),
        state=state,
        policies=_policies(),
    )
    ref = Ref(item_id="1", kind="image", external_key="echo:image:1")
    out = await orchestrator.reap_orphan(
        ref, LIB, "doc-x", PushPolicy(on_missing=DeleteAction.IGNORE)
    )
    assert out.action == "skipped_missing"
    assert ironrag.deletes == []


@pytest.mark.asyncio
async def test_cursor_library_timeout_does_not_abort_sweep_before_enumeration(
    tmp_path: Path,
) -> None:
    adapter = EchoAdapter(
        {"1": EchoPage(item_id="1", title="One", body="hello", updated_at="t1")}
    )
    state = _state(tmp_path)
    external_key = "echo:page:1"
    state.upsert(
        kind="page",
        item_id="1",
        change_token="t1",
        external_key=external_key,
        ironrag_document_id="doc-pre",
    )

    class TimeoutBackfillIronRag(FakeIronRag):
        async def get_document(self, document_id: str) -> dict[str, Any] | None:
            raise httpx.ReadTimeout("synthetic timeout")

    ironrag = TimeoutBackfillIronRag()
    ironrag.documents[(LIB, external_key)] = {
        "id": "doc-pre",
        "externalKey": external_key,
    }
    router = Router(_routing())
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=router,
        state=state,
        policies=_policies(),
        cursor_library_lookup_timeout_seconds=0.01,
    )
    manager = SyncManager(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        orchestrator=orchestrator,
        router=router,
        state=state,
        policies=_policies(),
        concurrency=1,
        interval_seconds=60,
        cursor_library_lookup_timeout_seconds=0.01,
    )

    report = await manager.run_once(reason="test")

    assert report.items_seen == 1
    assert report.noop_unchanged == 1
    assert report.errors == 0
    row = state.get("page", "1")
    assert row is not None
    assert row.ironrag_library_id == str(LIB)


@pytest.mark.asyncio
async def test_pre_enumeration_cursor_backfill_preserves_pushed_timestamp(
    tmp_path: Path,
) -> None:
    adapter = EchoAdapter({})
    state = _state(tmp_path)
    external_key = "echo:page:1"
    state.upsert(
        kind="page",
        item_id="1",
        change_token="t1",
        external_key=external_key,
        ironrag_document_id="doc-pre",
    )
    before = state.get("page", "1")
    assert before is not None

    ironrag = FakeIronRag()
    ironrag.documents[(LIB, external_key)] = {
        "id": "doc-pre",
        "externalKey": external_key,
    }
    router = Router(_routing())
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=router,
        state=state,
        policies=_policies(),
    )
    manager = SyncManager(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        orchestrator=orchestrator,
        router=router,
        state=state,
        policies=_policies(),
        concurrency=1,
        interval_seconds=60,
    )

    libraries = await manager._cursor_libraries_by_kind()

    row = state.get("page", "1")
    assert row is not None
    assert row.change_token == "t1"
    assert row.last_pushed_at == before.last_pushed_at
    assert row.ironrag_library_id == str(LIB)
    assert libraries == {"page": {LIB}}


@pytest.mark.asyncio
async def test_reaper_uses_partial_cursor_libraries_without_false_delete(
    tmp_path: Path,
) -> None:
    adapter = EchoAdapter({})
    state = _state(tmp_path)
    state.upsert(
        kind="page",
        item_id="known-old",
        change_token="t1",
        external_key="echo:page:known-old",
        ironrag_document_id="doc-known",
        ironrag_library_id=str(LIB),
    )
    state.upsert(
        kind="page",
        item_id="unknown-old",
        change_token="t1",
        external_key="echo:page:unknown-old",
        ironrag_document_id="doc-unknown",
    )

    class PartialBackfillIronRag(FakeIronRag):
        async def get_document(self, document_id: str) -> dict[str, Any] | None:
            if document_id == "doc-unknown":
                raise httpx.ReadTimeout("synthetic timeout")
            return await super().get_document(document_id)

    ironrag = PartialBackfillIronRag()
    ironrag.documents[(LIB, "echo:page:known-old")] = {
        "id": "doc-known",
        "externalKey": "echo:page:known-old",
    }
    ironrag.documents[(LIB3, "echo:page:unknown-old")] = {
        "id": "doc-unknown",
        "externalKey": "echo:page:unknown-old",
    }
    router = Router(_routing_to(LIB2))
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=router,
        state=state,
        policies=_policies(),
    )
    manager = SyncManager(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        orchestrator=orchestrator,
        router=router,
        state=state,
        policies=_policies(),
        concurrency=1,
        interval_seconds=60,
        cursor_library_lookup_timeout_seconds=0.01,
    )

    report = await manager.run_once(reason="test")

    assert report.reaped == 1
    assert report.errors == 0
    assert (LIB, "echo:page:known-old") not in ironrag.documents
    assert (LIB3, "echo:page:unknown-old") in ironrag.documents


@pytest.mark.asyncio
async def test_idempotency_key_is_content_addressed(tmp_path: Path) -> None:
    """The default idempotency key must derive from the payload bytes, not
    the change_token, so a re-rendered payload for the same logical version
    does not collide (409) with a stuck prior attempt, and the upload and
    replace key spaces stay separate."""
    adapter = EchoAdapter({})
    state = _state(tmp_path)
    ironrag = FakeIronRag()
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=Router(_routing()),
        state=state,
        policies=_policies(),
    )

    def _item(payload: bytes) -> SourceItem:
        return SourceItem(
            ref=SourceItemRef(
                item_id="1",
                kind="page",
                external_key="echo:page:1",
                change_token="vSAME",
            ),
            payload=payload,
            mime_type="text/html",
            file_name="1.html",
            title="One",
        )

    route = Router(_routing()).resolve(_item(b"a").ref)

    await orchestrator.push_item(_item(b"<p>render A</p>"), route, PushPolicy())
    first_key = ironrag.uploads[0]["idempotency_key"]
    # Same change_token, different bytes → different key (no false conflict).
    state.delete("page", "1")
    orchestrator.reset_sweep_cache()
    ironrag.documents.clear()
    await orchestrator.push_item(_item(b"<p>render B (different)</p>"), route, PushPolicy())
    second_key = ironrag.uploads[1]["idempotency_key"]

    assert "vSAME" not in first_key, "key must not embed change_token"
    assert ":upload:" in first_key, "op must scope the key space"
    assert first_key != second_key, "different payload must yield a different key"


@pytest.mark.asyncio
async def test_noop_persists_doc_id_to_cursor(tmp_path: Path) -> None:
    """A seed cursor that knows change_token but not the document id must be
    upgraded with the discovered id on the first sweep, so later sweeps
    short-circuit with zero list-endpoint calls."""
    adapter = EchoAdapter(
        {"1": EchoPage(item_id="1", title="One", body="hello", updated_at="t1")}
    )
    state = _state(tmp_path)
    # Seed cursor: change_token known, document id absent.
    state.upsert(
        kind="page",
        item_id="1",
        change_token="t1",
        external_key="echo:page:1",
        ironrag_document_id=None,
    )
    ironrag = FakeIronRag()
    ironrag.documents[(LIB, "echo:page:1")] = {
        "id": "doc-pre",
        "externalKey": "echo:page:1",
    }
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=Router(_routing()),
        state=state,
        policies=_policies(),
    )

    ref = await _first(adapter.iter_items())
    out = await orchestrator.push_ref(ref)
    assert out.action == "noop_unchanged"
    assert out.ironrag_document_id == "doc-pre"
    assert ironrag.find_calls == 1
    row = state.get("page", "1")
    assert row is not None
    assert row.ironrag_document_id == "doc-pre"

    # Second sweep: cursor now knows the id, so no further find calls.
    out2 = await orchestrator.push_ref(ref)
    assert out2.action == "noop_unchanged"
    assert ironrag.find_calls == 1, "cursor must short-circuit the second sweep"


@pytest.mark.asyncio
async def test_dependent_uploaded_with_parent_external_key(tmp_path: Path) -> None:
    """A page with an image dependent must upload the dependent declaring its
    source page as parent (parent_external_key == parent.ref.external_key),
    while the primary page uploads with parent_external_key None. This is the
    single orchestrator injection point that gives every connector correct
    parentage without an adapter change."""

    class PageWithImageAdapter(EchoAdapter):
        async def fetch(self, ref: SourceItemRef) -> SourceItem | None:
            page = await super().fetch(ref)
            if page is None:
                return None
            image = SourceItem(
                ref=SourceItemRef(
                    item_id="img-1",
                    kind="image",
                    external_key=self.external_key("image", "img-1"),
                    change_token="i1",
                ),
                payload=b"\x89PNG\r\n\x1a\n synthetic image bytes",
                mime_type="image/png",
                file_name="img-1.png",
                title="Synthetic image",
            )
            return SourceItem(
                ref=page.ref,
                payload=page.payload,
                mime_type=page.mime_type,
                file_name=page.file_name,
                title=page.title,
                dependents=(image,),
            )

    adapter = PageWithImageAdapter(
        {"1": EchoPage(item_id="1", title="One", body="hello", updated_at="t1")}
    )
    state = _state(tmp_path)
    ironrag = FakeIronRag()
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=Router(_routing()),
        state=state,
        policies=_policies(),
    )
    ref = await _first(adapter.iter_items())
    out = await orchestrator.push_ref(ref)

    assert out.action == "created"
    assert len(out.dependent_outcomes) == 1
    assert out.dependent_outcomes[0].action == "created"

    by_key = {u["external_key"]: u for u in ironrag.uploads}
    primary = by_key["echo:page:1"]
    dependent = by_key["echo:image:img-1"]
    assert primary["parent_external_key"] is None, "primary stays role=primary"
    assert dependent["parent_external_key"] == "echo:page:1", (
        "dependent must declare its source page as parent"
    )


async def _first(it: Any) -> Any:
    async for x in it:
        return x
    raise AssertionError("empty iter")
