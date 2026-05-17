from __future__ import annotations

from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from echo_connector.adapter import EchoAdapter, EchoPage

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

WS = UUID("00000000-0000-0000-0000-000000000099")
LIB = UUID("00000000-0000-0000-0000-000000000000")


class FakeIronRag:
    def __init__(self) -> None:
        self.documents: dict[tuple[UUID, str], dict[str, Any]] = {}
        self.uploads: list[dict[str, Any]] = []
        self.replaces: list[dict[str, Any]] = []
        self.deletes: list[str] = []
        self.duplicate_for_key: str | None = None
        self.next_doc_id = 100

    async def find_document_by_external_key(
        self, library_id: UUID, external_key: str
    ) -> dict[str, Any] | None:
        return self.documents.get((library_id, external_key))

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
    ) -> dict[str, Any]:
        self.uploads.append(
            {
                "library_id": library_id,
                "external_key": external_key,
                "size": len(file_bytes),
                "idempotency_key": idempotency_key,
                "mime_type": mime_type,
                "document_hint": document_hint,
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
    )

    class BlindIronRag(FakeIronRag):
        async def find_document_by_external_key(self, library_id, external_key):
            return None  # IronRAG bug surrogate: list endpoint ignores filter

    ironrag = BlindIronRag()
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


async def _first(it: Any) -> Any:
    async for x in it:
        return x
    raise AssertionError("empty iter")
