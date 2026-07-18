from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from echo_connector.adapter import EchoAdapter, EchoPage

from ironrag_connector.ironrag import (
    DocumentResource,
    IronRagConflictError,
    IronRagDuplicateContentError,
    IronRagError,
    IronRagMutationTimeoutError,
    IronRagNotFoundError,
    OperationHandle,
    OperationProgress,
    OperationStatus,
    OperationStatusValue,
    ProblemDetails,
)
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
    ResolvedLibraryTarget,
    Router,
    RoutingConfig,
)
from ironrag_connector.source import SourceItem, SourceItemRef
from ironrag_connector.state import StateStore
from ironrag_connector.sync import SyncManager

WS = UUID("00000000-0000-0000-0000-000000000099")
LIB = UUID("00000000-0000-0000-0000-000000000000")
LIB2 = UUID("00000000-0000-0000-0000-000000000002")


def _doc_id(n: int) -> UUID:
    """Deterministic synthetic document/operation id for test fixtures."""
    return UUID(int=n)


DOC_PRE = _doc_id(1)
DOC_OLD = _doc_id(2)
DOC_IMAGE_PRE = _doc_id(3)


def _document(doc_id: UUID, library_id: UUID, external_key: str) -> DocumentResource:
    return DocumentResource(
        id=doc_id, library_id=library_id, external_key=external_key, status="ready"
    )


def _duplicate_content_error(existing_id: UUID) -> IronRagDuplicateContentError:
    problem = ProblemDetails(
        type="urn:ironrag:error:duplicate_content",
        title="Duplicate Content",
        status=409,
        detail="synthetic duplicate content",
        code="duplicate_content",
        existing_document_id=existing_id,
    )
    return IronRagDuplicateContentError(problem)


def _conflict_error() -> IronRagConflictError:
    problem = ProblemDetails(
        type="urn:ironrag:error:conflicting_mutation",
        title="Conflicting Mutation",
        status=409,
        detail="synthetic conflicting mutation",
        code="conflicting_mutation",
    )
    return IronRagConflictError(problem)


def _not_found_error() -> IronRagNotFoundError:
    problem = ProblemDetails(
        type="urn:ironrag:error:not_found",
        title="Not Found",
        status=404,
        detail="synthetic not found",
        code="not_found",
    )
    return IronRagNotFoundError(problem)


def _ready_operation(op_id: UUID) -> OperationStatus:
    return OperationStatus(
        id=op_id,
        workspace_id=WS,
        library_id=LIB,
        operation_kind="revise_document",
        status=OperationStatusValue.READY,
        created_at=datetime.now(tz=UTC),
        progress=OperationProgress(),
    )


class FakeIronRag:
    """In-memory double for :class:`IronRagClient` matching its redesigned
    interface: synchronous ``create_document``, async ``create_revision``/
    ``delete_document`` (submit -> poll via ``wait_for_operation``), and
    ``find_document``/``get_document``/``list_documents`` reads."""

    def __init__(self) -> None:
        self.documents: dict[tuple[UUID, str], DocumentResource] = {}
        self.creates: list[dict[str, Any]] = []
        self.revisions: list[dict[str, Any]] = []
        self.deletes: list[str] = []
        self.duplicate_for_key: str | None = None
        self.revision_conflicts: set[str] = set()
        self.find_calls = 0
        self._next_doc_id = 100
        self._next_op_id = 1000
        self._duplicate_existing_id: UUID | None = None
        self._operations: dict[UUID, OperationStatus] = {}

    def _new_doc_id(self) -> UUID:
        doc_id = _doc_id(self._next_doc_id)
        self._next_doc_id += 1
        return doc_id

    def _new_op_id(self) -> UUID:
        op_id = _doc_id(self._next_op_id)
        self._next_op_id += 1
        return op_id

    async def find_document(self, library_id: UUID, external_key: str) -> DocumentResource | None:
        self.find_calls += 1
        return self.documents.get((library_id, external_key))

    async def get_document(self, document_id: UUID | str) -> DocumentResource | None:
        for doc in self.documents.values():
            if str(doc.id) == str(document_id):
                return doc
        return None

    async def create_document(
        self,
        library_id: UUID,
        *,
        external_key: str,
        file_bytes: bytes | None = None,
        file_name: str | None = None,
        mime_type: str | None = None,
        title: str | None = None,
        document_hint: str | None = None,
        parent_external_key: str | None = None,
    ) -> DocumentResource:
        self.creates.append(
            {
                "library_id": library_id,
                "external_key": external_key,
                "size": len(file_bytes) if file_bytes is not None else 0,
                "mime_type": mime_type,
                "document_hint": document_hint,
                "parent_external_key": parent_external_key,
            }
        )
        if self.duplicate_for_key == external_key:
            if self._duplicate_existing_id is None:
                self._duplicate_existing_id = self._new_doc_id()
            raise _duplicate_content_error(self._duplicate_existing_id)
        doc = DocumentResource(
            id=self._new_doc_id(),
            library_id=library_id,
            external_key=external_key,
            status="ready",
            file_name=file_name,
            document_hint=document_hint,
        )
        self.documents[(library_id, external_key)] = doc
        return doc

    async def create_revision(
        self,
        document_id: UUID | str,
        *,
        mode: str,
        markdown: str | None = None,
        appended_text: str | None = None,
        file_bytes: bytes | None = None,
        file_name: str | None = None,
        mime_type: str | None = None,
        idempotency_key: str,
    ) -> OperationHandle:
        doc_id_str = str(document_id)
        if doc_id_str in self.revision_conflicts:
            raise _conflict_error()
        if not any(str(doc.id) == doc_id_str for doc in self.documents.values()):
            raise _not_found_error()
        size = len(file_bytes) if file_bytes is not None else len(appended_text or markdown or "")
        self.revisions.append(
            {
                "document_id": doc_id_str,
                "mode": mode,
                "size": size,
                "idempotency_key": idempotency_key,
            }
        )
        op_id = self._new_op_id()
        self._operations[op_id] = _ready_operation(op_id)
        return OperationHandle(operation_id=op_id)

    async def delete_document(
        self, document_id: UUID | str, *, idempotency_key: str
    ) -> OperationHandle | None:
        found_key = None
        for key, doc in self.documents.items():
            if str(doc.id) == str(document_id):
                found_key = key
                break
        if found_key is None:
            return None
        del self.documents[found_key]
        self.deletes.append(str(document_id))
        op_id = self._new_op_id()
        self._operations[op_id] = _ready_operation(op_id)
        return OperationHandle(operation_id=op_id)

    async def wait_for_operation(
        self,
        operation_id: UUID | str,
        *,
        poll_interval: float | None = None,
        budget: float | None = None,
    ) -> OperationStatus:
        return self._operations[UUID(str(operation_id))]

    async def list_documents(
        self,
        library_id: UUID,
        *,
        search: str | None = None,
        external_key: str | None = None,
        status: Any = (),
        include_deleted: bool = False,
        limit: int = 200,
    ) -> AsyncIterator[DocumentResource]:
        for (lib, key), doc in list(self.documents.items()):
            if lib != library_id:
                continue
            if external_key and key != external_key:
                continue
            yield doc


def _library_ref(library_id: UUID) -> str:
    return f"tests/library-{library_id.hex}"


def _router_to(library_id: UUID) -> Router:
    library_ref = _library_ref(library_id)
    config = RoutingConfig.model_validate({"default": {"library": library_ref}})
    return Router(
        config,
        resolved_targets={
            library_ref: ResolvedLibraryTarget(
                library_ref=library_ref,
                workspace_id=WS,
                library_id=library_id,
            )
        },
    )


def _router() -> Router:
    return _router_to(LIB)


def _policies(default: PushPolicy | None = None) -> PolicyOverrides:
    return PolicyOverrides(default=default or PushPolicy(), by_kind={})


def _state(tmp_path: Path) -> StateStore:
    return StateStore(tmp_path / "s.sqlite")


@pytest.mark.asyncio
async def test_create_new_item(tmp_path: Path) -> None:
    adapter = EchoAdapter({"1": EchoPage(item_id="1", title="One", body="hello", updated_at="t1")})
    state = _state(tmp_path)
    ironrag = FakeIronRag()
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=_router(),
        state=state,
        policies=_policies(),
    )
    ref = await _first(adapter.iter_items())
    out = await orchestrator.push_ref(ref)
    assert out.action == "created"
    created_doc = ironrag.documents[(LIB, "echo:page:1")]
    assert out.ironrag_document_id == str(created_doc.id)
    row = state.get("page", "1")
    assert row is not None
    assert row.change_token == "t1"


@pytest.mark.asyncio
async def test_document_hint_forwards_on_create(tmp_path: Path) -> None:
    adapter = EchoAdapter({})
    state = _state(tmp_path)
    ironrag = FakeIronRag()
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=_router(),
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
    route = _router().resolve(item.ref)

    out = await orchestrator.push_item(item, route, PushPolicy())

    assert out.action == "created"
    assert ironrag.creates[0]["document_hint"] == "https://docs.example/hinted"


@pytest.mark.asyncio
async def test_unchanged_short_circuits_to_noop(tmp_path: Path) -> None:
    adapter = EchoAdapter({"1": EchoPage(item_id="1", title="One", body="hello", updated_at="t1")})
    state = _state(tmp_path)
    ironrag = FakeIronRag()
    ironrag.documents[(LIB, "echo:page:1")] = _document(DOC_PRE, LIB, "echo:page:1")
    state.upsert(
        kind="page",
        item_id="1",
        change_token="t1",
        external_key="echo:page:1",
        ironrag_document_id=str(DOC_PRE),
        ironrag_library_id=str(LIB),
    )
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=_router(),
        state=state,
        policies=_policies(),
    )
    ref = await _first(adapter.iter_items())
    out = await orchestrator.push_ref(ref)
    assert out.action == "noop_unchanged"
    assert ironrag.creates == []
    assert ironrag.revisions == []


@pytest.mark.asyncio
async def test_changed_item_replaces(tmp_path: Path) -> None:
    adapter = EchoAdapter({"1": EchoPage(item_id="1", title="One", body="hello2", updated_at="t2")})
    state = _state(tmp_path)
    ironrag = FakeIronRag()
    ironrag.documents[(LIB, "echo:page:1")] = _document(DOC_PRE, LIB, "echo:page:1")
    state.upsert(
        kind="page",
        item_id="1",
        change_token="t1",
        external_key="echo:page:1",
        ironrag_document_id=str(DOC_PRE),
        ironrag_library_id=str(LIB),
    )
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=_router(),
        state=state,
        policies=_policies(),
    )
    ref = await _first(adapter.iter_items())
    out = await orchestrator.push_ref(ref)
    assert out.action == "replaced"
    assert ironrag.revisions and ironrag.revisions[0]["document_id"] == str(DOC_PRE)
    row = state.get("page", "1")
    assert row is not None
    assert row.change_token == "t2"


@pytest.mark.asyncio
async def test_conflicting_replace_is_deferred_without_advancing_cursor(
    tmp_path: Path,
) -> None:
    adapter = EchoAdapter({"1": EchoPage(item_id="1", title="One", body="hello2", updated_at="t2")})
    state = _state(tmp_path)
    ironrag = FakeIronRag()
    ironrag.documents[(LIB, "echo:page:1")] = _document(DOC_PRE, LIB, "echo:page:1")
    ironrag.revision_conflicts.add(str(DOC_PRE))
    state.upsert(
        kind="page",
        item_id="1",
        change_token="t1",
        external_key="echo:page:1",
        ironrag_document_id=str(DOC_PRE),
        ironrag_library_id=str(LIB),
    )
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=_router(),
        state=state,
        policies=_policies(),
    )

    ref = await _first(adapter.iter_items())
    out = await orchestrator.push_ref(ref)

    assert out.action == "skipped_changed"
    assert out.ironrag_document_id == str(DOC_PRE)
    assert out.deferred is True
    assert ironrag.creates == []
    assert ironrag.revisions == []
    row = state.get("page", "1")
    assert row is not None
    assert row.change_token == "t1"


@pytest.mark.asyncio
async def test_on_changed_skip_does_not_replace(tmp_path: Path) -> None:
    adapter = EchoAdapter({"1": EchoPage(item_id="1", title="One", body="hi", updated_at="t2")})
    state = _state(tmp_path)
    ironrag = FakeIronRag()
    ironrag.documents[(LIB, "echo:page:1")] = _document(DOC_PRE, LIB, "echo:page:1")
    policy = PushPolicy(on_changed=UpdateAction.SKIP)
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=_router(),
        state=state,
        policies=_policies(policy),
    )
    ref = await _first(adapter.iter_items())
    out = await orchestrator.push_ref(ref)
    assert out.action == "skipped_changed"
    assert ironrag.revisions == []


@pytest.mark.asyncio
async def test_on_new_skip_does_not_create(tmp_path: Path) -> None:
    adapter = EchoAdapter({"1": EchoPage(item_id="1", title="One", body="hi", updated_at="t1")})
    state = _state(tmp_path)
    ironrag = FakeIronRag()
    policy = PushPolicy(on_new=UpsertAction.SKIP)
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=_router(),
        state=state,
        policies=_policies(policy),
    )
    ref = await _first(adapter.iter_items())
    out = await orchestrator.push_ref(ref)
    assert out.action == "skipped_new"
    assert ironrag.creates == []


@pytest.mark.asyncio
async def test_duplicate_content_skip(tmp_path: Path) -> None:
    adapter = EchoAdapter({"1": EchoPage(item_id="1", title="One", body="hi", updated_at="t1")})
    state = _state(tmp_path)
    ironrag = FakeIronRag()
    ironrag.duplicate_for_key = "echo:page:1"
    policy = PushPolicy(on_duplicate_content=DuplicateContentAction.SKIP)
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=_router(),
        state=state,
        policies=_policies(policy),
    )
    ref = await _first(adapter.iter_items())
    out = await orchestrator.push_ref(ref)
    assert out.action == "skipped_duplicate_content"
    assert ironrag._duplicate_existing_id is not None
    assert out.ironrag_document_id == str(ironrag._duplicate_existing_id)


@pytest.mark.asyncio
async def test_duplicate_content_fail_raises(tmp_path: Path) -> None:
    adapter = EchoAdapter({"1": EchoPage(item_id="1", title="One", body="hi", updated_at="t1")})
    state = _state(tmp_path)
    ironrag = FakeIronRag()
    ironrag.duplicate_for_key = "echo:page:1"
    policy = PushPolicy(on_duplicate_content=DuplicateContentAction.FAIL)
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=_router(),
        state=state,
        policies=_policies(policy),
    )
    ref = await _first(adapter.iter_items())
    with pytest.raises(IronRagError, match="duplicate content"):
        await orchestrator.push_ref(ref)


@pytest.mark.asyncio
async def test_cursor_wins_over_server_find(tmp_path: Path) -> None:
    """Cursor with a known document_id must short-circuit the server lookup.

    Simulates a deployment where a server-side find is unreliable:
    ``BlindIronRag.find_document`` always returns None. Without the
    cursor-wins fix the orchestrator would re-create and trip a
    unique-violation server-side.
    """
    adapter = EchoAdapter({"1": EchoPage(item_id="1", title="One", body="hi", updated_at="t2")})
    state = _state(tmp_path)
    state.upsert(
        kind="page",
        item_id="1",
        change_token="t1",
        external_key="echo:page:1",
        ironrag_document_id=str(DOC_PRE),
        ironrag_library_id=str(LIB),
    )

    class BlindIronRag(FakeIronRag):
        async def find_document(
            self, library_id: UUID, external_key: str
        ) -> DocumentResource | None:
            return None  # server-side find surrogate: always misses

    ironrag = BlindIronRag()
    ironrag.documents[(LIB, "echo:page:1")] = _document(DOC_PRE, LIB, "echo:page:1")
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=_router(),
        state=state,
        policies=_policies(),
    )
    ref = await _first(adapter.iter_items())
    out = await orchestrator.push_ref(ref)
    assert out.action == "replaced"
    assert ironrag.creates == [], "must not create -- cursor knew doc_id"
    assert ironrag.revisions and ironrag.revisions[0]["document_id"] == str(DOC_PRE)


@pytest.mark.asyncio
async def test_replace_timeout_is_deferred_without_advancing_cursor(tmp_path: Path) -> None:
    adapter = EchoAdapter({"1": EchoPage(item_id="1", title="One", body="hi", updated_at="t2")})
    state = _state(tmp_path)
    state.upsert(
        kind="page",
        item_id="1",
        change_token="t1",
        external_key="echo:page:1",
        ironrag_document_id=str(DOC_PRE),
        ironrag_library_id=str(LIB),
    )

    class TimeoutIronRag(FakeIronRag):
        async def wait_for_operation(
            self,
            operation_id: UUID | str,
            *,
            poll_interval: float | None = None,
            budget: float | None = None,
        ) -> OperationStatus:
            raise IronRagMutationTimeoutError("synthetic mutation timeout")

    ironrag = TimeoutIronRag()
    ironrag.documents[(LIB, "echo:page:1")] = _document(DOC_PRE, LIB, "echo:page:1")
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=_router(),
        state=state,
        policies=_policies(),
    )

    ref = await _first(adapter.iter_items())
    out = await orchestrator.push_ref(ref)

    assert out.action == "skipped_changed"
    assert out.ironrag_document_id == str(DOC_PRE)
    assert out.deferred is True
    row = state.get("page", "1")
    assert row is not None
    assert row.change_token == "t1", "deferred revision must retry the new version later"


@pytest.mark.asyncio
async def test_revise_404_falls_back_to_create(tmp_path: Path) -> None:
    """A cursor/find pointed at a doc IronRAG no longer has (manual delete,
    library reset). The typed 404 must invalidate the cursor and fall back
    to create so the next sweep is consistent."""
    adapter = EchoAdapter({"1": EchoPage(item_id="1", title="One", body="hi", updated_at="t2")})
    state = _state(tmp_path)
    state.upsert(
        kind="page",
        item_id="1",
        change_token="t1",
        external_key="echo:page:1",
        ironrag_document_id=str(DOC_PRE),
        ironrag_library_id=str(LIB),
    )

    class MissingDocumentIronRag(FakeIronRag):
        async def create_revision(self, *args: Any, **kwargs: Any) -> OperationHandle:
            raise _not_found_error()

    ironrag = MissingDocumentIronRag()
    # No pre-seeded document for "echo:page:1" -- `find_document` (used by
    # `push_item`'s existing-document lookup) also finds nothing, but the
    # cursor still claims a document id, exercising the create_revision-404
    # path via the cursor branch instead.
    ironrag.documents[(LIB, "echo:page:1")] = _document(DOC_PRE, LIB, "echo:page:1")
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=_router(),
        state=state,
        policies=_policies(),
    )

    ref = await _first(adapter.iter_items())
    out = await orchestrator.push_ref(ref)

    assert out.action == "created"
    created_doc = ironrag.documents[(LIB, "echo:page:1")]
    assert out.ironrag_document_id == str(created_doc.id)


@pytest.mark.asyncio
async def test_route_move_creates_new_target_and_reaps_old_target(tmp_path: Path) -> None:
    """After routing config moves a kind to a new library, the orchestrator
    creates the document under the new target AND reaps the stale document
    from the previously-routed library. The redesigned reaper gets this
    library set from a pre-sweep snapshot of cursor rows (every row already
    carries a non-null ``library_id``, schema NOT NULL, S7.6) taken before
    enumeration rewrites the moved item's own cursor row -- a synchronous
    local read, unlike the removed `_cursor_libraries_by_kind`, which needed
    remote lookups specifically to discover an unknown library_id."""
    adapter = EchoAdapter({"1": EchoPage(item_id="1", title="One", body="hello", updated_at="t1")})
    state = _state(tmp_path)
    external_key = "echo:page:1"
    state.upsert(
        kind="page",
        item_id="1",
        change_token="t1",
        external_key=external_key,
        ironrag_document_id=str(DOC_OLD),
        ironrag_library_id=str(LIB),
    )
    ironrag = FakeIronRag()
    ironrag.documents[(LIB, external_key)] = _document(DOC_OLD, LIB, external_key)
    router = _router_to(LIB2)
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
    assert (LIB, external_key) not in ironrag.documents, "old-library doc must be reaped"
    assert (LIB2, external_key) in ironrag.documents
    row = state.get("page", "1")
    assert row is not None
    assert row.ironrag_library_id == str(LIB2)


@pytest.mark.asyncio
async def test_unmoved_kind_is_not_reaped_from_libraries_it_never_used(
    tmp_path: Path,
) -> None:
    """A kind whose items were never routed to some other library must not
    have that unrelated library swept in -- the pre-sweep snapshot only
    widens the reap scope to libraries a *cursor row for this kind* actually
    names, not every library the connector has ever touched."""
    adapter = EchoAdapter({})
    state = _state(tmp_path)
    external_key = "echo:page:stable"
    state.upsert(
        kind="page",
        item_id="stable",
        change_token="t1",
        external_key=external_key,
        ironrag_document_id=str(DOC_PRE),
        ironrag_library_id=str(LIB),
    )
    ironrag = FakeIronRag()
    ironrag.documents[(LIB, external_key)] = _document(DOC_PRE, LIB, external_key)
    # A document that happens to live in LIB2, a library this kind's cursor
    # never references -- must survive the sweep untouched.
    ironrag.documents[(LIB2, "echo:page:unrelated")] = _document(
        DOC_OLD, LIB2, "echo:page:unrelated"
    )
    router = _router()
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

    assert report.reaped == 1, "the stable doc itself has no source item this sweep"
    assert (LIB, external_key) not in ironrag.documents, "stable doc has no source item -- reaped"
    assert (LIB2, "echo:page:unrelated") in ironrag.documents, "unrelated library left untouched"


@pytest.mark.asyncio
async def test_reap_respects_ignore_policy(tmp_path: Path) -> None:
    adapter = EchoAdapter({})
    state = _state(tmp_path)
    ironrag = FakeIronRag()
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=_router(),
        state=state,
        policies=_policies(),
    )
    ref = SourceItemRef(item_id="1", kind="image", external_key="echo:image:1")
    out = await orchestrator.reap_orphan(
        ref, LIB, str(DOC_PRE), PushPolicy(on_missing=DeleteAction.IGNORE)
    )
    assert out.action == "skipped_missing"
    assert ironrag.deletes == []


@pytest.mark.asyncio
async def test_reap_orphan_deletes_and_polls_to_terminal(tmp_path: Path) -> None:
    adapter = EchoAdapter({})
    state = _state(tmp_path)
    ironrag = FakeIronRag()
    ironrag.documents[(LIB, "echo:image:1")] = _document(DOC_PRE, LIB, "echo:image:1")
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=_router(),
        state=state,
        policies=_policies(),
    )
    ref = SourceItemRef(item_id="1", kind="image", external_key="echo:image:1")
    out = await orchestrator.reap_orphan(ref, LIB, str(DOC_PRE), PushPolicy())
    assert out.action == "deleted"
    assert ironrag.deletes == [str(DOC_PRE)]
    assert (LIB, "echo:image:1") not in ironrag.documents


@pytest.mark.asyncio
async def test_revision_idempotency_key_is_content_addressed(tmp_path: Path) -> None:
    """The revision idempotency key must derive from the payload bytes, not
    the change_token, so a re-rendered payload for the same logical version
    does not collide (409 idempotency_conflict) with a stuck prior attempt.
    Create no longer takes an idempotency key at all (plan S7.1/S7.2) -- only
    revisions and deletes do -- so this is exercised via a replace."""
    adapter = EchoAdapter({})
    state = _state(tmp_path)
    ironrag = FakeIronRag()
    ironrag.documents[(LIB, "echo:page:1")] = _document(DOC_PRE, LIB, "echo:page:1")
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=_router(),
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

    route = _router().resolve(_item(b"a").ref)

    await orchestrator.push_item(_item(b"<p>render A</p>"), route, PushPolicy())
    first_key = ironrag.revisions[0]["idempotency_key"]
    # Same change_token, different bytes -> different key (no false conflict).
    state.upsert(
        kind="page",
        item_id="1",
        change_token="t-reset",
        external_key="echo:page:1",
        ironrag_document_id=str(DOC_PRE),
        ironrag_library_id=str(LIB),
    )
    orchestrator.reset_sweep_cache()
    await orchestrator.push_item(_item(b"<p>render B (different)</p>"), route, PushPolicy())
    second_key = ironrag.revisions[1]["idempotency_key"]

    assert "vSAME" not in first_key, "key must not embed change_token"
    assert ":revision:" in first_key, "op must scope the key space"
    assert first_key != second_key, "different payload must yield a different key"


@pytest.mark.asyncio
async def test_noop_persists_doc_id_to_cursor(tmp_path: Path) -> None:
    """A seed cursor that knows change_token but not the document id must be
    upgraded with the discovered id on the first sweep, so later sweeps
    short-circuit with zero find calls."""
    adapter = EchoAdapter({"1": EchoPage(item_id="1", title="One", body="hello", updated_at="t1")})
    state = _state(tmp_path)
    ironrag = FakeIronRag()
    ironrag.documents[(LIB, "echo:page:1")] = _document(DOC_PRE, LIB, "echo:page:1")
    # Seed cursor: change_token known, document id absent -- must still
    # supply a library id, since it is NOT NULL from schema creation.
    state.upsert(
        kind="page",
        item_id="1",
        change_token="t1",
        external_key="echo:page:1",
        ironrag_document_id=None,
        ironrag_library_id=str(LIB),
    )
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=_router(),
        state=state,
        policies=_policies(),
    )

    ref = await _first(adapter.iter_items())
    out = await orchestrator.push_ref(ref)
    assert out.action == "noop_unchanged"
    assert out.ironrag_document_id == str(DOC_PRE)
    assert ironrag.find_calls == 1
    row = state.get("page", "1")
    assert row is not None
    assert row.ironrag_document_id == str(DOC_PRE)

    # Second sweep: cursor now knows the id, so no further find calls.
    out2 = await orchestrator.push_ref(ref)
    assert out2.action == "noop_unchanged"
    assert ironrag.find_calls == 1, "cursor must short-circuit the second sweep"


@pytest.mark.asyncio
async def test_dependent_uploaded_with_parent_external_key(tmp_path: Path) -> None:
    """A page with an image dependent must create the dependent declaring its
    source page as parent (parent_external_key == parent.ref.external_key),
    while the primary page creates with parent_external_key None. This is the
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
        router=_router(),
        state=state,
        policies=_policies(),
    )
    ref = await _first(adapter.iter_items())
    out = await orchestrator.push_ref(ref)

    assert out.action == "created"
    assert len(out.dependent_outcomes) == 1
    assert out.dependent_outcomes[0].action == "created"

    by_key = {c["external_key"]: c for c in ironrag.creates}
    primary = by_key["echo:page:1"]
    dependent = by_key["echo:image:img-1"]
    assert primary["parent_external_key"] is None, "primary stays role=primary"
    assert dependent["parent_external_key"] == "echo:page:1", (
        "dependent must declare its source page as parent"
    )


@pytest.mark.asyncio
async def test_dependent_waits_when_primary_replace_is_deferred(tmp_path: Path) -> None:
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

    class TimeoutReviseIronRag(FakeIronRag):
        async def wait_for_operation(
            self,
            operation_id: UUID | str,
            *,
            poll_interval: float | None = None,
            budget: float | None = None,
        ) -> OperationStatus:
            raise IronRagMutationTimeoutError("synthetic mutation timeout")

    adapter = PageWithImageAdapter(
        {"1": EchoPage(item_id="1", title="One", body="hello", updated_at="t2")}
    )
    state = _state(tmp_path)
    state.upsert(
        kind="page",
        item_id="1",
        change_token="t1",
        external_key="echo:page:1",
        ironrag_document_id=str(DOC_PRE),
        ironrag_library_id=str(LIB),
    )
    ironrag = TimeoutReviseIronRag()
    ironrag.documents[(LIB, "echo:page:1")] = _document(DOC_PRE, LIB, "echo:page:1")
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=_router(),
        state=state,
        policies=_policies(),
    )

    out = await orchestrator.push_ref(await _first(adapter.iter_items()))

    assert out.action == "skipped_changed"
    assert out.deferred is True
    assert out.dependent_outcomes == ()
    assert ironrag.creates == []
    row = state.get("page", "1")
    assert row is not None
    assert row.change_token == "t1"


@pytest.mark.asyncio
async def test_dependent_deferral_restores_parent_cursor_for_retry(
    tmp_path: Path,
) -> None:
    """The dependent image already has an IronRAG document (so its write is
    a revise, not a create -- only revise supports poll-timeout deferral in
    the redesigned client, since create is now a single synchronous call
    with no polling phase to time out on). Its first revision poll times
    out; the orchestrator must defer the whole item (restoring the parent's
    cursor for retry) rather than partially advancing state, and the second
    sweep must succeed once the transient failure clears."""

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

    class OneTimeoutImageReviseIronRag(FakeIronRag):
        def __init__(self) -> None:
            super().__init__()
            self.image_op_polls = 0
            self._image_op_ids: set[UUID] = set()

        async def create_revision(self, document_id: Any, **kwargs: Any) -> OperationHandle:
            handle = await super().create_revision(document_id, **kwargs)
            if str(document_id) == str(DOC_IMAGE_PRE):
                self._image_op_ids.add(handle.operation_id)
            return handle

        async def wait_for_operation(
            self,
            operation_id: UUID | str,
            *,
            poll_interval: float | None = None,
            budget: float | None = None,
        ) -> OperationStatus:
            op_id = UUID(str(operation_id))
            if op_id in self._image_op_ids:
                self.image_op_polls += 1
                if self.image_op_polls == 1:
                    raise IronRagMutationTimeoutError("synthetic mutation timeout")
            return await super().wait_for_operation(operation_id)

    adapter = PageWithImageAdapter(
        {"1": EchoPage(item_id="1", title="One", body="hello", updated_at="t2")}
    )
    state = _state(tmp_path)
    state.upsert(
        kind="page",
        item_id="1",
        change_token="t1",
        external_key="echo:page:1",
        ironrag_document_id=str(DOC_PRE),
        ironrag_library_id=str(LIB),
    )
    state.upsert(
        kind="image",
        item_id="img-1",
        change_token="i0",
        external_key="echo:image:img-1",
        ironrag_document_id=str(DOC_IMAGE_PRE),
        ironrag_library_id=str(LIB),
    )
    ironrag = OneTimeoutImageReviseIronRag()
    ironrag.documents[(LIB, "echo:page:1")] = _document(DOC_PRE, LIB, "echo:page:1")
    ironrag.documents[(LIB, "echo:image:img-1")] = _document(
        DOC_IMAGE_PRE, LIB, "echo:image:img-1"
    )
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,  # type: ignore[arg-type]
        router=_router(),
        state=state,
        policies=_policies(),
    )
    ref = await _first(adapter.iter_items())

    first = await orchestrator.push_ref(ref)

    assert first.action == "replaced"
    assert first.deferred is True
    assert len(first.dependent_outcomes) == 1
    assert first.dependent_outcomes[0].action == "skipped_changed"
    assert first.dependent_outcomes[0].deferred is True
    assert ironrag.image_op_polls == 1
    parent_row = state.get("page", "1")
    assert parent_row is not None
    assert parent_row.change_token == "t1"
    image_row = state.get("image", "img-1")
    assert image_row is not None
    assert image_row.change_token == "i0"

    orchestrator.reset_sweep_cache()
    second = await orchestrator.push_ref(ref)

    assert second.action == "replaced"
    assert second.deferred is False
    assert len(second.dependent_outcomes) == 1
    assert second.dependent_outcomes[0].action == "replaced"
    assert ironrag.image_op_polls == 2
    page_row_after = state.get("page", "1")
    assert page_row_after is not None
    assert page_row_after.change_token == "t2"
    image_row_after = state.get("image", "img-1")
    assert image_row_after is not None
    assert image_row_after.change_token == "i1"


async def _first(it: Any) -> Any:
    async for x in it:
        return x
    raise AssertionError("empty iter")
