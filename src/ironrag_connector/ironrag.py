"""Async IronRAG content client — targets the redesigned `/v1` REST surface.

Endpoints
=========

* ``GET /v1/catalog/workspaces`` and
  ``GET /v1/catalog/workspaces/{id}/libraries`` — resolve canonical
  ``workspace-slug/library-slug`` routing refs once per routing snapshot.
  Unaffected by the content-domain redesign.
* ``GET /v1/content/libraries/{libraryId}/documents`` — the single
  read-collection interface: cursor pagination, ``search`` (a
  case-insensitive substring/ILIKE filter on ``external_key`` only --
  there is no dedicated exact-match filter), ``includeDeleted``,
  ``status``. Backs both :meth:`IronRagClient.find_document` and
  :meth:`IronRagClient.list_documents`; both reconstruct an exact-key
  lookup by passing the key as ``search`` and filtering the (typically
  single) page of matches client-side for an exact ``externalKey``
  equality, since ``search`` narrows on the same column.
* ``POST /v1/content/libraries/{libraryId}/documents`` — content-negotiated
  create (``application/json`` for metadata/pointer documents,
  ``multipart/form-data`` for byte uploads). Synchronous: 201 + Location.
* ``POST /v1/content/documents/{id}/revisions`` — content-negotiated
  revision (JSON ``{mode: "append"|"replace"}`` for text, multipart for a
  file replace). Asynchronous: 202 + ``Location`` to the canonical
  ``/v1/ops/operations/{operationId}``.
* ``DELETE /v1/content/documents/{id}`` — asynchronous in effect (always
  creates an ``ops_async_operation`` row to poll), but returns a plain
  200 with the operation id in the JSON body rather than 202 + Location
  like revisions. Handled uniformly through the same typed body field.
* ``GET /v1/content/documents/{id}`` — single document detail.
* ``GET /v1/ops/operations/{operationId}`` — the canonical poll target
  for every asynchronous mutation. :meth:`IronRagClient.wait_for_operation`
  is the one poll-to-terminal primitive every mutating call funnels
  through.

Errors
======

Every non-2xx response is RFC 9457 ``application/problem+json``:
``{type, title, status, detail, code, requestId?, ...extensions}``, with
extension members (e.g. ``existingDocumentId`` on a 409 duplicate-content
conflict) flattened at the top level rather than nested. Error handling
here is entirely typed field access — no regex or substring sniffing of
error messages.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Mapping, Sequence
from datetime import datetime
from enum import StrEnum
from typing import Any, Literal, Protocol
from uuid import UUID

import httpx
from pydantic import AliasChoices, BaseModel, ConfigDict, Field

from .config import BaseConnectorSettings
from .observability import get_logger
from .routing import ResolvedLibraryTarget, normalize_library_ref

log = get_logger(__name__)

DEFAULT_REWALK_CONCURRENCY = 4
"""Conservative default fan-out for a post-upgrade full re-walk (S7.6).

Bounds how many in-flight create/revision calls a caller issues while
consuming :meth:`IronRagClient.walk_all_documents` or driving a sweep --
this module never fetches list pages concurrently (cursor pagination is
inherently sequential; page N+1 needs page N's cursor). Operators should
confirm this default against each source system's documented rate limit
before the first production re-walk (plan S7.6/S9 step 2).
"""


# ---------------------------------------------------------------------------
# Typed response models
# ---------------------------------------------------------------------------


class DocumentResource(BaseModel):
    """One document as returned by list/find/get/create.

    The list endpoint (``ContentDocumentListItem``) and the get/create
    endpoints (``ContentDocumentDetailResponse``, unwrapped by
    :func:`_extract_document_payload`) are genuinely different response
    shapes, not just naming drift: the list row carries a derived
    ``status`` bucket (``queued``/``processing``/``ready``/``failed``/
    ``canceled``), while the detail/create row instead carries the raw
    ``documentState`` lifecycle value (``active``/``deleted``) with no
    ``status`` field at all. ``status`` accepts either source key so one
    model can populate from both surfaces; a value from ``documentState``
    is the raw lifecycle state, not the derived status bucket -- callers
    that need the canonical bucket must go through :meth:`IronRagClient.
    list_documents`/:meth:`~IronRagClient.find_document`, not
    :meth:`~IronRagClient.get_document`/:meth:`~IronRagClient.create_document`.
    """

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    id: UUID
    library_id: UUID = Field(alias="libraryId")
    workspace_id: UUID | None = Field(default=None, alias="workspaceId")
    external_key: str = Field(alias="externalKey")
    status: str | None = Field(
        default=None, validation_alias=AliasChoices("status", "documentState")
    )
    file_name: str | None = Field(default=None, alias="fileName")
    document_hint: str | None = Field(default=None, alias="documentHint")
    uploaded_at: datetime | None = Field(default=None, alias="uploadedAt")


class DocumentPage(BaseModel):
    """One cursor page from ``GET .../documents``."""

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    items: list[DocumentResource] = Field(default_factory=list)
    next_cursor: str | None = Field(default=None, alias="nextCursor")
    total: int | None = None


class OperationStatusValue(StrEnum):
    """Canonical ``ops_async_operation.status`` values (server-authoritative)."""

    ACCEPTED = "accepted"
    PROCESSING = "processing"
    READY = "ready"
    FAILED = "failed"
    SUPERSEDED = "superseded"
    CANCELED = "canceled"

    @property
    def is_terminal(self) -> bool:
        return self in (
            OperationStatusValue.READY,
            OperationStatusValue.FAILED,
            OperationStatusValue.SUPERSEDED,
            OperationStatusValue.CANCELED,
        )


class OperationProgress(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    total: int = 0
    completed: int = 0
    failed: int = 0
    in_flight: int = Field(default=0, alias="inFlight")


class OperationStatus(BaseModel):
    """``GET /v1/ops/operations/{operationId}`` response body.

    The server's ``AsyncOperationDetailResponse`` applies
    ``#[serde(flatten)]`` to its ``OpsAsyncOperation`` row -- the row's
    fields (``id``, ``workspaceId``, ``operationKind``, ``status``, ...)
    sit directly at the top level of the JSON object, as siblings of
    ``progress``, not nested under an ``"operation"`` key. This model
    mirrors that flattened shape; there is no separate ``AsyncOperation``
    wrapper type.
    """

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    id: UUID
    workspace_id: UUID = Field(alias="workspaceId")
    library_id: UUID | None = Field(default=None, alias="libraryId")
    operation_kind: str = Field(alias="operationKind")
    status: OperationStatusValue
    failure_code: str | None = Field(default=None, alias="failureCode")
    created_at: datetime = Field(alias="createdAt")
    completed_at: datetime | None = Field(default=None, alias="completedAt")
    progress: OperationProgress


class OperationHandle(BaseModel):
    """202-Accepted admission: the operation id to poll to terminal."""

    model_config = ConfigDict(populate_by_name=True)

    operation_id: UUID


class ProblemDetails(BaseModel):
    """RFC 9457 ``application/problem+json`` error body.

    Known extension members (currently just ``existingDocumentId`` on the
    document-create duplicate-content conflict) are declared as typed
    optional fields; ``extra="allow"`` preserves any other extension
    losslessly on ``model_extra`` without the client needing to know about
    it ahead of time.
    """

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    type: str
    title: str
    status: int
    detail: str
    code: str
    request_id: str | None = Field(default=None, alias="requestId")
    existing_document_id: UUID | None = Field(default=None, alias="existingDocumentId")


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class IronRagError(RuntimeError):
    """IronRAG returned a non-recoverable status."""


class IronRagCatalogError(IronRagError):
    """IronRAG could not resolve a canonical catalog reference."""


class IronRagMutationTimeoutError(IronRagError):
    """An async operation did not reach a terminal state inside the poll budget."""


class IronRagProblemError(IronRagError):
    """Base class for typed RFC 9457 problem+json errors.

    Carries the fully parsed :class:`ProblemDetails` so callers branch on
    ``.problem.code`` (and, for the duplicate-content conflict,
    ``.existing_document_id``) instead of sniffing the message text.
    """

    def __init__(self, problem: ProblemDetails) -> None:
        self.problem = problem
        super().__init__(problem.detail)


class IronRagNotFoundError(IronRagProblemError):
    """404 -- the referenced resource does not exist."""


class IronRagDuplicateContentError(IronRagProblemError):
    """409 ``duplicate_content`` -- an active document with this external key,
    or identical bytes, already exists. Carries the conflicting document id
    via the typed ``existingDocumentId`` problem+json extension member."""

    @property
    def existing_document_id(self) -> UUID | None:
        return self.problem.existing_document_id


class IronRagConflictError(IronRagProblemError):
    """409 for any conflict other than duplicate content (e.g. a document
    with a mutation already in flight)."""


class IronRagOperationFailedError(IronRagError):
    """A polled operation reached terminal status ``failed``."""

    def __init__(self, status: OperationStatus) -> None:
        self.status = status
        super().__init__(
            f"operation {status.id} failed"
            + (f" ({status.failure_code})" if status.failure_code else "")
        )


def _parse_problem(response: httpx.Response) -> ProblemDetails:
    try:
        body: Any = response.json()
    except ValueError:
        body = {}
    if not isinstance(body, dict):
        body = {}
    body.setdefault("type", "urn:ironrag:error:unknown")
    body.setdefault("title", "Error")
    body.setdefault("status", response.status_code)
    body.setdefault("detail", response.text[:400] or response.reason_phrase)
    body.setdefault("code", "unknown")
    return ProblemDetails.model_validate(body)


def _raise_for_problem(response: httpx.Response) -> None:
    problem = _parse_problem(response)
    if response.status_code == 404:
        raise IronRagNotFoundError(problem)
    if response.status_code == 409 and problem.code == "duplicate_content":
        raise IronRagDuplicateContentError(problem)
    if response.status_code == 409:
        raise IronRagConflictError(problem)
    raise IronRagProblemError(problem)


def _extract_document_payload(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    """Unwrap the ``{"document": ...}`` envelope used by create/get responses.

    Some server responses nest a canonical document object a second level
    deep (detail responses wrap the storage row inside a richer envelope).
    Handle one level of that nesting by merging the outer scalar fields
    (which may carry ``externalKey``/``libraryId`` the inner row omits)
    under the inner object.
    """
    doc = payload.get("document", payload)
    if isinstance(doc, Mapping) and isinstance(doc.get("document"), Mapping):
        inner = doc["document"]
        merged = {**doc, **inner}
        merged.pop("document", None)
        return merged
    return doc if isinstance(doc, Mapping) else payload


def _operation_id_from_response(body: Mapping[str, Any]) -> UUID:
    """Extract the operation id to poll from a mutation-admission body.

    ``ContentMutationDetailResponse`` carries a top-level ``asyncOperationId``
    on every mutation admission this client issues -- revisions (202
    Accepted) and deletes alike. Deliberately does NOT key off the
    ``Location`` header: create-revision sets it (202 + Location to
    ``/v1/ops/operations/{operationId}``) but delete-document does not (its
    handler returns a plain 200 with the operation id only in the body,
    despite carrying the same async-admission semantics) -- reading the
    typed body field uniformly covers both without the caller needing to
    know which status/header combination a given mutation kind uses.
    """
    raw = body.get("asyncOperationId")
    if raw is None:
        raise IronRagError("IronRAG accepted the mutation but returned no operation id")
    return UUID(str(raw))


class WalkCheckpointStore(Protocol):
    """Injectable checkpoint persistence for :meth:`IronRagClient.walk_all_documents`.

    The connector's own local state store (SQLite cursor) is the intended
    implementation -- this client has no persistence of its own.
    """

    def load_cursor(self) -> str | None: ...

    def save_cursor(self, cursor: str | None) -> None: ...


class IronRagClient:
    def __init__(
        self,
        settings: BaseConnectorSettings,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._settings = settings
        self._owns_client = client is None
        self._default_poll_interval = settings.operation_poll_interval_seconds
        self._default_poll_budget = settings.operation_poll_budget_seconds
        self._client = client or httpx.AsyncClient(
            base_url=settings.ironrag_base_url.rstrip("/"),
            timeout=settings.request_timeout_seconds,
            headers={
                "Authorization": f"Bearer {settings.ironrag_api_token}",
                "Accept": "application/json",
            },
        )

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> IronRagClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    # -- catalog -----------------------------------------------------------

    async def resolve_library_refs(
        self,
        library_refs: set[str],
    ) -> dict[str, ResolvedLibraryTarget]:
        """Resolve canonical ``workspace/library`` refs to internal UUIDs.

        IronRAG catalog list endpoints are permission-filtered, so a missing
        ref is deliberately indistinguishable from a ref the caller cannot
        discover. Resolution happens once for the complete routing snapshot;
        document mutations continue using UUIDs internally.
        """
        normalized_refs = {normalize_library_ref(ref) for ref in library_refs}
        if normalized_refs != library_refs:
            raise IronRagCatalogError("library refs must already be canonical")
        if not normalized_refs:
            return {}

        workspaces_payload = await self._catalog_list(
            "/v1/catalog/workspaces",
            resource="workspaces",
        )
        workspaces = _index_catalog_rows(workspaces_payload, resource="workspace")

        refs_by_workspace: dict[str, set[str]] = {}
        for library_ref in normalized_refs:
            workspace_slug, _ = library_ref.split("/", 1)
            refs_by_workspace.setdefault(workspace_slug, set()).add(library_ref)

        result: dict[str, ResolvedLibraryTarget] = {}
        for workspace_slug in sorted(refs_by_workspace):
            workspace = workspaces.get(workspace_slug)
            if workspace is None:
                refs = ", ".join(sorted(refs_by_workspace[workspace_slug]))
                raise IronRagCatalogError(
                    f"IronRAG workspace slug '{workspace_slug}' is not visible "
                    f"for library ref(s): {refs}"
                )
            workspace_id = _catalog_uuid(workspace, "id", "workspace", workspace_slug)
            libraries_payload = await self._catalog_list(
                f"/v1/catalog/workspaces/{workspace_id}/libraries",
                resource=f"libraries in workspace '{workspace_slug}'",
            )
            libraries = _index_catalog_rows(libraries_payload, resource="library")

            for library_ref in sorted(refs_by_workspace[workspace_slug]):
                _, library_slug = library_ref.split("/", 1)
                library = libraries.get(library_slug)
                if library is None:
                    raise IronRagCatalogError(f"IronRAG library ref '{library_ref}' is not visible")
                row_workspace_id = _catalog_uuid(
                    library,
                    "workspaceId",
                    "library",
                    library_ref,
                )
                if row_workspace_id != workspace_id:
                    raise IronRagCatalogError(
                        f"IronRAG library ref '{library_ref}' returned workspaceId "
                        f"{row_workspace_id}, expected {workspace_id}"
                    )
                result[library_ref] = ResolvedLibraryTarget(
                    library_ref=library_ref,
                    workspace_id=workspace_id,
                    library_id=_catalog_uuid(library, "id", "library", library_ref),
                )
        return result

    async def _catalog_list(
        self,
        path: str,
        *,
        resource: str,
    ) -> list[Mapping[str, Any]]:
        response = await self._client.get(path)
        if response.status_code >= 400:
            raise IronRagCatalogError(
                f"IronRAG catalog {resource} -> {response.status_code}: {response.text[:400]}"
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise IronRagCatalogError(f"IronRAG catalog {resource} returned invalid JSON") from exc
        if not isinstance(payload, list) or not all(isinstance(item, Mapping) for item in payload):
            raise IronRagCatalogError(
                f"IronRAG catalog {resource} must return a JSON array of objects"
            )
        return payload

    # -- documents -----------------------------------------------------------

    async def find_document(self, library_id: UUID, external_key: str) -> DocumentResource | None:
        """Resolve a document by its exact external key.

        There is no dedicated exact-match filter on the list endpoint --
        only a ``search`` substring/ILIKE filter on ``external_key``
        (plan S7.1 describes an exact filter as an aspiration; the landed
        API does not implement one). This narrows server-side via
        ``search=external_key`` and returns the first page item whose
        ``external_key`` equals the requested key exactly, which is still
        a single bounded request rather than the old substring-ILIKE +
        client filter + full-scan-fallback dance.
        """
        async for document in self.list_documents(
            library_id, external_key=external_key, limit=50
        ):
            return document
        return None

    async def list_documents(
        self,
        library_id: UUID,
        *,
        search: str | None = None,
        external_key: str | None = None,
        status: Sequence[str] = (),
        include_deleted: bool = False,
        limit: int = 200,
    ) -> AsyncIterator[DocumentResource]:
        """Walk every page of the library's document collection.

        The only read-collection interface (plan S7.1) -- cursor pagination
        to ``nextCursor is None``, never a ``total``-driven offset fallback.

        ``external_key`` is not a server-side parameter (see
        :meth:`find_document`): when set, it is sent as ``search`` to
        narrow server-side, and every yielded item is additionally
        filtered to an exact ``external_key`` match client-side, so this
        behaves as an exact-key list (normally 0 or 1 result) rather than
        a substring search. Passing both ``search`` and ``external_key``
        is not supported -- they would need conflicting narrowing.
        """
        if search and external_key:
            raise ValueError("list_documents accepts either search or external_key, not both")
        server_search = external_key or search
        cursor: str | None = None
        while True:
            params: dict[str, Any] = {"limit": limit}
            if server_search:
                params["search"] = server_search
            if status:
                params["status"] = ",".join(status)
            if include_deleted:
                params["includeDeleted"] = "true"
            if cursor:
                params["cursor"] = cursor

            response = await self._client.get(
                f"/v1/content/libraries/{library_id}/documents", params=params
            )
            if response.status_code == 404:
                return
            if response.status_code >= 400:
                _raise_for_problem(response)
            page = DocumentPage.model_validate(response.json())
            for item in page.items:
                if external_key and item.external_key != external_key:
                    continue
                yield item
            if not page.next_cursor:
                return
            cursor = page.next_cursor

    async def walk_all_documents(
        self,
        library_id: UUID,
        *,
        concurrency: int = DEFAULT_REWALK_CONCURRENCY,
        resume_from_checkpoint: bool = True,
        checkpoint_store: WalkCheckpointStore | None = None,
    ) -> AsyncIterator[DocumentResource]:
        """Checkpointed full walk of every document in ``library_id``.

        List pagination is inherently sequential (page N+1 needs page N's
        cursor), so ``concurrency`` does not parallelize the walk itself --
        it is the fan-out bound the caller should apply when it issues
        create/revision calls in response to each yielded item (the same
        semaphore pattern :class:`~ironrag_connector.sync.SyncManager`
        already uses for a sweep). ``checkpoint_store`` persists the
        in-flight cursor after every page so an interrupted or
        rate-limited re-walk resumes instead of restarting (plan S7.6).
        """
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        cursor = (
            checkpoint_store.load_cursor()
            if resume_from_checkpoint and checkpoint_store is not None
            else None
        )
        limit = 200
        while True:
            params: dict[str, Any] = {"limit": limit}
            if cursor:
                params["cursor"] = cursor
            response = await self._client.get(
                f"/v1/content/libraries/{library_id}/documents", params=params
            )
            if response.status_code == 404:
                return
            if response.status_code >= 400:
                _raise_for_problem(response)
            page = DocumentPage.model_validate(response.json())
            for item in page.items:
                yield item
            cursor = page.next_cursor
            if checkpoint_store is not None:
                checkpoint_store.save_cursor(cursor)
            if not cursor:
                return

    async def get_document(self, document_id: UUID | str) -> DocumentResource | None:
        response = await self._client.get(f"/v1/content/documents/{document_id}")
        if response.status_code == 404:
            return None
        if response.status_code >= 400:
            _raise_for_problem(response)
        payload = response.json()
        return DocumentResource.model_validate(_extract_document_payload(payload))

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
        """Create a document. Content-negotiated: passing ``file_bytes``
        sends a multipart upload; omitting it sends a JSON metadata-only
        create. One creation door, one HTTP path (plan S7.1/S7.2).

        No ``idempotency_key`` parameter: create dedup is the server's
        exact ``externalKey`` uniqueness check plus the typed
        :class:`IronRagDuplicateContentError` 409, not an idempotency key
        (plan S7.1) -- content-hash idempotency keys apply to revisions and
        deletes only.

        Raises :class:`IronRagDuplicateContentError` on a 409 conflict;
        the error carries ``existing_document_id`` for the caller to adopt.
        """
        log.info("ironrag.create_document.start", external_key=external_key)
        if file_bytes is not None:
            if not file_name or not mime_type:
                raise ValueError("file_name and mime_type are required when file_bytes is set")
            files = {"file": (file_name, file_bytes, mime_type)}
            # `library_id` is required by the server's multipart parser even
            # though this endpoint is already scoped by the path segment --
            # the parser (shared plumbing) rejects a multipart body with no
            # `library_id` field with 400 "missing library_id" before the
            # handler gets a chance to ignore the value in favor of the path
            # parameter. Send it anyway; the server discards it.
            data: dict[str, Any] = {
                "library_id": str(library_id),
                "external_key": external_key,
            }
            if title:
                data["title"] = title
            if document_hint is not None:
                data["document_hint"] = document_hint
            if parent_external_key is not None:
                data["parent_external_key"] = parent_external_key
            response = await self._client.post(
                f"/v1/content/libraries/{library_id}/documents",
                data=data,
                files=files,
            )
        else:
            json_body: dict[str, Any] = {"externalKey": external_key}
            if title:
                json_body["title"] = title
            if document_hint is not None:
                json_body["documentHint"] = document_hint
            if parent_external_key is not None:
                json_body["parentExternalKey"] = parent_external_key
            response = await self._client.post(
                f"/v1/content/libraries/{library_id}/documents",
                json=json_body,
            )

        if response.status_code >= 400:
            _raise_for_problem(response)
        payload = response.json()
        document = DocumentResource.model_validate(_extract_document_payload(payload))
        log.info(
            "ironrag.create_document.created",
            external_key=external_key,
            document_id=str(document.id),
        )
        return document

    async def create_revision(
        self,
        document_id: UUID | str,
        *,
        mode: Literal["append", "replace"],
        markdown: str | None = None,
        appended_text: str | None = None,
        file_bytes: bytes | None = None,
        file_name: str | None = None,
        mime_type: str | None = None,
        idempotency_key: str,
    ) -> OperationHandle:
        """Create a new revision. Content-negotiated, mirroring
        :meth:`create_document`: ``file_bytes`` sends a multipart file
        replace; otherwise a JSON ``{mode, appendedText|markdown}`` body.
        Always asynchronous -- returns an :class:`OperationHandle` to poll
        via :meth:`wait_for_operation`.
        """
        log.info(
            "ironrag.create_revision.start",
            document_id=str(document_id),
            mode=mode,
        )
        if file_bytes is not None:
            if not file_name or not mime_type:
                raise ValueError("file_name and mime_type are required when file_bytes is set")
            files = {"file": (file_name, file_bytes, mime_type)}
            data = {"idempotency_key": idempotency_key}
            response = await self._client.post(
                f"/v1/content/documents/{document_id}/revisions",
                data=data,
                files=files,
            )
        else:
            json_body: dict[str, Any] = {"mode": mode, "idempotencyKey": idempotency_key}
            if mode == "append":
                if appended_text is None:
                    raise ValueError("appended_text is required when mode='append'")
                json_body["appendedText"] = appended_text
            else:
                if markdown is None:
                    raise ValueError("markdown is required when mode='replace' without file_bytes")
                json_body["markdown"] = markdown
            response = await self._client.post(
                f"/v1/content/documents/{document_id}/revisions",
                json=json_body,
            )

        if response.status_code >= 400:
            _raise_for_problem(response)
        body = response.json() if response.content else {}
        operation_id = _operation_id_from_response(body)
        log.info(
            "ironrag.create_revision.accepted",
            document_id=str(document_id),
            operation_id=str(operation_id),
        )
        return OperationHandle(operation_id=operation_id)

    async def delete_document(
        self, document_id: UUID | str, *, idempotency_key: str
    ) -> OperationHandle | None:
        """Delete a document. Returns ``None`` when IronRAG already has no
        such document (idempotent no-op); otherwise an
        :class:`OperationHandle` to poll to terminal.

        Deletes are asynchronous like revisions in effect (an
        ``ops_async_operation`` row is always created and must be polled to
        terminal), but NOT in HTTP shape: the handler returns a plain 200
        with the operation id only in the JSON body, not 202 + Location like
        create-revision. Both are handled uniformly via the typed body field
        (see :func:`_operation_id_from_response`), so this difference is
        invisible to callers of this method.
        """
        log.info("ironrag.delete_document.start", document_id=str(document_id))
        response = await self._client.request(
            "DELETE",
            f"/v1/content/documents/{document_id}",
            headers={"Idempotency-Key": idempotency_key},
        )
        if response.status_code == 404:
            return None
        if response.status_code >= 400:
            _raise_for_problem(response)
        body = response.json() if response.content else {}
        operation_id = _operation_id_from_response(body)
        log.info(
            "ironrag.delete_document.accepted",
            document_id=str(document_id),
            operation_id=str(operation_id),
        )
        return OperationHandle(operation_id=operation_id)

    # -- async operations -----------------------------------------------------

    async def get_operation(self, operation_id: UUID | str) -> OperationStatus:
        response = await self._client.get(f"/v1/ops/operations/{operation_id}")
        if response.status_code >= 400:
            _raise_for_problem(response)
        return OperationStatus.model_validate(response.json())

    async def wait_for_operation(
        self,
        operation_id: UUID | str,
        *,
        poll_interval: float | None = None,
        budget: float | None = None,
    ) -> OperationStatus:
        """Poll ``GET /v1/ops/operations/{operationId}`` to a terminal state.

        The one poll-to-terminal primitive every mutating call funnels
        through (plan S7.2/S7.3). Terminal states:

        * ``ready`` -- returned normally.
        * ``superseded`` / ``canceled`` -- returned normally; the caller
          decides how to react (a later write won, or the operation was
          explicitly canceled -- neither is a client-side failure).
        * ``failed`` -- raises :class:`IronRagOperationFailedError`.

        Raises :class:`IronRagMutationTimeoutError` if no terminal state is
        reached inside ``budget`` seconds.
        """
        resolved_interval = (
            poll_interval if poll_interval is not None else self._default_poll_interval
        )
        resolved_budget = budget if budget is not None else self._default_poll_budget
        deadline = time.monotonic() + resolved_budget
        while True:
            status = await self.get_operation(operation_id)
            if status.status.is_terminal:
                if status.status is OperationStatusValue.FAILED:
                    raise IronRagOperationFailedError(status)
                return status
            if time.monotonic() >= deadline:
                raise IronRagMutationTimeoutError(
                    f"operation {operation_id} did not reach a terminal state "
                    f"within {resolved_budget:.1f}s"
                )
            await asyncio.sleep(resolved_interval)


def _index_catalog_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    resource: str,
) -> dict[str, Mapping[str, Any]]:
    indexed: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        slug = row.get("slug")
        if not isinstance(slug, str) or not slug:
            raise IronRagCatalogError(f"IronRAG catalog {resource} row has an invalid slug")
        if slug in indexed:
            raise IronRagCatalogError(
                f"IronRAG catalog returned duplicate {resource} slug '{slug}'"
            )
        indexed[slug] = row
    return indexed


def _catalog_uuid(
    row: Mapping[str, Any],
    key: str,
    resource: str,
    catalog_ref: str,
) -> UUID:
    raw = row.get(key)
    try:
        return UUID(str(raw))
    except (TypeError, ValueError, AttributeError) as exc:
        raise IronRagCatalogError(
            f"IronRAG {resource} '{catalog_ref}' returned invalid {key}"
        ) from exc
