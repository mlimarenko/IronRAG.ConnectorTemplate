from __future__ import annotations

from collections.abc import Mapping, Sequence
from uuid import UUID

import httpx
import pytest

from ironrag_connector.config import BaseConnectorSettings
from ironrag_connector.ironrag import (
    IronRagCatalogError,
    IronRagClient,
    IronRagConflictError,
    IronRagDuplicateContentError,
    IronRagError,
    IronRagMutationTimeoutError,
    IronRagNotFoundError,
    IronRagOperationFailedError,
    OperationStatusValue,
)

LIB = UUID("00000000-0000-0000-0000-000000000000")
LIB_2 = UUID("00000000-0000-0000-0000-000000000001")
WS = UUID("00000000-0000-0000-0000-000000000099")
WS_2 = UUID("00000000-0000-0000-0000-000000000098")
DOC = UUID("00000000-0000-0000-0000-0000000000d1")
OP = UUID("00000000-0000-0000-0000-0000000000f1")


def _settings() -> BaseConnectorSettings:
    return BaseConnectorSettings(
        ironrag_base_url="http://ironrag.example.com",
        ironrag_api_token="token",
        request_timeout_seconds=60.0,
        operation_poll_interval_seconds=0.05,
        operation_poll_budget_seconds=1.0,
    )


def _client(handler: httpx.MockTransport) -> IronRagClient:
    return IronRagClient(
        _settings(),
        client=httpx.AsyncClient(
            base_url="http://ironrag.example.com",
            transport=handler,
        ),
    )


def _problem(code: str, *, status: int, **extensions: object) -> dict[str, object]:
    return {
        "type": f"urn:ironrag:error:{code}",
        "title": code.replace("_", " ").title(),
        "status": status,
        "detail": f"synthetic {code}",
        "code": code,
        **extensions,
    }


# ---------------------------------------------------------------------------
# resolve_library_refs -- unaffected by the content-domain redesign.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_library_refs_uses_permission_filtered_catalog_snapshot() -> None:
    requests: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        if request.url.path == "/v1/catalog/workspaces":
            return httpx.Response(
                200,
                json=[
                    {"id": str(WS), "slug": "main", "displayName": "Main workspace"},
                    {"id": str(WS_2), "slug": "partner", "displayName": "Partner workspace"},
                ],
            )
        if request.url.path == f"/v1/catalog/workspaces/{WS}/libraries":
            return httpx.Response(
                200,
                json={
                    "items": [
                        {
                            "id": str(LIB),
                            "workspaceId": str(WS),
                            "slug": "product-docs",
                            "displayName": "Product documentation",
                        }
                    ],
                    "totalCount": 1,
                    "offset": 0,
                    "limit": 100,
                },
            )
        if request.url.path == f"/v1/catalog/workspaces/{WS_2}/libraries":
            return httpx.Response(
                200,
                json={
                    "items": [
                        {
                            "id": str(LIB_2),
                            "workspaceId": str(WS_2),
                            "slug": "archive",
                            "displayName": "Archive",
                        }
                    ],
                    "totalCount": 1,
                    "offset": 0,
                    "limit": 100,
                },
            )
        raise AssertionError(f"unexpected request: {request.url}")

    client = _client(httpx.MockTransport(handle))
    targets = await client.resolve_library_refs({"main/product-docs", "partner/archive"})
    await client.aclose()

    assert targets["main/product-docs"].workspace_id == WS
    assert targets["main/product-docs"].library_id == LIB
    assert targets["partner/archive"].workspace_id == WS_2
    assert targets["partner/archive"].library_id == LIB_2
    assert requests == [
        "/v1/catalog/workspaces",
        f"/v1/catalog/workspaces/{WS}/libraries",
        f"/v1/catalog/workspaces/{WS_2}/libraries",
    ]


@pytest.mark.asyncio
async def test_resolve_library_refs_never_falls_back_to_display_name() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/catalog/workspaces"
        return httpx.Response(
            200,
            json=[{"id": str(WS), "slug": "main", "displayName": "Friendly Main"}],
        )

    client = _client(httpx.MockTransport(handle))
    with pytest.raises(IronRagCatalogError, match="Friendly Main/product-docs"):
        await client.resolve_library_refs({"Friendly Main/product-docs"})
    await client.aclose()


@pytest.mark.asyncio
async def test_resolve_library_refs_rejects_catalog_workspace_mismatch() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/catalog/workspaces":
            return httpx.Response(200, json=[{"id": str(WS), "slug": "main"}])
        return httpx.Response(
            200,
            json={
                "items": [
                    {
                        "id": str(LIB),
                        "workspaceId": str(WS_2),
                        "slug": "product-docs",
                    }
                ],
                "totalCount": 1,
                "offset": 0,
                "limit": 100,
            },
        )

    client = _client(httpx.MockTransport(handle))
    with pytest.raises(IronRagCatalogError, match="workspaceId"):
        await client.resolve_library_refs({"main/product-docs"})
    await client.aclose()


@pytest.mark.asyncio
async def test_resolve_library_refs_surfaces_catalog_authorization_failure() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, json={"error": "forbidden"})

    client = _client(httpx.MockTransport(handle))
    with pytest.raises(IronRagCatalogError, match="403"):
        await client.resolve_library_refs({"main/product-docs"})
    await client.aclose()


@pytest.mark.asyncio
async def test_resolve_library_refs_rejects_ambiguous_catalog_snapshot() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=[
                {"id": str(WS), "slug": "main"},
                {"id": str(WS_2), "slug": "main"},
            ],
        )

    client = _client(httpx.MockTransport(handle))
    with pytest.raises(IronRagCatalogError, match="duplicate workspace slug 'main'"):
        await client.resolve_library_refs({"main/product-docs"})
    await client.aclose()


# ---------------------------------------------------------------------------
# resolve_library_refs -- the two catalog list endpoints have DIFFERENT
# response shapes. `/v1/catalog/workspaces` returns a bare array;
# `/v1/catalog/workspaces/{id}/libraries` returns a bounded
# `{items, totalCount, offset, limit}` page. The fake below mirrors the real
# library endpoint: substring `search` over slug OR displayName, `limit`
# clamped to 100, `totalCount` scoped to the search filter.
# ---------------------------------------------------------------------------

CATALOG_SERVER_DEFAULT_PAGE_LIMIT = 50
CATALOG_SERVER_MAX_PAGE_LIMIT = 100


def _workspaces_response() -> httpx.Response:
    return httpx.Response(200, json=[{"id": str(WS), "slug": "main", "displayName": "Main"}])


def _library_row(library_id: UUID, slug: str, display_name: str) -> dict[str, str]:
    return {
        "id": str(library_id),
        "workspaceId": str(WS),
        "slug": slug,
        "displayName": display_name,
    }


def _library_page(
    rows: Sequence[Mapping[str, str]],
    request: httpx.Request,
) -> httpx.Response:
    """Serve one page the way the real library endpoint does."""
    search = request.url.params.get("search")
    needle = search.lower() if search else None
    matches = [
        row
        for row in rows
        if needle is None or needle in row["slug"].lower() or needle in row["displayName"].lower()
    ]
    offset = int(request.url.params.get("offset", 0))
    requested = int(request.url.params.get("limit", CATALOG_SERVER_DEFAULT_PAGE_LIMIT))
    limit = min(requested, CATALOG_SERVER_MAX_PAGE_LIMIT)
    return httpx.Response(
        200,
        json={
            "items": list(matches[offset : offset + limit]),
            "totalCount": len(matches),
            "offset": offset,
            "limit": limit,
        },
    )


def _crowded_rows(*, include_exact: bool) -> list[dict[str, str]]:
    """250 substring decoys, one display-name-only decoy, optionally the target.

    Every row matches `search=alpha`, so the narrowed result is still far
    wider than a single page and the exact slug sits at the very end.
    """
    rows = [
        _library_row(UUID(int=0x1000 + index), f"alpha-{index:03d}", f"Alpha decoy {index}")
        for index in range(1, 251)
    ]
    rows.append(_library_row(UUID(int=0x2000), "beta-suite", "Alpha Suite"))
    if include_exact:
        rows.append(_library_row(LIB, "alpha", "Alpha"))
    return rows


@pytest.mark.asyncio
async def test_resolve_library_refs_reads_the_library_page_envelope() -> None:
    rows = [_library_row(LIB, "product-docs", "Product documentation")]

    def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/catalog/workspaces":
            return _workspaces_response()
        assert request.url.path == f"/v1/catalog/workspaces/{WS}/libraries"
        return _library_page(rows, request)

    client = _client(httpx.MockTransport(handle))
    targets = await client.resolve_library_refs({"main/product-docs"})
    await client.aclose()

    assert targets["main/product-docs"].workspace_id == WS
    assert targets["main/product-docs"].library_id == LIB


@pytest.mark.asyncio
async def test_resolve_library_refs_walks_library_pages_past_the_first() -> None:
    rows = _crowded_rows(include_exact=True)
    library_requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/catalog/workspaces":
            return _workspaces_response()
        library_requests.append(request)
        return _library_page(rows, request)

    client = _client(httpx.MockTransport(handle))
    targets = await client.resolve_library_refs({"main/alpha"})
    await client.aclose()

    # The exact slug is the last of 252 matching rows: a single-page read
    # cannot see it, and a "first matching row wins" read picks a decoy.
    assert targets["main/alpha"].library_id == LIB
    assert targets["main/alpha"].workspace_id == WS
    assert len(library_requests) >= 2


@pytest.mark.asyncio
async def test_resolve_library_refs_reports_scan_counts_for_a_missing_ref() -> None:
    rows = _crowded_rows(include_exact=False)

    def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/catalog/workspaces":
            return _workspaces_response()
        return _library_page(rows, request)

    client = _client(httpx.MockTransport(handle))
    with pytest.raises(IronRagCatalogError) as excinfo:
        await client.resolve_library_refs({"main/alpha"})
    await client.aclose()

    message = str(excinfo.value)
    assert "main/alpha" in message
    assert "scanned 251 of 251" in message


@pytest.mark.asyncio
async def test_resolve_library_refs_rejects_a_truncated_library_page_walk() -> None:
    """A server that promises more rows than it serves must fail loudly."""
    served = _crowded_rows(include_exact=False)[:100]

    def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/catalog/workspaces":
            return _workspaces_response()
        offset = int(request.url.params.get("offset", 0))
        return httpx.Response(
            200,
            json={
                "items": served if offset == 0 else [],
                "totalCount": 2500,
                "offset": offset,
                "limit": 100,
            },
        )

    client = _client(httpx.MockTransport(handle))
    with pytest.raises(IronRagCatalogError) as excinfo:
        await client.resolve_library_refs({"main/alpha"})
    await client.aclose()

    message = str(excinfo.value)
    assert "incomplete" in message
    assert "scanned 100 of 2500" in message


@pytest.mark.asyncio
async def test_resolve_library_refs_rejects_a_library_response_without_a_page() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/catalog/workspaces":
            return _workspaces_response()
        return httpx.Response(200, json={"totalCount": 1})

    client = _client(httpx.MockTransport(handle))
    with pytest.raises(IronRagCatalogError, match="items"):
        await client.resolve_library_refs({"main/alpha"})
    await client.aclose()


# ---------------------------------------------------------------------------
# find_document / list_documents -- the list endpoint has no exact-match
# filter, only `search` (ILIKE substring on external_key). Both methods
# reconstruct an exact lookup from that surface.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_find_document_uses_search_and_matches_exactly() -> None:
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.params.get("search") == "confluence:page:42"
        assert "externalKey" not in request.url.params
        return httpx.Response(
            200,
            json={
                "items": [
                    {
                        "id": "00000000-0000-0000-0000-000000000420",
                        "libraryId": str(LIB),
                        "externalKey": "confluence:page:420",
                        "status": "ready",
                    },
                    {
                        "id": "00000000-0000-0000-0000-000000000042",
                        "libraryId": str(LIB),
                        "externalKey": "confluence:page:42",
                        "status": "ready",
                    },
                ],
                "nextCursor": None,
            },
        )

    client = _client(httpx.MockTransport(handle))
    found = await client.find_document(LIB, "confluence:page:42")
    await client.aclose()

    assert found is not None
    assert str(found.id) == "00000000-0000-0000-0000-000000000042"
    assert len(requests) == 1, "must resolve in a single request when the match is on page one"


@pytest.mark.asyncio
async def test_find_document_returns_none_when_absent() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"items": [], "nextCursor": None})

    client = _client(httpx.MockTransport(handle))
    found = await client.find_document(LIB, "confluence:page:99")
    await client.aclose()

    assert found is None


@pytest.mark.asyncio
async def test_list_documents_follows_cursor_pages() -> None:
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            assert request.url.params.get("cursor") is None
            return httpx.Response(
                200,
                json={
                    "items": [
                        {
                            "id": "00000000-0000-0000-0000-000000000001",
                            "libraryId": str(LIB),
                            "externalKey": "source:page:1",
                            "status": "ready",
                        }
                    ],
                    "nextCursor": "next-page",
                },
            )
        assert request.url.params.get("cursor") == "next-page"
        return httpx.Response(
            200,
            json={
                "items": [
                    {
                        "id": "00000000-0000-0000-0000-000000000002",
                        "libraryId": str(LIB),
                        "externalKey": "source:page:2",
                        "status": "ready",
                    }
                ],
                "nextCursor": None,
            },
        )

    client = _client(httpx.MockTransport(handle))
    documents = [doc async for doc in client.list_documents(LIB)]
    await client.aclose()

    assert [str(d.id)[-1] for d in documents] == ["1", "2"]
    assert len(requests) == 2


@pytest.mark.asyncio
async def test_list_documents_external_key_narrows_via_search_and_filters_exact() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        assert request.url.params.get("search") == "source:page:1"
        return httpx.Response(
            200,
            json={
                "items": [
                    {
                        "id": "00000000-0000-0000-0000-000000000010",
                        "libraryId": str(LIB),
                        "externalKey": "source:page:10",
                        "status": "ready",
                    },
                    {
                        "id": "00000000-0000-0000-0000-000000000001",
                        "libraryId": str(LIB),
                        "externalKey": "source:page:1",
                        "status": "ready",
                    },
                ],
                "nextCursor": None,
            },
        )

    client = _client(httpx.MockTransport(handle))
    documents = [
        doc async for doc in client.list_documents(LIB, external_key="source:page:1")
    ]
    await client.aclose()

    assert [d.external_key for d in documents] == ["source:page:1"]


@pytest.mark.asyncio
async def test_list_documents_rejects_search_and_external_key_together() -> None:
    client = _client(httpx.MockTransport(lambda request: httpx.Response(200, json={})))
    with pytest.raises(ValueError, match="either search or external_key"):
        async for _ in client.list_documents(LIB, search="a", external_key="b"):
            pass
    await client.aclose()


# ---------------------------------------------------------------------------
# walk_all_documents -- the rate-limited/resumable re-walk primitive (S7.6).
# ---------------------------------------------------------------------------


class _RecordingCheckpointStore:
    def __init__(self, initial: str | None = None) -> None:
        self.saved: list[str | None] = []
        self._cursor = initial

    def load_cursor(self) -> str | None:
        return self._cursor

    def save_cursor(self, cursor: str | None) -> None:
        self._cursor = cursor
        self.saved.append(cursor)


@pytest.mark.asyncio
async def test_walk_all_documents_saves_checkpoint_after_every_page() -> None:
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(
                200,
                json={
                    "items": [
                        {
                            "id": "00000000-0000-0000-0000-000000000001",
                            "libraryId": str(LIB),
                            "externalKey": "source:page:1",
                            "status": "ready",
                        }
                    ],
                    "nextCursor": "page-2",
                },
            )
        return httpx.Response(
            200,
            json={
                "items": [
                    {
                        "id": "00000000-0000-0000-0000-000000000002",
                        "libraryId": str(LIB),
                        "externalKey": "source:page:2",
                        "status": "ready",
                    }
                ],
                "nextCursor": None,
            },
        )

    store = _RecordingCheckpointStore()
    client = _client(httpx.MockTransport(handle))
    documents = [doc async for doc in client.walk_all_documents(LIB, checkpoint_store=store)]
    await client.aclose()

    assert [d.external_key for d in documents] == ["source:page:1", "source:page:2"]
    assert store.saved == ["page-2", None]


@pytest.mark.asyncio
async def test_walk_all_documents_resumes_from_checkpoint() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        assert request.url.params.get("cursor") == "resume-here"
        return httpx.Response(200, json={"items": [], "nextCursor": None})

    store = _RecordingCheckpointStore(initial="resume-here")
    client = _client(httpx.MockTransport(handle))
    documents = [doc async for doc in client.walk_all_documents(LIB, checkpoint_store=store)]
    await client.aclose()

    assert documents == []


@pytest.mark.asyncio
async def test_walk_all_documents_ignores_checkpoint_when_resume_disabled() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        assert request.url.params.get("cursor") is None
        return httpx.Response(200, json={"items": [], "nextCursor": None})

    store = _RecordingCheckpointStore(initial="should-be-ignored")
    client = _client(httpx.MockTransport(handle))
    documents = [
        doc
        async for doc in client.walk_all_documents(
            LIB, checkpoint_store=store, resume_from_checkpoint=False
        )
    ]
    await client.aclose()

    assert documents == []


@pytest.mark.asyncio
async def test_walk_all_documents_rejects_non_positive_concurrency() -> None:
    client = _client(httpx.MockTransport(lambda request: httpx.Response(200, json={})))
    with pytest.raises(ValueError, match="concurrency must be >= 1"):
        async for _ in client.walk_all_documents(LIB, concurrency=0):
            pass
    await client.aclose()


# ---------------------------------------------------------------------------
# get_document
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_document_unwraps_document_envelope() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        assert request.url.path == f"/v1/content/documents/{DOC}"
        return httpx.Response(
            200,
            json={
                "document": {
                    "id": str(DOC),
                    "libraryId": str(LIB),
                    "externalKey": "source:page:1",
                    "documentState": "active",
                },
                "fileName": "page.md",
            },
        )

    client = _client(httpx.MockTransport(handle))
    document = await client.get_document(DOC)
    await client.aclose()

    assert document is not None
    assert document.id == DOC
    assert document.external_key == "source:page:1"


@pytest.mark.asyncio
async def test_get_document_returns_none_on_404() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json=_problem("not_found", status=404))

    client = _client(httpx.MockTransport(handle))
    document = await client.get_document(DOC)
    await client.aclose()

    assert document is None


# ---------------------------------------------------------------------------
# create_document -- content-negotiated JSON vs multipart.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_document_json_path_sends_camel_case_body() -> None:
    bodies: list[bytes] = []

    def handle(request: httpx.Request) -> httpx.Response:
        assert request.url.path == f"/v1/content/libraries/{LIB}/documents"
        assert request.headers["content-type"].startswith("application/json")
        bodies.append(request.content)
        return httpx.Response(
            201,
            json={
                "document": {
                    "document": {
                        "id": str(DOC),
                        "libraryId": str(LIB),
                        "externalKey": "source:page:1",
                        "documentState": "active",
                    },
                },
                "mutation": {"mutation": {"id": str(OP)}, "items": []},
            },
            headers={"Location": f"/v1/content/documents/{DOC}"},
        )

    client = _client(httpx.MockTransport(handle))
    document = await client.create_document(
        LIB,
        external_key="source:page:1",
        title="Page",
        document_hint="https://example.invalid/page",
        parent_external_key="source:page:0",
    )
    await client.aclose()

    assert document.id == DOC
    body = bodies[0].decode()
    assert '"externalKey":"source:page:1"' in body
    assert '"documentHint"' in body
    assert '"parentExternalKey"' in body


@pytest.mark.asyncio
async def test_create_document_multipart_omits_redundant_library_id_field() -> None:
    """The library is identified by the path segment. IronRAG's multipart
    parser rejects anything outside its allow-list with 400 "unknown
    multipart field", so sending a redundant `library_id` form field makes
    every upload fail. Regression test for that contract."""
    bodies: list[bytes] = []

    def handle(request: httpx.Request) -> httpx.Response:
        assert request.headers["content-type"].startswith("multipart/form-data")
        bodies.append(request.content)
        return httpx.Response(
            201,
            json={
                "document": {
                    "document": {
                        "id": str(DOC),
                        "libraryId": str(LIB),
                        "externalKey": "source:image:1",
                        "documentState": "active",
                    },
                },
                "mutation": {"mutation": {"id": str(OP)}, "items": []},
            },
        )

    client = _client(httpx.MockTransport(handle))
    await client.create_document(
        LIB,
        external_key="source:image:1",
        file_bytes=b"\x89PNG synthetic",
        file_name="img.png",
        mime_type="image/png",
        parent_external_key="source:page:1",
    )
    await client.aclose()

    body = bodies[0]
    assert b'name="library_id"' not in body
    assert b'name="parent_external_key"' in body
    assert b"source:page:1" in body


@pytest.mark.asyncio
async def test_create_document_multipart_omits_parent_external_key_when_unset() -> None:
    bodies: list[bytes] = []

    def handle(request: httpx.Request) -> httpx.Response:
        bodies.append(request.content)
        return httpx.Response(
            201,
            json={
                "document": {
                    "document": {
                        "id": str(DOC),
                        "libraryId": str(LIB),
                        "externalKey": "source:page:1",
                        "documentState": "active",
                    },
                },
                "mutation": {"mutation": {"id": str(OP)}, "items": []},
            },
        )

    client = _client(httpx.MockTransport(handle))
    await client.create_document(
        LIB,
        external_key="source:page:1",
        file_bytes=b"page body",
        file_name="page.md",
        mime_type="text/markdown",
    )
    await client.aclose()

    assert b'name="parent_external_key"' not in bodies[0]


@pytest.mark.asyncio
async def test_create_document_raises_typed_duplicate_content_error() -> None:
    existing_id = str(DOC)

    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            409,
            json=_problem(
                "duplicate_content",
                status=409,
                existingDocumentId=existing_id,
            ),
        )

    client = _client(httpx.MockTransport(handle))
    with pytest.raises(IronRagDuplicateContentError) as excinfo:
        await client.create_document(
            LIB,
            external_key="source:page:1",
            file_bytes=b"page body",
            file_name="page.md",
            mime_type="text/markdown",
        )
    await client.aclose()

    assert excinfo.value.existing_document_id == DOC


@pytest.mark.asyncio
async def test_create_document_requires_file_metadata_when_bytes_given() -> None:
    client = _client(httpx.MockTransport(lambda request: httpx.Response(201, json={})))
    with pytest.raises(ValueError, match="file_name and mime_type"):
        await client.create_document(LIB, external_key="k", file_bytes=b"x")
    await client.aclose()


# ---------------------------------------------------------------------------
# create_revision -- always async (202 + Location), content-negotiated.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_revision_append_json_body() -> None:
    bodies: list[bytes] = []

    def handle(request: httpx.Request) -> httpx.Response:
        assert request.url.path == f"/v1/content/documents/{DOC}/revisions"
        bodies.append(request.content)
        return httpx.Response(
            202,
            json={"mutation": {"id": str(OP)}, "items": [], "asyncOperationId": str(OP)},
            headers={"Location": f"/v1/ops/operations/{OP}"},
        )

    client = _client(httpx.MockTransport(handle))
    handle_result = await client.create_revision(
        DOC, mode="append", appended_text="more text", idempotency_key="idem-1"
    )
    await client.aclose()

    assert handle_result.operation_id == OP
    body = bodies[0].decode()
    assert '"mode":"append"' in body
    assert '"appendedText":"more text"' in body
    assert '"idempotencyKey":"idem-1"' in body


@pytest.mark.asyncio
async def test_create_revision_replace_multipart_file() -> None:
    bodies: list[bytes] = []

    def handle(request: httpx.Request) -> httpx.Response:
        assert request.headers["content-type"].startswith("multipart/form-data")
        bodies.append(request.content)
        return httpx.Response(202, json={"asyncOperationId": str(OP)})

    client = _client(httpx.MockTransport(handle))
    handle_result = await client.create_revision(
        DOC,
        mode="replace",
        file_bytes=b"new bytes",
        file_name="page.md",
        mime_type="text/markdown",
        idempotency_key="idem-2",
    )
    await client.aclose()

    assert handle_result.operation_id == OP
    assert b'name="idempotency_key"' in bodies[0]


@pytest.mark.asyncio
async def test_create_revision_replace_requires_markdown_without_file() -> None:
    client = _client(httpx.MockTransport(lambda request: httpx.Response(202, json={})))
    with pytest.raises(ValueError, match="markdown is required"):
        await client.create_revision(DOC, mode="replace", idempotency_key="k")
    await client.aclose()


@pytest.mark.asyncio
async def test_create_revision_raises_conflict_error_on_409() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(409, json=_problem("conflicting_mutation", status=409))

    client = _client(httpx.MockTransport(handle))
    with pytest.raises(IronRagConflictError):
        await client.create_revision(
            DOC, mode="append", appended_text="x", idempotency_key="k"
        )
    await client.aclose()


@pytest.mark.asyncio
async def test_create_revision_raises_not_found_on_404() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json=_problem("not_found", status=404))

    client = _client(httpx.MockTransport(handle))
    with pytest.raises(IronRagNotFoundError):
        await client.create_revision(
            DOC, mode="append", appended_text="x", idempotency_key="k"
        )
    await client.aclose()


# ---------------------------------------------------------------------------
# delete_document -- 200 (not 202) with the operation id only in the body.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_document_returns_handle_from_200_body() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        assert request.method == "DELETE"
        assert request.url.path == f"/v1/content/documents/{DOC}"
        assert request.headers["idempotency-key"] == "del-1"
        return httpx.Response(200, json={"mutation": {"id": "m1"}, "asyncOperationId": str(OP)})

    client = _client(httpx.MockTransport(handle))
    handle_result = await client.delete_document(DOC, idempotency_key="del-1")
    await client.aclose()

    assert handle_result is not None
    assert handle_result.operation_id == OP


@pytest.mark.asyncio
async def test_delete_document_returns_none_on_404() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json=_problem("not_found", status=404))

    client = _client(httpx.MockTransport(handle))
    result = await client.delete_document(DOC, idempotency_key="del-2")
    await client.aclose()

    assert result is None


# ---------------------------------------------------------------------------
# get_operation / wait_for_operation -- flattened AsyncOperationDetailResponse.
# ---------------------------------------------------------------------------


def _operation_body(status: str, **overrides: object) -> dict[str, object]:
    body: dict[str, object] = {
        "id": str(OP),
        "workspaceId": str(WS),
        "libraryId": str(LIB),
        "operationKind": "revise_document",
        "status": status,
        "createdAt": "2026-07-17T00:00:00Z",
        "progress": {"total": 0, "completed": 0, "failed": 0, "inFlight": 0},
    }
    body.update(overrides)
    return body


@pytest.mark.asyncio
async def test_get_operation_parses_flattened_response_not_nested() -> None:
    """`AsyncOperationDetailResponse` applies `#[serde(flatten)]` to the
    operation row -- its fields sit at the top level alongside `progress`,
    not nested under an `"operation"` key. Regression test for that shape."""

    def handle(request: httpx.Request) -> httpx.Response:
        assert request.url.path == f"/v1/ops/operations/{OP}"
        return httpx.Response(200, json=_operation_body("ready"))

    client = _client(httpx.MockTransport(handle))
    status = await client.get_operation(OP)
    await client.aclose()

    assert status.id == OP
    assert status.status is OperationStatusValue.READY
    assert status.progress.total == 0


@pytest.mark.asyncio
async def test_wait_for_operation_polls_until_terminal() -> None:
    responses = iter(["accepted", "processing", "ready"])

    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_operation_body(next(responses)))

    client = _client(httpx.MockTransport(handle))
    status = await client.wait_for_operation(OP)
    await client.aclose()

    assert status.status is OperationStatusValue.READY


@pytest.mark.asyncio
async def test_wait_for_operation_raises_on_failed_status() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_operation_body("failed", failureCode="boom"))

    client = _client(httpx.MockTransport(handle))
    with pytest.raises(IronRagOperationFailedError, match="boom"):
        await client.wait_for_operation(OP)
    await client.aclose()


@pytest.mark.asyncio
async def test_wait_for_operation_raises_timeout_when_never_terminal() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_operation_body("processing"))

    client = _client(httpx.MockTransport(handle))
    with pytest.raises(IronRagMutationTimeoutError):
        await client.wait_for_operation(OP, poll_interval=0.001, budget=0.01)
    await client.aclose()


# ---------------------------------------------------------------------------
# Error mapping generic cases.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generic_conflict_is_not_duplicate_content() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(409, json=_problem("stale_revision", status=409))

    client = _client(httpx.MockTransport(handle))
    with pytest.raises(IronRagConflictError):
        await client.create_revision(
            DOC, mode="append", appended_text="x", idempotency_key="k"
        )
    await client.aclose()


@pytest.mark.asyncio
async def test_unparseable_error_body_falls_back_to_generic_problem() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, content=b"not json")

    client = _client(httpx.MockTransport(handle))
    with pytest.raises(IronRagError):
        await client.get_operation(OP)
    await client.aclose()
