from __future__ import annotations

from uuid import UUID

import httpx
import pytest

from ironrag_connector.config import BaseConnectorSettings
from ironrag_connector.ironrag import (
    IronRagClient,
    IronRagMutationTimeoutError,
    document_library_id,
)

LIB = UUID("00000000-0000-0000-0000-000000000000")


def _settings() -> BaseConnectorSettings:
    return BaseConnectorSettings(
        ironrag_base_url="http://ironrag.example.com",
        ironrag_api_token="token",
        request_timeout_seconds=60.0,
    )


def _client(handler: httpx.MockTransport) -> IronRagClient:
    return IronRagClient(
        _settings(),
        client=httpx.AsyncClient(
            base_url="http://ironrag.example.com",
            transport=handler,
        ),
    )


@pytest.mark.asyncio
async def test_find_uses_search_and_matches_exactly() -> None:
    """A single search-narrowed request resolves the exact external key,
    ignoring substring siblings (``...:42`` vs ``...:420``)."""
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.params.get("search") == "confluence:page:42"
        return httpx.Response(
            200,
            json={
                "items": [
                    {"id": "doc-420", "externalKey": "confluence:page:420"},
                    {"id": "doc-42", "externalKey": "confluence:page:42"},
                ],
                "nextCursor": None,
            },
        )

    client = _client(httpx.MockTransport(handle))
    found = await client.find_document_by_external_key(LIB, "confluence:page:42")
    await client.aclose()

    assert found is not None
    assert found["id"] == "doc-42"
    assert len(requests) == 1, "must resolve in a single request"


@pytest.mark.asyncio
async def test_find_falls_back_when_search_rejected() -> None:
    """If the backend rejects the search filter (400/422), the lookup
    retries as a plain paginated scan and still matches client-side."""
    seen_search_param: list[str | None] = []

    def handle(request: httpx.Request) -> httpx.Response:
        search = request.url.params.get("search")
        seen_search_param.append(search)
        if search is not None:
            return httpx.Response(422, json={"error": "search unsupported"})
        return httpx.Response(
            200,
            json={
                "items": [{"id": "doc-42", "externalKey": "confluence:page:42"}],
                "nextCursor": None,
            },
        )

    client = _client(httpx.MockTransport(handle))
    found = await client.find_document_by_external_key(LIB, "confluence:page:42")
    await client.aclose()

    assert found is not None
    assert found["id"] == "doc-42"
    assert seen_search_param == ["confluence:page:42", None]


@pytest.mark.asyncio
async def test_find_returns_none_when_absent() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"items": [], "nextCursor": None})

    client = _client(httpx.MockTransport(handle))
    found = await client.find_document_by_external_key(LIB, "confluence:page:99")
    await client.aclose()

    assert found is None


@pytest.mark.asyncio
async def test_get_document_unwraps_document_envelope_and_library_id() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/content/documents/doc-1"
        return httpx.Response(
            200,
            json={
                "document": {
                    "id": "doc-1",
                    "externalKey": "source:page:1",
                    "libraryId": str(LIB),
                }
            },
        )

    client = _client(httpx.MockTransport(handle))
    document = await client.get_document("doc-1")
    await client.aclose()

    assert document is not None
    assert document["id"] == "doc-1"
    assert document_library_id(document) == str(LIB)


@pytest.mark.asyncio
async def test_list_by_prefix_follows_cursor_pages() -> None:
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.params.get("externalKeyPrefix") == "source:page:"
        if len(requests) == 1:
            assert request.url.params.get("cursor") is None
            assert request.url.params.get("offset") is None
            return httpx.Response(
                200,
                json={
                    "items": [
                        {"id": "doc-1", "externalKey": "source:page:1"},
                        {"id": "foreign", "externalKey": "other:page:1"},
                    ],
                    "nextCursor": "next-page",
                },
            )
        assert request.url.params.get("cursor") == "next-page"
        assert request.url.params.get("offset") is None
        return httpx.Response(
            200,
            json={
                "items": [{"id": "doc-2", "externalKey": "source:page:2"}],
                "nextCursor": None,
            },
        )

    client = _client(httpx.MockTransport(handle))
    pairs = await client.list_documents_by_external_key_prefix(
        LIB, "source:page:", page_size=2
    )
    await client.aclose()

    assert pairs == [("source:page:1", "doc-1"), ("source:page:2", "doc-2")]
    assert len(requests) == 2


@pytest.mark.asyncio
async def test_list_by_prefix_falls_back_when_server_prefix_is_rejected() -> None:
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            assert request.url.params.get("externalKeyPrefix") == "source:page:"
            return httpx.Response(422, json={"error": "unsupported filter"})
        assert request.url.params.get("externalKeyPrefix") is None
        return httpx.Response(
            200,
            json={
                "items": [
                    {"id": "doc-1", "externalKey": "source:page:1"},
                    {"id": "foreign", "externalKey": "other:page:1"},
                ],
                "nextCursor": None,
            },
        )

    client = _client(httpx.MockTransport(handle))
    pairs = await client.list_documents_by_external_key_prefix(LIB, "source:page:")
    await client.aclose()

    assert pairs == [("source:page:1", "doc-1")]
    assert len(requests) == 2


@pytest.mark.asyncio
async def test_list_by_prefix_supports_legacy_offset_total_pages() -> None:
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        offset = request.url.params.get("offset")
        if offset is None:
            return httpx.Response(
                200,
                json={
                    "items": [
                        {"id": "doc-1", "externalKey": "source:page:1"},
                        {"id": "doc-2", "externalKey": "source:page:2"},
                    ],
                    "total": 3,
                },
            )
        assert offset == "2"
        return httpx.Response(
            200,
            json={
                "items": [{"id": "doc-3", "externalKey": "source:page:3"}],
                "total": 3,
            },
        )

    client = _client(httpx.MockTransport(handle))
    pairs = await client.list_documents_by_external_key_prefix(
        LIB, "source:page:", page_size=2
    )
    await client.aclose()

    assert pairs == [
        ("source:page:1", "doc-1"),
        ("source:page:2", "doc-2"),
        ("source:page:3", "doc-3"),
    ]
    assert len(requests) == 2


@pytest.mark.asyncio
async def test_upload_sends_parent_external_key_part() -> None:
    """When parent_external_key is set it ships as a multipart form part; when
    omitted the part is absent (so the backend leaves the doc role=primary)."""
    bodies: list[bytes] = []

    def handle(request: httpx.Request) -> httpx.Response:
        bodies.append(request.content)
        return httpx.Response(200, json={"document": {"id": "doc-1"}})

    client = _client(httpx.MockTransport(handle))
    await client.upload_document(
        library_id=LIB,
        external_key="source:image:1",
        file_bytes=b"\x89PNG synthetic",
        file_name="img.png",
        mime_type="image/png",
        title="Img",
        idempotency_key="k1",
        parent_external_key="source:page:1",
    )
    await client.upload_document(
        library_id=LIB,
        external_key="source:page:1",
        file_bytes=b"page body",
        file_name="page.md",
        mime_type="text/markdown",
        title="Page",
        idempotency_key="k2",
    )
    await client.aclose()

    assert b'name="parent_external_key"' in bodies[0]
    assert b"source:page:1" in bodies[0]
    assert b'name="parent_external_key"' not in bodies[1], (
        "primary upload must omit the field entirely"
    )


@pytest.mark.asyncio
async def test_upload_timeout_raises_mutation_timeout() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("synthetic timeout", request=request)

    client = _client(httpx.MockTransport(handle))

    with pytest.raises(IronRagMutationTimeoutError, match="upload admission timed out"):
        await client.upload_document(
            library_id=LIB,
            external_key="source:page:1",
            file_bytes=b"page body",
            file_name="page.md",
            mime_type="text/markdown",
            title="Page",
            idempotency_key="k1",
        )

    await client.aclose()


@pytest.mark.asyncio
async def test_replace_timeout_raises_mutation_timeout() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("synthetic timeout", request=request)

    client = _client(httpx.MockTransport(handle))

    with pytest.raises(IronRagMutationTimeoutError, match="replace admission timed out"):
        await client.replace_document(
            document_id="doc-1",
            file_bytes=b"page body",
            file_name="page.md",
            mime_type="text/markdown",
            idempotency_key="k1",
        )

    await client.aclose()


@pytest.mark.asyncio
async def test_delete_timeout_raises_mutation_timeout() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("synthetic timeout", request=request)

    client = _client(httpx.MockTransport(handle))

    with pytest.raises(IronRagMutationTimeoutError, match="delete admission timed out"):
        await client.delete_document("doc-1", "k1")

    await client.aclose()
