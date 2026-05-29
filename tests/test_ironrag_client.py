from __future__ import annotations

from uuid import UUID

import httpx
import pytest

from ironrag_connector.config import BaseConnectorSettings
from ironrag_connector.ironrag import IronRagClient

LIB = UUID("00000000-0000-0000-0000-000000000000")


def _settings() -> BaseConnectorSettings:
    return BaseConnectorSettings(
        ironrag_base_url="http://ironrag.example.com",
        ironrag_api_token="token",
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
