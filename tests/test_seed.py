from __future__ import annotations

from pathlib import Path
from uuid import UUID

import pytest
from pytest_httpx import HTTPXMock

from ironrag_connector.config import BaseConnectorSettings
from ironrag_connector.seed import _seed_async

WS = UUID("00000000-0000-0000-0000-000000000099")
LIB = UUID("00000000-0000-0000-0000-000000000000")


class SeedAdapter:
    name = "seed-test"
    kinds = ("page",)
    primary_kinds = ("page",)

    def parse_external_key(self, external_key: str) -> tuple[str, str] | None:
        return None

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_seed_resolves_friendly_ref_before_scanning_library(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    routing = tmp_path / "routing.yaml"
    routing.write_text(
        """
default:
  library: main/knowledge-base
rules:
  - match: {tag: reference}
    target: {library: main/knowledge-base}
""".strip(),
        encoding="utf-8",
    )
    settings = BaseConnectorSettings(
        ironrag_base_url="http://ironrag.example.com",
        ironrag_api_token="test-token",
        routing_config_path=routing,
        state_db_path=tmp_path / "state.sqlite",
    )
    httpx_mock.add_response(
        url="http://ironrag.example.com/v1/catalog/workspaces",
        json=[{"id": str(WS), "slug": "main", "displayName": "Main"}],
    )
    httpx_mock.add_response(
        url=f"http://ironrag.example.com/v1/catalog/workspaces/{WS}/libraries",
        json=[
            {
                "id": str(LIB),
                "workspaceId": str(WS),
                "slug": "knowledge-base",
                "displayName": "Knowledge base",
            }
        ],
    )
    httpx_mock.add_response(
        url=f"http://ironrag.example.com/v1/content/documents?libraryId={LIB}&limit=200&offset=0",
        json={"items": []},
    )

    counts = await _seed_async(settings, SeedAdapter())  # type: ignore[arg-type]

    assert counts == {"libraries": 1, "docs_scanned": 0, "rows_inserted": 0}
    requests = httpx_mock.get_requests()
    assert [request.url.path for request in requests] == [
        "/v1/catalog/workspaces",
        f"/v1/catalog/workspaces/{WS}/libraries",
        "/v1/content/documents",
    ]
    assert all(request.headers["Authorization"] == "Bearer test-token" for request in requests)
