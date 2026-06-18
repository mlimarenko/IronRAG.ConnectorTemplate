"""One-shot cursor seeder.

When you start a connector against an IronRAG library that already
contains documents owned by the same adapter (for example after a
manual bulk upload, or when migrating from another connector), the
SQLite cursor at ``STATE_DB_PATH`` is empty. The first sweep would
then attempt to ``upload`` every existing item and trip IronRAG's
``(library_id, external_key)`` unique constraint with a 500.

This module walks every IronRAG document under each routing-target
library, fetches its detail to recover the canonical ``external_key``,
asks the adapter to parse it back into ``(kind, item_id)``, and writes
a cursor row for each match. After it finishes, the periodic sweep can
short-circuit known items via the cursor instead of relying on the
list endpoint (which on some IronRAG deployments does not expose
``external_key`` at all).

Connectors expose this as a ``__main__``-compatible entry point:

    from ironrag_connector import seed_cursor

    def main() -> None:
        settings = MyConnectorSettings()
        adapter = MyAdapter(settings)
        seed_cursor(settings, adapter)

then run once with ``uv run python -m my_connector.seed`` (or whatever
script entry the connector ships).
"""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID

import httpx

from .config import BaseConnectorSettings
from .observability import configure_logging, get_logger
from .routing import Router, load_routing_config
from .source import SourceAdapter
from .state import StateStore

log = get_logger(__name__)


async def _seed_async(
    settings: BaseConnectorSettings, adapter: SourceAdapter
) -> dict[str, int]:
    router = Router(load_routing_config(settings.routing_config_path))
    state = StateStore(settings.state_db_path)
    counts: dict[str, int] = {"libraries": 0, "docs_scanned": 0, "rows_inserted": 0}

    async with httpx.AsyncClient(
        base_url=settings.ironrag_base_url.rstrip("/"),
        timeout=settings.request_timeout_seconds,
        headers={"Authorization": f"Bearer {settings.ironrag_api_token}"},
    ) as client:
        for library_id in router.target_libraries():
            counts["libraries"] += 1
            log.info("seed.library.start", library_id=str(library_id))
            await _seed_library(library_id, client, adapter, state, counts)
            log.info(
                "seed.library.done",
                library_id=str(library_id),
                rows_inserted=counts["rows_inserted"],
            )

    state.close()
    return counts


async def _seed_library(
    library_id: UUID,
    client: httpx.AsyncClient,
    adapter: SourceAdapter,
    state: StateStore,
    counts: dict[str, int],
) -> None:
    cursor: str | None = None
    page_size = 200
    while True:
        params: dict[str, str | int] = {
            "libraryId": str(library_id),
            "limit": page_size,
        }
        if cursor:
            params["cursor"] = cursor
        else:
            params["offset"] = 0

        resp = await client.get("/v1/content/documents", params=params)
        if resp.status_code >= 400:
            log.warning(
                "seed.list_error",
                library_id=str(library_id),
                status=resp.status_code,
                body=resp.text[:300],
            )
            return
        payload = resp.json()
        items: list[dict[str, Any]] = (
            payload.get("items") or payload.get("documents") or []
        )
        if not items:
            return

        for item in items:
            counts["docs_scanned"] += 1
            doc_id = item.get("id")
            if not doc_id:
                continue
            detail = await client.get(f"/v1/content/documents/{doc_id}")
            if detail.status_code >= 400:
                continue
            body = detail.json()
            doc = body.get("document") or body
            external_key = doc.get("external_key") or doc.get("externalKey")
            if not external_key:
                continue
            parsed = adapter.parse_external_key(external_key)
            if parsed is None:
                continue
            kind, item_id = parsed
            state.upsert(
                kind=kind,
                item_id=item_id,
                change_token=None,
                external_key=external_key,
                ironrag_document_id=str(doc_id),
                ironrag_library_id=str(library_id),
            )
            counts["rows_inserted"] += 1

        cursor = payload.get("nextCursor") or payload.get("next_cursor")
        if not cursor:
            return


def seed_cursor(
    settings: BaseConnectorSettings, adapter: SourceAdapter
) -> dict[str, int]:
    """Synchronous wrapper for use from a connector's ``main`` script.

    Returns a counts dict: ``libraries``, ``docs_scanned``,
    ``rows_inserted``. The cursor's ``change_token`` is left ``None`` for
    every seeded row — the next sweep will pick up the live token from
    the source and decide whether to replace.
    """
    configure_logging(settings.log_level)
    return asyncio.run(_seed_async(settings, adapter))
