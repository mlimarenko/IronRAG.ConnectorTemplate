"""Async IronRAG content-mutation client.

Endpoints
=========

The connector talks to four IronRAG endpoints:

* ``GET /v1/content/documents?libraryId=&externalKey=`` — find an existing
  document by the canonical external key the adapter coined.
* ``POST /v1/content/documents/upload`` (multipart) — create a new document.
* ``POST /v1/content/documents/{id}/replace`` (multipart) — replace bytes
  on an existing document.
* ``DELETE /v1/content/documents/{id}`` — soft-delete a document.

Workspace and library are passed per call, so a single connector instance
can drive many IronRAG libraries from one process.

Duplicate-content handling
==========================

When IronRAG returns 409 with ``errorKind == "conflict"`` and an error
message starting with ``"conflict: duplicate content"``, ``upload_document``
returns a sentinel dict instead of raising::

    {"document": {"id": "<existing-uuid-or-None>"}, "duplicate_of_existing": True}

The orchestrator detects ``duplicate_of_existing=True`` and applies the
``on_duplicate_content`` policy for the item's ``kind`` (skip vs fail).
"""

from __future__ import annotations

import re
from typing import Any
from uuid import UUID

import httpx

from .config import BaseConnectorSettings
from .observability import get_logger

log = get_logger(__name__)

_EXISTING_DOC_RE = re.compile(
    r"document\s+([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
    re.IGNORECASE,
)


class IronRagError(RuntimeError):
    """IronRAG returned a non-recoverable status."""


class IronRagClient:
    def __init__(
        self,
        settings: BaseConnectorSettings,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._settings = settings
        self._owns_client = client is None
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

    async def find_document_by_external_key(
        self, library_id: UUID, external_key: str
    ) -> dict[str, Any] | None:
        """Cursor-paginate ``/v1/content/documents`` and match externalKey
        client-side. IronRAG ≥ 0.4.11 exposes ``externalKey`` on every
        list item, so a single pass is enough — server-side filter is
        not yet wired so we walk pages and stop on first match."""
        cursor: str | None = None
        page_size = 200
        while True:
            params: dict[str, Any] = {
                "libraryId": str(library_id),
                "limit": page_size,
            }
            if cursor:
                params["cursor"] = cursor

            response = await self._client.get("/v1/content/documents", params=params)
            if response.status_code == 404:
                return None
            if response.status_code >= 400:
                raise IronRagError(
                    f"IronRAG list documents → {response.status_code}: "
                    f"{response.text[:400]}"
                )

            payload = response.json()
            items: list[dict[str, Any]] = payload.get(
                "items", payload.get("documents", [])
            )
            for item in items:
                if item.get("externalKey") == external_key:
                    return item

            cursor = payload.get("nextCursor") or payload.get("next_cursor")
            if not cursor or not items:
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
    ) -> dict[str, Any]:
        files = {"file": (file_name, file_bytes, mime_type)}
        data: dict[str, Any] = {
            "library_id": str(library_id),
            "external_key": external_key,
            "idempotency_key": idempotency_key,
        }
        if title:
            data["title"] = title
        if document_hint is not None:
            data["document_hint"] = document_hint
        response = await self._client.post(
            "/v1/content/documents/upload", data=data, files=files
        )

        if response.status_code == 409:
            try:
                body = response.json()
            except Exception:
                body = {}
            error_kind = body.get("errorKind", "")
            error_msg = body.get("error", "") or body.get("message", "") or ""
            if error_kind == "conflict" and "duplicate content" in error_msg.lower():
                match = _EXISTING_DOC_RE.search(error_msg)
                existing_id: str | None = match.group(1) if match else None
                log.debug(
                    "ironrag.upload.duplicate_content",
                    external_key=external_key,
                    existing_id=existing_id,
                )
                return {
                    "document": {"id": existing_id},
                    "duplicate_of_existing": True,
                }

        if response.status_code >= 400:
            raise IronRagError(
                f"IronRAG upload → {response.status_code}: {response.text[:400]}"
            )
        return response.json()

    async def replace_document(
        self,
        *,
        document_id: UUID | str,
        file_bytes: bytes,
        file_name: str,
        mime_type: str,
        idempotency_key: str,
        document_hint: str | None = None,
    ) -> dict[str, Any] | None:
        """Replace document bytes. Returns ``None`` if IronRAG reports
        the document no longer exists (404/410) — caller must invalidate
        its cursor and retry as upload."""
        files = {"file": (file_name, file_bytes, mime_type)}
        data = {"idempotency_key": idempotency_key}
        if document_hint is not None:
            data["document_hint"] = document_hint
        response = await self._client.post(
            f"/v1/content/documents/{document_id}/replace", data=data, files=files
        )
        if response.status_code in (404, 410):
            return None
        if response.status_code >= 400:
            raise IronRagError(
                f"IronRAG replace → {response.status_code}: {response.text[:400]}"
            )
        return response.json()

    async def list_documents_by_external_key_prefix(
        self,
        library_id: UUID,
        prefix: str,
        *,
        page_size: int = 200,
    ) -> list[tuple[str, str]]:
        results: list[tuple[str, str]] = []
        offset = 0
        server_filter_supported: bool | None = None

        while True:
            params: dict[str, str | int] = {
                "libraryId": str(library_id),
                "limit": page_size,
                "offset": offset,
            }
            if server_filter_supported is not False:
                params["externalKeyPrefix"] = prefix

            response = await self._client.get("/v1/content/documents", params=params)

            if response.status_code in (400, 422) and server_filter_supported is None:
                server_filter_supported = False
                continue

            if server_filter_supported is not False:
                server_filter_supported = True

            if response.status_code == 404:
                break
            if response.status_code >= 400:
                raise IronRagError(
                    f"IronRAG list documents → {response.status_code}: "
                    f"{response.text[:400]}"
                )

            payload = response.json()
            items: list[dict[str, Any]] = payload.get(
                "documents", payload.get("data", payload.get("items", []))
            )
            for item in items:
                key = item.get("externalKey") or ""
                doc_id = item.get("id")
                if key.startswith(prefix) and doc_id:
                    results.append((key, str(doc_id)))

            total = payload.get("total", offset + len(items))
            offset += page_size
            if offset >= total or not items:
                break

        return results

    async def delete_document(
        self, document_id: UUID | str, idempotency_key: str
    ) -> None:
        response = await self._client.request(
            "DELETE",
            f"/v1/content/documents/{document_id}",
            headers={"Idempotency-Key": idempotency_key},
        )
        if response.status_code in (200, 202, 204, 404):
            return
        raise IronRagError(
            f"IronRAG delete → {response.status_code}: {response.text[:400]}"
        )
