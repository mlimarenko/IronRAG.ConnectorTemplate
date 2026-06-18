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
from collections.abc import Mapping
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
        """Find a document by its exact external key.

        IronRAG's list endpoint has no exact ``externalKey`` filter, but it
        does expose ``search`` (a case-insensitive ``ILIKE`` on
        ``external_key`` backed by a pg_trgm index). We pass the external
        key as ``search`` to narrow the server-side result to the handful
        of substring matches in a single request, then compare
        ``externalKey`` exactly client-side (``search`` is a substring
        match, so ``confluence:page:42`` would also match
        ``confluence:page:420``). If the backend rejects ``search`` we fall
        back to a full cursor walk and match client-side.

        This replaces the previous full-library pagination (~one page per
        200 docs, for *every* lookup) that dominated request volume against
        large libraries.
        """
        cursor: str | None = None
        page_size = 200
        use_search = True
        while True:
            params: dict[str, Any] = {
                "libraryId": str(library_id),
                "limit": page_size,
            }
            if use_search:
                params["search"] = external_key
            if cursor:
                params["cursor"] = cursor

            response = await self._client.get("/v1/content/documents", params=params)
            if (
                response.status_code in (400, 422)
                and use_search
                and cursor is None
            ):
                # Backend does not understand the search filter — retry the
                # whole lookup as a plain paginated scan.
                use_search = False
                continue
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
        parent_external_key: str | None = None,
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
        if parent_external_key is not None:
            # Declares the source-side parent so IronRAG marks this document
            # as attached context of (or an attachment to) that parent. The
            # backend derives document_role from the declared parent + this
            # revision's media class; the connector sends no role itself.
            data["parent_external_key"] = parent_external_key
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
        return _json_object(response)

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
        return _json_object(response)

    async def get_document(self, document_id: UUID | str) -> dict[str, Any] | None:
        response = await self._client.get(f"/v1/content/documents/{document_id}")
        if response.status_code in (404, 410):
            return None
        if response.status_code >= 400:
            raise IronRagError(
                f"IronRAG get document → {response.status_code}: {response.text[:400]}"
            )
        payload = _json_object(response)
        document = payload.get("document") or payload
        if not isinstance(document, dict):
            raise IronRagError("IronRAG document response was not a JSON object")
        return document

    async def list_documents_by_external_key_prefix(
        self,
        library_id: UUID,
        prefix: str,
        *,
        page_size: int = 200,
    ) -> list[tuple[str, str]]:
        results: list[tuple[str, str]] = []
        cursor: str | None = None
        offset = 0
        use_offset = False
        server_filter_supported: bool | None = None

        while True:
            params: dict[str, str | int] = {
                "libraryId": str(library_id),
                "limit": page_size,
            }
            if cursor:
                params["cursor"] = cursor
            elif use_offset or offset:
                params["offset"] = offset
            if server_filter_supported is not False:
                params["externalKeyPrefix"] = prefix

            response = await self._client.get("/v1/content/documents", params=params)

            if response.status_code in (400, 422) and server_filter_supported is None:
                server_filter_supported = False
                cursor = None
                offset = 0
                use_offset = False
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

            next_cursor = payload.get("nextCursor") or payload.get("next_cursor")
            if next_cursor:
                cursor = str(next_cursor)
                use_offset = False
                continue

            total = _optional_int(payload.get("total"))
            if total is not None:
                offset += len(items)
                if not items or offset >= total:
                    break
                cursor = None
                use_offset = True
                continue

            if not items:
                break
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


def _optional_int(value: object) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _json_object(response: httpx.Response) -> dict[str, Any]:
    payload = response.json()
    if not isinstance(payload, dict):
        raise IronRagError("IronRAG response was not a JSON object")
    return payload


def document_library_id(document: Mapping[str, Any]) -> str | None:
    for key in ("libraryId", "library_id", "libraryID"):
        value = document.get(key)
        if value:
            return str(value)
    library = document.get("library")
    if isinstance(library, Mapping):
        value = library.get("id")
        if value:
            return str(value)
    return None
