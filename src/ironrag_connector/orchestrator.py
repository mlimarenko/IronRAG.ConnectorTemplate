"""Push one :class:`SourceItem` into IronRAG under the resolved policy.

The orchestrator is policy-aware but transport-agnostic: it does not
know about webhooks vs sweeps. Both code paths converge on
:meth:`Orchestrator.push_item` and receive the same outcome object.

Dispatch flow per item
======================

1. Look up an existing IronRAG document by ``ref.external_key`` in the
   target library.
2. Compare ``ref.change_token`` against the value stored in
   :class:`StateStore`. If equal AND a document exists, short-circuit
   to ``noop_unchanged``.
3. Choose action per policy:
   * No existing doc and ``on_new=create`` → ``upload_document``.
   * No existing doc and ``on_new=skip`` → ``skipped_new``.
   * Existing doc and ``on_changed=replace`` → ``replace_document``.
   * Existing doc and ``on_changed=skip`` → ``skipped_changed``.
4. On 409 ``duplicate content``: apply ``on_duplicate_content``.
5. Update the cursor with the new ``change_token`` and document id.
"""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass, field, replace
from typing import (
    Any,
    Literal,
)
from uuid import UUID, uuid4

import httpx

from .ironrag import IronRagClient, IronRagError, document_library_id
from .observability import get_logger
from .policy import (
    DeleteAction,
    DuplicateContentAction,
    PushPolicy,
    UpdateAction,
    UpsertAction,
)
from .routing import PolicyOverrides, ResolvedRoute, Router, RoutingError
from .source import SourceAdapter, SourceItem, SourceItemRef
from .state import CursorRow, StateStore

log = get_logger(__name__)

OutcomeAction = Literal[
    "created",
    "replaced",
    "deleted",
    "noop_unchanged",
    "skipped_new",
    "skipped_changed",
    "skipped_missing",
    "skipped_duplicate_content",
    "skipped_duplicate_fail",
    "unrouted",
    "fetch_returned_none",
]


@dataclass(frozen=True)
class OrchestrationOutcome:
    ref: SourceItemRef
    action: OutcomeAction
    workspace_id: UUID | None
    library_id: UUID | None
    rule_description: str | None
    ironrag_document_id: str | None
    detail: str
    dependent_outcomes: tuple[OrchestrationOutcome, ...] = field(default_factory=tuple)


def _default_idempotency_key(connector: str, item: SourceItem, op: str) -> str:
    """Compose an idempotency key scoped to the mutation type AND payload.

    IronRAG rejects 409 ``idempotency_conflict`` if the same key reaches
    the server twice carrying a *different* payload. Keying on a content
    hash of the bytes (rather than the source ``change_token``) makes the
    key identify the request precisely:

    * Identical bytes retried after a partial failure (timeout, proxy 502
      after the server already applied the write) reuse the same key and
      dedupe into a single mutation.
    * A payload that re-renders differently for the same logical version
      (e.g. Confluence ``export_view`` HTML is not byte-stable across
      fetches) gets a fresh key and applies cleanly instead of colliding
      with the stuck prior attempt.

    The ``op`` suffix keeps upload and replace in separate key spaces so a
    create and a later replace of the same item never collide.
    """
    digest = hashlib.sha256(item.payload).hexdigest()[:32]
    return f"{connector}:{op}:{item.ref.kind}:{item.ref.item_id}:{digest}"


class Orchestrator:
    def __init__(
        self,
        *,
        adapter: SourceAdapter,
        ironrag: IronRagClient,
        router: Router,
        state: StateStore,
        policies: PolicyOverrides,
        cursor_library_lookup_timeout_seconds: float = 5.0,
    ) -> None:
        self._adapter = adapter
        self._ironrag = ironrag
        self._router = router
        self._state = state
        self._policies = policies
        self._cursor_library_lookup_timeout = cursor_library_lookup_timeout_seconds
        # In-sweep dedup: collapses repeated push_item calls for the
        # same external_key down to one IronRAG mutation. Necessary
        # because dependents (images, attachments) can be reached from
        # multiple parents in one sweep — without this every shared
        # image triggers a "same idempotency key used for a different
        # mutation request" 409 from IronRAG.
        self._sweep_pushed: dict[tuple[UUID, str], str | None] = {}

    def reset_sweep_cache(self) -> None:
        self._sweep_pushed.clear()

    async def push_ref(self, ref: SourceItemRef) -> OrchestrationOutcome:
        """Resolve route, fetch payload, push under the kind's policy."""
        try:
            route = self._router.resolve(ref)
        except RoutingError as exc:
            log.warning(
                "orchestrator.unrouted",
                kind=ref.kind,
                item_id=ref.item_id,
                reason=str(exc),
            )
            return OrchestrationOutcome(
                ref=ref,
                action="unrouted",
                workspace_id=None,
                library_id=None,
                rule_description=None,
                ironrag_document_id=None,
                detail=str(exc),
            )

        policy = self._policies.for_kind(ref.kind)

        cursor = await self._resolve_cursor_library(
            self._state.get(ref.kind, ref.item_id)
        )
        if (
            cursor is not None
            and cursor.change_token is not None
            and ref.change_token is not None
            and cursor.change_token == ref.change_token
        ):
            # Cursor and source agree on change_token. If we know the doc
            # id from cursor, trust it (saves an HTTP call and dodges the
            # IronRAG list-endpoint externalKey gap). Otherwise fall back
            # to a server lookup to confirm the doc still exists.
            if cursor.ironrag_document_id and _cursor_matches_route(cursor, route):
                return OrchestrationOutcome(
                    ref=ref,
                    action="noop_unchanged",
                    workspace_id=route.workspace_id,
                    library_id=route.library_id,
                    rule_description=route.rule_description,
                    ironrag_document_id=cursor.ironrag_document_id,
                    detail=(
                        f"change_token unchanged ({ref.change_token}); "
                        "cursor knows document id"
                    ),
                )
            existing = await self._ironrag.find_document_by_external_key(
                route.library_id, ref.external_key
            )
            if existing is not None:
                doc_id = str(existing.get("id"))
                # Persist the discovered document id into the cursor so the
                # next sweep short-circuits on the cursor branch above with
                # zero HTTP. Without this the connector re-scans the library
                # list endpoint for every unchanged item on every sweep — the
                # dominant request volume on large libraries whose seed cursor
                # carried a change_token but no document id.
                self._state.upsert(
                    kind=ref.kind,
                    item_id=ref.item_id,
                    change_token=ref.change_token,
                    external_key=ref.external_key,
                    ironrag_document_id=doc_id,
                    ironrag_library_id=str(route.library_id),
                )
                return OrchestrationOutcome(
                    ref=ref,
                    action="noop_unchanged",
                    workspace_id=route.workspace_id,
                    library_id=route.library_id,
                    rule_description=route.rule_description,
                    ironrag_document_id=doc_id,
                    detail=(
                        f"change_token unchanged ({ref.change_token}); "
                        "server confirms document present"
                    ),
                )
            if _cursor_ownership_unknown(cursor):
                raise IronRagError(
                    "legacy cursor has a document id, but IronRAG did not expose "
                    f"its owning library for {ref.external_key}; refusing to upload "
                    "a possible duplicate"
                )

        item = await self._adapter.fetch(ref)
        if item is None:
            log.info(
                "orchestrator.fetch_returned_none",
                kind=ref.kind,
                item_id=ref.item_id,
            )
            return OrchestrationOutcome(
                ref=ref,
                action="fetch_returned_none",
                workspace_id=route.workspace_id,
                library_id=route.library_id,
                rule_description=route.rule_description,
                ironrag_document_id=None,
                detail="adapter.fetch returned None",
            )

        primary = await self.push_item(item, route, policy)

        # Single injection point for parent linkage: every dependent
        # (a page's attachments and inline images) is uploaded declaring its
        # source item as parent via parent_external_key. The orchestrator
        # already knows the parent ref here, so no SourceItem.parent field is
        # needed — this is the one source of truth for the link. confluence,
        # bookstack, and gitrepos inherit correct parentage with no adapter
        # change because they all emit attachments/images via
        # SourceItem.dependents. The backend derives document_role
        # (attached_context for image media, attachment otherwise) from the
        # declared parent; primary items pass no parent and stay role=primary.
        dep_outcomes: list[OrchestrationOutcome] = []
        for dep in item.dependents:
            dep_policy = self._policies.for_kind(dep.ref.kind)
            try:
                dep_route = self._router.resolve(dep.ref)
            except RoutingError:
                dep_route = route  # inherit parent's route if dependent unrouted
            dep_outcomes.append(
                await self.push_item(
                    dep,
                    dep_route,
                    dep_policy,
                    parent_external_key=item.ref.external_key,
                )
            )

        return OrchestrationOutcome(
            ref=primary.ref,
            action=primary.action,
            workspace_id=primary.workspace_id,
            library_id=primary.library_id,
            rule_description=primary.rule_description,
            ironrag_document_id=primary.ironrag_document_id,
            detail=primary.detail,
            dependent_outcomes=tuple(dep_outcomes),
        )

    async def push_item(
        self,
        item: SourceItem,
        route: ResolvedRoute,
        policy: PushPolicy,
        parent_external_key: str | None = None,
    ) -> OrchestrationOutcome:
        cache_key = (route.library_id, item.ref.external_key)
        if cache_key in self._sweep_pushed:
            cached_doc_id = self._sweep_pushed[cache_key]
            return _outcome(
                item.ref,
                route,
                "noop_unchanged",
                cached_doc_id,
                "already pushed this external_key earlier in the same sweep",
            )

        # Separate idempotency keys per mutation operation: IronRAG
        # rejects 409 if the same key reaches both upload and replace.
        base_key = item.idempotency_key
        upload_key = (
            f"{base_key}:upload" if base_key
            else _default_idempotency_key(self._adapter.name, item, "upload")
        )
        replace_key = (
            f"{base_key}:replace" if base_key
            else _default_idempotency_key(self._adapter.name, item, "replace")
        )

        # Cursor wins over server-side find. The persistent SQLite cursor is
        # the source of truth for "do we own a document for this external_key
        # already?" — the IronRAG list endpoint does not expose externalKey on
        # every deployment, so trusting find blindly would let us re-upload an
        # existing doc and trigger a unique-violation 500.
        cursor = await self._resolve_cursor_library(
            self._state.get(item.ref.kind, item.ref.item_id)
        )
        if (
            cursor is not None
            and cursor.ironrag_document_id
            and _cursor_matches_route(cursor, route)
        ):
            existing: dict[str, Any] | None = {"id": cursor.ironrag_document_id}
        else:
            existing = await self._ironrag.find_document_by_external_key(
                route.library_id, item.ref.external_key
            )

        if existing is None:
            if _cursor_ownership_unknown(cursor):
                raise IronRagError(
                    "legacy cursor has a document id, but IronRAG did not expose "
                    f"its owning library for {item.ref.external_key}; refusing to "
                    "upload a possible duplicate"
                )
            if policy.on_new is UpsertAction.SKIP:
                return _outcome(item.ref, route, "skipped_new", None, "policy on_new=skip")
            try:
                result = await self._ironrag.upload_document(
                    library_id=route.library_id,
                    external_key=item.ref.external_key,
                    file_bytes=item.payload,
                    file_name=item.file_name,
                    mime_type=item.mime_type,
                    title=item.title,
                    document_hint=item.document_hint,
                    idempotency_key=upload_key,
                    parent_external_key=parent_external_key,
                )
            except IronRagError as exc:
                log.warning(
                    "orchestrator.upload_failed",
                    external_key=item.ref.external_key,
                    error=str(exc),
                )
                raise

            if result.get("duplicate_of_existing"):
                doc = result.get("document") or {}
                existing_id = str(doc.get("id")) if doc.get("id") else None
                if policy.on_duplicate_content is DuplicateContentAction.FAIL:
                    raise IronRagError(
                        f"duplicate content for {item.ref.external_key}; existing "
                        f"document {existing_id}"
                    )
                self._state.upsert(
                    kind=item.ref.kind,
                    item_id=item.ref.item_id,
                    change_token=item.ref.change_token,
                    external_key=item.ref.external_key,
                    ironrag_document_id=existing_id,
                    ironrag_library_id=str(route.library_id),
                )
                self._sweep_pushed[cache_key] = existing_id
                return _outcome(
                    item.ref,
                    route,
                    "skipped_duplicate_content",
                    existing_id,
                    f"identical bytes already in library ({len(item.payload)} bytes)",
                )

            document = result.get("document") or result
            new_id = (
                str(document.get("id"))
                if isinstance(document, dict) and document.get("id")
                else None
            )
            self._state.upsert(
                kind=item.ref.kind,
                item_id=item.ref.item_id,
                change_token=item.ref.change_token,
                external_key=item.ref.external_key,
                ironrag_document_id=new_id,
                ironrag_library_id=str(route.library_id),
            )
            self._sweep_pushed[cache_key] = new_id
            return _outcome(
                item.ref,
                route,
                "created",
                new_id,
                f"created {len(item.payload)} bytes ({item.mime_type})",
            )

        if policy.on_changed is UpdateAction.SKIP:
            self._state.upsert(
                kind=item.ref.kind,
                item_id=item.ref.item_id,
                change_token=item.ref.change_token,
                external_key=item.ref.external_key,
                ironrag_document_id=str(existing.get("id")),
                ironrag_library_id=str(route.library_id),
            )
            self._sweep_pushed[cache_key] = str(existing.get("id"))
            return _outcome(
                item.ref,
                route,
                "skipped_changed",
                str(existing.get("id")),
                "policy on_changed=skip",
            )

        document_id = existing["id"]
        try:
            replace_result = await self._ironrag.replace_document(
                document_id=document_id,
                file_bytes=item.payload,
                file_name=item.file_name,
                mime_type=item.mime_type,
                idempotency_key=replace_key,
                document_hint=item.document_hint,
            )
        except IronRagError as exc:
            if _is_conflicting_mutation(exc):
                log.info(
                    "orchestrator.replace_deferred",
                    external_key=item.ref.external_key,
                    document_id=str(document_id),
                    reason="conflicting_mutation",
                )
                self._sweep_pushed[cache_key] = str(document_id)
                return _outcome(
                    item.ref,
                    route,
                    "skipped_changed",
                    str(document_id),
                    "document has a pending mutation; retry next sweep",
                )
            raise
        if replace_result is None:
            # Cursor pointed at a doc IronRAG no longer has (manual delete,
            # library reset). Invalidate the cursor and fall back to upload
            # so the next sweep is consistent.
            log.warning(
                "orchestrator.replace_404_invalidates_cursor",
                external_key=item.ref.external_key,
                stale_document_id=str(document_id),
            )
            self._state.delete(item.ref.kind, item.ref.item_id)
            upload_result = await self._ironrag.upload_document(
                library_id=route.library_id,
                external_key=item.ref.external_key,
                file_bytes=item.payload,
                file_name=item.file_name,
                mime_type=item.mime_type,
                title=item.title,
                document_hint=item.document_hint,
                idempotency_key=upload_key,
                parent_external_key=parent_external_key,
            )
            doc = upload_result.get("document") or upload_result
            new_id = (
                str(doc.get("id"))
                if isinstance(doc, dict) and doc.get("id")
                else None
            )
            self._state.upsert(
                kind=item.ref.kind,
                item_id=item.ref.item_id,
                change_token=item.ref.change_token,
                external_key=item.ref.external_key,
                ironrag_document_id=new_id,
                ironrag_library_id=str(route.library_id),
            )
            self._sweep_pushed[cache_key] = new_id
            return _outcome(
                item.ref,
                route,
                "created",
                new_id,
                "stale cursor; doc was gone — re-created upstream",
            )
        self._state.upsert(
            kind=item.ref.kind,
            item_id=item.ref.item_id,
            change_token=item.ref.change_token,
            external_key=item.ref.external_key,
            ironrag_document_id=str(document_id),
            ironrag_library_id=str(route.library_id),
        )
        self._sweep_pushed[cache_key] = str(document_id)
        return _outcome(
            item.ref,
            route,
            "replaced",
            str(document_id),
            f"replaced {len(item.payload)} bytes ({item.mime_type})",
        )

    async def reap_orphan(
        self,
        ref: SourceItemRef,
        library_id: UUID,
        ironrag_document_id: str,
        policy: PushPolicy,
    ) -> OrchestrationOutcome:
        if policy.on_missing is DeleteAction.IGNORE:
            return OrchestrationOutcome(
                ref=ref,
                action="skipped_missing",
                workspace_id=None,
                library_id=library_id,
                rule_description=None,
                ironrag_document_id=ironrag_document_id,
                detail=f"policy on_missing=ignore for kind={ref.kind}",
        )
        idempotency_key = f"{self._adapter.name}:reap:{ref.kind}:{ref.item_id}:{uuid4()}"
        await self._ironrag.delete_document(ironrag_document_id, idempotency_key)
        cursor = self._state.get(ref.kind, ref.item_id)
        if (
            cursor is not None
            and cursor.ironrag_document_id == ironrag_document_id
            and cursor.ironrag_library_id == str(library_id)
        ):
            self._state.delete(ref.kind, ref.item_id)
        return OrchestrationOutcome(
            ref=ref,
            action="deleted",
            workspace_id=None,
            library_id=library_id,
            rule_description=None,
            ironrag_document_id=ironrag_document_id,
            detail="reaped orphan",
        )

    async def delete_by_ref(self, ref: SourceItemRef) -> OrchestrationOutcome:
        """Resolve route + delete the IronRAG document for ``ref``.

        Used by webhook handlers that receive a vendor delete event:
        translate to a SourceItemRef and call this. Honors the kind's
        ``on_missing`` policy.
        """
        try:
            route = self._router.resolve(ref)
        except RoutingError as exc:
            return OrchestrationOutcome(
                ref=ref,
                action="unrouted",
                workspace_id=None,
                library_id=None,
                rule_description=None,
                ironrag_document_id=None,
                detail=str(exc),
            )
        existing = await self._ironrag.find_document_by_external_key(
            route.library_id, ref.external_key
        )
        if existing is None:
            return OrchestrationOutcome(
                ref=ref,
                action="skipped_missing",
                workspace_id=route.workspace_id,
                library_id=route.library_id,
                rule_description=route.rule_description,
                ironrag_document_id=None,
                detail="no IronRAG document for this ref",
            )
        policy = self._policies.for_kind(ref.kind)
        return await self.reap_orphan(ref, route.library_id, str(existing["id"]), policy)

    async def _resolve_cursor_library(self, cursor: CursorRow | None) -> CursorRow | None:
        if cursor is None or cursor.ironrag_library_id or not cursor.ironrag_document_id:
            return cursor
        try:
            async with asyncio.timeout(self._cursor_library_lookup_timeout):
                document = await self._ironrag.get_document(cursor.ironrag_document_id)
        except TimeoutError:
            log.warning(
                "orchestrator.cursor_library_lookup_timeout",
                kind=cursor.kind,
                item_id=cursor.item_id,
                document_id=cursor.ironrag_document_id,
                timeout_seconds=self._cursor_library_lookup_timeout,
            )
            return cursor
        except (IronRagError, httpx.TransportError) as exc:
            log.warning(
                "orchestrator.cursor_library_lookup_error",
                kind=cursor.kind,
                item_id=cursor.item_id,
                document_id=cursor.ironrag_document_id,
                error_type=type(exc).__name__,
                error=str(exc) or repr(exc),
            )
            return cursor
        if document is None:
            self._state.delete(cursor.kind, cursor.item_id)
            return None
        library_id = document_library_id(document)
        if library_id is None:
            log.warning(
                "orchestrator.cursor_library_unknown",
                kind=cursor.kind,
                item_id=cursor.item_id,
                document_id=cursor.ironrag_document_id,
            )
            return cursor
        self._state.upsert(
            kind=cursor.kind,
            item_id=cursor.item_id,
            change_token=cursor.change_token,
            external_key=cursor.external_key,
            ironrag_document_id=cursor.ironrag_document_id,
            ironrag_library_id=library_id,
        )
        return replace(cursor, ironrag_library_id=library_id)


def _outcome(
    ref: SourceItemRef,
    route: ResolvedRoute,
    action: OutcomeAction,
    document_id: str | None,
    detail: str,
) -> OrchestrationOutcome:
    return OrchestrationOutcome(
        ref=ref,
        action=action,
        workspace_id=route.workspace_id,
        library_id=route.library_id,
        rule_description=route.rule_description,
        ironrag_document_id=document_id,
        detail=detail,
    )


def _cursor_matches_route(cursor: CursorRow, route: ResolvedRoute) -> bool:
    return cursor.ironrag_library_id == str(route.library_id)


def _cursor_ownership_unknown(cursor: CursorRow | None) -> bool:
    return bool(cursor and cursor.ironrag_document_id and cursor.ironrag_library_id is None)


def _is_conflicting_mutation(exc: IronRagError) -> bool:
    text = str(exc).lower()
    return "conflicting_mutation" in text or "still processing a previous mutation" in text
