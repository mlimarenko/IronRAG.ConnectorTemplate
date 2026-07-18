"""Push one :class:`SourceItem` into IronRAG under the resolved policy.

The orchestrator is policy-aware but transport-agnostic: it does not
know about webhooks vs sweeps. Both code paths converge on
:meth:`Orchestrator.push_item` and receive the same outcome object.

Dispatch flow per item
=======================

1. Look up an existing IronRAG document by ``ref.external_key`` in the
   target library (cursor first, then a single exact-match
   ``find_document`` call if the cursor doesn't already know it).
2. Compare ``ref.change_token`` against the value stored in
   :class:`StateStore`. If equal AND a document exists, short-circuit
   to ``noop_unchanged``.
3. Choose action per policy:
   * No existing doc and ``on_new=create`` -> ``create_document`` (201,
     synchronous).
   * No existing doc and ``on_new=skip`` -> ``skipped_new``.
   * Existing doc and ``on_changed=replace`` -> ``create_revision`` (202,
     polled to terminal via ``wait_for_operation``).
   * Existing doc and ``on_changed=skip`` -> ``skipped_changed``.
4. On a 409 ``duplicate_content`` create conflict: apply
   ``on_duplicate_content``, adopting the typed ``existingDocumentId``
   into the cursor on skip so the next sweep revises instead of
   re-creating.
5. Update the cursor with the new ``change_token`` and document id.

Async mutations (revisions, deletes) submit then poll: submission
(``create_revision``/``delete_document``) hands back an operation id,
:meth:`IronRagClient.wait_for_operation` polls it to a terminal state,
and this module reacts to that terminal state (ready = success,
superseded/canceled = defer to next sweep, failed = propagate).
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Literal
from uuid import UUID, uuid4

from .ironrag import (
    IronRagClient,
    IronRagConflictError,
    IronRagDuplicateContentError,
    IronRagError,
    IronRagMutationTimeoutError,
    IronRagNotFoundError,
    OperationStatusValue,
)
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
    deferred: bool = False
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

    Only revisions and deletes take an idempotency key (plan S7.1) --
    create dedup is the server's exact-externalKey + typed 409 conflict,
    not an idempotency key, so there is no ``create`` variant of this key.
    """
    digest = hashlib.sha256(item.payload).hexdigest()[:32]
    return f"{connector}:{op}:{item.ref.kind}:{item.ref.item_id}:{digest}"


def _primary_deferred(outcome: OrchestrationOutcome) -> bool:
    """Primary write is intentionally retried later, so dependents wait too."""
    return outcome.deferred


class Orchestrator:
    def __init__(
        self,
        *,
        adapter: SourceAdapter,
        ironrag: IronRagClient,
        router: Router,
        state: StateStore,
        policies: PolicyOverrides,
    ) -> None:
        self._adapter = adapter
        self._ironrag = ironrag
        self._router = router
        self._state = state
        self._policies = policies
        # In-sweep dedup: collapses repeated push_item calls for the
        # same external_key down to one IronRAG mutation. Necessary
        # because dependents (images, attachments) can be reached from
        # multiple parents in one sweep -- without this every shared
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

        cursor = self._state.get(ref.kind, ref.item_id)
        if (
            cursor is not None
            and cursor.change_token is not None
            and ref.change_token is not None
            and cursor.change_token == ref.change_token
        ):
            # Cursor and source agree on change_token. If we know the doc
            # id from cursor, trust it (saves an HTTP call). Otherwise
            # fall back to one exact-match server lookup to confirm the
            # document still exists.
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
            found = await self._ironrag.find_document(route.library_id, ref.external_key)
            if found is not None:
                doc_id = str(found.id)
                # Persist the discovered document id into the cursor so the
                # next sweep short-circuits on the cursor branch above with
                # zero HTTP.
                self._state.backfill_document_identity(
                    kind=ref.kind,
                    item_id=ref.item_id,
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
            # Cursor claimed a token match but IronRAG has no such document
            # (manual delete, library reset). Fall through to fetch+push,
            # which re-creates it.

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

        parent_cursor_before_push = self._state.get(ref.kind, ref.item_id)
        primary = await self.push_item(item, route, policy)
        if _primary_deferred(primary):
            log.info(
                "orchestrator.dependents_deferred",
                kind=ref.kind,
                item_id=ref.item_id,
                action=primary.action,
                detail=primary.detail,
            )
            return primary

        # Single injection point for parent linkage: every dependent
        # (a page's attachments and inline images) is uploaded declaring its
        # source item as parent via parent_external_key. The orchestrator
        # already knows the parent ref here, so no SourceItem.parent field is
        # needed -- this is the one source of truth for the link. The
        # backend derives document_role (attached_context for image media,
        # attachment otherwise) from the declared parent; primary items pass
        # no parent and stay role=primary.
        dep_outcomes: list[OrchestrationOutcome] = []
        for dep in item.dependents:
            dep_policy = self._policies.for_kind(dep.ref.kind)
            try:
                dep_route = self._router.resolve(dep.ref)
            except RoutingError:
                dep_route = route  # inherit parent's route if dependent unrouted
            dep_outcome = await self.push_item(
                dep,
                dep_route,
                dep_policy,
                parent_external_key=item.ref.external_key,
            )
            dep_outcomes.append(dep_outcome)
            if dep_outcome.deferred:
                break

        has_deferred_dependent = any(dep.deferred for dep in dep_outcomes)
        if has_deferred_dependent:
            self._state.restore(
                parent_cursor_before_push,
                kind=ref.kind,
                item_id=ref.item_id,
            )
            log.info(
                "orchestrator.primary_deferred_by_dependent",
                kind=ref.kind,
                item_id=ref.item_id,
                action=primary.action,
                dependent_count=len(dep_outcomes),
            )

        return OrchestrationOutcome(
            ref=primary.ref,
            action=primary.action,
            workspace_id=primary.workspace_id,
            library_id=primary.library_id,
            rule_description=primary.rule_description,
            ironrag_document_id=primary.ironrag_document_id,
            detail=(
                f"{primary.detail}; dependent write deferred; retry next sweep"
                if has_deferred_dependent
                else primary.detail
            ),
            deferred=primary.deferred or has_deferred_dependent,
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

        # Cursor wins over server-side find. The persistent SQLite cursor is
        # the source of truth for "do we own a document for this
        # external_key already?" -- trusting a server find blindly on every
        # call would cost an HTTP round trip per item even when the cursor
        # already knows the answer.
        cursor = self._state.get(item.ref.kind, item.ref.item_id)
        existing_document_id: str | None = None
        if cursor is not None and cursor.ironrag_document_id and _cursor_matches_route(
            cursor, route
        ):
            existing_document_id = cursor.ironrag_document_id
        else:
            found = await self._ironrag.find_document(route.library_id, item.ref.external_key)
            if found is not None:
                existing_document_id = str(found.id)
                self._state.backfill_document_identity(
                    kind=item.ref.kind,
                    item_id=item.ref.item_id,
                    external_key=item.ref.external_key,
                    ironrag_document_id=existing_document_id,
                    ironrag_library_id=str(route.library_id),
                )

        if existing_document_id is None:
            return await self._create(item, route, policy, cache_key, parent_external_key)

        if policy.on_changed is UpdateAction.SKIP:
            self._state.upsert(
                kind=item.ref.kind,
                item_id=item.ref.item_id,
                change_token=item.ref.change_token,
                external_key=item.ref.external_key,
                ironrag_document_id=existing_document_id,
                ironrag_library_id=str(route.library_id),
            )
            self._sweep_pushed[cache_key] = existing_document_id
            return _outcome(
                item.ref,
                route,
                "skipped_changed",
                existing_document_id,
                "policy on_changed=skip",
            )

        return await self._revise(item, route, existing_document_id, cache_key, parent_external_key)

    async def _create(
        self,
        item: SourceItem,
        route: ResolvedRoute,
        policy: PushPolicy,
        cache_key: tuple[UUID, str],
        parent_external_key: str | None,
    ) -> OrchestrationOutcome:
        if policy.on_new is UpsertAction.SKIP:
            return _outcome(item.ref, route, "skipped_new", None, "policy on_new=skip")
        try:
            document = await self._ironrag.create_document(
                route.library_id,
                external_key=item.ref.external_key,
                file_bytes=item.payload,
                file_name=item.file_name,
                mime_type=item.mime_type,
                title=item.title,
                document_hint=item.document_hint,
                parent_external_key=parent_external_key,
            )
        except IronRagDuplicateContentError as exc:
            existing_id = str(exc.existing_document_id) if exc.existing_document_id else None
            if policy.on_duplicate_content is DuplicateContentAction.FAIL:
                raise IronRagError(
                    f"duplicate content for {item.ref.external_key}; "
                    f"existing document {existing_id}"
                ) from exc
            self._state.upsert(
                kind=item.ref.kind,
                item_id=item.ref.item_id,
                change_token=item.ref.change_token,
                external_key=item.ref.external_key,
                ironrag_document_id=existing_id,
                ironrag_library_id=str(route.library_id),
            )
            self._sweep_pushed[cache_key] = existing_id
            log.debug(
                "orchestrator.create.duplicate_content",
                external_key=item.ref.external_key,
                existing_document_id=existing_id,
            )
            return _outcome(
                item.ref,
                route,
                "skipped_duplicate_content",
                existing_id,
                f"identical bytes already in library ({len(item.payload)} bytes)",
            )

        new_id = str(document.id)
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

    async def _revise(
        self,
        item: SourceItem,
        route: ResolvedRoute,
        document_id: str,
        cache_key: tuple[UUID, str],
        parent_external_key: str | None,
    ) -> OrchestrationOutcome:
        revision_key = (
            f"{item.idempotency_key}:revision"
            if item.idempotency_key
            else _default_idempotency_key(self._adapter.name, item, "revision")
        )
        try:
            handle = await self._ironrag.create_revision(
                document_id,
                mode="replace",
                file_bytes=item.payload,
                file_name=item.file_name,
                mime_type=item.mime_type,
                idempotency_key=revision_key,
            )
        except IronRagNotFoundError:
            # Cursor/find pointed at a doc IronRAG no longer has (manual
            # delete, library reset). Invalidate the cursor and fall back
            # to create so the next sweep is consistent.
            log.warning(
                "orchestrator.revise_404_invalidates_cursor",
                external_key=item.ref.external_key,
                stale_document_id=document_id,
            )
            self._state.delete(item.ref.kind, item.ref.item_id)
            policy = self._policies.for_kind(item.ref.kind)
            return await self._create(item, route, policy, cache_key, parent_external_key)
        except IronRagConflictError:
            # A mutation is already in flight for this document (typed 409,
            # not a regex match on the error message).
            log.info(
                "orchestrator.revise_deferred",
                external_key=item.ref.external_key,
                document_id=document_id,
                reason="conflicting_mutation",
            )
            self._sweep_pushed[cache_key] = document_id
            return _outcome(
                item.ref,
                route,
                "skipped_changed",
                document_id,
                "document has a pending mutation; retry next sweep",
                deferred=True,
            )

        try:
            status = await self._ironrag.wait_for_operation(handle.operation_id)
        except IronRagMutationTimeoutError as exc:
            log.info(
                "orchestrator.revise_deferred",
                external_key=item.ref.external_key,
                document_id=document_id,
                reason="operation_poll_timeout",
                error=str(exc),
            )
            self._sweep_pushed[cache_key] = document_id
            return _outcome(
                item.ref,
                route,
                "skipped_changed",
                document_id,
                "revision operation did not reach a terminal state; retry next sweep",
                deferred=True,
            )

        if status.status in (
            OperationStatusValue.SUPERSEDED,
            OperationStatusValue.CANCELED,
        ):
            log.info(
                "orchestrator.revise_deferred",
                external_key=item.ref.external_key,
                document_id=document_id,
                reason=status.status.value,
            )
            self._sweep_pushed[cache_key] = document_id
            return _outcome(
                item.ref,
                route,
                "skipped_changed",
                document_id,
                f"revision operation {status.status.value}; retry next sweep",
                deferred=True,
            )

        self._state.upsert(
            kind=item.ref.kind,
            item_id=item.ref.item_id,
            change_token=item.ref.change_token,
            external_key=item.ref.external_key,
            ironrag_document_id=document_id,
            ironrag_library_id=str(route.library_id),
        )
        self._sweep_pushed[cache_key] = document_id
        return _outcome(
            item.ref,
            route,
            "replaced",
            document_id,
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
        handle = await self._ironrag.delete_document(
            ironrag_document_id, idempotency_key=idempotency_key
        )
        if handle is not None:
            # Delete is async like revisions; polled serially per orphan.
            # The reaper runs after item enumeration and is not on the
            # per-item hot path, so N sequential poll-to-terminal calls for
            # N orphans is an accepted cost, not batched (no batch delete
            # endpoint in the SDK's scope -- see plan S7.2).
            await self._ironrag.wait_for_operation(handle.operation_id)
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
        found = await self._ironrag.find_document(route.library_id, ref.external_key)
        if found is None:
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
        return await self.reap_orphan(ref, route.library_id, str(found.id), policy)


def _outcome(
    ref: SourceItemRef,
    route: ResolvedRoute,
    action: OutcomeAction,
    document_id: str | None,
    detail: str,
    *,
    deferred: bool = False,
) -> OrchestrationOutcome:
    return OrchestrationOutcome(
        ref=ref,
        action=action,
        workspace_id=route.workspace_id,
        library_id=route.library_id,
        rule_description=route.rule_description,
        ironrag_document_id=document_id,
        detail=detail,
        deferred=deferred,
    )


def _cursor_matches_route(cursor: CursorRow, route: ResolvedRoute) -> bool:
    return cursor.ironrag_library_id == str(route.library_id)
