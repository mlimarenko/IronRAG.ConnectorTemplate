"""Periodic full-sweep synchroniser.

The sync loop is the framework's heartbeat. Each pass:

1. Calls ``adapter.iter_items()`` to enumerate every primary item.
2. For each ref: hands it to :meth:`Orchestrator.push_ref`, which
   handles fetch + policy + push. Concurrency is bounded by
   ``sync_concurrency``.
3. After the enumeration completes successfully, runs the orphan reaper:
   for every ``kind`` the adapter declares, lists IronRAG documents
   under the adapter's external-key prefix and deletes any whose item is
   no longer in the cursor — *or* whose item is in the cursor but was
   not seen in this sweep, depending on policy.

The reaper is gated on a clean enumeration: a partial sweep would falsely
delete every item that did not happen to be listed before the network
broke, so we refuse to reap if the enumeration raised.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from .ironrag import IronRagClient, IronRagError
from .observability import get_logger
from .orchestrator import OrchestrationOutcome, Orchestrator
from .policy import DeleteAction
from .routing import PolicyOverrides, Router
from .source import SourceAdapter
from .state import StateStore

log = get_logger(__name__)


@dataclass
class SyncReport:
    started_at: datetime
    finished_at: datetime
    items_seen: int = 0
    created: int = 0
    replaced: int = 0
    noop_unchanged: int = 0
    skipped: int = 0
    skipped_duplicate_content: int = 0
    skipped_missing: int = 0
    unrouted: int = 0
    reaped: int = 0
    errors: int = 0
    by_kind: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "duration_seconds": (self.finished_at - self.started_at).total_seconds(),
            "items_seen": self.items_seen,
            "created": self.created,
            "replaced": self.replaced,
            "noop_unchanged": self.noop_unchanged,
            "skipped": self.skipped,
            "skipped_duplicate_content": self.skipped_duplicate_content,
            "skipped_missing": self.skipped_missing,
            "unrouted": self.unrouted,
            "reaped": self.reaped,
            "errors": self.errors,
            "by_kind": self.by_kind,
        }


class SyncManager:
    def __init__(
        self,
        *,
        adapter: SourceAdapter,
        ironrag: IronRagClient,
        orchestrator: Orchestrator,
        router: Router,
        state: StateStore,
        policies: PolicyOverrides,
        concurrency: int,
        interval_seconds: int,
    ) -> None:
        self._adapter = adapter
        self._ironrag = ironrag
        self._orchestrator = orchestrator
        self._router = router
        self._state = state
        self._policies = policies
        self._concurrency = concurrency
        self._interval = interval_seconds

    async def run_once(self, *, reason: str) -> SyncReport:
        started = datetime.now(tz=UTC)
        log.info("sync.start", reason=reason, connector=self._adapter.name)
        report = SyncReport(started_at=started, finished_at=started)
        # Reset the orchestrator's in-sweep dedup cache so the same
        # external_key reached from multiple parents collapses to one
        # IronRAG mutation per sweep.
        self._orchestrator.reset_sweep_cache()

        sem = asyncio.Semaphore(self._concurrency)
        seen: dict[str, set[str]] = {k: set() for k in self._adapter.kinds}
        tasks: list[asyncio.Task[None]] = []

        async def process(ref: Any) -> None:
            seen.setdefault(ref.kind, set()).add(ref.item_id)
            report.items_seen += 1
            report.by_kind[ref.kind] = report.by_kind.get(ref.kind, 0) + 1
            async with sem:
                try:
                    outcome = await self._orchestrator.push_ref(ref)
                except Exception as exc:
                    log.error(
                        "sync.item_error",
                        kind=ref.kind,
                        item_id=ref.item_id,
                        external_key=ref.external_key,
                        error_type=type(exc).__name__,
                        error=str(exc) or repr(exc),
                    )
                    report.errors += 1
                    return
            _log_outcome(outcome)
            _tally(report, outcome)
            for dep in outcome.dependent_outcomes:
                seen.setdefault(dep.ref.kind, set()).add(dep.ref.item_id)
                _log_outcome(dep)
                _tally(report, dep)

        sweep_completed = False
        try:
            async for ref in self._adapter.iter_items():
                tasks.append(asyncio.create_task(process(ref)))
            await asyncio.gather(*tasks, return_exceptions=False)
            sweep_completed = True
        except Exception as exc:
            log.error(
                "sync.enumeration_error",
                error_type=type(exc).__name__,
                error=str(exc) or repr(exc),
            )
            report.errors += 1

        if sweep_completed:
            await self._reap(seen, report)

        report.finished_at = datetime.now(tz=UTC)
        log.info(
            "sync.done",
            reason=reason,
            **{
                k: v
                for k, v in report.as_dict().items()
                if k not in ("started_at", "finished_at")
            },
        )
        return report

    async def _reap(self, seen: dict[str, set[str]], report: SyncReport) -> None:
        """Delete IronRAG docs whose source item vanished.

        Only ``primary_kinds`` are reaped — kinds enumerated directly by
        ``iter_items()``. Dependent kinds (attachments, images) don't
        participate because their absence from ``seen`` may simply mean
        the parent noop'd and we didn't re-fetch.
        """
        target_libs = self._router.target_libraries()
        primary = getattr(self._adapter, "primary_kinds", self._adapter.kinds)
        for kind in primary:
            policy = self._policies.for_kind(kind)
            if policy.on_missing is DeleteAction.IGNORE:
                continue
            prefix = self._adapter.external_key(kind, "")
            if not prefix.endswith(":"):
                prefix = prefix + ""  # adapter may already include trailing colon
            for library_id in target_libs:
                try:
                    pairs = await self._ironrag.list_documents_by_external_key_prefix(
                        library_id, prefix
                    )
                except IronRagError as exc:
                    log.warning(
                        "sync.reap.list_error",
                        library_id=str(library_id),
                        prefix=prefix,
                        error=str(exc),
                    )
                    continue
                for external_key, document_id in pairs:
                    parsed = self._adapter.parse_external_key(external_key)
                    if parsed is None:
                        continue
                    parsed_kind, parsed_item_id = parsed
                    if parsed_kind != kind:
                        continue
                    if parsed_item_id in seen.get(kind, set()):
                        continue
                    cursor = self._state.get_by_external_key(external_key)
                    from .source import SourceItemRef

                    reap_ref = SourceItemRef(
                        item_id=parsed_item_id,
                        kind=kind,
                        external_key=external_key,
                    )
                    try:
                        await self._orchestrator.reap_orphan(
                            reap_ref, library_id, document_id, policy
                        )
                        report.reaped += 1
                        if cursor is not None:
                            self._state.delete(cursor.kind, cursor.item_id)
                    except IronRagError as exc:
                        log.warning(
                            "sync.reap.delete_error",
                            external_key=external_key,
                            document_id=document_id,
                            error=str(exc),
                        )
                        report.errors += 1

    async def run_forever(self, cancel_event: asyncio.Event) -> None:
        while True:
            try:
                await asyncio.wait_for(cancel_event.wait(), timeout=float(self._interval))
                return
            except TimeoutError:
                pass
            try:
                await self.run_once(reason="periodic")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error("sync.periodic_error", error=str(exc))


def _log_outcome(outcome: OrchestrationOutcome) -> None:
    """One-line per-document decision log: kind, id, action, reason, doc_id, library."""
    level = log.warning if outcome.action in ("unrouted", "fetch_returned_none") else log.info
    level(
        f"sync.item.{outcome.action}",
        kind=outcome.ref.kind,
        item_id=outcome.ref.item_id,
        external_key=outcome.ref.external_key,
        ironrag_document_id=outcome.ironrag_document_id,
        library_id=str(outcome.library_id) if outcome.library_id else None,
        rule=outcome.rule_description,
        detail=outcome.detail,
        title=(outcome.ref.raw or {}).get("name")
        or (outcome.ref.raw or {}).get("title"),
    )


def _tally(report: SyncReport, outcome: OrchestrationOutcome) -> None:
    action = outcome.action
    if action == "created":
        report.created += 1
    elif action == "replaced":
        report.replaced += 1
    elif action == "noop_unchanged":
        report.noop_unchanged += 1
    elif action in ("skipped_new", "skipped_changed"):
        report.skipped += 1
    elif action == "skipped_duplicate_content":
        report.skipped_duplicate_content += 1
    elif action in ("skipped_missing", "fetch_returned_none"):
        report.skipped_missing += 1
    elif action == "unrouted":
        report.unrouted += 1
    elif action == "deleted":
        report.reaped += 1
