from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from ironrag_connector.orchestrator import OrchestrationOutcome
from ironrag_connector.policy import PushPolicy
from ironrag_connector.routing import PolicyOverrides, Router, RoutingConfig
from ironrag_connector.source import SourceItemRef
from ironrag_connector.state import StateStore
from ironrag_connector.sync import SyncAlreadyRunningError, SyncManager

WS = UUID("00000000-0000-0000-0000-000000000099")
LIB = UUID("00000000-0000-0000-0000-000000000000")


class EmptyBlockingAdapter:
    name = "blocking"
    kinds = ("page",)
    primary_kinds = ("page",)

    def __init__(self, started: asyncio.Event, release: asyncio.Event) -> None:
        self._started = started
        self._release = release

    async def iter_items(self) -> Any:
        self._started.set()
        await self._release.wait()
        if False:
            yield SourceItemRef(
                item_id="never",
                kind="page",
                external_key="blocking:page:never",
            )

    def external_key(self, kind: str, item_id: str) -> str:
        return f"blocking:{kind}:{item_id}"

    def parse_external_key(self, external_key: str) -> tuple[str, str] | None:
        parts = external_key.split(":", 2)
        if len(parts) != 3 or parts[0] != "blocking":
            return None
        return parts[1], parts[2]


class OneRefAdapter(EmptyBlockingAdapter):
    async def iter_items(self) -> Any:
        yield SourceItemRef(item_id="1", kind="page", external_key="blocking:page:1")


class FakeIronRag:
    async def list_documents_by_external_key_prefix(
        self, *_: Any, **__: Any
    ) -> list[tuple[str, str]]:
        return []

    async def get_document(self, *_: Any, **__: Any) -> dict[str, Any] | None:
        return None


class NoopOrchestrator:
    def reset_sweep_cache(self) -> None:
        pass

    async def push_ref(self, ref: SourceItemRef) -> OrchestrationOutcome:
        return OrchestrationOutcome(
            ref=ref,
            action="noop_unchanged",
            workspace_id=WS,
            library_id=LIB,
            rule_description=None,
            ironrag_document_id="doc-1",
            detail="test",
        )

    async def reap_orphan(self, *_: Any, **__: Any) -> OrchestrationOutcome:
        raise AssertionError("no documents should be reaped in these tests")


class BlockingOrchestrator(NoopOrchestrator):
    def __init__(self, started: asyncio.Event, cancelled: asyncio.Event) -> None:
        self._started = started
        self._cancelled = cancelled

    async def push_ref(self, ref: SourceItemRef) -> OrchestrationOutcome:
        self._started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self._cancelled.set()
            raise
        raise AssertionError("blocking push_ref should not return")


def _router() -> Router:
    return Router(
        RoutingConfig.model_validate(
            {"default": {"workspace": str(WS), "library": str(LIB)}}
        )
    )


def _manager(
    tmp_path: Path,
    *,
    adapter: Any,
    orchestrator: NoopOrchestrator,
) -> SyncManager:
    router = _router()
    return SyncManager(
        adapter=adapter,
        ironrag=FakeIronRag(),  # type: ignore[arg-type]
        orchestrator=orchestrator,  # type: ignore[arg-type]
        router=router,
        state=StateStore(tmp_path / "state.sqlite"),
        policies=PolicyOverrides(default=PushPolicy(), by_kind={}),
        concurrency=1,
        interval_seconds=60,
    )


@pytest.mark.asyncio
async def test_run_once_rejects_overlapping_sweeps(tmp_path: Path) -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    manager = _manager(
        tmp_path,
        adapter=EmptyBlockingAdapter(started, release),
        orchestrator=NoopOrchestrator(),
    )

    first = asyncio.create_task(manager.run_once(reason="first"))
    await asyncio.wait_for(started.wait(), timeout=1)

    with pytest.raises(SyncAlreadyRunningError):
        await manager.run_once(reason="second")

    release.set()
    report = await asyncio.wait_for(first, timeout=1)
    assert report.errors == 0


@pytest.mark.asyncio
async def test_run_once_cancellation_cancels_item_tasks(tmp_path: Path) -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()
    manager = _manager(
        tmp_path,
        adapter=OneRefAdapter(asyncio.Event(), asyncio.Event()),
        orchestrator=BlockingOrchestrator(started, cancelled),
    )

    task = asyncio.create_task(manager.run_once(reason="cancel-test"))
    await asyncio.wait_for(started.wait(), timeout=1)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.wait_for(cancelled.wait(), timeout=1)
