"""FastAPI factory and process bootstrap.

A connector script typically does::

    from ironrag_connector import build_app
    from my_connector.adapter import MyAdapter
    from my_connector.config import MySettings

    settings = MySettings()  # extends BaseConnectorSettings
    adapter = MyAdapter(settings)
    app = build_app(settings, adapter)

``build_app`` wires routing, state, policy, orchestrator, sync manager,
pidfile, IronRAG client, and exposes:

* ``GET /health`` — liveness probe, no auth.
* ``POST /sync/run`` — manual sweep trigger, requires admin bearer.
* ``POST /webhook/{name}`` — generic adapter-driven webhook intake.
  The adapter declares a :class:`WebhookHandler` for each ``name`` via
  :meth:`SourceAdapter.webhook_handlers` (optional); the framework does
  bearer-auth and JSON parsing, then hands the parsed payload to the
  handler.

The lifespan acquires the pidfile, starts the periodic sweep task, and
releases everything on shutdown.
"""

from __future__ import annotations

import asyncio
import hmac
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse

from .config import BaseConnectorSettings, PolicyDefaults, RunMode
from .ironrag import IronRagClient
from .observability import configure_logging, get_logger
from .orchestrator import Orchestrator
from .pidfile import PidfileLock
from .routing import Router, RoutingReloader, load_routing_config
from .source import SourceAdapter
from .state import StateStore
from .sync import SyncAlreadyRunningError, SyncManager

log = get_logger(__name__)


WebhookCallable = Callable[[dict[str, Any]], Awaitable[Any]]


@dataclass
class WebhookHandler:
    """Adapter-provided hook the framework mounts at /webhook/{name}."""

    name: str
    handler: WebhookCallable
    """Called with the parsed JSON body; return value is JSON-encoded."""

    extra_auth: Callable[[Request, bytes], None] | None = None
    """Optional callback for HMAC verification on top of the bearer."""


WebhookFactory = Callable[[Orchestrator], list[WebhookHandler]]


def build_app(
    settings: BaseConnectorSettings,
    adapter: SourceAdapter,
    *,
    webhook_handlers: list[WebhookHandler] | None = None,
    webhook_factory: WebhookFactory | None = None,
) -> FastAPI:
    """Build the FastAPI app.

    Pass either ``webhook_handlers`` (static list) or ``webhook_factory``
    (callback receiving the framework-owned Orchestrator). The factory
    form is preferred: it lets handlers reuse the same orchestrator the
    sweep uses, so cursor state, routing decisions, and dedup cache stay
    coherent across webhook + sweep paths.
    """
    configure_logging(settings.log_level)
    routing = load_routing_config(settings.routing_config_path)
    router = Router(routing)
    policy_defaults = PolicyDefaults().as_push_policy()
    policies = router.build_policies(policy_defaults)
    ironrag = IronRagClient(settings)
    routing_reloader = RoutingReloader(
        path=settings.routing_config_path,
        router=router,
        policies=policies,
        defaults=policy_defaults,
        resolver=ironrag.resolve_library_refs,
    )
    log.info(
        "routing.loaded",
        rules=len(routing.rules),
        has_default=routing.default is not None,
        path=str(settings.routing_config_path),
        policy_overrides=list(routing.policies.keys()),
    )

    state = StateStore(settings.state_db_path)
    orchestrator = Orchestrator(
        adapter=adapter,
        ironrag=ironrag,
        router=router,
        state=state,
        policies=policies,
        cursor_library_lookup_timeout_seconds=(settings.cursor_library_lookup_timeout_seconds),
    )
    sync_manager = SyncManager(
        adapter=adapter,
        ironrag=ironrag,
        orchestrator=orchestrator,
        router=router,
        state=state,
        policies=policies,
        concurrency=settings.sync_concurrency,
        interval_seconds=settings.sync_interval_seconds,
        item_timeout_seconds=settings.sync_item_timeout_seconds,
        cursor_library_lookup_timeout_seconds=(settings.cursor_library_lookup_timeout_seconds),
        cursor_library_lookup_max_rows_per_sweep=(
            settings.cursor_library_lookup_max_rows_per_sweep
        ),
        reaper_list_timeout_seconds=settings.reaper_list_timeout_seconds,
        routing_reloader=routing_reloader,
    )

    pidfile_path = settings.pidfile_path or Path(f"/tmp/ironrag-connector-{adapter.name}.pid")
    pidfile = PidfileLock(pidfile_path)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        pidfile.acquire()
        async with AsyncExitStack() as resources:
            resources.callback(pidfile.release)
            resources.callback(state.close)
            resources.push_async_callback(adapter.close)
            resources.push_async_callback(ironrag.aclose)

            await router.initialize(ironrag.resolve_library_refs)
            log.info(
                "routing.resolved",
                library_refs=sorted(router.target_library_refs()),
                libraries=len(router.target_libraries()),
            )

            cancel_event = asyncio.Event()
            sync_task: asyncio.Task[None] | None = None
            startup_task: asyncio.Task[None] | None = None

            if settings.run_mode is not RunMode.WEBHOOK:
                if settings.sync_run_on_startup:

                    async def _startup_sync() -> None:
                        try:
                            await sync_manager.run_once(reason="startup")
                        except SyncAlreadyRunningError as exc:
                            log.info("sync.startup_skipped", reason=str(exc))
                        except Exception as exc:
                            log.error("sync.startup_error", error=str(exc))

                    startup_task = asyncio.create_task(_startup_sync())

                sync_task = asyncio.create_task(sync_manager.run_forever(cancel_event))

            log.info(
                "connector.lifespan.up",
                run_mode=settings.run_mode.value,
                sync_active=sync_task is not None,
                webhook_mounted=settings.run_mode is not RunMode.POLL and bool(webhook_handlers),
            )

            try:
                yield
            finally:
                cancel_event.set()
                for task in (sync_task, startup_task):
                    if task is not None:
                        task.cancel()
                        with suppress(asyncio.CancelledError):
                            await task

    app = FastAPI(
        title=f"IronRAG connector — {adapter.name}",
        version="0.1.0",
        lifespan=lifespan,
    )

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "connector": adapter.name}

    @app.post("/sync/run")
    async def trigger_sync(request: Request) -> JSONResponse:
        _require_admin_bearer(settings, request)
        try:
            report = await sync_manager.run_once(reason="manual")
        except SyncAlreadyRunningError as exc:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=str(exc),
            ) from exc
        return JSONResponse(status_code=status.HTTP_200_OK, content=report.as_dict())

    resolved_handlers: list[WebhookHandler] = list(webhook_handlers or [])
    if webhook_factory is not None:
        resolved_handlers.extend(webhook_factory(orchestrator))

    if settings.run_mode is not RunMode.POLL:
        for handler in resolved_handlers:
            _mount_webhook(app, settings, orchestrator, handler, routing_reloader)
    elif resolved_handlers:
        log.info(
            "connector.webhook_handlers_ignored",
            reason="run_mode=poll",
            handler_names=[h.name for h in resolved_handlers],
        )

    return app


def _mount_webhook(
    app: FastAPI,
    settings: BaseConnectorSettings,
    orchestrator: Orchestrator,
    handler: WebhookHandler,
    routing_reloader: RoutingReloader,
) -> None:
    name = handler.name

    async def _webhook(request: Request) -> JSONResponse:
        body = await request.body()
        _require_admin_bearer(settings, request)
        if handler.extra_auth is not None:
            handler.extra_auth(request, body)
        await routing_reloader.reload_if_changed()
        try:
            import json

            payload = json.loads(body.decode("utf-8") or "{}")
        except (UnicodeDecodeError, ValueError) as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"invalid JSON payload: {exc}",
            ) from exc
        result = await handler.handler(payload)
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={"webhook": name, "result": result},
        )

    app.post(f"/webhook/{name}")(_webhook)


def _require_admin_bearer(settings: BaseConnectorSettings, request: Request) -> None:
    expected = settings.admin_bearer_token
    if not expected:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                "connector misconfigured: ADMIN_BEARER_TOKEN is required to "
                "authorise admin endpoints"
            ),
        )
    auth = request.headers.get("authorization") or ""
    if not hmac.compare_digest(auth.encode("utf-8"), f"Bearer {expected}".encode()):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid bearer token",
        )
