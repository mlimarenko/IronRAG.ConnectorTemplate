"""IronRAG connector framework.

Public surface
==============

To write a new connector you implement :class:`SourceAdapter` and hand it to
:func:`build_app`. The framework owns: the IronRAG HTTP client, the routing
layer (YAML), the persistent cursor state (SQLite), the push policies
(create/update/delete and duplicate handling per item kind), the periodic
sync loop, the orphan reaper, the manual-trigger and webhook FastAPI
endpoints, the single-instance pidfile lock, and structured logging.
"""

from __future__ import annotations

from .config import BaseConnectorSettings, PolicyDefaults, RunMode
from .ironrag import IronRagClient, IronRagError
from .observability import configure_logging, get_logger
from .orchestrator import OrchestrationOutcome, Orchestrator
from .policy import DeleteAction, PushPolicy, UpdateAction, UpsertAction
from .routing import (
    PolicyOverrides,
    ResolvedRoute,
    Router,
    RoutingConfig,
    RoutingError,
    load_routing_config,
)
from .seed import seed_cursor
from .server import build_app
from .source import SourceAdapter, SourceItem, SourceItemRef
from .state import StateStore
from .sync import SyncAlreadyRunningError, SyncManager, SyncReport

__all__ = [
    "BaseConnectorSettings",
    "DeleteAction",
    "IronRagClient",
    "IronRagError",
    "OrchestrationOutcome",
    "Orchestrator",
    "PolicyDefaults",
    "PolicyOverrides",
    "PushPolicy",
    "ResolvedRoute",
    "Router",
    "RoutingConfig",
    "RoutingError",
    "RunMode",
    "SourceAdapter",
    "SourceItem",
    "SourceItemRef",
    "StateStore",
    "SyncAlreadyRunningError",
    "SyncManager",
    "SyncReport",
    "UpdateAction",
    "UpsertAction",
    "build_app",
    "configure_logging",
    "get_logger",
    "load_routing_config",
    "seed_cursor",
]
