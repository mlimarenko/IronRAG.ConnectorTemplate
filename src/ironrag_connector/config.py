"""Runtime configuration shared by every connector built on the framework.

Adapter-specific credentials (vendor base URL, API token, signing
secret) belong in a subclass:

    class MyConnectorSettings(BaseConnectorSettings):
        vendor_base_url: str
        vendor_api_token: str
        vendor_webhook_secret: str | None = None

Pydantic loads ``.env.local`` and ``.env`` so secrets stay outside the
process environment when desired. ``extra="ignore"`` lets the same
``.env`` file power several connectors at once during dev.
"""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from .policy import (
    DeleteAction,
    DuplicateContentAction,
    PushPolicy,
    UpdateAction,
    UpsertAction,
)


class RunMode(StrEnum):
    """How the connector picks up changes from the source.

    * ``poll`` — only the periodic sweep runs. Webhook handlers passed
      to ``build_app`` are ignored. Pick this when the vendor has no
      webhook support or you want a single, predictable cadence.
    * ``webhook`` — webhook handlers are mounted, the periodic sweep is
      disabled. Manual ``/sync/run`` still works for catch-up. Pick this
      when the vendor's webhooks are reliable and ordered.
    * ``both`` — sweep AND webhooks run side by side. The sweep is the
      reconciliation safety net for missed/dropped webhook deliveries.
      Default and recommended for most connectors.
    """

    POLL = "poll"
    WEBHOOK = "webhook"
    BOTH = "both"


class PolicyDefaults(BaseSettings):
    """Global default policy. Per-kind overrides live in routing.yaml."""

    model_config = SettingsConfigDict(env_prefix="DEFAULT_POLICY_", extra="ignore")

    on_new: UpsertAction = UpsertAction.CREATE
    on_changed: UpdateAction = UpdateAction.REPLACE
    on_missing: DeleteAction = DeleteAction.DELETE
    on_duplicate_content: DuplicateContentAction = DuplicateContentAction.SKIP

    def as_push_policy(self) -> PushPolicy:
        return PushPolicy(
            on_new=self.on_new,
            on_changed=self.on_changed,
            on_missing=self.on_missing,
            on_duplicate_content=self.on_duplicate_content,
        )


class BaseConnectorSettings(BaseSettings):
    """Everything the framework itself needs. Subclass for vendor creds."""

    model_config = SettingsConfigDict(env_file=(".env.local", ".env"), extra="ignore")

    # --- IronRAG ---
    ironrag_base_url: str
    ironrag_api_token: str
    request_timeout_seconds: float = Field(60.0, ge=1.0)
    cursor_library_lookup_timeout_seconds: float = Field(default=5.0, ge=0.1)
    """Timeout for best-effort legacy cursor library lookups.

    Keep this separate from ``request_timeout_seconds``: uploads may need a
    generous timeout, but one metadata lookup must not block a whole sweep.
    """
    cursor_library_lookup_max_rows_per_sweep: int = Field(default=16, ge=0)
    """Maximum legacy cursor rows to backfill before source enumeration.

    Rows beyond this limit are left for per-item lazy resolution or a later
    sweep, which keeps large old cursors from delaying the sync hot path.
    """

    # --- Routing ---
    routing_config_path: Path = Field(default=Path("routing.yaml"))

    # --- Run mode ---
    run_mode: RunMode = RunMode.BOTH

    # --- Sync loop (only consulted when run_mode != webhook) ---
    sync_interval_seconds: int = Field(default=1800, ge=60)
    sync_run_on_startup: bool = False
    sync_concurrency: int = Field(default=4, ge=1, le=64)
    sync_item_timeout_seconds: float = Field(default=300.0, ge=1.0)
    """Maximum wall-clock time for one source ref, including dependents."""

    # --- Persistent state ---
    state_db_path: Path = Field(default=Path("./connector-state.sqlite"))

    # --- HTTP server ---
    host: str = "0.0.0.0"
    port: int = 8088
    log_level: str = "info"
    admin_bearer_token: str | None = None

    # --- Process lock ---
    pidfile_path: Path | None = None
    """Defaults to ``/tmp/<connector-name>.pid`` if unset."""

    def policy_defaults(self) -> PushPolicy:
        return PolicyDefaults().as_push_policy()
