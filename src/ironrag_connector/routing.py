"""YAML routing config and per-kind policy overrides.

Routing config schema
=====================

::

    default:
      workspace: <uuid>
      library:   <uuid>

    rules:
      - description: "Free text for logs"
        match:                      # any keys; matched against ref.routing_facts
          shelf: engineering        # exact string equality
          tag: archive              # if fact is a list/tuple, membership
        target:
          workspace: <uuid>         # optional; falls back to default.workspace
          library:   <uuid>

    policies:                       # optional per-kind overrides
      page:
        on_missing: delete
      image:
        on_missing: ignore

The ``match`` fact bag is whatever the adapter chose to emit from
:meth:`SourceAdapter.iter_items` via ``SourceItemRef.routing_facts``. The
framework does not reserve any key names; ``shelf``, ``book``, ``tag``
are conventional but not mandatory.

If the operator omits ``default`` AND no rule matches an item, the item
is recorded as ``unrouted`` and skipped (logged loudly).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

import yaml
from pydantic import BaseModel, Field, field_validator

from .observability import get_logger
from .policy import (
    DeleteAction,
    DuplicateContentAction,
    PolicyOverride,
    PushPolicy,
    UpdateAction,
    UpsertAction,
)
from .source import SourceItemRef

log = get_logger(__name__)


class RouteTarget(BaseModel):
    workspace: UUID | None = None
    library: UUID


class RouteRule(BaseModel):
    description: str | None = None
    match: dict[str, Any]
    target: RouteTarget

    @field_validator("match")
    @classmethod
    def _match_not_empty(cls, v: dict[str, Any]) -> dict[str, Any]:
        if not v:
            raise ValueError("rule.match must declare at least one criterion")
        return v


class DefaultRoute(BaseModel):
    workspace: UUID
    library: UUID


class PolicyOverrideModel(BaseModel):
    on_new: UpsertAction | None = None
    on_changed: UpdateAction | None = None
    on_missing: DeleteAction | None = None
    on_duplicate_content: DuplicateContentAction | None = None

    def as_override(self) -> PolicyOverride:
        return PolicyOverride(
            on_new=self.on_new,
            on_changed=self.on_changed,
            on_missing=self.on_missing,
            on_duplicate_content=self.on_duplicate_content,
        )


class RoutingConfig(BaseModel):
    default: DefaultRoute | None = None
    rules: list[RouteRule] = Field(default_factory=list)
    policies: dict[str, PolicyOverrideModel] = Field(default_factory=dict)

    @field_validator("rules")
    @classmethod
    def _rules_target_workspace_or_default(
        cls, rules: list[RouteRule], info: Any
    ) -> list[RouteRule]:
        default_present = info.data.get("default") is not None
        for idx, rule in enumerate(rules):
            if rule.target.workspace is None and not default_present:
                raise ValueError(
                    f"rules[{idx}].target.workspace omitted but no `default.workspace` "
                    "is configured to inherit from"
                )
        return rules


@dataclass(frozen=True)
class ResolvedRoute:
    workspace_id: UUID
    library_id: UUID
    rule_description: str
    """Empty string means the default route was used."""


@dataclass
class PolicyOverrides:
    """Resolved per-kind policy table built from YAML + env defaults."""

    default: PushPolicy
    by_kind: dict[str, PushPolicy]

    def for_kind(self, kind: str) -> PushPolicy:
        return self.by_kind.get(kind, self.default)


class RoutingError(RuntimeError):
    """Thrown when neither a rule nor a default could resolve an item."""


def load_routing_config(path: Path) -> RoutingConfig:
    if not path.is_file():
        raise FileNotFoundError(f"routing config not found at {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"routing config at {path} must be a YAML mapping")
    return RoutingConfig.model_validate(raw)


class Router:
    """Resolve a SourceItemRef to a (workspace, library) target."""

    def __init__(self, config: RoutingConfig) -> None:
        self._config = config

    def replace_config(self, config: RoutingConfig) -> None:
        """Swap routing rules in-place so existing users see the new config."""
        self._config = config

    def target_libraries(self) -> set[UUID]:
        libs: set[UUID] = set()
        if self._config.default:
            libs.add(self._config.default.library)
        for rule in self._config.rules:
            libs.add(rule.target.library)
        return libs

    def resolve(self, ref: SourceItemRef) -> ResolvedRoute:
        facts = ref.routing_facts or {}
        for rule in self._config.rules:
            if _facts_match(rule.match, facts):
                workspace = rule.target.workspace or (
                    self._config.default.workspace if self._config.default else None
                )
                if workspace is None:
                    raise RoutingError(
                        f"rule '{rule.description or '<unnamed>'}' matched item "
                        f"{ref.kind}:{ref.item_id} but neither rule.target.workspace "
                        "nor default.workspace is set"
                    )
                return ResolvedRoute(
                    workspace_id=workspace,
                    library_id=rule.target.library,
                    rule_description=rule.description or "<unnamed rule>",
                )
        if self._config.default is None:
            raise RoutingError(
                f"no rule matched {ref.kind}:{ref.item_id} and no default is set"
            )
        return ResolvedRoute(
            workspace_id=self._config.default.workspace,
            library_id=self._config.default.library,
            rule_description="",
        )

    def build_policies(self, defaults: PushPolicy) -> PolicyOverrides:
        by_kind = {
            kind: defaults.merged_with(model.as_override())
            for kind, model in self._config.policies.items()
        }
        return PolicyOverrides(default=defaults, by_kind=by_kind)


class RoutingReloader:
    """Reload routing.yaml into the framework-owned router when it changes."""

    def __init__(
        self,
        *,
        path: Path,
        router: Router,
        policies: PolicyOverrides,
        defaults: PushPolicy,
    ) -> None:
        self._path = path
        self._router = router
        self._policies = policies
        self._defaults = defaults
        self._mtime_ns = _mtime_ns(path)

    def reload_if_changed(self) -> bool:
        try:
            current_mtime_ns = _mtime_ns(self._path)
        except OSError as exc:
            log.error(
                "routing.reload_error",
                path=str(self._path),
                error_type=type(exc).__name__,
                error=str(exc) or repr(exc),
            )
            return False
        if current_mtime_ns == self._mtime_ns:
            return False
        try:
            config = load_routing_config(self._path)
        except Exception as exc:
            log.error(
                "routing.reload_error",
                path=str(self._path),
                error_type=type(exc).__name__,
                error=str(exc) or repr(exc),
            )
            return False
        self._router.replace_config(config)
        updated = self._router.build_policies(self._defaults)
        self._policies.default = updated.default
        self._policies.by_kind = updated.by_kind
        self._mtime_ns = current_mtime_ns
        log.info(
            "routing.reloaded",
            path=str(self._path),
            rules=len(config.rules),
            has_default=config.default is not None,
            policy_overrides=list(config.policies.keys()),
        )
        return True


def _facts_match(criteria: dict[str, Any], facts: dict[str, Any]) -> bool:
    for key, expected in criteria.items():
        actual = facts.get(key)
        if actual is None:
            return False
        if isinstance(actual, (list, tuple, set)):
            if expected not in actual:
                return False
        else:
            if actual != expected:
                return False
    return True


def known_kinds(rules: Iterable[RouteRule]) -> set[str]:
    return set()  # reserved for future per-rule kind filters


def _mtime_ns(path: Path) -> int:
    return path.stat().st_mtime_ns
