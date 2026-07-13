from __future__ import annotations

import os
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

from ironrag_connector.policy import DeleteAction, PushPolicy
from ironrag_connector.routing import (
    ResolvedLibraryTarget,
    Router,
    RoutingConfig,
    RoutingError,
    RoutingReloader,
    load_routing_config,
)
from ironrag_connector.source import SourceItemRef

WS_DEFAULT = UUID("00000000-0000-0000-0000-000000000099")
WS_PARTNER = UUID("00000000-0000-0000-0000-000000000098")
LIB_DEFAULT = UUID("00000000-0000-0000-0000-000000000000")
LIB_ENG = UUID("00000000-0000-0000-0000-000000000001")
LIB_ARCHIVE = UUID("00000000-0000-0000-0000-000000000002")

REF_DEFAULT = "main/product-docs"
REF_ENG = "main/engineering"
REF_ARCHIVE = "partner/archive"


def _ref(item_id: str, facts: dict[str, object] | None = None) -> SourceItemRef:
    return SourceItemRef(
        item_id=item_id,
        kind="page",
        external_key=f"x:page:{item_id}",
        change_token=None,
        routing_facts=facts or {},
    )


def _config() -> RoutingConfig:
    return RoutingConfig.model_validate(
        {
            "default": {"library": REF_DEFAULT},
            "rules": [
                {
                    "description": "engineering shelf",
                    "match": {"shelf": "engineering"},
                    "target": {"library": REF_ENG},
                },
                {
                    "description": "archive tag",
                    "match": {"tag": "archive"},
                    "target": {"library": REF_ARCHIVE},
                },
            ],
            "policies": {
                "page": {"on_missing": "delete"},
                "image": {"on_missing": "ignore"},
            },
        }
    )


def _targets() -> dict[str, ResolvedLibraryTarget]:
    return {
        REF_DEFAULT: ResolvedLibraryTarget(
            library_ref=REF_DEFAULT,
            workspace_id=WS_DEFAULT,
            library_id=LIB_DEFAULT,
        ),
        REF_ENG: ResolvedLibraryTarget(
            library_ref=REF_ENG,
            workspace_id=WS_DEFAULT,
            library_id=LIB_ENG,
        ),
        REF_ARCHIVE: ResolvedLibraryTarget(
            library_ref=REF_ARCHIVE,
            workspace_id=WS_PARTNER,
            library_id=LIB_ARCHIVE,
        ),
    }


def _router() -> Router:
    return Router(_config(), resolved_targets=_targets())


def test_rule_match_wins_over_default() -> None:
    resolved = _router().resolve(_ref("1", {"shelf": "engineering"}))
    assert resolved.workspace_id == WS_DEFAULT
    assert resolved.library_id == LIB_ENG
    assert resolved.library_ref == REF_ENG
    assert resolved.rule_description == "engineering shelf"


def test_default_falls_back_when_no_rule_matches() -> None:
    resolved = _router().resolve(_ref("2", {"shelf": "random"}))
    assert resolved.library_id == LIB_DEFAULT
    assert resolved.library_ref == REF_DEFAULT
    assert resolved.rule_description == ""


def test_facts_list_membership() -> None:
    resolved = _router().resolve(_ref("3", {"tag": ["other", "archive"]}))
    assert resolved.library_id == LIB_ARCHIVE


def test_missing_default_and_no_match_raises() -> None:
    cfg = RoutingConfig.model_validate(
        {"rules": [{"match": {"shelf": "x"}, "target": {"library": REF_ENG}}]}
    )
    router = Router(cfg, resolved_targets={REF_ENG: _targets()[REF_ENG]})
    with pytest.raises(RoutingError):
        router.resolve(_ref("4", {}))


@pytest.mark.parametrize(
    "target",
    [
        {"workspace": str(WS_DEFAULT), "library": str(LIB_DEFAULT)},
        {"library": str(LIB_DEFAULT)},
        {"library": "main"},
        {"library": "main/product-docs/extra"},
        {"library": " /product-docs"},
    ],
)
def test_legacy_or_malformed_targets_are_rejected(target: dict[str, str]) -> None:
    with pytest.raises(ValidationError):
        RoutingConfig.model_validate({"default": target})


def test_unknown_routing_keys_are_rejected() -> None:
    with pytest.raises(ValidationError, match="extra_forbidden"):
        RoutingConfig.model_validate(
            {"default": {"library": REF_DEFAULT}, "fallback": "silently-ignored-before"}
        )


def test_router_requires_catalog_resolution_before_use() -> None:
    router = Router(_config())
    with pytest.raises(RoutingError, match="not resolved"):
        router.resolve(_ref("1"))


@pytest.mark.asyncio
async def test_router_resolves_all_unique_library_refs_in_one_snapshot() -> None:
    calls: list[set[str]] = []

    async def resolve(refs: set[str]) -> dict[str, ResolvedLibraryTarget]:
        calls.append(refs)
        return _targets()

    router = Router(_config())
    await router.initialize(resolve)

    assert calls == [{REF_DEFAULT, REF_ENG, REF_ARCHIVE}]
    assert router.target_libraries() == {LIB_DEFAULT, LIB_ENG, LIB_ARCHIVE}


def test_policy_overrides_applied(tmp_path: Path) -> None:
    yaml_path = tmp_path / "r.yaml"
    yaml_path.write_text(
        f"""
default: {{ library: {REF_DEFAULT} }}
policies:
  image:
    on_missing: ignore
""".strip()
    )
    cfg = load_routing_config(yaml_path)
    router = Router(cfg, resolved_targets={REF_DEFAULT: _targets()[REF_DEFAULT]})
    defaults = PushPolicy()
    table = router.build_policies(defaults)
    assert table.for_kind("page").on_missing is DeleteAction.DELETE
    assert table.for_kind("image").on_missing is DeleteAction.IGNORE


@pytest.mark.asyncio
async def test_routing_reloader_updates_router_and_policy_table_atomically(
    tmp_path: Path,
) -> None:
    yaml_path = tmp_path / "routing.yaml"
    yaml_path.write_text(f"default: {{ library: {REF_DEFAULT} }}\n", encoding="utf-8")
    router = Router(
        load_routing_config(yaml_path),
        resolved_targets={REF_DEFAULT: _targets()[REF_DEFAULT]},
    )
    policies = router.build_policies(PushPolicy())

    async def resolve(refs: set[str]) -> dict[str, ResolvedLibraryTarget]:
        return {ref: _targets()[ref] for ref in refs}

    reloader = RoutingReloader(
        path=yaml_path,
        router=router,
        policies=policies,
        defaults=PushPolicy(),
        resolver=resolve,
    )

    assert router.resolve(_ref("1")).library_id == LIB_DEFAULT
    assert policies.for_kind("image").on_missing is DeleteAction.DELETE

    yaml_path.write_text(
        f"""
default: {{ library: {REF_ENG} }}
policies:
  image:
    on_missing: ignore
""".strip(),
        encoding="utf-8",
    )
    _bump_mtime(yaml_path)

    assert await reloader.reload_if_changed() is True
    assert router.resolve(_ref("1")).library_id == LIB_ENG
    assert policies.for_kind("image").on_missing is DeleteAction.IGNORE


@pytest.mark.asyncio
async def test_routing_reloader_keeps_previous_snapshot_when_resolution_fails(
    tmp_path: Path,
) -> None:
    yaml_path = tmp_path / "routing.yaml"
    yaml_path.write_text(f"default: {{ library: {REF_DEFAULT} }}\n", encoding="utf-8")
    router = Router(
        load_routing_config(yaml_path),
        resolved_targets={REF_DEFAULT: _targets()[REF_DEFAULT]},
    )
    policies = router.build_policies(PushPolicy())

    attempts = 0

    async def fail_once(refs: set[str]) -> dict[str, ResolvedLibraryTarget]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RoutingError("catalog ref is not visible")
        return {ref: _targets()[ref] for ref in refs}

    reloader = RoutingReloader(
        path=yaml_path,
        router=router,
        policies=policies,
        defaults=PushPolicy(),
        resolver=fail_once,
    )

    yaml_path.write_text(f"default: {{ library: {REF_ENG} }}\n", encoding="utf-8")
    _bump_mtime(yaml_path)

    assert await reloader.reload_if_changed() is False
    assert router.resolve(_ref("1")).library_id == LIB_DEFAULT

    # Failed candidates do not advance the accepted mtime, so the same file
    # is retried and can become active after a transient catalog outage.
    assert await reloader.reload_if_changed() is True
    assert router.resolve(_ref("1")).library_id == LIB_ENG


def _bump_mtime(path: Path) -> None:
    next_mtime_ns = path.stat().st_mtime_ns + 1_000_000
    os.utime(path, ns=(next_mtime_ns, next_mtime_ns))
