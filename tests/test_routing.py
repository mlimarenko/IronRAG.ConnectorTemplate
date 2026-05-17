from __future__ import annotations

from pathlib import Path
from uuid import UUID

import pytest

from ironrag_connector.policy import DeleteAction, PushPolicy
from ironrag_connector.routing import (
    Router,
    RoutingConfig,
    RoutingError,
    load_routing_config,
)
from ironrag_connector.source import SourceItemRef

WS = UUID("00000000-0000-0000-0000-000000000099")
LIB_DEFAULT = UUID("00000000-0000-0000-0000-000000000000")
LIB_ENG = UUID("00000000-0000-0000-0000-000000000001")
LIB_ARCHIVE = UUID("00000000-0000-0000-0000-000000000002")


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
            "default": {"workspace": str(WS), "library": str(LIB_DEFAULT)},
            "rules": [
                {
                    "description": "engineering shelf",
                    "match": {"shelf": "engineering"},
                    "target": {"library": str(LIB_ENG)},
                },
                {
                    "description": "archive tag",
                    "match": {"tag": "archive"},
                    "target": {"library": str(LIB_ARCHIVE)},
                },
            ],
            "policies": {
                "page": {"on_missing": "delete"},
                "image": {"on_missing": "ignore"},
            },
        }
    )


def test_rule_match_wins_over_default() -> None:
    router = Router(_config())
    resolved = router.resolve(_ref("1", {"shelf": "engineering"}))
    assert resolved.library_id == LIB_ENG
    assert resolved.rule_description == "engineering shelf"


def test_default_falls_back_when_no_rule_matches() -> None:
    router = Router(_config())
    resolved = router.resolve(_ref("2", {"shelf": "random"}))
    assert resolved.library_id == LIB_DEFAULT
    assert resolved.rule_description == ""


def test_facts_list_membership() -> None:
    router = Router(_config())
    resolved = router.resolve(_ref("3", {"tag": ["other", "archive"]}))
    assert resolved.library_id == LIB_ARCHIVE


def test_missing_default_and_no_match_raises() -> None:
    cfg = RoutingConfig.model_validate(
        {
            "rules": [
                {
                    "match": {"shelf": "x"},
                    "target": {"workspace": str(WS), "library": str(LIB_ENG)},
                }
            ]
        }
    )
    router = Router(cfg)
    with pytest.raises(RoutingError):
        router.resolve(_ref("4", {}))


def test_policy_overrides_applied(tmp_path: Path) -> None:
    yaml_path = tmp_path / "r.yaml"
    yaml_path.write_text(
        f"""
default: {{ workspace: {WS}, library: {LIB_DEFAULT} }}
policies:
  image:
    on_missing: ignore
""".strip()
    )
    cfg = load_routing_config(yaml_path)
    router = Router(cfg)
    defaults = PushPolicy()
    table = router.build_policies(defaults)
    assert table.for_kind("page").on_missing is DeleteAction.DELETE
    assert table.for_kind("image").on_missing is DeleteAction.IGNORE
