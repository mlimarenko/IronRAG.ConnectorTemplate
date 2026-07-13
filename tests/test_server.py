from __future__ import annotations

from pathlib import Path
from typing import Any, ClassVar
from uuid import UUID

import pytest

from ironrag_connector.config import BaseConnectorSettings, RunMode
from ironrag_connector.ironrag import IronRagCatalogError
from ironrag_connector.routing import ResolvedLibraryTarget
from ironrag_connector.server import build_app

WS = UUID("00000000-0000-0000-0000-000000000099")
LIB = UUID("00000000-0000-0000-0000-000000000000")
LIBRARY_REF = "tests/default-library"


class EmptyAdapter:
    name = "server-test"
    kinds = ("page",)
    primary_kinds = ("page",)

    def __init__(self) -> None:
        self.closed = False

    async def iter_items(self) -> Any:
        if False:
            yield

    async def fetch(self, ref: Any) -> None:
        return None

    def external_key(self, kind: str, item_id: str) -> str:
        return f"server-test:{kind}:{item_id}"

    def parse_external_key(self, external_key: str) -> tuple[str, str] | None:
        return None

    async def close(self) -> None:
        self.closed = True


class FakeIronRagClient:
    fail_resolution = False
    instances: ClassVar[list[FakeIronRagClient]] = []

    def __init__(self, *_: Any, **__: Any) -> None:
        self.closed = False
        self.resolution_calls: list[set[str]] = []
        self.instances.append(self)

    async def resolve_library_refs(self, refs: set[str]) -> dict[str, ResolvedLibraryTarget]:
        self.resolution_calls.append(refs)
        if self.fail_resolution:
            raise IronRagCatalogError("catalog target is not visible")
        return {
            LIBRARY_REF: ResolvedLibraryTarget(
                library_ref=LIBRARY_REF,
                workspace_id=WS,
                library_id=LIB,
            )
        }

    async def aclose(self) -> None:
        self.closed = True


def _settings(tmp_path: Path) -> BaseConnectorSettings:
    routing_path = tmp_path / "routing.yaml"
    routing_path.write_text(f"default: {{ library: {LIBRARY_REF} }}\n", encoding="utf-8")
    return BaseConnectorSettings(
        ironrag_base_url="http://ironrag.invalid",
        ironrag_api_token="test-token",
        routing_config_path=routing_path,
        state_db_path=tmp_path / "state.sqlite",
        pidfile_path=tmp_path / "connector.pid",
        run_mode=RunMode.WEBHOOK,
    )


@pytest.mark.asyncio
async def test_lifespan_resolves_routing_before_serving(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    FakeIronRagClient.fail_resolution = False
    FakeIronRagClient.instances.clear()
    monkeypatch.setattr("ironrag_connector.server.IronRagClient", FakeIronRagClient)
    adapter = EmptyAdapter()
    settings = _settings(tmp_path)
    app = build_app(settings, adapter)  # type: ignore[arg-type]

    async with app.router.lifespan_context(app):
        client = FakeIronRagClient.instances[-1]
        assert client.resolution_calls == [{LIBRARY_REF}]
        assert settings.pidfile_path is not None and settings.pidfile_path.exists()
        assert not client.closed
        assert not adapter.closed

    assert client.closed
    assert adapter.closed
    assert settings.pidfile_path is not None and not settings.pidfile_path.exists()


@pytest.mark.asyncio
async def test_lifespan_resolution_failure_aborts_and_cleans_resources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    FakeIronRagClient.fail_resolution = True
    FakeIronRagClient.instances.clear()
    monkeypatch.setattr("ironrag_connector.server.IronRagClient", FakeIronRagClient)
    adapter = EmptyAdapter()
    settings = _settings(tmp_path)
    app = build_app(settings, adapter)  # type: ignore[arg-type]

    with pytest.raises(IronRagCatalogError, match="not visible"):
        async with app.router.lifespan_context(app):
            raise AssertionError("lifespan must not start")

    client = FakeIronRagClient.instances[-1]
    assert client.resolution_calls == [{LIBRARY_REF}]
    assert client.closed
    assert adapter.closed
    assert settings.pidfile_path is not None and not settings.pidfile_path.exists()
