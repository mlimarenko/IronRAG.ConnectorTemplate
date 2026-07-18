"""Run the echo connector against a real IronRAG instance for smoke testing."""

from __future__ import annotations

import uvicorn

from ironrag_connector import BaseConnectorSettings, build_app

from .adapter import EchoAdapter, EchoPage


def main() -> None:
    settings = BaseConnectorSettings()
    pages = {
        "hello": EchoPage(
            item_id="hello",
            title="Hello",
            body="# Hello\n\nthis is the echo connector.",
            updated_at="2026-01-01T00:00:00Z",
            tag="example",
        ),
    }
    adapter = EchoAdapter(pages)
    app = build_app(settings, adapter)
    uvicorn.run(app, host=settings.host, port=settings.port, log_level=settings.log_level)


if __name__ == "__main__":
    main()
