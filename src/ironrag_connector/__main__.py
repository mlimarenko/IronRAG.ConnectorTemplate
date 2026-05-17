"""Reference entry point.

Most concrete connectors will ship their own ``__main__`` that picks the
right settings class and adapter; this one exists so the framework
package itself is runnable in CI and for smoke tests against the example
adapter.
"""

from __future__ import annotations

import uvicorn


def main() -> None:
    raise SystemExit(
        "ironrag_connector has no default entry point — implement a SourceAdapter "
        "and build your own __main__ that calls ironrag_connector.build_app(). "
        "See examples/echo_connector for a working reference."
    )


if __name__ == "__main__":
    main()


# Re-export for convenience when a downstream connector does
# `uvicorn my_connector.server:app`.
__all__ = ["uvicorn"]
