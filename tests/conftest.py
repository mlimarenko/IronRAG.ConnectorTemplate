"""Shared test fixtures."""

from __future__ import annotations

import sys
from pathlib import Path

# Add examples/ to sys.path so tests can `import echo_connector`.
_ROOT = Path(__file__).resolve().parents[1]
_ECHO_SRC = _ROOT / "examples" / "echo_connector" / "src"
if str(_ECHO_SRC) not in sys.path:
    sys.path.insert(0, str(_ECHO_SRC))
