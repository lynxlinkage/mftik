"""Compose acceptance for B4-09. Skipped unless opted in.

CI's integration job runs this marker and has no compose stack. Set
``MFTIK_E2E_COMPOSE=1`` and run ``scripts/b4_09_e2e.py`` (this test does
that) on a machine with docker compose.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.e2e

_SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "b4_09_e2e.py"


def test_compose_paper_path_and_controller_rolls() -> None:
    if os.getenv("MFTIK_E2E_COMPOSE") != "1":
        pytest.skip("set MFTIK_E2E_COMPOSE=1 to run the compose acceptance")
    completed = subprocess.run(
        [sys.executable, str(_SCRIPT)],
        check=False,
        text=True,
    )
    assert completed.returncode == 0
