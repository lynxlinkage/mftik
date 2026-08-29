"""The listing type must import without loading the exchange barrel."""

from __future__ import annotations

import subprocess
import sys


def test_importing_symbols_does_not_cycle_through_exchange() -> None:
    """``from mftik.symbols import SymbolClient`` used to fail: listed.py
    imported tickers, which ran exchange/__init__, which imported listing,
    which re-entered the still-initializing listed module.
    """
    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            "from mftik.symbols import ListedInstrument, SymbolClient; "
            "assert ListedInstrument is not None; "
            "assert SymbolClient is not None",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
