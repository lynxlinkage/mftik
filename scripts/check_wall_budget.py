#!/usr/bin/env python3
"""Exit 1 when a ``just test`` wall time exceeds the F30 budget.

The unit+component CI step runs this against the elapsed time of that
step. 120 s is the cap on ubuntu-latest, excluding ``uv sync`` and
service startup.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(
    0, str(Path(__file__).resolve().parents[1] / "packages" / "common" / "tests")
)

from tier_budget import wall_budget_failure  # noqa: E402


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: check_wall_budget.py ELAPSED_SECONDS", file=sys.stderr)
        return 2
    try:
        elapsed = float(argv[1])
    except ValueError:
        print(f"not a duration: {argv[1]!r}", file=sys.stderr)
        return 2
    message = wall_budget_failure(elapsed)
    if message is not None:
        print(message, file=sys.stderr)
        return 1
    print(f"just test wall time {elapsed:.2f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
