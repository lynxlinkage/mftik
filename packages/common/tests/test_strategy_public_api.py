"""What ``mftik.strategy`` promises a strategy author it will still export.

A strategy may import the standard library, ``mftik``, and its own files —
nothing else. So a name the docs tell a strategy to use has to be reachable
from the package they tell it to import from, and has to stay reachable:
there is no second place to get ``Strategy`` from when a module path moves.
"""

from __future__ import annotations

import mftik.strategy as strategy
from mftik.strategy import Strategy


def test_every_exported_name_is_there() -> None:
    missing = [name for name in strategy.__all__ if not hasattr(strategy, name)]
    assert missing == []
    assert Strategy is strategy.Strategy
