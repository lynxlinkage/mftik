"""What ``mftik.strategy`` promises a strategy author it will still export.

A strategy may import the standard library, ``mftik``, and its own files —
nothing else. So a name the docs tell a strategy to use has to be reachable
from the package they tell it to import from, and has to stay reachable:
there is no second place to get ``breathe`` from when a module path moves.
"""

from __future__ import annotations

import asyncio

import mftik.strategy as strategy
from mftik.strategy import Strategy, breathe, slice_deadline
from mftik.strategy import tape as tape_mod


def test_every_exported_name_is_there() -> None:
    missing = [name for name in strategy.__all__ if not hasattr(strategy, name)]
    assert missing == []
    assert Strategy is strategy.Strategy


def test_the_pacing_helpers_are_public() -> None:
    """A warm-up loop is pointed at these two by name, from here."""
    assert {"breathe", "slice_deadline"} <= set(strategy.__all__)
    assert breathe is tape_mod.breathe
    assert slice_deadline is tape_mod.slice_deadline


async def test_breathe_yields_only_once_the_slice_is_spent() -> None:
    """The per-record cost is a clock read; the reschedule is the exception."""
    ran = False

    async def sibling() -> None:
        nonlocal ran
        ran = True

    task = asyncio.create_task(sibling())
    deadline = slice_deadline()
    assert await breathe(deadline) == deadline
    assert not ran

    assert await breathe(deadline - 1.0) > deadline
    assert ran

    await task
