"""The old import paths, which strategy trees on disk still use.

``Strategy`` and ``TimerToken`` moved to :mod:`mftik.strategy` so they ship in
the package a strategy author installs. Every tree added before that move —
this node's own, and every copy pulled from a peer — imports them from
``mftik_sts``, and those trees are source that gets loaded, not source that
gets migrated. So the old spelling has to keep resolving to the same objects,
not to a copy: ``load_local_registry`` checks ``issubclass(cls, Strategy)``
against the class this package exports, and two distinct ``Strategy`` classes
would fail that check for every legacy tree at once.
"""

from __future__ import annotations

import mftik.strategy
import mftik_sts.strategy
import mftik_sts.timer


def test_strategy_is_the_same_class() -> None:
    assert mftik_sts.strategy.Strategy is mftik.strategy.Strategy


def test_timer_names_are_the_same_objects() -> None:
    assert mftik_sts.timer.Timer is mftik.strategy.Timer
    assert mftik_sts.timer.TimerToken is mftik.strategy.TimerToken
    assert mftik_sts.timer.now_ms is mftik.strategy.now_ms


def test_a_legacy_subclass_is_a_strategy() -> None:
    """What the registry loader asks of every tree it imports."""

    class Legacy(mftik_sts.strategy.Strategy):
        name = "legacy"

    assert issubclass(Legacy, mftik.strategy.Strategy)
