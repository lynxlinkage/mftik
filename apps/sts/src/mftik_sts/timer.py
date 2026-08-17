"""Where the timer used to live. See :mod:`mftik_sts.strategy` for why it stays.

``TimerToken`` is the one name besides ``Strategy`` that a strategy tree spells
out — it annotates the token ``timer.token()`` hands back — so the old path has
to go on resolving for the trees that already import it.

New strategies should import from :mod:`mftik.strategy`.
"""

from mftik.strategy.timer import Timer, TimerToken, now_ms

__all__ = ["Timer", "TimerToken", "now_ms"]
