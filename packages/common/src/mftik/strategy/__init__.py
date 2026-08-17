"""What a strategy is written against.

Subclass :class:`Strategy`, override the hooks you care about, and reach the
platform through the accessors it binds — ``self.oms``, ``self.ledger``,
``self.mds``, ``self.tape``, ``self.symbols``, ``self.timer``. Nothing here
needs a database or a running STS, which is what lets it be installed beside
a strategy on a developer's machine rather than only inside the node.

The names re-exported here are the ones a strategy spells out: the base class,
and the types its own annotations mention. The accessor classes stay reachable
at their module paths (``mftik.strategy.oms`` and so on) — a strategy is handed
an instance, so naming the class is the rarer case.
"""

from mftik.strategy.base import Strategy
from mftik.strategy.session import SessionView
from mftik.strategy.tape import TapeSlice
from mftik.strategy.timer import Timer, TimerToken, now_ms

__all__ = [
    "SessionView",
    "Strategy",
    "TapeSlice",
    "Timer",
    "TimerToken",
    "now_ms",
]
