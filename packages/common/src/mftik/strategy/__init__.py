"""What a strategy is written against.

Subclass :class:`Strategy`, override the hooks you care about, and reach the
platform through the accessors it binds — ``self.oms``, ``self.ledger``,
``self.mds``, ``self.md``, ``self.td``, ``self.tape``, ``self.artifacts``,
``self.symbols``, ``self.timer``. Nothing here
needs a database or a running STS, which is what lets it be installed beside
a strategy on a developer's machine rather than only inside the node.

The names re-exported here are the ones a strategy spells out: the base class
and the types its own annotations mention. The accessor classes stay reachable
at their module paths (``mftik.strategy.oms`` and so on) — a strategy is handed
an instance, so naming the class is the rarer case.
"""

from mftik.strategy.base import Strategy
from mftik.strategy.budget import HookSlow
from mftik.strategy.errors import NotReady, OffloadWorkerLost
from mftik.strategy.harness import SentCancel, SentOrder, StrategyHarness
from mftik.strategy.md import FeedState
from mftik.strategy.offload import OffloadPool
from mftik.strategy.ready import Ready
from mftik.strategy.session import SessionView
from mftik.strategy.tape import TapeFeedNotAttached, TapeSlice
from mftik.strategy.td import AccountState
from mftik.strategy.timer import Timer, TimerToken, now_ms
from mftik.strategy.universe import UniverseChange

__all__ = [
    "AccountState",
    "FeedState",
    "HookSlow",
    "NotReady",
    "OffloadPool",
    "OffloadWorkerLost",
    "Ready",
    "SentCancel",
    "SentOrder",
    "SessionView",
    "Strategy",
    "StrategyHarness",
    "TapeFeedNotAttached",
    "TapeSlice",
    "Timer",
    "TimerToken",
    "UniverseChange",
    "now_ms",
]
