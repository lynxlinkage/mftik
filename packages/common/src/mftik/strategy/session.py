"""The session, as a strategy sees it.

A strategy is handed its session and reaches back through it — for the broker,
for the symbol plane, to end the run. The session itself is STS's
(``mftik_sts.session.session.StsSession``): it owns the lease, the feeds and
the teardown, none of which belong in a package a strategy author installs.

So what travels here is the surface, not the class. ``StsSession`` satisfies
this structurally, which is what lets the base strategy be typed against its
session without this package importing the app that runs it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from mftik.broker import Broker
    from mftik.strategy.eventlog import EventLog
    from mftik.symbols import SymbolClient


@runtime_checkable
class SessionView(Protocol):
    """What a strategy may use of the session it is bound to."""

    #: Identifies this run. Names its md topic, its log stream, and its row.
    session_id: str
    #: Qualified registry key (``CrossArb``, ``private::Tiny``). Null when
    #: the deploy never recorded one. The kind id Alert matches on — not
    #: ``session_id`` and not the short ``Strategy.name``.
    type: str | None
    #: The 16-bit slot packed into every ``client_order_id`` this session
    #: mints — how a fill on an account-wide feed is traced back to one run.
    cid_slot: int
    broker: Broker
    symbols: SymbolClient
    #: Written at the session's dispatch points, not from the strategy. A
    #: strategy never calls this; the accessors it does call record through
    #: :func:`mftik.strategy.eventlog.session_log`.
    event_log: EventLog

    def request_exit(
        self, reason: str = "strategy_exit", *, failed: bool = False
    ) -> None:
        """End this session — ``done``, or ``failed`` with ``reason`` kept."""
        ...

    async def remember(self, key: str, value: str) -> None:
        """Persist one fact for a rebuilt session to have back."""
        ...
