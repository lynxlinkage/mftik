"""BinanceUM's atoms. Interface only — the implementation is B7-02b.

See :mod:`mftik.exchange.atoms` for what an atom is, what each function
promises, and the invariants A1 to A6 every venue's module keeps.

**Two endpoints, and the split is load-bearing.** Since Binance retired
``fstream.binance.com/ws`` the book streams answer on ``/public`` and the tape,
candles, stats, mark price and liquidations on ``/market``
(:mod:`.protocol`). A subscribe sent to the wrong one is acknowledged and then
never pushes, so the endpoint is part of an atom's identity and
:func:`~.streams.group_of` is what decides it. Two endpoints means two
connections, which under atoms means two *processes* (F17) rather than two
sockets inside one client.

**The one-to-many case lives here.** ``@ticker`` carries the rolling 24h stats
and **no quote at all**, so the platform's ``ticker`` feed is two atoms —
``btcusdt@ticker`` on ``market`` and ``btcusdt@bookTicker`` on ``public`` —
composed by the ``quote_stats`` projector on the STS ingress (F19).
``@ticker`` decodes to :class:`~mftik.exchange.models.TickerStats`,
``@bookTicker`` to a :class:`~mftik.exchange.models.BestQuote`, and the join
emits a :class:`~mftik.exchange.models.Ticker` on every stats print once a
quote has arrived. Nothing is emitted before that: a ticker whose bid, ask and
last are the same number is not a ticker.

This is also the normal case of one feed's atoms landing on two connections,
which is what F19's "any atom down, the feed is down" rule is for.

**One tape.** Futures publishes ``@aggTrade`` and no raw ``@trade``, so the
platform's ``trade`` and ``aggtrade`` topics resolve to the same atom and the
tape records it once (F20).
"""

from __future__ import annotations

from mftik.exchange.atoms import (
    Atom,
    AtomOptions,
    AtomPlan,
    Capacity,
    Frame,
    JoinPolicy,
)
from mftik.exchange.binance.future.streams import MARKET, PUBLIC
from mftik.exchange.models import InstrumentScoped
from mftik.exchange.tickers import UniversalTicker

#: Endpoint names, as :mod:`.streams` spells them.
ENDPOINTS = (PUBLIC, MARKET)


def atoms_for(topic: str, ticker: UniversalTicker, opts: AtomOptions) -> AtomPlan:
    """Which Binance futures streams one platform feed needs. B7-02b."""
    raise NotImplementedError("IF-08: BinanceUM atoms_for is B7-02b")


def decode(atom: Atom, frame: Frame) -> list[InstrumentScoped]:
    """One stream payload → the platform events it carries. B7-02b."""
    raise NotImplementedError("IF-08: BinanceUM decode is B7-02b")


def capacity(endpoint: str) -> Capacity:
    """Per-connection limits for ``public`` or ``market``. B7-02b measures them."""
    raise NotImplementedError("IF-08: BinanceUM capacity is B7-02b")


def join_policy(atom: Atom) -> JoinPolicy:
    """Late-joiner semantics for one stream. B7-02b."""
    raise NotImplementedError("IF-08: BinanceUM join_policy is B7-02b")


__all__ = [
    "ENDPOINTS",
    "MARKET",
    "PUBLIC",
    "atoms_for",
    "capacity",
    "decode",
    "join_policy",
]
