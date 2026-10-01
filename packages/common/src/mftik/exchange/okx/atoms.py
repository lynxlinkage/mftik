"""OKX's atoms. Interface only — the implementation is B7-02d.

See :mod:`mftik.exchange.atoms` for what an atom is, what each function
promises, and the invariants A1 to A6 every venue's module keeps.

**Two endpoints**, by channel rather than by market: books, trades, tickers,
funding and open interest answer on ``/ws/v5/public``, candles only on
``/ws/v5/business`` (:mod:`.feed`). The market is in the subscribe arg, not in
the URL, so ``spot`` and ``swap`` share a connection and ``kline_*`` does not.

**A structured subscribe, normalized to one string.** OKX subscribes with
``{"channel", "instId", "instType"}`` rather than a name, and
:func:`~.channels.arg_key` already renders that triple as the socket's
identity. An atom's ``channel`` is that rendering — ``tickers:BTC-USDT-SWAP``
— which is A1: the string is the venue's own parameters, flattened, with no
platform vocabulary in it.

**Always-snapshot books.** ``bbo-tbt`` and ``books5`` push the whole top of
book every time, so their late-joiner policy is
:attr:`~mftik.exchange.atoms.JoinPolicy.NEXT_PUSH` and nothing is folded. The
deeper ``books`` channel is snapshot-then-delta and therefore
:attr:`~mftik.exchange.atoms.JoinPolicy.SNAPSHOT_REPLAY`.
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
from mftik.exchange.models import InstrumentScoped
from mftik.exchange.tickers import UniversalTicker

#: Market pushes: books, trades, tickers, funding, open interest.
PUBLIC = "public"
#: Candles, and only candles.
BUSINESS = "business"

ENDPOINTS = (PUBLIC, BUSINESS)


def atoms_for(topic: str, ticker: UniversalTicker, opts: AtomOptions) -> AtomPlan:
    """Which OKX channels one platform feed needs. B7-02d."""
    raise NotImplementedError("IF-08: OKX atoms_for is B7-02d")


def decode(atom: Atom, frame: Frame) -> list[InstrumentScoped]:
    """One OKX ``data`` row → the platform events it carries. B7-02d."""
    raise NotImplementedError("IF-08: OKX decode is B7-02d")


def capacity(endpoint: str) -> Capacity:
    """Per-connection limits for ``public`` or ``business``. B7-02d measures them."""
    raise NotImplementedError("IF-08: OKX capacity is B7-02d")


def join_policy(atom: Atom) -> JoinPolicy:
    """Late-joiner semantics for one OKX channel. B7-02d."""
    raise NotImplementedError("IF-08: OKX join_policy is B7-02d")


__all__ = [
    "BUSINESS",
    "ENDPOINTS",
    "PUBLIC",
    "atoms_for",
    "capacity",
    "decode",
    "join_policy",
]
