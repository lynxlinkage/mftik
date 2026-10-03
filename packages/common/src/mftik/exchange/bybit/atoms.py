"""Bybit's atoms. Interface only — the implementation is B7-02c.

See :mod:`mftik.exchange.atoms` for what an atom is, what each function
promises, and the invariants A1 to A6 every venue's module keeps.

**One endpoint per product**, by Bybit's design: ``/v5/public/{product}`` with
``product`` in ``spot``, ``linear``, ``inverse``, ``option``
(:mod:`.protocol`). The topic strings are *identical* across them —
``publicTrade.BTCUSDT`` is the spot tape or the perp tape depending only on
which socket it was subscribed on — which is why the endpoint is part of an
atom's identity and not a detail of where it was placed.

**The ticker channel is shared.** ``tickers.{symbol}`` serves the platform's
``ticker``, ``funding_rate`` and ``open_interest`` topics off one
subscription, so those resolve to one atom and :func:`decode` returns what the
row happens to carry. Bybit sends deltas, so a frame may name none of the
fields a given consumer reads; that is an empty list rather than an error, and
it is why this atom's late-joiner policy is
:attr:`~mftik.exchange.atoms.JoinPolicy.SILENT`.

**The book is folded.** ``orderbook.{depth}.{symbol}`` is snapshot-then-delta,
so the connection worker holds the fold (F21) and a joiner gets
:attr:`~mftik.exchange.atoms.JoinPolicy.SNAPSHOT_REPLAY`. A gap resyncs that
one atom — unsubscribe, then subscribe — without touching its siblings on the
connection.
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
from mftik.exchange.bybit.protocol import PRODUCTS
from mftik.exchange.models import InstrumentScoped
from mftik.exchange.tickers import UniversalTicker

#: Endpoint names: one public socket per Bybit product.
ENDPOINTS = PRODUCTS


def atoms_for(topic: str, ticker: UniversalTicker, opts: AtomOptions) -> AtomPlan:
    """Which Bybit topics one platform feed needs. B7-02c."""
    raise NotImplementedError("IF-08: Bybit atoms_for is B7-02c")


def decode(atom: Atom, frame: Frame) -> list[InstrumentScoped]:
    """One Bybit ``data`` payload → the platform events it carries. B7-02c."""
    raise NotImplementedError("IF-08: Bybit decode is B7-02c")


def capacity(endpoint: str) -> Capacity:
    """Per-connection limits for one product socket. B7-02c measures them."""
    raise NotImplementedError("IF-08: Bybit capacity is B7-02c")


def join_policy(atom: Atom) -> JoinPolicy:
    """Late-joiner semantics for one Bybit topic. B7-02c."""
    raise NotImplementedError("IF-08: Bybit join_policy is B7-02c")


__all__ = [
    "ENDPOINTS",
    "atoms_for",
    "capacity",
    "decode",
    "join_policy",
]
