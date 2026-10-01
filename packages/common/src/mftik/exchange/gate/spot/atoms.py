"""Gate spot's atoms. Interface only — the implementation is B7-02e.

See :mod:`mftik.exchange.atoms` for what an atom is, what each function
promises, and the invariants A1 to A6 every venue's module keeps.

**One endpoint**, ``/v4/ws/spot``. Gate's spot plane is its own venue — its own
host and its own credential — so there is nothing else to split over
(:mod:`mftik.exchange.venues`).

**This definition is what fixes MDS-4.** Gate subscribes a channel with a
payload list, and the socket keyed its wire identity on the whole call:
``subscribe_tickers("BTC_USDT")`` and
``subscribe_tickers("BTC_USDT", "ETH_USDT")`` were two different keys, so
``BTC_USDT`` went out twice — on the second subscribe and again on every
restore. An atom is **one contract on one channel**
(``spot.tickers:BTC_USDT``), so overlapping calls cannot exist: the reconciler
diffs a set of atoms and batches the frame itself. One identity, one
subscribe, by construction rather than by a ledger key.

**The ticker channel is shared**, as on Bybit: ``spot.tickers`` serves the
platform's ``ticker`` topic and whatever stats ride the same row, so a joiner
is :attr:`~mftik.exchange.atoms.JoinPolicy.SILENT` until the next row carrying
the field it reads.
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

#: The one socket every spot channel is subscribed on.
PUBLIC = "public"


def atoms_for(topic: str, ticker: UniversalTicker, opts: AtomOptions) -> AtomPlan:
    """Which Gate spot channels one platform feed needs. B7-02e."""
    raise NotImplementedError("IF-08: Gate spot atoms_for is B7-02e")


def decode(atom: Atom, frame: Frame) -> list[InstrumentScoped]:
    """One Gate ``result`` payload → the platform events it carries. B7-02e."""
    raise NotImplementedError("IF-08: Gate spot decode is B7-02e")


def capacity(endpoint: str) -> Capacity:
    """Per-connection limits for ``public``. B7-02e measures them."""
    raise NotImplementedError("IF-08: Gate spot capacity is B7-02e")


def join_policy(atom: Atom) -> JoinPolicy:
    """Late-joiner semantics for one Gate spot channel. B7-02e."""
    raise NotImplementedError("IF-08: Gate spot join_policy is B7-02e")


__all__ = [
    "PUBLIC",
    "atoms_for",
    "capacity",
    "decode",
    "join_policy",
]
