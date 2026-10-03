"""GateFutures' atoms. Interface only — the implementation is B7-02e.

See :mod:`mftik.exchange.atoms` for what an atom is, what each function
promises, and the invariants A1 to A6 every venue's module keeps.

**One endpoint**, ``/v4/ws/usdt``. Gate's futures plane signs separately from
its spot plane and is a venue of its own
(:mod:`mftik.exchange.venues`), so it has its own adapter and its own atoms.

**One contract, one atom** — ``futures.tickers:BTC_USDT`` — which is what makes
MDS-4's identity problem disappear rather than be worked around; see
:mod:`mftik.exchange.gate.spot.atoms` for what that bug was.

**The ticker channel is shared** across the platform's ``ticker``,
``funding_rate`` and ``open_interest`` topics: one atom, several events per
frame, and a joiner that is
:attr:`~mftik.exchange.atoms.JoinPolicy.SILENT` until the next row naming the
field it reads. ``futures.public_liquidates`` is the venue's liquidation
channel and is recorded to the tape (F20).
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

#: The one socket every futures channel is subscribed on.
PUBLIC = "public"


def atoms_for(topic: str, ticker: UniversalTicker, opts: AtomOptions) -> AtomPlan:
    """Which Gate futures channels one platform feed needs. B7-02e."""
    raise NotImplementedError("IF-08: GateFutures atoms_for is B7-02e")


def decode(atom: Atom, frame: Frame) -> list[InstrumentScoped]:
    """One Gate ``result`` payload → the platform events it carries. B7-02e."""
    raise NotImplementedError("IF-08: GateFutures decode is B7-02e")


def capacity(endpoint: str) -> Capacity:
    """Per-connection limits for ``public``. B7-02e measures them."""
    raise NotImplementedError("IF-08: GateFutures capacity is B7-02e")


def join_policy(atom: Atom) -> JoinPolicy:
    """Late-joiner semantics for one Gate futures channel. B7-02e."""
    raise NotImplementedError("IF-08: GateFutures join_policy is B7-02e")


__all__ = [
    "PUBLIC",
    "atoms_for",
    "capacity",
    "decode",
    "join_policy",
]
