"""Binance spot's atoms. Interface only — the implementation is B7-02b.

See :mod:`mftik.exchange.atoms` for what an atom is, what each function
promises, and the invariants A1 to A6 every venue's module keeps.

**One endpoint**, and always the *combined* one: a partial-depth payload
carries no symbol, so on a raw socket a book arrives with nothing to say which
instrument it belongs to. The combined host wraps every push as
``{"stream": <name>, "data": {…}}``, which is also what lets a connection
worker route a frame to the atom it belongs to (:mod:`.streams`).

**``ticker`` is one atom here**, unlike on the two futures venues: spot's
``@ticker`` carries ``b``/``B``/``a``/``A``, so the quote is already on the
stats row and no join is needed. The projector is ``passthrough``. That one
difference between three Binance venues is why the plan is per venue and not
per brand.

**``trade`` and ``aggtrade`` are two atoms here**, also unlike futures: spot
publishes a raw ``@trade`` beside ``@aggTrade``, so a consumer that needs real
trade ids can have them, and the tape records whichever of the two has demand
(F20).
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

#: The one combined market-streams host every channel is subscribed on.
STREAM = "stream"


def atoms_for(topic: str, ticker: UniversalTicker, opts: AtomOptions) -> AtomPlan:
    """Which spot streams one platform feed needs. B7-02b."""
    raise NotImplementedError("IF-08: Binance spot atoms_for is B7-02b")


def decode(atom: Atom, frame: Frame) -> list[InstrumentScoped]:
    """One stream payload → the platform events it carries. B7-02b."""
    raise NotImplementedError("IF-08: Binance spot decode is B7-02b")


def capacity(endpoint: str) -> Capacity:
    """Per-connection limits for ``stream``. B7-02b measures them."""
    raise NotImplementedError("IF-08: Binance spot capacity is B7-02b")


def join_policy(atom: Atom) -> JoinPolicy:
    """Late-joiner semantics for one stream. B7-02b."""
    raise NotImplementedError("IF-08: Binance spot join_policy is B7-02b")


__all__ = [
    "STREAM",
    "atoms_for",
    "capacity",
    "decode",
    "join_policy",
]
