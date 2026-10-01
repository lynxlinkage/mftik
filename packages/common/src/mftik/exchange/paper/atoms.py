"""Paper's atoms. Interface only — the implementation is B7-02g.

See :mod:`mftik.exchange.atoms` for what an atom is, what each function
promises, and the invariants A1 to A6 every venue's module keeps.

**A simulated venue still has atoms**, and it gets them for the same reason
every other venue does: MD places, counts and reconciles atoms, so a venue
without them would need a second code path through the controller, the
reconciler and the tape — on the one venue every test and every example
strategy runs against.

**One endpoint**, ``public``, and one channel per ``(topic, symbol)``. There is
no wire to be verbatim about: the paper engine is in-process, so its channel
names are its own and A1 holds trivially. What matters is that the shape is the
real one — a frame in, platform models out, a capacity that can be reached —
because a paper venue that could not fill a connection would hide every
placement bug until the first real one.

**Every push is complete**, so the late-joiner policy is
:attr:`~mftik.exchange.atoms.JoinPolicy.NEXT_PUSH` and there is nothing to
fold.
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

#: The one in-process feed every paper channel is served on.
PUBLIC = "public"


def atoms_for(topic: str, ticker: UniversalTicker, opts: AtomOptions) -> AtomPlan:
    """Which paper channels one platform feed needs. B7-02g."""
    raise NotImplementedError("IF-08: Paper atoms_for is B7-02g")


def decode(atom: Atom, frame: Frame) -> list[InstrumentScoped]:
    """One paper engine payload → the platform events it carries. B7-02g."""
    raise NotImplementedError("IF-08: Paper decode is B7-02g")


def capacity(endpoint: str) -> Capacity:
    """Per-connection limits for ``public``. B7-02g states them."""
    raise NotImplementedError("IF-08: Paper capacity is B7-02g")


def join_policy(atom: Atom) -> JoinPolicy:
    """Late-joiner semantics for one paper channel. B7-02g."""
    raise NotImplementedError("IF-08: Paper join_policy is B7-02g")


__all__ = [
    "PUBLIC",
    "atoms_for",
    "capacity",
    "decode",
    "join_policy",
]
