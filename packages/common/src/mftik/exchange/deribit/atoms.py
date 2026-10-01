"""Deribit's atoms. Interface only — the implementation is B7-02a.

See :mod:`mftik.exchange.atoms` for what an atom is, what each function
promises, and the invariants A1 to A6 every venue's module keeps.

**One endpoint**, ``public``: spot, linear and inverse perps, dated futures
and options all answer on the same socket, and the channel names the
instrument.

**The many-to-one case lives here.** ``ticker.{instrument}.100ms`` is a single
channel whose row carries the quote, the open interest, the funding rate and —
on an option — the greeks and the IVs. Five platform topics (``ticker``,
``bestquote``, ``funding_rate``, ``open_interest``, ``greeks``) therefore
resolve to **one** atom, and :func:`decode` returns several events from the one
frame instead. That is the opposite direction from Binance's ``ticker``, and
the reason :func:`decode` returns a list at all (F19).

It also halves the capacity an option chain needs: two expiries × eleven
strikes × two sides asking for ``ticker`` and ``greeks`` is 44 atoms, not 88
(§6.4).

**Late joiners.** The ticker-shared topics are
:attr:`~mftik.exchange.atoms.JoinPolicy.SILENT`: a Deribit row names only the
fields that moved, so a consumer joining mid-stream waits for the next one
carrying the field it reads. The depth-capped ``book.*`` channels push a whole
truncated book every time and are
:attr:`~mftik.exchange.atoms.JoinPolicy.NEXT_PUSH`; the unbounded
``book.{instrument}.{interval}`` channel is snapshot-then-increments, so it is
:attr:`~mftik.exchange.atoms.JoinPolicy.SNAPSHOT_REPLAY` off the connection
worker's fold.
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

#: The one socket every public channel is subscribed on.
PUBLIC = "public"


def atoms_for(topic: str, ticker: UniversalTicker, opts: AtomOptions) -> AtomPlan:
    """Which Deribit channels one platform feed needs. B7-02a."""
    raise NotImplementedError("IF-08: Deribit atoms_for is B7-02a")


def decode(atom: Atom, frame: Frame) -> list[InstrumentScoped]:
    """One Deribit notification body → the platform events it carries. B7-02a."""
    raise NotImplementedError("IF-08: Deribit decode is B7-02a")


def capacity(endpoint: str) -> Capacity:
    """Per-connection limits for ``public``. B7-02a measures them."""
    raise NotImplementedError("IF-08: Deribit capacity is B7-02a")


def join_policy(atom: Atom) -> JoinPolicy:
    """Late-joiner semantics for one Deribit channel. B7-02a."""
    raise NotImplementedError("IF-08: Deribit join_policy is B7-02a")


__all__ = [
    "PUBLIC",
    "atoms_for",
    "capacity",
    "decode",
    "join_policy",
]
