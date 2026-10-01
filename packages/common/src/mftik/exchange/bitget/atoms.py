"""Bitget's atoms. Interface only — the implementation is B7-02f.

See :mod:`mftik.exchange.atoms` for what an atom is, what each function
promises, and the invariants A1 to A6 every venue's module keeps.

**One public endpoint**, ``/v3/ws/public``. The market is in the subscribe arg
as ``instType`` (``spot``, ``usdt-futures``, ``usdc-futures``), not in the URL,
so every book shares the connection (:mod:`.channels`).

**A structured subscribe, normalized to one string**, the same shape as OKX:
Bitget subscribes with ``{"instType", "topic", "symbol"}`` and
:func:`~.channels.arg_key` is already the socket's identity for it. An atom's
``channel`` is that rendering.

**One atom with no symbol.** The futures ``liquidation`` topic is subscribed
per ``instType`` and carries every contract's prints, so it is one atom serving
many instruments — the one place on this venue where an atom is not
per-instrument, and a case :func:`decode` has to stamp from the payload rather
than from the subscription.
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

#: The one public socket every market channel is subscribed on.
PUBLIC = "public"


def atoms_for(topic: str, ticker: UniversalTicker, opts: AtomOptions) -> AtomPlan:
    """Which Bitget channels one platform feed needs. B7-02f."""
    raise NotImplementedError("IF-08: Bitget atoms_for is B7-02f")


def decode(atom: Atom, frame: Frame) -> list[InstrumentScoped]:
    """One Bitget ``data`` row → the platform events it carries. B7-02f."""
    raise NotImplementedError("IF-08: Bitget decode is B7-02f")


def capacity(endpoint: str) -> Capacity:
    """Per-connection limits for ``public``. B7-02f measures them."""
    raise NotImplementedError("IF-08: Bitget capacity is B7-02f")


def join_policy(atom: Atom) -> JoinPolicy:
    """Late-joiner semantics for one Bitget channel. B7-02f."""
    raise NotImplementedError("IF-08: Bitget join_policy is B7-02f")


__all__ = [
    "PUBLIC",
    "atoms_for",
    "capacity",
    "decode",
    "join_policy",
]
