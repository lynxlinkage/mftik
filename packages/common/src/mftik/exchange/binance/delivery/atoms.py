"""BinanceCM's atoms. Interface only — the implementation is B7-02b.

See :mod:`mftik.exchange.atoms` for what an atom is, what each function
promises, and the invariants A1 to A6 every venue's module keeps.

**One endpoint.** dapi was not part of the 2026 ``fstream`` split, so every
market stream answers on the combined ``dstream`` host and there is no routing
table to get wrong (:mod:`.streams`).

**``ticker`` is two atoms here too.** The COIN-M 24h ticker carries no quote
either, so the platform's ``ticker`` feed is ``@ticker`` plus ``@bookTicker``
joined by the ``quote_stats`` projector (F19) — the same shape as
:mod:`mftik.exchange.binance.future.atoms`, with both atoms on the one
endpoint rather than split across two.

**Sizes are contracts.** A COIN-M contract is denominated in USD, so every
public size this venue reports — and therefore everything :func:`decode`
produces — is in contracts rather than base. The models already say so; it is
repeated here because the atom's channel is the last place it is visible.
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
    """Which COIN-M streams one platform feed needs. B7-02b."""
    raise NotImplementedError("IF-08: BinanceCM atoms_for is B7-02b")


def decode(atom: Atom, frame: Frame) -> list[InstrumentScoped]:
    """One stream payload → the platform events it carries. B7-02b."""
    raise NotImplementedError("IF-08: BinanceCM decode is B7-02b")


def capacity(endpoint: str) -> Capacity:
    """Per-connection limits for ``stream``. B7-02b measures them."""
    raise NotImplementedError("IF-08: BinanceCM capacity is B7-02b")


def join_policy(atom: Atom) -> JoinPolicy:
    """Late-joiner semantics for one stream. B7-02b."""
    raise NotImplementedError("IF-08: BinanceCM join_policy is B7-02b")


__all__ = [
    "STREAM",
    "atoms_for",
    "capacity",
    "decode",
    "join_policy",
]
