"""What ``on_universe_change`` is handed when a selector moves.

A ``select:`` block in ``strategy.yml`` declares a shape rather than a list of
contracts — "the two nearest BTC expiries, ATM ± 5 strikes, calls and puts", or
"the front quarterly future". The MD controller derives the members from the SYM
listing and a reference price as a pure function, and when that derivation
produces a different set the session is told through this one hook (F33, §6.4).

One hook for every kind of movement, including a roll: a rolling future's new
front contract arrives as :attr:`UniverseChange.current` alongside the
``added`` / ``removed`` sets, not as a separate ``on_roll``.

**State authority (§3.3):** the universe, its ``epoch`` and the recentring state
are the MD controller's, persisted so that a controller restart continues from
where it was instead of re-centring. This object is a description of a
transition, not state the strategy is expected to keep — the current membership
is :meth:`~mftik.strategy.md.StrategyMd.universe`.

**Invariants:**

* **I-SEL1** no event for a contract before it appears in ``added``, and none
  after it appears in ``removed``.
* ``epoch`` increases. A change carrying an epoch no higher than the last one
  is a replay and should be ignored.
* A member that reaches its listed expiry arrives as ``on_feed_end`` first, and
  then in a ``removed`` set.
* A roll does not remove the contract it rolled off. The old future stays in the
  universe until it expires, so both books are live across a roll and
  ``current`` is the only thing that moved.
* Nothing here is driven by the strategy. There is no pin, by decision, so a
  strategy cannot hold a contract in the universe.

IF-06 defines the shape. The selector that fills it in lands in B9.
"""

from __future__ import annotations

from dataclasses import dataclass

from mftik.exchange.tickers import UniversalTicker


@dataclass(frozen=True)
class UniverseChange:
    """One transition of a selector's membership (§6.4).

    ``added`` and ``removed`` are disjoint, and at least one of them is
    non-empty unless ``current`` moved on its own — which is exactly what a
    roll inside an already-selected set looks like.
    """

    #: Contracts that are now members. Their atoms are already subscribed and
    #: their events may arrive from the moment this hook returns (I-SEL1).
    added: tuple[UniversalTicker, ...] = ()
    #: Contracts that are no longer members. Nothing of theirs will arrive
    #: after this hook, including anything already queued when it was called.
    removed: tuple[UniversalTicker, ...] = ()
    #: The selector's generation, monotonically increasing. Survives an MD
    #: controller restart, so it is safe to compare across one.
    epoch: int = 0
    #: The front contract of a ``rolling_future``. None for an
    #: ``option_chain``, which has no single current member.
    current: UniversalTicker | None = None
