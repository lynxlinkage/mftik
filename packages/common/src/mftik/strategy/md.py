"""Strategy-side reads of market-data *availability*, not of market data.

Three different questions live here, and none of them is "what is the price":

* :meth:`StrategyMd.state` — is this feed live or down right now (F14, §5.6)
* :meth:`StrategyMd.universe` / :meth:`StrategyMd.current` — which contracts a
  ``select:`` block has chosen, and which one a rolling future is on (F33, §6.4)
* :meth:`StrategyMd.subscribe` — add feeds while the session is running (§5.1)

Prices arrive on the feed hooks (``on_ticker`` and friends) and history comes
from :mod:`mftik.strategy.mds`, which is a different accessor with a confusingly
similar name: ``self.mds`` asks a venue a question once, ``self.md`` asks the
platform about the subscriptions this session holds.

**State authority (§3.3):** nothing here is owned by the SDK, and nothing here
is a cache a strategy should fold into its own picture.

* feed live / down is the MD connection worker's. It broadcasts on ``md.w.*``;
  the session's ingress follows that and notifies through ``on_md_update``.
  Ten seconds of silence from a worker reads as ``down`` — the one place a
  state is inferred rather than reported, and it only ever produces a
  notification, never a reclaim.
* the universe, its ``epoch`` and the recentring state are the MD controller's,
  derived by a pure function from the SYM listing and a reference price and
  persisted so a controller restart does not re-centre. The session sees it on
  ``md.universe.{session_id}`` and is told through ``on_universe_change``.
* a runtime subscribe does not make this session an authority on anything. It
  patches the session's MD intent, which Postgres holds and the MD controller
  reads.

**Invariants:**

* **I-SEL1** a contract produces no event before it appears in
  ``change.added`` and none after it appears in ``change.removed``. The ingress
  subscribes the new atoms and delivers the hook before it drops the removed
  ones, and discards queued events belonging to them, so the ordering a
  strategy sees is the ordering it can rely on.
* A composite feed is ``down`` if any of its atoms is down and ``live`` only
  when all of them are back (F19), so one feed key is one answer here however
  many atoms the venue needed for it.
* ``state`` reports connectivity, never freshness. A thin option that has not
  printed in an hour is ``live``; whether its last print is too old is
  ``event.age``'s question.

IF-06 defines the surface and returns null data. The ingress that fills it in
lands in B5-05 (feed state) and B9 (universe, ``current``); the runtime
subscribe lands in B8-05.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from mftik.exchange.tickers import UniversalTicker

if TYPE_CHECKING:
    from mftik.strategy.base import Strategy

#: What a feed can be. ``live`` is subscribed and being delivered; ``down`` is
#: every other way a feed can stop arriving — the venue dropped the socket,
#: the connection worker changed incarnation, the ingress lost NATS. A feed
#: that has ended for good does not land here: that is ``on_feed_end``.
FeedState = Literal["live", "down"]


class StrategyMd:
    """Feed availability and selector universes, as a strategy reads them.

    Read-only, and deliberately so: every answer here has an authority
    elsewhere (see the module docstring), and the useful thing a strategy can
    do with one is decide — widen, hedge, wait, fail — not store it.
    """

    def __init__(self) -> None:
        self._strategy: Strategy | None = None

    def bind(self, strategy: Strategy) -> None:
        self._strategy = strategy

    def state(self, feed: str) -> FeedState | None:
        """``"live"`` or ``"down"`` for ``feed``, or None if it is not held.

        ``feed`` is a feed key as declared in ``strategy.yml`` —
        ``ticker.Deribit_Perp_BTCUSD``, ``kline_1m.Paper_Spot_BTCUSDT``.

        None means this session has no such subscription, which is a different
        answer from ``"down"`` and worth telling apart: a typo in a feed key
        would otherwise read as a feed that is merely having a bad day.

        The push side of the same fact is ``on_md_update``. Use that to react
        to a change and this to ask at a moment of the strategy's choosing —
        in a timer, or before sizing an order that needs two books.
        """
        return None

    def universe(self, name: str) -> frozenset[UniversalTicker]:
        """The contracts currently selected by the ``select:`` block ``name``.

        ``name`` is the ``select:`` name from ``strategy.yml`` (``btc_chain``),
        not a feed key: one selector produces many contracts and each of those
        carries whichever ``topics:`` the block asked for.

        Empty for a selector that has not derived anything yet, and empty for a
        name that was never declared. A strategy wanting those apart should
        read ``ready.missing_feeds`` in ``on_ready``, which says which declared
        selectors were still empty then.

        The set is the same one ``on_universe_change`` last described. Membership
        changes are the hook's business; this is for reading the membership back
        without having kept it.
        """
        return frozenset()

    def current(self, name: str) -> UniversalTicker | None:
        """The front contract of the ``rolling_future`` selector ``name``.

        None for a selector that has not chosen one yet, and None for an
        ``option_chain``, which has no single current member.

        A roll moves this ``roll_before`` the expiry, and the contract it moved
        off stays in :meth:`universe` until it actually expires — so both books
        are live across a roll and a strategy that wants the new one has to ask
        rather than assume the only member it has is current (§6.4).
        """
        return None

    async def subscribe(self, *feeds: str) -> bool:
        """Add ``feeds`` to this session's subscriptions while it runs.

        One patch of this session's MD intent, so a set of feeds arrives
        together rather than as a sequence a half-applied failure could leave
        in the middle. True once MD has taken it; False if it refused, and
        nothing was added.

        A feed added here does not affect ``on_ready``, which has already
        happened — its readiness arrives as ``on_md_update`` instead (F12).

        Raises :class:`NotImplementedError` until B8-05.
        """
        raise NotImplementedError("IF-06")
