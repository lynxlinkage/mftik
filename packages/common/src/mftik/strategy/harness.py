"""A fake session to test a strategy against, in one process (§9.2).

A strategy test should be about the strategy. The old ones are not: they stand
up something session-shaped, hand it doubles for the broker and the OMS, and
then drive the strategy by calling its hooks directly — so each test file has
re-invented the platform, and every change to the platform has to be made again
in all of them.

The harness is the one place that happens. It binds a strategy to a session that
exists only in memory, lets a test inject the events a real session would
deliver, and records the orders the strategy sent so a test can assert on them.
Nothing is published, nothing is subscribed, no plane has to be running, and no
test needs to know which subject anything travels on.

What a test does with it::

    harness = StrategyHarness(MyStrategy(), md=["ticker.Paper_Spot_BTCUSDT"])
    await harness.start()                     # on_start
    await harness.ready()                     # on_ready, with nothing missing
    await harness.feed(Ticker(...))           # → on_ticker
    assert harness.submitted[-1].side is Side.BUY
    await harness.account_event(api_id, Fill(...))   # → on_fill
    await harness.stop()                      # on_stop

**State authority (§3.3):** the harness stands in for the session worker, so for
the duration of a test it holds what that worker holds — the lifecycle phase,
the delivered events, the ``client_order_id`` sequence. It does not stand in for
TD or MD: an account state or a universe is whatever the test said it was, which
is the point. Nothing it holds is persisted or shared between tests.

**Invariants it preserves, because a test that does not see them is testing
something the platform does not do:**

* **I1** ordering around teardown: events injected after :meth:`stop` has begun
  still reach the strategy until ``on_stop`` returns, which is what makes a
  cancel in ``on_stop`` answerable.
* Order entry before :meth:`ready` raises
  :class:`~mftik.strategy.errors.NotReady` (F12), so a test cannot accidentally
  prove a strategy works by trading in ``on_start``.
* An account set to ``unavailable`` refuses submits locally with
  :attr:`~mftik.protocol.reject_codes.RejectCode.TD_UNAVAILABLE` (F14).
* **I-SEL1** a contract delivers no event before the ``added`` that introduced
  it and none after the ``removed`` that took it away.

IF-06 defines the API. It is built, and the 224 strategy implementation tests
move onto it, in B5-08 (F16).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from mftik.exchange.models import OrderType, Side, TimeInForce
from mftik.exchange.oms import OmsView
from mftik.strategy.base import Strategy
from mftik.strategy.md import FeedState
from mftik.strategy.ready import Ready
from mftik.strategy.td import AccountState
from mftik.strategy.universe import UniverseChange


@dataclass(frozen=True)
class SentOrder:
    """One ``submit_order`` the strategy made, as the harness saw it.

    ``accepted`` is what the call returned — False for an order the harness
    refused locally, which is the same answer TD being unavailable would give.
    """

    api_id: int
    client_order_id: str
    ticker: str
    side: Side
    qty: Decimal | None = None
    quote_qty: Decimal | None = None
    type: OrderType = OrderType.LIMIT
    price: Decimal | None = None
    tif: TimeInForce | None = None
    reduce_only: bool = False
    accepted: bool = True


@dataclass(frozen=True)
class SentCancel:
    """One ``cancel_order`` the strategy made."""

    api_id: int
    client_order_id: str
    accepted: bool = True


class StrategyHarness:
    """An in-process session for one strategy under test.

    Construct it with the strategy instance and the deployment it should
    believe in — which accounts, which feeds, which ``sts.config`` — then drive
    it through the lifecycle and the injectors below. The strategy is bound
    straight away, so ``self.paras`` and the accessors are usable before
    :meth:`start`.
    """

    def __init__(
        self,
        strategy: Strategy,
        *,
        session_id: str = "aaaaaa",
        strategy_type: str | None = None,
        td: Mapping[str, int] | None = None,
        md: Sequence[str] | None = None,
        paras: Mapping[str, Any] | None = None,
    ) -> None:
        self.strategy = strategy
        self.session_id = session_id
        self.strategy_type = strategy_type or type(strategy).__name__
        #: Account name → ``api_id``, as ``td:`` in ``strategy.yml`` resolves.
        self.td = dict(td or {})
        #: Feed keys this session believes it subscribed.
        self.md = list(md or [])
        self.paras = dict(paras or {})

    # --- lifecycle ---------------------------------------------------------

    async def start(self) -> None:
        """Run ``on_start``. Order entry is refused until :meth:`ready`.

        Raises :class:`NotImplementedError` until B5-08.
        """
        raise NotImplementedError("IF-06")

    async def ready(self, *, missing_feeds: Sequence[str] = ()) -> Ready:
        """Run ``on_ready`` and return the :class:`Ready` it was handed.

        Pass ``missing_feeds`` to test the degraded start — a strategy that must
        have every leg should fail there, and one that can work with fewer
        should say how.

        Raises :class:`NotImplementedError` until B5-08.
        """
        raise NotImplementedError("IF-06")

    async def stop(self) -> None:
        """Run ``on_stop``, with the injectors still live (I1).

        Raises :class:`NotImplementedError` until B5-08.
        """
        raise NotImplementedError("IF-06")

    # --- injection ---------------------------------------------------------

    async def feed(self, event: Any) -> None:
        """Deliver one market-data event to the hook that takes it.

        A :class:`~mftik.exchange.models.Ticker` reaches ``on_ticker``, a
        :class:`~mftik.exchange.models.Kline` reaches ``on_kline``, and so on;
        the test names the event, not the hook.

        Raises :class:`NotImplementedError` until B5-08.
        """
        raise NotImplementedError("IF-06")

    async def account_event(self, api_id: int, event: Any) -> None:
        """Deliver one private event for ``api_id`` to the hook that takes it.

        Orders, fills, rejects, balances and positions, routed by type the same
        way :meth:`feed` routes market data. Account-wide, as the real fan-out
        is: an event carrying another session's ``client_order_id`` is still
        delivered, because filtering it is ``self.owns``'s job and a strategy
        that forgets to is a strategy with a bug worth reproducing.

        Raises :class:`NotImplementedError` until B5-08.
        """
        raise NotImplementedError("IF-06")

    async def md_update(
        self, feed: str, state: FeedState, reason: str = "test"
    ) -> None:
        """Move a feed's state and call ``on_md_update`` (F14).

        The state sticks: :meth:`~mftik.strategy.md.StrategyMd.state` answers
        with it afterwards.

        Raises :class:`NotImplementedError` until B5-08.
        """
        raise NotImplementedError("IF-06")

    async def td_update(
        self, api_id: int, state: AccountState, reason: str = "test"
    ) -> None:
        """Move an account's state and call ``on_td_update`` (F14).

        The state sticks, and ``unavailable`` makes order entry refuse locally —
        which is the behaviour a test of a strategy's degraded path needs.

        Raises :class:`NotImplementedError` until B5-08.
        """
        raise NotImplementedError("IF-06")

    async def resync(
        self, api_id: int, cause: str, view: OmsView | None = None
    ) -> None:
        """Call ``on_resync`` with a settled book (F13).

        ``cause`` is ``"reconnect"`` or ``"account_reset"``. ``view`` defaults
        to the harness's own book, which is what the strategy's own orders and
        fills have made of it.

        Raises :class:`NotImplementedError` until B5-08.
        """
        raise NotImplementedError("IF-06")

    async def universe_change(self, name: str, change: UniverseChange) -> None:
        """Apply a selector change and call ``on_universe_change`` (F33).

        Applied in I-SEL1 order: ``added`` becomes deliverable before the hook
        runs, ``removed`` stops being deliverable after it returns, and a
        :meth:`feed` event for a contract outside the current universe is
        dropped rather than delivered.

        Raises :class:`NotImplementedError` until B5-08.
        """
        raise NotImplementedError("IF-06")

    # --- assertion ---------------------------------------------------------

    @property
    def submitted(self) -> tuple[SentOrder, ...]:
        """Every ``submit_order`` the strategy made, in order."""
        return ()

    @property
    def cancelled(self) -> tuple[SentCancel, ...]:
        """Every ``cancel_order`` the strategy made, in order."""
        return ()

    def view(self, api_id: int) -> OmsView:
        """The book the harness is keeping for ``api_id``.

        What ``self.oms.view()`` answers inside the strategy, so a test can
        check that the two agree without reaching into the strategy.
        """
        return OmsView()
