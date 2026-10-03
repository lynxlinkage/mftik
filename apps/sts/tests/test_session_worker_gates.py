"""Gates the session worker owns, without a socket.

Order entry before ``on_ready``, the pending-table handoff, local feed
resolution, delivery through the ingress, and dispatch onto a hook. Time is a
:class:`~mftik.clock.FakeClock` or a number the test passes in. Nothing
here sleeps or opens NATS.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace

import pytest
from mftik.broker.errors import RequestTimeoutError
from mftik.clock import FakeClock
from mftik.exchange.atoms import JoinPolicy
from mftik.exchange.models import BookLevel, OrderBook, OrderType, Side
from mftik.protocol import (
    MD_ORDERBOOK,
    Envelope,
    StsCreateSessionRequest,
    UntypedEnvelope,
)
from mftik.strategy import Strategy
from mftik.strategy.errors import NotReady
from mftik.strategy.eventlog import EventLog
from mftik_sts.session_worker.dispatch import dispatch_md
from mftik_sts.session_worker.events import Inbound, StreamKind
from mftik_sts.session_worker.ingress import TD_DELIVERY_OVERFLOW, Ingress
from mftik_sts.session_worker.pending import PendingTable
from mftik_sts.session_worker.readiness import resolve_feeds
from mftik_sts.session_worker.runner import StrategyRunner


def _spec() -> StsCreateSessionRequest:
    return StsCreateSessionRequest(
        session_id="abc123", created_by=1, strategy="noop"
    )


def _book(event_id: str) -> Inbound:
    return Inbound(
        kind=StreamKind.ORDERBOOK,
        feed="orderbook.Paper_Spot_BTCUSDT",
        recv_ts=0.0,
        body=b"{}",
        event_id=event_id,
    )


def _td(event_id: str) -> Inbound:
    return Inbound(
        kind=StreamKind.TD,
        feed="td.7",
        recv_ts=0.0,
        body=b"{}",
        event_id=event_id,
    )


def _open(capacity: int) -> tuple[Ingress, StrategyRunner]:
    """Ingress in ``on_ready``. Delivery is held. Orders are already allowed."""
    ingress = Ingress(_spec(), all_capacity=capacity, must_capacity=capacity)
    ingress.start()
    runner = StrategyRunner(ingress, Strategy())
    runner.start()
    runner.begin_on_start()
    runner.end_on_start()
    runner.begin_on_ready()
    return ingress, runner


def _drain(ingress: Ingress) -> list[Inbound]:
    pulled: list[Inbound] = []
    while True:
        event = ingress.pull()
        if event is None:
            return pulled
        pulled.append(event)


def _offer_td_then_md(ingress: Ingress) -> None:
    ingress.offer(_td("fill"))
    for index in range(10):
        ingress.offer(_book(f"m{index}"))


class _SeesBooks(Strategy):
    def __init__(self) -> None:
        super().__init__()
        self.books: list[OrderBook] = []

    async def on_order_book(self, book: OrderBook) -> None:
        self.books.append(book)


def _bound(phase: str | None) -> tuple[Strategy, SimpleNamespace]:
    strategy = Strategy()
    session = SimpleNamespace(
        session_id="abc123",
        type="t",
        event_log=EventLog("abc123", directory=None),
        broker=None,
    )
    if phase is not None:
        session.order_phase = phase
    strategy.bind(session)  # type: ignore[arg-type]
    session.strategy = strategy
    return strategy, session


async def _submit(strategy: Strategy) -> None:
    await strategy.oms.submit_order(
        7,
        ticker="Paper_Spot_BTCUSDT",
        side=Side.BUY,
        type=OrderType.LIMIT,
        qty=Decimal("1"),
        price=Decimal("1"),
    )


async def test_order_entry_before_on_ready_raises_not_ready() -> None:
    for phase in ("boot", "load", "on_start"):
        strategy, _session = _bound(phase)
        with pytest.raises(NotReady):
            await _submit(strategy)
        with pytest.raises(NotReady):
            await strategy.oms.cancel_order(7, "cid")


async def test_an_inflight_cancel_before_on_ready_is_not_ready() -> None:
    """The gate sits in front of the local inflight refusal."""
    strategy, _session = _bound("on_start")
    strategy.oms._inflight.add("cid")
    with pytest.raises(NotReady):
        await strategy.oms.cancel_order(7, "cid")


async def test_a_session_without_order_phase_is_not_gated() -> None:
    """The shell and the harness have no ``order_phase``. They stay ungated."""
    strategy, _session = _bound(None)
    with pytest.raises(Exception) as caught:
        await _submit(strategy)
    assert not isinstance(caught.value, NotReady)


async def test_order_entry_from_on_ready_onward_is_not_refused() -> None:
    for phase in ("ready", "running", "stopping"):
        strategy, _session = _bound(phase)
        with pytest.raises(Exception) as caught:
            await _submit(strategy)
        assert not isinstance(caught.value, NotReady)


async def test_pending_resolves_before_the_deadline() -> None:
    clock = FakeClock()
    loop = asyncio.get_running_loop()
    table = PendingTable()
    future: asyncio.Future[str] = loop.create_future()
    table.register(
        "req",
        loop=loop,
        future=future,
        deadline=clock.monotonic() + 2.0,
        subject="td.order.7",
        timeout=2.0,
    )
    clock.advance(1.0)
    assert table.complete("req", '{"accepted":true}', now=clock.monotonic())
    assert await future == '{"accepted":true}'


async def test_pending_expires_when_the_ingress_clock_passes_the_deadline() -> None:
    clock = FakeClock()
    loop = asyncio.get_running_loop()
    table = PendingTable()
    future: asyncio.Future[str] = loop.create_future()
    table.register(
        "req",
        loop=loop,
        future=future,
        deadline=clock.monotonic() + 2.0,
        subject="td.order.7",
        timeout=2.0,
    )
    clock.advance(2.1)
    table.expire(clock.monotonic())
    with pytest.raises(RequestTimeoutError):
        await future


async def test_a_late_reply_is_a_timeout() -> None:
    clock = FakeClock()
    loop = asyncio.get_running_loop()
    table = PendingTable()
    future: asyncio.Future[str] = loop.create_future()
    table.register(
        "req",
        loop=loop,
        future=future,
        deadline=clock.monotonic() + 2.0,
        subject="td.order.7",
        timeout=2.0,
    )
    clock.advance(2.1)
    assert table.complete("req", '{"accepted":true}', now=clock.monotonic())
    with pytest.raises(RequestTimeoutError):
        await future


def test_paper_orderbook_resolves_and_other_feeds_are_missing() -> None:
    resolved, missing = resolve_feeds(
        [
            "orderbook.Paper_Spot_BTCUSDT",
            "ticker.Paper_Spot_BTCUSDT",
            "orderbook.Binance_Spot_BTCUSDT",
            "not-a-feed",
        ]
    )
    assert [feed.feed for feed in resolved] == ["orderbook.Paper_Spot_BTCUSDT"]
    assert missing == (
        "ticker.Paper_Spot_BTCUSDT",
        "orderbook.Binance_Spot_BTCUSDT",
        "not-a-feed",
    )
    atom = resolved[0].atoms[0]
    assert atom.policy is JoinPolicy.NEXT_PUSH
    assert atom.subject.startswith("md.a.Paper.")


def test_a_held_td_event_is_pulled_with_the_newest_book() -> None:
    """Capacity 4. One TD then ten books on one feed, while delivery is held.

    Order books are ``latest``. The ten prints are one slot, the newest,
    and none of them is a drop. Advancing to RUNNING pulls the TD first,
    then that book. The session does not fail.
    """
    ingress, runner = _open(4)
    reasons: list[str] = []
    ingress.set_failure_callback(reasons.append)
    _offer_td_then_md(ingress)
    assert ingress.delivery.dropped == 0
    assert ingress.delivery.mark("m0") is not None
    runner.end_on_ready()
    pulled = _drain(ingress)
    assert [event.event_id for event in pulled] == ["fill", "m9"]
    assert reasons == []
    runner.finish()
    ingress.close()


def test_a_ready_td_event_is_pulled_with_the_newest_book() -> None:
    """The same burst, offered straight into the running queue."""
    ingress, runner = _open(4)
    reasons: list[str] = []
    ingress.set_failure_callback(reasons.append)
    runner.end_on_ready()
    _offer_td_then_md(ingress)
    pulled = _drain(ingress)
    assert [event.event_id for event in pulled] == ["fill", "m9"]
    assert ingress.delivery.dropped == 0
    assert reasons == []
    runner.finish()
    ingress.close()


def test_td_only_overflow_fails_the_session() -> None:
    """Nothing but TD, past capacity, fails instead of dropping a fill.

    The event that did not fit was not accepted. The ones that did are
    still pulled, in order.
    """
    for held in (True, False):
        ingress, runner = _open(4)
        reasons: list[str] = []
        ingress.set_failure_callback(reasons.append)
        if not held:
            runner.end_on_ready()
        for index in range(5):
            ingress.offer(_td(f"t{index}"))
        assert reasons == [TD_DELIVERY_OVERFLOW]
        if held:
            runner.end_on_ready()
        pulled = _drain(ingress)
        assert [event.event_id for event in pulled] == [
            f"t{index}" for index in range(4)
        ]
        assert ingress.delivery.mark("t4") is None
        assert ingress.delivery.dropped == 0
        runner.finish()
        ingress.close()


def test_latest_keeps_the_newest_book() -> None:
    ingress = Ingress(_spec(), all_capacity=1, must_capacity=1)
    ingress.start()
    runner = StrategyRunner(ingress, Strategy())
    runner.start()
    runner.begin_on_start()
    runner.end_on_start()
    runner.begin_on_ready()
    runner.end_on_ready()
    ingress.offer(_book("old"))
    ingress.offer(_book("new"))
    got = ingress.pull()
    assert got is not None
    assert got.event_id == "new"
    assert ingress.delivery.mark("old") is not None
    assert ingress.delivery.dropped == 0
    assert ingress.pull() is None
    runner.finish()
    ingress.close()


async def test_dispatch_md_calls_the_hook_on_the_strategy_thread_side() -> None:
    strategy = _SeesBooks()
    book = OrderBook(
        universal_ticker="Paper_Spot_BTCUSDT",
        bids=[BookLevel(price=Decimal("1"), qty=Decimal("1"))],
        asks=[BookLevel(price=Decimal("2"), qty=Decimal("1"))],
    )
    envelope = Envelope[OrderBook].wrap(
        book, type=MD_ORDERBOOK, source="test", seq=1
    )
    await dispatch_md(
        strategy,
        EventLog("abc123", directory=None),
        UntypedEnvelope.from_json(envelope.to_json()),
        swallow=False,
    )
    assert len(strategy.books) == 1
    assert strategy.books[0].universal_ticker == "Paper_Spot_BTCUSDT"
