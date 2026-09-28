"""MD cuts dated instruments at listed expiry and does not reopen them."""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.exchange import PaperExchange
from mftik.exchange.models import FeedEnd
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import (
    MD_FEED_END,
    MD_LEASE_ACK,
    MD_ORDERBOOK,
    MD_SUBSCRIBE,
    MD_UNSUBSCRIBE,
    STS_LEASE_HEARTBEAT,
    Envelope,
    LeaseHeartbeat,
    MdAttachRequest,
    MdLeaseAck,
    MdSubscribe,
    MdUnsubscribe,
    SymbolInfo,
    Topics,
)
from mftik.symbols import SymbolNotFoundError
from mftik_md.session import PaperPublicFactory, SessionManager
from mftik_md.session.manager import AttachError
from mftik_md.session.venue import VenueSession

TICKER = UniversalTicker.parse("Paper_Spot_BTCUSDT")
ORDERBOOK = Topics.md_feed("orderbook", TICKER)
TICKER_FEED = Topics.md_feed("ticker", TICKER)


class StubSymbols:
    """One listed expiry for whatever ticker MD asks about."""

    def __init__(self, expiry: float | None, *, is_active: bool = True) -> None:
        self.expiry = expiry
        self.is_active = is_active

    async def get(self, ticker: UniversalTicker, **_: object) -> SymbolInfo:
        return SymbolInfo(
            universal_ticker=str(ticker),
            base="BTC",
            quote="USDT",
            exch_ticker=ticker.symbol,
            expiry=self.expiry,
            is_active=self.is_active,
        )


async def _md_lease_publisher(
    broker: Broker, session_id: str, stop: asyncio.Event, *, interval: float = 0.1
) -> None:
    token = 0
    topic = Topics.sts_md_session(session_id)
    while not stop.is_set():
        token += 1
        await broker.publish(
            topic,
            Envelope[LeaseHeartbeat].wrap(
                LeaseHeartbeat(session_id=session_id, token=token),
                type=STS_LEASE_HEARTBEAT,
                source="sts",
                session_id=session_id,
            ),
        )
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except TimeoutError:
            continue


async def _wait_until(pred, *, timeout: float = 3.0) -> None:  # noqa: ANN001
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if pred():
            return
        await asyncio.sleep(0.02)
    raise TimeoutError("condition not met")


@pytest.fixture
async def broker() -> Broker:
    async with a_broker("test-md-expiry") as client:
        yield client


@pytest.fixture
async def paper() -> PaperExchange:
    async with PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")},
        tick_interval=0.05,
        seed=1,
        volatility_bps=0,
    ) as ex:
        yield ex


async def _attach(
    sessions: SessionManager,
    session_id: str,
    feeds: list[str],
) -> None:
    await sessions.attach(
        MdAttachRequest(
            session_id=session_id,
            created_by=1,
            subscriptions=feeds,
            timeout=3.0,
        )
    )


@pytest.mark.asyncio
async def test_symbol_not_found_fails_attach_and_a_later_one_can_expire(
    broker: Broker, paper: PaperExchange
) -> None:
    class Later:
        def __init__(self) -> None:
            self.calls = 0

        async def get(self, ticker: UniversalTicker, **_: object) -> SymbolInfo:
            self.calls += 1
            if self.calls == 1:
                raise SymbolNotFoundError(str(ticker))
            return _info(ticker, time.time() + 0.3)

    symbols = Later()
    sessions = SessionManager(
        PaperPublicFactory(broker, paper),
        broker,
        lease_grace=2.0,
        symbols=symbols,  # type: ignore[arg-type]
    )
    session_id = "sts-md-not-found"
    stop = asyncio.Event()
    hb_task = asyncio.create_task(_md_lease_publisher(broker, session_id, stop))
    events: list[FeedEnd] = []

    async def _collect() -> None:
        async for env in broker.subscribe(Topics.md_session(session_id), stop=stop):
            if env.type == MD_FEED_END:
                events.append(FeedEnd.model_validate(env.payload))

    collect_task = asyncio.create_task(_collect())
    await asyncio.sleep(0.05)
    with pytest.raises(AttachError, match="symbol not found") as raised:
        await _attach(sessions, session_id, [ORDERBOOK])
    assert raised.value.code == "VENUE_SYMBOL_NOT_FOUND"
    assert session_id not in sessions._links  # noqa: SLF001
    assert TICKER not in sessions._timeless  # noqa: SLF001
    await _attach(sessions, session_id, [ORDERBOOK])
    await _wait_until(lambda: len(events) == 1, timeout=3.0)
    assert events[0].state == "expired"
    assert events[0].code == "expired"
    assert events[0].topic == "orderbook"

    stop.set()
    await asyncio.gather(hb_task, collect_task, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_already_expired_cuts_every_feed_and_notifies(
    broker: Broker, paper: PaperExchange
) -> None:
    listed = time.time() - 30
    sessions = SessionManager(
        PaperPublicFactory(broker, paper),
        broker,
        lease_grace=2.0,
        symbols=StubSymbols(listed),  # type: ignore[arg-type]
    )
    session_id = "sts-md-expired"
    stop = asyncio.Event()
    hb_task = asyncio.create_task(_md_lease_publisher(broker, session_id, stop))
    events: list[FeedEnd] = []
    books: list[dict] = []

    async def _collect() -> None:
        async for env in broker.subscribe(Topics.md_session(session_id), stop=stop):
            if env.type == MD_FEED_END:
                events.append(FeedEnd.model_validate(env.payload))
            elif env.type == MD_ORDERBOOK:
                books.append(env.payload)

    collect_task = asyncio.create_task(_collect())
    await asyncio.sleep(0.05)
    result = await sessions.attach(
        MdAttachRequest(
            session_id=session_id,
            created_by=1,
            subscriptions=[ORDERBOOK, TICKER_FEED],
            timeout=3.0,
        )
    )
    await _wait_until(lambda: len(events) == 2)
    assert result.subscriptions == []
    assert sessions.feed_refcount(ORDERBOOK) == 0
    assert sessions.feed_refcount(TICKER_FEED) == 0
    assert {event.topic for event in events} == {"orderbook", "ticker"}
    assert {event.universal_ticker for event in events} == {str(TICKER)}
    assert {event.expiry for event in events} == {listed}
    assert {event.state for event in events} == {"expired"}
    assert {event.code for event in events} == {"expired"}
    assert sessions._venues == {}  # noqa: SLF001

    after = len(books)
    await asyncio.sleep(0.2)
    assert len(books) == after
    assert TICKER in sessions._expired  # noqa: SLF001

    stop.set()
    await asyncio.gather(hb_task, collect_task, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_expiry_timer_then_refuses_resubscribe(
    broker: Broker, paper: PaperExchange
) -> None:
    listed = time.time() + 0.35
    sessions = SessionManager(
        PaperPublicFactory(broker, paper),
        broker,
        lease_grace=2.0,
        symbols=StubSymbols(listed),  # type: ignore[arg-type]
    )
    session_id = "sts-md-expiry-timer"
    stop = asyncio.Event()
    hb_task = asyncio.create_task(_md_lease_publisher(broker, session_id, stop))
    events: list[FeedEnd] = []
    acks: list[int] = []

    async def _collect() -> None:
        async for env in broker.subscribe(Topics.md_session(session_id), stop=stop):
            if env.type == MD_LEASE_ACK:
                acks.append(MdLeaseAck.model_validate(env.payload).token)
            elif env.type == MD_FEED_END:
                events.append(FeedEnd.model_validate(env.payload))

    collect_task = asyncio.create_task(_collect())
    await asyncio.sleep(0.05)
    result = await sessions.attach(
        MdAttachRequest(
            session_id=session_id,
            created_by=1,
            subscriptions=[ORDERBOOK],
            timeout=3.0,
        )
    )
    assert ORDERBOOK in result.subscriptions
    await _wait_until(lambda: len(acks) >= 1)
    await _wait_until(lambda: len(events) == 1, timeout=3.0)
    assert events[0].topic == "orderbook"
    assert events[0].state == "expired"
    assert events[0].code == "expired"
    assert sessions.feed_refcount(ORDERBOOK) == 0
    assert session_id in sessions._links  # noqa: SLF001
    assert not sessions._links[session_id].subscriptions  # noqa: SLF001

    await broker.publish(
        Topics.sts_md_session(session_id),
        Envelope[MdSubscribe].wrap(
            MdSubscribe(session_id=session_id, feed=ORDERBOOK),
            type=MD_SUBSCRIBE,
            source="sts",
            session_id=session_id,
        ),
    )
    await _wait_until(lambda: len(events) == 2)
    assert events[1].topic == "orderbook"
    assert events[1].state == "expired"
    assert events[1].expiry == listed
    assert sessions.feed_refcount(ORDERBOOK) == 0

    stop.set()
    await asyncio.gather(hb_task, collect_task, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_detach_before_expiry_does_not_notify_or_tombstone(
    broker: Broker, paper: PaperExchange
) -> None:
    listed = time.time() + 0.4
    sessions = SessionManager(
        PaperPublicFactory(broker, paper),
        broker,
        lease_grace=2.0,
        symbols=StubSymbols(listed),  # type: ignore[arg-type]
    )
    session_id = "sts-md-expiry-detach"
    stop = asyncio.Event()
    hb_task = asyncio.create_task(_md_lease_publisher(broker, session_id, stop))
    events: list[FeedEnd] = []

    async def _collect() -> None:
        async for env in broker.subscribe(Topics.md_session(session_id), stop=stop):
            if env.type == MD_FEED_END:
                events.append(FeedEnd.model_validate(env.payload))

    collect_task = asyncio.create_task(_collect())
    await asyncio.sleep(0.05)
    await _attach(sessions, session_id, [ORDERBOOK])
    await sessions.detach(session_id=session_id, reason="test")
    await asyncio.sleep(0.6)
    assert events == []
    assert TICKER not in sessions._expired  # noqa: SLF001

    stop.set()
    await asyncio.gather(hb_task, collect_task, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_no_listed_expiry_leaves_feed_running(
    broker: Broker, paper: PaperExchange
) -> None:
    sessions = SessionManager(
        PaperPublicFactory(broker, paper),
        broker,
        lease_grace=2.0,
        symbols=StubSymbols(None),  # type: ignore[arg-type]
    )
    session_id = "sts-md-no-expiry"
    stop = asyncio.Event()
    hb_task = asyncio.create_task(_md_lease_publisher(broker, session_id, stop))
    events: list[FeedEnd] = []

    async def _collect() -> None:
        async for env in broker.subscribe(Topics.md_session(session_id), stop=stop):
            if env.type == MD_FEED_END:
                events.append(FeedEnd.model_validate(env.payload))

    collect_task = asyncio.create_task(_collect())
    await asyncio.sleep(0.05)
    result = await sessions.attach(
        MdAttachRequest(
            session_id=session_id,
            created_by=1,
            subscriptions=[ORDERBOOK],
            timeout=3.0,
        )
    )
    await asyncio.sleep(0.25)
    assert ORDERBOOK in result.subscriptions
    assert sessions.feed_refcount(ORDERBOOK) == 1
    assert events == []
    assert TICKER not in sessions._expired  # noqa: SLF001

    stop.set()
    await asyncio.gather(hb_task, collect_task, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_second_attach_after_expiry_is_notified_not_resubscribed(
    broker: Broker, paper: PaperExchange
) -> None:
    listed = time.time() - 5
    sessions = SessionManager(
        PaperPublicFactory(broker, paper),
        broker,
        lease_grace=2.0,
        symbols=StubSymbols(listed),  # type: ignore[arg-type]
    )
    first = "sts-md-exp-a"
    second = "sts-md-exp-b"
    stop = asyncio.Event()
    hb_a = asyncio.create_task(_md_lease_publisher(broker, first, stop))
    hb_b = asyncio.create_task(_md_lease_publisher(broker, second, stop))
    events_a: list[FeedEnd] = []
    events_b: list[FeedEnd] = []

    async def _collect(session_id: str, bucket: list[FeedEnd]) -> None:
        async for env in broker.subscribe(Topics.md_session(session_id), stop=stop):
            if env.type == MD_FEED_END:
                bucket.append(FeedEnd.model_validate(env.payload))

    collect_a = asyncio.create_task(_collect(first, events_a))
    collect_b = asyncio.create_task(_collect(second, events_b))
    await asyncio.sleep(0.05)
    result_a = await sessions.attach(
        MdAttachRequest(
            session_id=first,
            created_by=1,
            subscriptions=[ORDERBOOK, TICKER_FEED],
            timeout=3.0,
        )
    )
    await _wait_until(lambda: len(events_a) == 2)
    result_b = await sessions.attach(
        MdAttachRequest(
            session_id=second,
            created_by=1,
            subscriptions=[ORDERBOOK, TICKER_FEED],
            timeout=3.0,
        )
    )
    await _wait_until(lambda: len(events_b) == 2)
    assert result_a.subscriptions == []
    assert result_b.subscriptions == []
    assert {event.topic for event in events_a} == {"orderbook", "ticker"}
    assert {event.topic for event in events_b} == {"orderbook", "ticker"}
    assert {event.state for event in events_b} == {"expired"}
    assert len(events_a) == 2
    assert sessions.feed_refcount(ORDERBOOK) == 0

    stop.set()
    await asyncio.gather(hb_a, hb_b, collect_a, collect_b, return_exceptions=True)
    await sessions.close_all()


def _info(ticker: UniversalTicker, expiry: float | None) -> SymbolInfo:
    return SymbolInfo(
        universal_ticker=str(ticker),
        base="BTC",
        quote="USDT",
        exch_ticker=ticker.symbol,
        expiry=expiry,
    )


@pytest.mark.asyncio
async def test_runtime_subscribe_does_not_stall_lease_acks(
    broker: Broker, paper: PaperExchange
) -> None:
    started = asyncio.Event()

    class SlowSymbols:
        async def get(self, ticker: UniversalTicker, **_: object) -> SymbolInfo:
            started.set()
            await asyncio.sleep(1.2)
            return _info(ticker, None)

    sessions = SessionManager(
        PaperPublicFactory(broker, paper),
        broker,
        lease_grace=2.0,
        symbols=SlowSymbols(),  # type: ignore[arg-type]
    )
    session_id = "sts-md-slow-lookup"
    stop = asyncio.Event()
    hb_task = asyncio.create_task(
        _md_lease_publisher(broker, session_id, stop, interval=0.1)
    )
    acks: list[int] = []

    async def _collect() -> None:
        async for env in broker.subscribe(Topics.md_session(session_id), stop=stop):
            if env.type == MD_LEASE_ACK:
                acks.append(MdLeaseAck.model_validate(env.payload).token)

    collect_task = asyncio.create_task(_collect())
    await asyncio.sleep(0.05)
    await sessions.attach(
        MdAttachRequest(
            session_id=session_id,
            created_by=1,
            subscriptions=[],
            timeout=3.0,
        )
    )
    await _wait_until(lambda: len(acks) >= 1)
    await broker.publish(
        Topics.sts_md_session(session_id),
        Envelope[MdSubscribe].wrap(
            MdSubscribe(session_id=session_id, feed=ORDERBOOK),
            type=MD_SUBSCRIBE,
            source="sts",
            session_id=session_id,
        ),
    )
    await _wait_until(started.is_set)
    before = len(acks)
    await asyncio.sleep(0.45)
    assert len(acks) > before, "lease acks stalled while the symbol plane was slow"

    stop.set()
    await asyncio.gather(hb_task, collect_task, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_failed_lookup_retries_and_still_cuts(
    broker: Broker, paper: PaperExchange
) -> None:
    class FlakySymbols:
        def __init__(self) -> None:
            self.calls = 0

        async def get(self, ticker: UniversalTicker, **_: object) -> SymbolInfo:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("sym down")
            return _info(ticker, time.time() - 1)

    sessions = SessionManager(
        PaperPublicFactory(broker, paper),
        broker,
        lease_grace=2.0,
        symbols=FlakySymbols(),  # type: ignore[arg-type]
        expiry_lookup_retry_s=0.05,
    )
    session_id = "sts-md-lookup-retry"
    stop = asyncio.Event()
    hb_task = asyncio.create_task(_md_lease_publisher(broker, session_id, stop))
    events: list[FeedEnd] = []

    async def _collect() -> None:
        async for env in broker.subscribe(Topics.md_session(session_id), stop=stop):
            if env.type == MD_FEED_END:
                events.append(FeedEnd.model_validate(env.payload))

    collect_task = asyncio.create_task(_collect())
    await asyncio.sleep(0.05)
    result = await sessions.attach(
        MdAttachRequest(
            session_id=session_id,
            created_by=1,
            subscriptions=[ORDERBOOK],
            timeout=3.0,
        )
    )
    assert ORDERBOOK in result.subscriptions
    await _wait_until(lambda: len(events) == 1, timeout=3.0)
    assert sessions.feed_refcount(ORDERBOOK) == 0
    assert TICKER in sessions._expired  # noqa: SLF001

    stop.set()
    await asyncio.gather(hb_task, collect_task, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_ensure_feed_after_expiry_does_not_leave_an_orphan_pump(
    broker: Broker, paper: PaperExchange, monkeypatch: pytest.MonkeyPatch
) -> None:
    listed = time.time() + 0.2
    real = VenueSession.ensure_feed

    async def slow_ensure(
        self: VenueSession, topic: str, ticker: UniversalTicker
    ) -> None:
        if topic == "orderbook":
            await asyncio.sleep(0.45)
        await real(self, topic, ticker)

    monkeypatch.setattr(VenueSession, "ensure_feed", slow_ensure)
    sessions = SessionManager(
        PaperPublicFactory(broker, paper),
        broker,
        lease_grace=2.0,
        symbols=StubSymbols(listed),  # type: ignore[arg-type]
    )
    session_id = "sts-md-orphan-pump"
    stop = asyncio.Event()
    hb_task = asyncio.create_task(_md_lease_publisher(broker, session_id, stop))
    events: list[FeedEnd] = []

    async def _collect() -> None:
        async for env in broker.subscribe(Topics.md_session(session_id), stop=stop):
            if env.type == MD_FEED_END:
                events.append(FeedEnd.model_validate(env.payload))

    collect_task = asyncio.create_task(_collect())
    await asyncio.sleep(0.05)
    await sessions.attach(
        MdAttachRequest(
            session_id=session_id,
            created_by=1,
            subscriptions=[TICKER_FEED],
            timeout=3.0,
        )
    )
    await broker.publish(
        Topics.sts_md_session(session_id),
        Envelope[MdSubscribe].wrap(
            MdSubscribe(session_id=session_id, feed=ORDERBOOK),
            type=MD_SUBSCRIBE,
            source="sts",
            session_id=session_id,
        ),
    )
    await _wait_until(lambda: len(events) >= 1, timeout=3.0)
    await asyncio.sleep(0.55)
    assert sessions.feed_refcount(ORDERBOOK) == 0
    assert sessions.feed_refcount(TICKER_FEED) == 0
    venue = sessions._venues.get("Paper")  # noqa: SLF001
    assert venue is None or venue.feed_count == 0

    stop.set()
    await asyncio.gather(hb_task, collect_task, return_exceptions=True)
    await sessions.close_all()


def _feed_idle(sessions: SessionManager, session_id: str) -> bool:
    link = sessions._links.get(session_id)  # noqa: SLF001
    if link is None:
        return True
    return link.feed_op is None or link.feed_op.done()


async def _publish_sub(broker: Broker, session_id: str, feed: str) -> None:
    await broker.publish(
        Topics.sts_md_session(session_id),
        Envelope[MdSubscribe].wrap(
            MdSubscribe(session_id=session_id, feed=feed),
            type=MD_SUBSCRIBE,
            source="sts",
            session_id=session_id,
        ),
    )


async def _publish_unsub(broker: Broker, session_id: str, feed: str) -> None:
    await broker.publish(
        Topics.sts_md_session(session_id),
        Envelope[MdUnsubscribe].wrap(
            MdUnsubscribe(session_id=session_id, feed=feed),
            type=MD_UNSUBSCRIBE,
            source="sts",
            session_id=session_id,
        ),
    )


@pytest.mark.asyncio
async def test_runtime_unsub_waits_for_in_flight_subscribe(
    broker: Broker, paper: PaperExchange
) -> None:
    """R1: sub-then-unsub must not apply backwards and leave the feed up."""
    started = asyncio.Event()

    class SlowSymbols:
        async def get(self, ticker: UniversalTicker, **_: object) -> SymbolInfo:
            started.set()
            await asyncio.sleep(0.35)
            return _info(ticker, None)

    sessions = SessionManager(
        PaperPublicFactory(broker, paper),
        broker,
        lease_grace=2.0,
        symbols=SlowSymbols(),  # type: ignore[arg-type]
    )
    session_id = "sts-md-sub-unsub-order"
    stop = asyncio.Event()
    hb_task = asyncio.create_task(_md_lease_publisher(broker, session_id, stop))
    await asyncio.sleep(0.05)
    await sessions.attach(
        MdAttachRequest(
            session_id=session_id,
            created_by=1,
            subscriptions=[],
            timeout=3.0,
        )
    )
    await _publish_sub(broker, session_id, ORDERBOOK)
    await _wait_until(started.is_set)
    await _publish_unsub(broker, session_id, ORDERBOOK)
    await _wait_until(lambda: _feed_idle(sessions, session_id), timeout=3.0)
    assert sessions.feed_refcount(ORDERBOOK) == 0
    assert sessions._venues == {}  # noqa: SLF001

    stop.set()
    await asyncio.gather(hb_task, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_detach_during_subscribe_does_not_leave_an_orphan_pump(
    broker: Broker, paper: PaperExchange
) -> None:
    """R2: a session that dies mid-lookup must not open a venue feed."""
    started = asyncio.Event()

    class SlowSymbols:
        async def get(self, ticker: UniversalTicker, **_: object) -> SymbolInfo:
            started.set()
            await asyncio.sleep(0.35)
            return _info(ticker, None)

    sessions = SessionManager(
        PaperPublicFactory(broker, paper),
        broker,
        lease_grace=2.0,
        symbols=SlowSymbols(),  # type: ignore[arg-type]
    )
    session_id = "sts-md-detach-during-sub"
    stop = asyncio.Event()
    hb_task = asyncio.create_task(_md_lease_publisher(broker, session_id, stop))
    await asyncio.sleep(0.05)
    await sessions.attach(
        MdAttachRequest(
            session_id=session_id,
            created_by=1,
            subscriptions=[],
            timeout=3.0,
        )
    )
    await _publish_sub(broker, session_id, ORDERBOOK)
    await _wait_until(started.is_set)
    await sessions.detach(session_id=session_id, reason="test")
    await asyncio.sleep(0.45)
    assert sessions.feed_refcount(ORDERBOOK) == 0
    assert sessions._venues == {}  # noqa: SLF001
    assert session_id not in sessions._links  # noqa: SLF001

    stop.set()
    await asyncio.gather(hb_task, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_inactive_settled_instrument_expires_without_opening(
    broker: Broker, paper: PaperExchange
) -> None:
    """G1: a deactivated settled row still yields feed end, and no pump."""
    listed = time.time() - 30
    sessions = SessionManager(
        PaperPublicFactory(broker, paper),
        broker,
        lease_grace=2.0,
        symbols=StubSymbols(listed, is_active=False),  # type: ignore[arg-type]
    )
    session_id = "sts-md-inactive-settled"
    stop = asyncio.Event()
    hb_task = asyncio.create_task(_md_lease_publisher(broker, session_id, stop))
    events: list[FeedEnd] = []

    async def _collect() -> None:
        async for env in broker.subscribe(Topics.md_session(session_id), stop=stop):
            if env.type == MD_FEED_END:
                events.append(FeedEnd.model_validate(env.payload))

    collect_task = asyncio.create_task(_collect())
    await asyncio.sleep(0.05)
    result = await sessions.attach(
        MdAttachRequest(
            session_id=session_id,
            created_by=1,
            subscriptions=[ORDERBOOK],
            timeout=3.0,
        )
    )
    await _wait_until(lambda: len(events) == 1)
    assert result.subscriptions == []
    assert events[0].expiry == listed
    assert events[0].topic == "orderbook"
    assert events[0].state == "expired"
    assert events[0].code == "expired"
    assert sessions.feed_refcount(ORDERBOOK) == 0
    assert sessions._venues == {}  # noqa: SLF001

    stop.set()
    await asyncio.gather(hb_task, collect_task, return_exceptions=True)
    await sessions.close_all()
