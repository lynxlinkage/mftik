"""MD cuts dated instruments at listed expiry and does not reopen them."""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.exchange import PaperExchange
from mftik.exchange.models import Expiry
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import (
    MD_EXPIRY,
    MD_LEASE_ACK,
    MD_ORDERBOOK,
    MD_SUBSCRIBE,
    STS_LEASE_HEARTBEAT,
    Envelope,
    LeaseHeartbeat,
    MdAttachRequest,
    MdLeaseAck,
    MdSubscribe,
    SymbolInfo,
    Topics,
)
from mftik_md.session import PaperPublicFactory, SessionManager

TICKER = UniversalTicker.parse("Paper_Spot_BTCUSDT")
ORDERBOOK = Topics.md_feed("orderbook", TICKER)
TICKER_FEED = Topics.md_feed("ticker", TICKER)


class StubSymbols:
    """One listed expiry for whatever ticker MD asks about."""

    def __init__(self, expiry: float | None) -> None:
        self.expiry = expiry

    async def get(self, ticker: UniversalTicker) -> SymbolInfo:
        return SymbolInfo(
            universal_ticker=str(ticker),
            base="BTC",
            quote="USDT",
            exch_ticker=ticker.symbol,
            expiry=self.expiry,
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
    events: list[Expiry] = []
    books: list[dict] = []

    async def _collect() -> None:
        async for env in broker.subscribe(
            Topics.md_session(session_id), stop=stop
        ):
            if env.type == MD_EXPIRY:
                events.append(Expiry.model_validate(env.payload))
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
    await _wait_until(lambda: len(events) == 1)
    assert result.subscriptions == []
    assert sessions.feed_refcount(ORDERBOOK) == 0
    assert sessions.feed_refcount(TICKER_FEED) == 0
    assert events[0].universal_ticker == str(TICKER)
    assert events[0].expiry == listed
    assert events[0].topics == ["orderbook", "ticker"]

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
    events: list[Expiry] = []
    acks: list[int] = []

    async def _collect() -> None:
        async for env in broker.subscribe(
            Topics.md_session(session_id), stop=stop
        ):
            if env.type == MD_LEASE_ACK:
                acks.append(MdLeaseAck.model_validate(env.payload).token)
            elif env.type == MD_EXPIRY:
                events.append(Expiry.model_validate(env.payload))

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
    assert events[0].topics == ["orderbook"]
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
    assert events[1].topics == ["orderbook"]
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
    events: list[Expiry] = []

    async def _collect() -> None:
        async for env in broker.subscribe(
            Topics.md_session(session_id), stop=stop
        ):
            if env.type == MD_EXPIRY:
                events.append(Expiry.model_validate(env.payload))

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
    events: list[Expiry] = []

    async def _collect() -> None:
        async for env in broker.subscribe(
            Topics.md_session(session_id), stop=stop
        ):
            if env.type == MD_EXPIRY:
                events.append(Expiry.model_validate(env.payload))

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
    events_a: list[Expiry] = []
    events_b: list[Expiry] = []

    async def _collect(session_id: str, bucket: list[Expiry]) -> None:
        async for env in broker.subscribe(
            Topics.md_session(session_id), stop=stop
        ):
            if env.type == MD_EXPIRY:
                bucket.append(Expiry.model_validate(env.payload))

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
    await _wait_until(lambda: len(events_a) == 1)
    result_b = await sessions.attach(
        MdAttachRequest(
            session_id=second,
            created_by=1,
            subscriptions=[ORDERBOOK, TICKER_FEED],
            timeout=3.0,
        )
    )
    await _wait_until(lambda: len(events_b) == 1)
    assert result_a.subscriptions == []
    assert result_b.subscriptions == []
    assert events_b[0].topics == ["orderbook", "ticker"]
    assert sessions.feed_refcount(ORDERBOOK) == 0

    stop.set()
    await asyncio.gather(
        hb_a, hb_b, collect_a, collect_b, return_exceptions=True
    )
    await sessions.close_all()
