"""A feed that ends tells the STS sessions that held that topic."""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.exchange import PaperExchange
from mftik.exchange.models import BookLevel, FeedEnd, OrderBook
from mftik.exchange.stream import SourceEnded
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import (
    MD_FEED_END,
    MD_ORDERBOOK,
    STS_LEASE_HEARTBEAT,
    Envelope,
    LeaseHeartbeat,
    MdAttachRequest,
    Topics,
)
from mftik.symbols import SymbolNotFoundError
from mftik_md.session import PaperPublicFactory, SessionManager
from mftik_md.session.manager import AttachError, StsLink
from mftik_md.session.venue import VenueSession

FAKE = UniversalTicker.parse("Fake_Spot_BTCUSDT")
TICKER_FEED = Topics.md_feed("ticker", FAKE)
TRADE_FEED = Topics.md_feed("trade", FAKE)
BOOK_FEED = Topics.md_feed("orderbook", FAKE)
GREEKS_FEED = Topics.md_feed("greeks", FAKE)

PAPER = UniversalTicker.parse("Paper_Spot_BTCUSDT")
PAPER_BOOK = Topics.md_feed("orderbook", PAPER)
PAPER_GREEKS = Topics.md_feed("greeks", PAPER)


class _Connector:
    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None


class HoldingBook(_Connector):
    """Order book stays up. Ticker fails symbol resolution."""

    def __init__(self) -> None:
        self.release = asyncio.Event()

    def stream_order_book(self, ticker: UniversalTicker):
        return self._book(ticker)

    def stream_ticker(self, ticker: UniversalTicker):
        return self._missing(ticker)

    async def _book(self, ticker: UniversalTicker):
        yield OrderBook(
            universal_ticker=str(ticker),
            bids=[BookLevel(price=Decimal("1"), qty=Decimal("1"))],
            asks=[BookLevel(price=Decimal("2"), qty=Decimal("1"))],
        )
        await self.release.wait()

    async def _missing(self, ticker: UniversalTicker):
        raise SymbolNotFoundError(f"no such instrument: {ticker}")
        yield None


class EndTogether(_Connector):
    """Every stream ends once ``release`` is set. One socket, many feeds."""

    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.closed = asyncio.Event()

    async def close(self) -> None:
        self.closed.set()

    def stream_ticker(self, ticker: UniversalTicker):
        return self._end()

    def stream_trades(self, ticker: UniversalTicker):
        return self._end()

    async def _end(self):
        await self.release.wait()
        if False:
            yield None


class Stay(_Connector):
    def __init__(self) -> None:
        self.closed = asyncio.Event()

    async def close(self) -> None:
        self.closed.set()

    def stream_ticker(self, ticker: UniversalTicker):
        return self._stay()

    def stream_trades(self, ticker: UniversalTicker):
        return self._stay()

    async def _stay(self):
        await asyncio.Event().wait()
        if False:
            yield None


class OneEnds(_Connector):
    """Ticker ends when ``release`` is set. Trades stays up."""

    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.closed = asyncio.Event()

    async def close(self) -> None:
        self.closed.set()

    def stream_ticker(self, ticker: UniversalTicker):
        return self._end()

    def stream_trades(self, ticker: UniversalTicker):
        return self._stay()

    async def _end(self):
        await self.release.wait()
        if False:
            yield None

    async def _stay(self):
        await asyncio.Event().wait()
        if False:
            yield None


class BooksOnly(_Connector):
    """No ``stream_greeks``. Subscribing greeks is rejected by name."""

    def stream_order_book(self, ticker: UniversalTicker):
        return self._stay()

    def stream_ticker(self, ticker: UniversalTicker):
        return self._stay()

    def stream_trades(self, ticker: UniversalTicker):
        return self._stay()

    async def _stay(self):
        await asyncio.Event().wait()
        if False:
            yield None


class Sequenced:
    def __init__(self, clients: list[object]) -> None:
        self.clients = clients
        self.built: list[str] = []

    async def create(self, venue: str) -> object:
        self.built.append(venue)
        client = self.clients.pop(0)
        if isinstance(client, Exception):
            raise client
        return client


class Gated:
    """First ``create`` waits, so a second session can join the key."""

    def __init__(self, result: object) -> None:
        self.result = result
        self.entered = asyncio.Event()
        self.gate = asyncio.Event()

    async def create(self, venue: str) -> object:
        self.entered.set()
        await self.gate.wait()
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


async def _lease(broker: Broker, session_id: str, stop: asyncio.Event) -> None:
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
            await asyncio.wait_for(stop.wait(), timeout=0.1)
        except TimeoutError:
            continue


async def _wait_until(pred, *, timeout: float = 3.0) -> None:  # noqa: ANN001
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if pred():
            return
        await asyncio.sleep(0.02)
    raise TimeoutError("condition not met")


async def _collect(
    broker: Broker, session_id: str, stop: asyncio.Event, bucket: list[FeedEnd]
) -> None:
    async for env in broker.subscribe(Topics.md_session(session_id), stop=stop):
        if env.type == MD_FEED_END:
            bucket.append(FeedEnd.model_validate(env.payload))


async def _attach(
    sessions: SessionManager, session_id: str, feeds: list[str]
) -> object:
    return await sessions.attach(
        MdAttachRequest(
            session_id=session_id,
            created_by=1,
            subscriptions=feeds,
            timeout=3.0,
        )
    )


@pytest.fixture
async def broker() -> Broker:
    async with a_broker("test-md-feed-end") as client:
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


def _sessions(factory: object, broker: Broker) -> SessionManager:
    return SessionManager(factory, broker, lease_grace=2.0)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_symbol_not_found_notifies_only_that_topic(broker: Broker) -> None:
    public = HoldingBook()
    sessions = _sessions(Sequenced([public]), broker)
    stop = asyncio.Event()
    holder = "sts-feed-holder"
    book_only = "sts-feed-book"
    hb = [
        asyncio.create_task(_lease(broker, holder, stop)),
        asyncio.create_task(_lease(broker, book_only, stop)),
    ]
    held: list[FeedEnd] = []
    other: list[FeedEnd] = []
    collectors = [
        asyncio.create_task(_collect(broker, holder, stop, held)),
        asyncio.create_task(_collect(broker, book_only, stop, other)),
    ]
    await asyncio.sleep(0.05)
    await _attach(sessions, holder, [BOOK_FEED, TICKER_FEED])
    await _attach(sessions, book_only, [BOOK_FEED])
    await _wait_until(lambda: len(held) == 1)
    await asyncio.sleep(0.1)
    assert other == []
    assert held[0].topic == "ticker"
    assert held[0].state == "down"
    assert held[0].code == "symbol_not_found"
    assert held[0].reason
    assert sessions.feed_refcount(TICKER_FEED) == 0
    assert sessions.feed_refcount(BOOK_FEED) == 2
    venue = sessions._venues["Fake"]  # noqa: SLF001
    assert venue.feed_count == 1
    assert TICKER_FEED not in sessions._links[holder].subscriptions  # noqa: SLF001

    public.release.set()
    stop.set()
    await asyncio.gather(*hb, *collectors, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_transport_notifies_each_feed_and_drops_the_venue(
    broker: Broker,
) -> None:
    public = EndTogether()
    factory = Sequenced([public, Stay()])
    sessions = _sessions(factory, broker)
    stop = asyncio.Event()
    session_id = "sts-feed-transport"
    bystander = "sts-feed-bystander"
    hb = [
        asyncio.create_task(_lease(broker, session_id, stop)),
        asyncio.create_task(_lease(broker, bystander, stop)),
    ]
    events: list[FeedEnd] = []
    others: list[FeedEnd] = []
    collectors = [
        asyncio.create_task(_collect(broker, session_id, stop, events)),
        asyncio.create_task(_collect(broker, bystander, stop, others)),
    ]
    await asyncio.sleep(0.05)
    await _attach(sessions, session_id, [TICKER_FEED, TRADE_FEED])
    await _attach(sessions, bystander, [])
    venue = sessions._venues["Fake"]  # noqa: SLF001
    assert venue.feed_count == 2

    public.release.set()
    await _wait_until(lambda: len(events) == 2)
    await _wait_until(public.closed.is_set)
    await asyncio.sleep(0.05)
    assert others == []
    assert {event.topic for event in events} == {"ticker", "trade"}
    assert {event.state for event in events} == {"down"}
    assert {event.code for event in events} == {"transport"}
    assert sessions.feed_refcount(TICKER_FEED) == 0
    assert sessions.feed_refcount(TRADE_FEED) == 0
    assert sessions._venues == {}  # noqa: SLF001
    assert factory.built == ["Fake"]

    await _attach_feed(sessions, session_id, TICKER_FEED)
    await _wait_until(lambda: sessions._venues["Fake"].feed_count == 1)  # noqa: SLF001
    assert factory.built == ["Fake", "Fake"]
    assert sessions.feed_refcount(TICKER_FEED) == 1

    stop.set()
    await asyncio.gather(*hb, *collectors, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_transport_leaves_a_live_sibling_and_its_refcount(
    broker: Broker,
) -> None:
    public = OneEnds()
    sessions = _sessions(Sequenced([public]), broker)
    stop = asyncio.Event()
    session_id = "sts-feed-sibling"
    hb = asyncio.create_task(_lease(broker, session_id, stop))
    events: list[FeedEnd] = []
    collect = asyncio.create_task(_collect(broker, session_id, stop, events))
    await asyncio.sleep(0.05)
    await _attach(sessions, session_id, [TICKER_FEED, TRADE_FEED])
    venue = sessions._venues["Fake"]  # noqa: SLF001
    assert venue.feed_count == 2

    public.release.set()
    await _wait_until(lambda: len(events) == 1)
    await asyncio.sleep(0.1)
    assert events[0].topic == "ticker"
    assert events[0].code == "transport"
    assert sessions.feed_refcount(TICKER_FEED) == 0
    assert sessions.feed_refcount(TRADE_FEED) == 1
    assert sessions._venues["Fake"] is venue  # noqa: SLF001
    assert venue.feed_count == 1
    assert venue.has_feed("trade", FAKE)
    assert not public.closed.is_set()

    stop.set()
    await asyncio.gather(hb, collect, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_ended_pump_does_not_drop_a_newer_venue(broker: Broker) -> None:
    ending = EndTogether()
    replacement = Stay()
    sessions = _sessions(Sequenced([ending, replacement]), broker)
    stop = asyncio.Event()
    session_id = "sts-feed-replaced"
    hb = asyncio.create_task(_lease(broker, session_id, stop))
    events: list[FeedEnd] = []
    collect = asyncio.create_task(_collect(broker, session_id, stop, events))
    await asyncio.sleep(0.05)
    await _attach(sessions, session_id, [TICKER_FEED])
    fresh = VenueSession(
        "Fake",
        replacement,
        on_update=sessions._dispatcher.publish,  # noqa: SLF001
        on_end=sessions._on_feed_end,  # noqa: SLF001
    )
    sessions._venues["Fake"] = fresh  # noqa: SLF001

    ending.release.set()
    await _wait_until(lambda: len(events) == 1)
    await asyncio.sleep(0.05)
    assert events[0].code == "transport"
    assert sessions._venues["Fake"] is fresh  # noqa: SLF001
    assert not replacement.closed.is_set()

    stop.set()
    await asyncio.gather(hb, collect, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_unparseable_feed_fails_the_attach(broker: Broker) -> None:
    sessions = _sessions(Sequenced([Stay()]), broker)
    stop = asyncio.Event()
    session_id = "sts-feed-bad"
    hb = asyncio.create_task(_lease(broker, session_id, stop))
    events: list[FeedEnd] = []
    collect = asyncio.create_task(_collect(broker, session_id, stop, events))
    await asyncio.sleep(0.05)
    with pytest.raises(AttachError, match="invalid md feed key") as raised:
        await _attach(sessions, session_id, ["not-a-feed"])
    assert raised.value.code == "invalid_feed"
    await asyncio.sleep(0.05)
    assert events == []
    assert session_id not in sessions._links  # noqa: SLF001

    stop.set()
    await asyncio.gather(hb, collect, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_stop_feed_does_not_notify(broker: Broker) -> None:
    public = EndTogether()
    sessions = _sessions(Sequenced([public]), broker)
    stop = asyncio.Event()
    session_id = "sts-feed-stop"
    hb = asyncio.create_task(_lease(broker, session_id, stop))
    events: list[FeedEnd] = []
    collect = asyncio.create_task(_collect(broker, session_id, stop, events))
    await asyncio.sleep(0.05)
    await _attach(sessions, session_id, [TICKER_FEED])
    link = sessions._links[session_id]  # noqa: SLF001
    await sessions._unsubscribe_feed(link, TICKER_FEED)  # noqa: SLF001
    await asyncio.sleep(0.15)
    assert events == []
    assert sessions.feed_refcount(TICKER_FEED) == 0

    stop.set()
    await asyncio.gather(hb, collect, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_subscribe_during_notify_opens_a_new_pump(broker: Broker) -> None:
    ending = EndTogether()
    factory = Sequenced([ending, Stay()])
    sessions = _sessions(factory, broker)
    stop = asyncio.Event()
    first = "sts-feed-race-a"
    second = "sts-feed-race-b"
    hb = [
        asyncio.create_task(_lease(broker, first, stop)),
        asyncio.create_task(_lease(broker, second, stop)),
    ]
    events: list[FeedEnd] = []
    later: list[FeedEnd] = []
    collectors = [
        asyncio.create_task(_collect(broker, first, stop, events)),
        asyncio.create_task(_collect(broker, second, stop, later)),
    ]
    await asyncio.sleep(0.05)
    await _attach(sessions, first, [TICKER_FEED])
    await _attach(sessions, second, [])
    link_b = sessions._links[second]  # noqa: SLF001
    real = sessions._emit_feed_end

    async def _emit_then_resubscribe(
        session_ids: list[str], ticker: UniversalTicker, **kwargs: object
    ) -> None:
        await sessions._subscribe_feed(link_b, TICKER_FEED, arm=False)  # noqa: SLF001
        await real(session_ids, ticker, **kwargs)  # type: ignore[arg-type]

    sessions._emit_feed_end = _emit_then_resubscribe  # type: ignore[method-assign]
    ending.release.set()
    await _wait_until(lambda: len(events) == 1)
    await _wait_until(lambda: sessions.feed_refcount(TICKER_FEED) == 1)
    assert events[0].code == "transport"
    assert events[0].topic == "ticker"
    assert later == []
    assert factory.built == ["Fake", "Fake"]
    assert sessions._venues["Fake"].feed_count == 1  # noqa: SLF001
    assert sessions._venues["Fake"].has_feed("ticker", FAKE)  # noqa: SLF001

    stop.set()
    await asyncio.gather(*hb, *collectors, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_unsupported_topic_notifies_every_waiter(broker: Broker) -> None:
    factory = Gated(BooksOnly())
    sessions = _sessions(factory, broker)
    stop = asyncio.Event()
    first = "sts-feed-reject-a"
    second = "sts-feed-reject-b"
    hb = [
        asyncio.create_task(_lease(broker, first, stop)),
        asyncio.create_task(_lease(broker, second, stop)),
    ]
    a: list[FeedEnd] = []
    b: list[FeedEnd] = []
    collectors = [
        asyncio.create_task(_collect(broker, first, stop, a)),
        asyncio.create_task(_collect(broker, second, stop, b)),
    ]
    await asyncio.sleep(0.05)
    await _attach(sessions, first, [])
    await _attach(sessions, second, [])
    link_a = sessions._links[first]  # noqa: SLF001
    link_b = sessions._links[second]  # noqa: SLF001
    opening = asyncio.create_task(
        sessions._subscribe_feed(link_a, GREEKS_FEED, arm=False)  # noqa: SLF001
    )
    await factory.entered.wait()
    await sessions._subscribe_feed(link_b, GREEKS_FEED, arm=False)  # noqa: SLF001
    factory.gate.set()
    await opening
    await _wait_until(lambda: len(a) == 1 and len(b) == 1)
    for event in (*a, *b):
        assert event.topic == "greeks"
        assert event.state == "rejected"
        assert event.code == "unsupported"
        assert event.reason
    assert sessions.feed_refcount(GREEKS_FEED) == 0
    assert GREEKS_FEED not in link_a.subscriptions
    assert GREEKS_FEED not in link_b.subscriptions

    stop.set()
    await asyncio.gather(*hb, *collectors, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_connect_failure_notifies_every_waiter(broker: Broker) -> None:
    factory = Gated(RuntimeError("connect refused"))
    sessions = _sessions(factory, broker)
    stop = asyncio.Event()
    first = "sts-feed-connect-a"
    second = "sts-feed-connect-b"
    hb = [
        asyncio.create_task(_lease(broker, first, stop)),
        asyncio.create_task(_lease(broker, second, stop)),
    ]
    a: list[FeedEnd] = []
    b: list[FeedEnd] = []
    collectors = [
        asyncio.create_task(_collect(broker, first, stop, a)),
        asyncio.create_task(_collect(broker, second, stop, b)),
    ]
    await asyncio.sleep(0.05)
    await _attach(sessions, first, [])
    await _attach(sessions, second, [])
    link_a: StsLink = sessions._links[first]  # noqa: SLF001
    link_b: StsLink = sessions._links[second]  # noqa: SLF001
    opening = asyncio.create_task(
        sessions._subscribe_feed(link_a, TICKER_FEED, arm=False)  # noqa: SLF001
    )
    await factory.entered.wait()
    await sessions._subscribe_feed(link_b, TICKER_FEED, arm=False)  # noqa: SLF001
    factory.gate.set()
    await opening
    await _wait_until(lambda: len(a) == 1 and len(b) == 1)
    for event in (*a, *b):
        assert event.state == "down"
        assert event.code == "connect"
        assert "connect refused" in event.reason
        assert event.topic == "ticker"
    assert sessions.feed_refcount(TICKER_FEED) == 0
    assert not link_a.subscriptions
    assert not link_b.subscriptions

    stop.set()
    await asyncio.gather(*hb, *collectors, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_attach_fails_when_a_feed_cannot_open(
    broker: Broker, paper: PaperExchange
) -> None:
    sessions = SessionManager(
        PaperPublicFactory(broker, paper), broker, lease_grace=2.0
    )
    stop = asyncio.Event()
    session_id = "sts-feed-attach"
    hb = asyncio.create_task(_lease(broker, session_id, stop))
    events: list[FeedEnd] = []
    collect = asyncio.create_task(_collect(broker, session_id, stop, events))
    await asyncio.sleep(0.05)
    with pytest.raises(AttachError, match="does not publish") as raised:
        await _attach(sessions, session_id, [PAPER_BOOK, PAPER_GREEKS])
    assert raised.value.code == "MD_VENUE_UNSUPPORTED_READ"
    await asyncio.sleep(0.05)
    assert events == []
    assert session_id not in sessions._links  # noqa: SLF001
    assert sessions.feed_refcount(PAPER_GREEKS) == 0
    assert sessions.feed_refcount(PAPER_BOOK) == 0
    assert "Paper" not in sessions._venues  # noqa: SLF001

    stop.set()
    await asyncio.gather(hb, collect, return_exceptions=True)
    await sessions.close_all()


class TwoSockets(_Connector):
    """Ticker and trades share one socket. The book is another."""

    def __init__(self) -> None:
        self.market_release = asyncio.Event()
        self.more_books: asyncio.Queue[OrderBook | None] = asyncio.Queue()
        self.ticker_opens = 0
        self.closed = False

    async def close(self) -> None:
        self.closed = True

    def stream_ticker(self, ticker: UniversalTicker):
        self.ticker_opens += 1
        return self._market(self.ticker_opens)

    def stream_trades(self, ticker: UniversalTicker):
        return self._market(1)

    def stream_order_book(self, ticker: UniversalTicker):
        return self._books(ticker)

    async def _market(self, generation: int):
        if generation == 1:
            await self.market_release.wait()
            return
        await asyncio.Event().wait()
        if False:
            yield None

    async def _books(self, ticker: UniversalTicker):
        yield OrderBook(
            universal_ticker=str(ticker),
            bids=[BookLevel(price=Decimal("1"), qty=Decimal("1"))],
            asks=[BookLevel(price=Decimal("2"), qty=Decimal("1"))],
        )
        while True:
            item = await self.more_books.get()
            if item is None:
                return
            yield item


class Sticky:
    """Every create returns the same connector."""

    def __init__(self, client: TwoSockets) -> None:
        self.client = client
        self.built: list[str] = []

    async def create(self, venue: str) -> TwoSockets:
        self.built.append(venue)
        return self.client


@pytest.mark.asyncio
async def test_one_socket_ending_leaves_the_other_up(broker: Broker) -> None:
    client = TwoSockets()
    factory = Sticky(client)
    sessions = _sessions(factory, broker)
    stop = asyncio.Event()
    session_id = "sts-feed-sockets"
    hb = asyncio.create_task(_lease(broker, session_id, stop))
    events: list[FeedEnd] = []
    books = 0

    async def _collect_all() -> None:
        nonlocal books
        async for env in broker.subscribe(
            Topics.md_session(session_id), stop=stop
        ):
            if env.type == MD_FEED_END:
                events.append(FeedEnd.model_validate(env.payload))
            elif env.type == MD_ORDERBOOK:
                books += 1

    collect = asyncio.create_task(_collect_all())
    await asyncio.sleep(0.05)
    await _attach(sessions, session_id, [TICKER_FEED, TRADE_FEED, BOOK_FEED])
    await _wait_until(lambda: books >= 1)
    venue = sessions._venues["Fake"]  # noqa: SLF001
    client.market_release.set()
    await _wait_until(lambda: len(events) == 2)
    assert {event.topic for event in events} == {"ticker", "trade"}
    for event in events:
        assert event.state == "down"
        assert event.code == "transport"
    assert sessions.feed_refcount(BOOK_FEED) == 1
    assert sessions.feed_refcount(TICKER_FEED) == 0
    assert sessions._venues["Fake"] is venue  # noqa: SLF001
    assert venue.feed_count == 1
    assert not client.closed

    await client.more_books.put(
        OrderBook(
            universal_ticker=str(FAKE),
            bids=[BookLevel(price=Decimal("3"), qty=Decimal("1"))],
            asks=[BookLevel(price=Decimal("4"), qty=Decimal("1"))],
        )
    )
    await _wait_until(lambda: books >= 2)
    await _attach_feed(sessions, session_id, TICKER_FEED)
    await _wait_until(lambda: client.ticker_opens == 2)
    await asyncio.sleep(0.05)
    assert len(events) == 2
    assert factory.built == ["Fake"]
    assert sessions.feed_refcount(TICKER_FEED) == 1
    assert venue.feed_count == 2
    assert venue.has_feed("ticker", FAKE)

    stop.set()
    await asyncio.gather(hb, collect, return_exceptions=True)
    await sessions.close_all()


class GaveUp(_Connector):
    def stream_ticker(self, ticker: UniversalTicker):
        return self._end()

    async def _end(self):
        raise SourceEnded("Fake giving up after 11 reconnect attempts")
        yield None


@pytest.mark.asyncio
async def test_transport_reason_is_the_sockets_own_words(broker: Broker) -> None:
    sessions = _sessions(Sequenced([GaveUp()]), broker)
    stop = asyncio.Event()
    session_id = "sts-feed-reason"
    hb = asyncio.create_task(_lease(broker, session_id, stop))
    events: list[FeedEnd] = []
    collect = asyncio.create_task(_collect(broker, session_id, stop, events))
    await asyncio.sleep(0.05)
    await _attach(sessions, session_id, [TICKER_FEED])
    await _wait_until(lambda: len(events) == 1)
    assert events[0].state == "down"
    assert events[0].code == "transport"
    assert events[0].reason == "Fake giving up after 11 reconnect attempts"

    stop.set()
    await asyncio.gather(hb, collect, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_a_failed_open_fails_every_attach_still_waiting(
    broker: Broker,
) -> None:
    factory = Gated(RuntimeError("connect refused"))
    sessions = _sessions(factory, broker)
    stop = asyncio.Event()
    first = "sts-feed-join-a"
    second = "sts-feed-join-b"
    hb = [
        asyncio.create_task(_lease(broker, first, stop)),
        asyncio.create_task(_lease(broker, second, stop)),
    ]
    a: list[FeedEnd] = []
    b: list[FeedEnd] = []
    collectors = [
        asyncio.create_task(_collect(broker, first, stop, a)),
        asyncio.create_task(_collect(broker, second, stop, b)),
    ]
    await asyncio.sleep(0.05)
    opening = asyncio.create_task(_attach(sessions, first, [TICKER_FEED]))
    await factory.entered.wait()
    joined = asyncio.create_task(_attach(sessions, second, [TICKER_FEED]))
    await _wait_until(lambda: sessions.feed_refcount(TICKER_FEED) == 2)
    factory.gate.set()
    with pytest.raises(AttachError, match="connect refused") as first_err:
        await opening
    with pytest.raises(AttachError, match="connect refused") as second_err:
        await joined
    assert first_err.value.code == "MD_VENUE_NOT_CONNECTED"
    assert second_err.value.code == "MD_VENUE_NOT_CONNECTED"
    await asyncio.sleep(0.05)
    assert a == []
    assert b == []
    assert first not in sessions._links  # noqa: SLF001
    assert second not in sessions._links  # noqa: SLF001
    assert sessions.feed_refcount(TICKER_FEED) == 0

    stop.set()
    await asyncio.gather(*hb, *collectors, return_exceptions=True)
    await sessions.close_all()


async def _attach_feed(sessions: SessionManager, session_id: str, feed: str) -> None:
    link = sessions._links[session_id]  # noqa: SLF001
    await sessions._subscribe_feed(link, feed, arm=False)  # noqa: SLF001
