"""MD fetch plane — one subject for everyone, answers to the caller's channel."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import fakeredis.aioredis
import pytest
from mftik.broker import Broker, BrokerConfig
from mftik.exchange.errors import ExchangeError
from mftik.exchange.intervals import InvalidIntervalError
from mftik.exchange.models import BestQuote, BookLevel, Kline, OrderBook
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import (
    MD_BESTQUOTE_RESULT,
    MD_FETCH_BESTQUOTE,
    MD_FETCH_KLINES,
    MD_FETCH_ORDERBOOK,
    MD_KLINES_RESULT,
    MD_ORDERBOOK_RESULT,
    Envelope,
    MdBestQuoteResult,
    MdFetchBestQuote,
    MdFetchKlines,
    MdFetchOrderBook,
    MdKlinesResult,
    MdOrderBookResult,
    MdQueryAck,
    QueryCode,
    Topics,
)
from mftik_md.fetch import FetchSession, NoReaderError

VENUE = "Gate"
SYMBOL = "BTCUSDT"
TICKER = UniversalTicker.of(VENUE, "Spot", SYMBOL)
REPLY = Topics.md_fetch_reply("caller-1")


def _kline() -> Kline:
    return Kline(
        universal_ticker=f"Paper_Spot_{SYMBOL}",
        interval="1h",
        open_time=1_700_000_000,
        open=Decimal("60100"),
        high=Decimal("60900"),
        low=Decimal("59900"),
        close=Decimal("60500"),
        volume=Decimal("100"),
        quote_volume=Decimal("6000000"),
        closed=True,
    )


class FakeReader:
    def __init__(self, venue: str = VENUE) -> None:
        self.venue = venue
        self.calls: list[tuple[str, str, int]] = []
        self.connects = 0
        self.closes = 0
        self.gate: asyncio.Event | None = None
        self.raises: BaseException | None = None
        self.klines: list[Kline] = [_kline()]
        self.book_calls: list[tuple[str, int]] = []
        self.book: OrderBook | None = None
        self.quote: BestQuote | None = None

    async def connect(self) -> None:
        self.connects += 1

    async def close(self) -> None:
        self.closes += 1

    async def fetch_klines(
        self, ticker: UniversalTicker, interval: str, *, limit: int
    ) -> list[Kline]:
        self.calls.append((ticker.symbol, interval, limit))
        if self.gate is not None:
            await self.gate.wait()
        if self.raises is not None:
            raise self.raises
        return list(self.klines)

    async def fetch_order_book(
        self, ticker: UniversalTicker, *, depth: int
    ) -> OrderBook:
        self.book_calls.append((ticker.symbol, depth))
        if self.gate is not None:
            await self.gate.wait()
        if self.raises is not None:
            raise self.raises
        return self.book or OrderBook(universal_ticker=str(ticker), bids=[], asks=[])

    async def fetch_best_quote(self, ticker: UniversalTicker) -> BestQuote | None:
        if self.gate is not None:
            await self.gate.wait()
        if self.raises is not None:
            raise self.raises
        return self.quote


class FakeFactory:
    def __init__(self, reader: FakeReader) -> None:
        self.reader = reader
        self.built: list[str] = []

    async def create(self, venue: str) -> FakeReader:
        self.built.append(venue)
        if venue == "Paper":
            raise NoReaderError("the paper venue serves no on-demand reads")
        if venue != VENUE:
            raise NoReaderError(f"no reader for venue {venue!r}")
        return self.reader


class GateStyleError(ExchangeError):
    """Stands in for ``GateRestError``: carries a venue ``label``."""

    def __init__(self, label: str, message: str) -> None:
        self.label = label
        super().__init__(f"{label}: {message}")


@pytest.fixture
async def broker() -> Broker:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test-fetch"),
        redis_client=redis,
    )
    await client.connect()
    yield client
    await client.close()
    await redis.aclose()


@pytest.fixture
def reader() -> FakeReader:
    return FakeReader()


class Caller:
    """Sends queries and collects whatever lands on its own reply channel."""

    def __init__(self, broker: Broker, channel: str = REPLY) -> None:
        self.broker = broker
        self.channel = channel
        self.results: list[Any] = []
        self._stop = asyncio.Event()
        self._task: asyncio.Task[Any] | None = None

    async def listen(self) -> None:
        models = {
            MD_KLINES_RESULT: MdKlinesResult,
            MD_ORDERBOOK_RESULT: MdOrderBookResult,
            MD_BESTQUOTE_RESULT: MdBestQuoteResult,
        }

        async def _pump() -> None:
            async for env in self.broker.subscribe(self.channel, stop=self._stop):
                model = models.get(env.type)
                if model is not None:
                    self.results.append(model.model_validate(env.payload))

        self._task = asyncio.create_task(_pump())
        await asyncio.sleep(0.05)

    async def close(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def ask(
        self,
        *,
        ticker: str = str(TICKER),
        interval: str = "1h",
        limit: int = 100,
        query_id: str = "q1",
        reply_channel: str | None = None,
        type: str = MD_FETCH_KLINES,
        payload: Any = None,
        timeout: float = 2.0,
    ) -> MdQueryAck:
        body = (
            payload
            if payload is not None
            else MdFetchKlines(
                reply_channel=self.channel if reply_channel is None else reply_channel,
                query_id=query_id,
                ticker=ticker,
                interval=interval,
                limit=limit,
            )
        )
        reply = await self.broker.request(
            Topics.md_fetch(),
            Envelope[Any].wrap(body, type=type, source="test"),
            timeout=timeout,
        )
        return MdQueryAck.model_validate(reply.payload)

    async def next_result(self, timeout: float = 2.0, model: Any = None) -> Any:
        deadline = asyncio.get_running_loop().time() + timeout
        while not self.results:
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError("no result arrived")
            await asyncio.sleep(0.02)
        result = self.results.pop(0)
        if model is not None:
            assert isinstance(result, model), type(result)
        return result


@pytest.fixture
async def fetch(broker: Broker, reader: FakeReader):
    session = FetchSession(broker, FakeFactory(reader))
    await session.start()
    await asyncio.sleep(0.05)
    yield session
    await session.stop()


@pytest.fixture
async def caller(broker: Broker):
    c = Caller(broker)
    await c.listen()
    yield c
    await c.close()


# --- the happy path --------------------------------------------------------


async def test_a_query_is_acked_then_answered_on_the_callers_channel(
    fetch: FetchSession, caller: Caller, reader: FakeReader
) -> None:
    ack = await caller.ask(interval="1h", limit=3)

    assert ack.accepted is True
    assert ack.error_code == QueryCode.NONE
    assert ack.query_id == "q1"

    result = await caller.next_result()
    assert result.ok is True
    assert result.query_id == "q1"
    assert result.klines[0].close == Decimal("60500")
    assert reader.calls == [(SYMBOL, "1h", 3)]


async def test_no_feed_subscription_is_needed(
    fetch: FetchSession, caller: Caller
) -> None:
    """The point of the plane: nothing was ever attached or subscribed, and
    the venue still answers."""
    assert fetch.venues == []

    await caller.ask()
    result = await caller.next_result()

    assert result.ok is True
    assert fetch.venues == [VENUE]


async def test_the_answer_follows_the_request_not_the_caller(
    broker: Broker, fetch: FetchSession
) -> None:
    """Routing rides on the request, so the session needs no idea who asked."""
    elsewhere = Caller(broker, channel=Topics.md_fetch_reply("somewhere-else"))
    await elsewhere.listen()
    unrelated = Caller(broker, channel=Topics.md_fetch_reply("unrelated"))
    await unrelated.listen()

    await unrelated.ask(reply_channel=elsewhere.channel, query_id="routed")

    result = await elsewhere.next_result()
    assert result.query_id == "routed"
    assert unrelated.results == []

    await elsewhere.close()
    await unrelated.close()


async def test_the_ack_lands_before_the_venue_answers(
    fetch: FetchSession, caller: Caller, reader: FakeReader
) -> None:
    reader.gate = asyncio.Event()

    ack = await caller.ask()
    assert ack.accepted is True
    await asyncio.sleep(0.05)
    assert reader.calls
    assert caller.results == []

    reader.gate.set()
    assert (await caller.next_result()).ok is True


async def test_a_slow_query_does_not_block_the_next_one(
    fetch: FetchSession, caller: Caller, reader: FakeReader
) -> None:
    reader.gate = asyncio.Event()

    assert (await caller.ask(query_id="slow")).accepted is True
    assert (await caller.ask(query_id="fast", timeout=1.0)).accepted is True

    reader.gate.set()
    ids = {(await caller.next_result()).query_id for _ in range(2)}
    assert ids == {"slow", "fast"}


async def test_a_venue_reader_is_built_once_and_kept(
    fetch: FetchSession, caller: Caller, reader: FakeReader
) -> None:
    """Built on first use and held for the process, so later queries do not
    pay for a connect."""
    for i in range(3):
        await caller.ask(query_id=f"q{i}")
        await caller.next_result()

    assert reader.connects == 1


async def test_concurrent_first_queries_build_one_reader(
    fetch: FetchSession, caller: Caller, reader: FakeReader
) -> None:
    """Without the per-venue lock each would build a client and one would be
    dropped still holding an open connection."""
    reader.gate = asyncio.Event()
    for i in range(4):
        await caller.ask(query_id=f"c{i}")
    reader.gate.set()
    for _ in range(4):
        await caller.next_result()

    assert reader.connects == 1


# --- refusals at the ack ---------------------------------------------------


async def test_unsupported_request_type_is_refused(
    fetch: FetchSession, caller: Caller
) -> None:
    ack = await caller.ask(type="md.fetch.something_else")
    assert ack.accepted is False
    assert ack.error_code == QueryCode.MD_UNSUPPORTED_REQUEST


async def test_unreadable_payload_is_refused(
    fetch: FetchSession, caller: Caller
) -> None:
    ack = await caller.ask(payload={"nonsense": True})
    assert ack.accepted is False
    assert ack.error_code == QueryCode.MD_INVALID_REQUEST


async def test_a_query_with_nowhere_to_answer_is_refused(
    fetch: FetchSession, caller: Caller
) -> None:
    """Taking a query whose answer cannot be delivered would be a lie."""
    ack = await caller.ask(reply_channel="")
    assert ack.accepted is False
    assert ack.error_code == QueryCode.MD_INVALID_REQUEST


async def test_too_many_in_flight_is_refused_at_the_ack(
    broker: Broker, reader: FakeReader, caller: Caller
) -> None:
    session = FetchSession(broker, FakeFactory(reader), max_in_flight=2)
    await session.start()
    await asyncio.sleep(0.05)
    reader.gate = asyncio.Event()

    assert (await caller.ask(query_id="a")).accepted is True
    assert (await caller.ask(query_id="b")).accepted is True
    overflow = await caller.ask(query_id="c")

    assert overflow.accepted is False
    assert overflow.error_code == QueryCode.MD_TOO_MANY_IN_FLIGHT

    reader.gate.set()
    await session.stop()


# --- failures after the ack ------------------------------------------------


async def test_a_venue_that_serves_no_reads_says_so(
    fetch: FetchSession, caller: Caller
) -> None:
    """Distinct from an empty answer, and settled before any call."""
    await caller.ask(ticker="Paper_Spot_BTCUSDT")
    result = await caller.next_result()

    assert result.ok is False
    assert result.error_code == QueryCode.MD_VENUE_UNSUPPORTED_READ


async def test_a_venue_failure_still_produces_a_result(
    fetch: FetchSession, caller: Caller, reader: FakeReader
) -> None:
    reader.raises = GateStyleError("TOO_MANY_REQUESTS", "slow down")

    assert (await caller.ask()).accepted is True
    result = await caller.next_result()

    assert result.ok is False
    assert result.klines == []
    assert result.error_code == QueryCode.VENUE_RATE_LIMITED
    assert "slow down" in result.reason


async def test_an_unsupported_interval_maps_to_its_own_code(
    fetch: FetchSession, caller: Caller, reader: FakeReader
) -> None:
    reader.raises = InvalidIntervalError("Gate serves no 2w candles")

    await caller.ask(interval="2w")
    result = await caller.next_result()

    assert result.ok is False
    assert result.error_code == QueryCode.MD_INTERVAL_NOT_SUPPORTED


async def test_an_empty_answer_is_a_success(
    fetch: FetchSession, caller: Caller, reader: FakeReader
) -> None:
    reader.klines = []

    await caller.ask()
    result = await caller.next_result()

    assert result.ok is True
    assert result.klines == []
    assert result.error_code == QueryCode.NONE


async def test_an_unmapped_venue_label_passes_through(
    fetch: FetchSession, caller: Caller, reader: FakeReader
) -> None:
    reader.raises = GateStyleError("SOME_NEW_LABEL", "who knows")

    await caller.ask()
    result = await caller.next_result()

    assert result.error_code == "SOME_NEW_LABEL"


# --- lifecycle -------------------------------------------------------------


async def test_stopping_closes_every_reader(
    broker: Broker, reader: FakeReader, caller: Caller
) -> None:
    session = FetchSession(broker, FakeFactory(reader))
    await session.start()
    await asyncio.sleep(0.05)
    await caller.ask()
    await caller.next_result()

    await session.stop()

    assert reader.closes == 1
    assert session.venues == []


# --- end to end ------------------------------------------------------------


async def test_a_strategy_with_no_market_data_gets_its_candles(
    broker: Broker, reader: FakeReader
) -> None:
    """The real STS session and the real fetch session, only the venue faked.

    No md_ids, no MD attach, no lease anywhere in the picture. That is the
    whole claim of this plane, and it only holds if both halves agree on the
    subject, the reply channel and the query id.
    """
    from mftik_sts.session.session import StsSession
    from mftik_sts.strategy import Strategy

    class Recording(Strategy):
        name = "fetch-e2e"
        id = 93

        def __init__(self) -> None:
            super().__init__()
            self.results: list[MdKlinesResult] = []

        async def on_fetch_klines(self, result: MdKlinesResult) -> None:
            self.results.append(result)

    session = FetchSession(broker, FakeFactory(reader))
    await session.start()
    await asyncio.sleep(0.05)

    strategy = Recording()
    sts = StsSession(
        session_id="sts-fetch-e2e",
        broker=broker,
        created_by=1,
        strategy=strategy,
        md_ids=[],
        heartbeat_interval=0.1,
    )
    await sts.start()
    await asyncio.sleep(0.05)

    query_id = await strategy.mds.fetch_klines(TICKER, " 1MO ", limit=5)
    assert query_id is not None, strategy.mds.last_reject_reason

    deadline = asyncio.get_running_loop().time() + 3.0
    while not strategy.results:
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("no result reached the strategy")
        await asyncio.sleep(0.02)

    result = strategy.results[0]
    assert result.query_id == query_id
    assert result.ok is True
    assert result.klines[0].close == Decimal("60500")
    # Normalized on the way out, and the venue saw the canonical spelling.
    assert reader.calls == [(SYMBOL, "1mo", 5)]

    await sts.stop()
    await session.stop()


# --- other reads -----------------------------------------------------------


async def test_an_order_book_query_comes_back_as_a_book(
    broker: Broker, caller: Caller
) -> None:
    reader = FakeReader()
    reader.book = OrderBook(
        universal_ticker=f"Paper_Spot_{SYMBOL}",
        bids=[BookLevel(price=Decimal("59999"), qty=Decimal("3"))],
        asks=[BookLevel(price=Decimal("60001"), qty=Decimal("1"))],
    )
    session = FetchSession(broker, FakeFactory(reader))
    await session.start()
    await asyncio.sleep(0.05)

    ack = await caller.ask(type=MD_FETCH_ORDERBOOK, payload=_book_req(depth=5))
    assert ack.accepted is True

    result = await caller.next_result(model=MdOrderBookResult)
    assert result.ok is True
    assert result.book.bids[0].price == Decimal("59999")
    assert reader.book_calls == [(SYMBOL, 5)]

    await session.stop()


async def test_a_best_quote_query_comes_back_as_a_quote(
    broker: Broker, caller: Caller
) -> None:
    reader = FakeReader()
    reader.quote = BestQuote(
        universal_ticker=f"Paper_Spot_{SYMBOL}",
        bid=Decimal("59999"),
        bid_qty=Decimal("3"),
        ask=Decimal("60001"),
        ask_qty=Decimal("1"),
    )
    session = FetchSession(broker, FakeFactory(reader))
    await session.start()
    await asyncio.sleep(0.05)

    await caller.ask(type=MD_FETCH_BESTQUOTE, payload=_quote_req())
    result = await caller.next_result(model=MdBestQuoteResult)

    assert result.ok is True
    assert result.quote.bid == Decimal("59999")
    assert result.quote.ask_qty == Decimal("1")

    await session.stop()


async def test_a_one_sided_book_is_a_success_with_no_quote(
    broker: Broker, caller: Caller
) -> None:
    """Not an error, and not a quote either — there is nothing to rest against,
    and zeros would answer that question wrongly rather than decline it."""
    reader = FakeReader()
    reader.quote = None
    session = FetchSession(broker, FakeFactory(reader))
    await session.start()
    await asyncio.sleep(0.05)

    await caller.ask(type=MD_FETCH_BESTQUOTE, payload=_quote_req())
    result = await caller.next_result(model=MdBestQuoteResult)

    assert result.ok is True
    assert result.quote is None
    assert result.error_code == QueryCode.NONE

    await session.stop()


async def test_a_read_the_venue_does_not_serve_is_refused_by_name(
    broker: Broker, caller: Caller
) -> None:
    """A reader without the method is the same answer as no reader at all."""

    class KlinesOnly(FakeReader):
        fetch_order_book = None

    session = FetchSession(broker, FakeFactory(KlinesOnly()))
    await session.start()
    await asyncio.sleep(0.05)

    await caller.ask(type=MD_FETCH_ORDERBOOK, payload=_book_req())
    result = await caller.next_result(model=MdOrderBookResult)

    assert result.ok is False
    assert result.error_code == QueryCode.MD_VENUE_UNSUPPORTED_READ
    assert "fetch_order_book" in result.reason

    await session.stop()


def _book_req(depth: int = 10) -> MdFetchOrderBook:
    return MdFetchOrderBook(
        reply_channel=REPLY, query_id="q1", ticker=str(TICKER), depth=depth
    )


def _quote_req() -> MdFetchBestQuote:
    return MdFetchBestQuote(
        reply_channel=REPLY, query_id="q1", ticker=str(TICKER)
    )
