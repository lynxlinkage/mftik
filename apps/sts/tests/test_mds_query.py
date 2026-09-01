"""STS market-data queries — ack, then the answer at ``on_fetch_klines``."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import fakeredis.aioredis
import pytest
from mftik.broker import Broker, BrokerConfig
from mftik.exchange.models import (
    BestQuote,
    BookLevel,
    FundingRate,
    Kline,
    OrderBook,
)
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import (
    MD_BESTQUOTE_RESULT,
    MD_FUNDING_HISTORY_RESULT,
    MD_KLINE,
    MD_KLINES_RESULT,
    MD_ORDERBOOK_RESULT,
    MD_QUERY_ACK,
    Envelope,
    MdBestQuoteResult,
    MdFetchFundingHistory,
    MdFetchKlines,
    MdFundingHistoryResult,
    MdKlinesResult,
    MdOrderBookResult,
    MdQueryAck,
    QueryCode,
    Topics,
    UntypedEnvelope,
)
from mftik.strategy import Strategy
from mftik_sts.session.session import StsSession

SESSION_ID = "sts-mds-1"
TICKER = UniversalTicker.parse("Gate_Spot_BTCUSDT")
FEED = f"orderbook.{TICKER}"


@pytest.fixture
async def broker() -> Broker:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test-mds"),
        redis_client=redis,
    )
    await client.connect()
    yield client
    await client.close()
    await redis.aclose()


class RecordingStrategy(Strategy):
    name = "mds-recording"
    id = 97

    def __init__(self) -> None:
        super().__init__()
        self.results: list[MdKlinesResult] = []
        self.klines: list[Kline] = []

    async def on_fetch_klines(self, result: MdKlinesResult) -> None:
        self.results.append(result)

    async def on_kline(self, kline: Kline) -> None:
        self.klines.append(kline)


def _kline() -> Kline:
    return Kline(
        universal_ticker="Gate_Spot_BTCUSDT",
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


class FakeMd:
    """Serves ``md.fetch`` the way MD's fetch session does."""

    def __init__(self, broker: Broker) -> None:
        self.broker = broker
        self.requests: list[MdFetchKlines] = []
        self.accept = True
        self.refuse_reason = "no candle reader for venue 'nowhere'"
        self.refuse_code: int | str = QueryCode.MD_VENUE_UNSUPPORTED_READ
        self.klines: list[Kline] = [_kline()]
        self.answer = True
        self._stop = asyncio.Event()
        self._task: asyncio.Task[Any] | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._serve(), name="fake-md-fetch")
        await asyncio.sleep(0.05)

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            # Never cancelled: the loop parks in a blocking BLPOP, and taking
            # it down mid-command leaves the reply unread on a pooled
            # connection. It retires on its own within a poll.
            await asyncio.gather(self._task, return_exceptions=True)

    async def _serve(self) -> None:
        async for req in self.broker.serve(Topics.md_fetch(), stop=self._stop):
            payload = MdFetchKlines.model_validate(req.envelope.payload)
            self.requests.append(payload)
            await req.reply(
                Envelope[MdQueryAck].wrap(
                    MdQueryAck(
                        query_id=payload.query_id,
                        accepted=self.accept,
                        reason="" if self.accept else self.refuse_reason,
                        error_code=(
                            QueryCode.NONE if self.accept else self.refuse_code
                        ),
                    ),
                    type=MD_QUERY_ACK,
                    source="md",
                )
            )
            if not self.accept or not self.answer:
                continue
            await self.broker.publish(
                payload.reply_channel,
                Envelope[MdKlinesResult].wrap(
                    MdKlinesResult(
                        query_id=payload.query_id,
                        ticker=payload.ticker,
                        interval=payload.interval,
                        klines=list(self.klines),
                    ),
                    type=MD_KLINES_RESULT,
                    source="md",
                ),
            )


async def _session(broker: Broker, strategy: Strategy, **kwargs) -> StsSession:
    sts = StsSession(
        session_id=SESSION_ID,
        broker=broker,
        created_by=1,
        strategy=strategy,
        heartbeat_interval=0.1,
        **kwargs,
    )
    await sts.start()
    await asyncio.sleep(0.05)
    return sts


async def _wait_until(pred, timeout: float = 2.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not pred():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition never became true")
        await asyncio.sleep(0.02)


# --- the point of the plane -------------------------------------------------


@pytest.mark.asyncio
async def test_a_session_with_no_feeds_can_still_query(broker: Broker) -> None:
    """No md_ids, no attach, no lease — and the query still works.

    This is what the fetch plane is for. The reply channel is subscribed
    unconditionally at session start, so a strategy that wants history and no
    market data never has to acquire a feed to ask.
    """
    strategy = RecordingStrategy()
    sts = await _session(broker, strategy, md_ids=[])
    md = FakeMd(broker)
    await md.start()

    query_id = await strategy.mds.fetch_klines(TICKER, "1h")

    assert query_id is not None, strategy.mds.last_reject_reason
    await _wait_until(lambda: strategy.results)
    assert strategy.results[0].query_id == query_id
    assert strategy.results[0].klines[0].close == Decimal("60500")

    await md.stop()
    await sts.stop()


@pytest.mark.asyncio
async def test_the_request_carries_this_session_s_reply_channel(
    broker: Broker,
) -> None:
    strategy = RecordingStrategy()
    sts = await _session(broker, strategy, md_ids=[])
    md = FakeMd(broker)
    await md.start()

    await strategy.mds.fetch_klines(TICKER, "1h")

    assert md.requests[0].reply_channel == Topics.md_fetch_reply(SESSION_ID)
    # Not the feed channel: that one only exists while a feed attach does.
    assert md.requests[0].reply_channel != Topics.md_session(SESSION_ID)

    await md.stop()
    await sts.stop()


# --- refusals ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_malformed_interval_is_refused_before_the_wire(
    broker: Broker,
) -> None:
    """MD could only repeat the same complaint a round trip later."""
    strategy = RecordingStrategy()
    sts = await _session(broker, strategy, md_ids=[FEED])

    query_id = await strategy.mds.fetch_klines(TICKER, "1M")

    assert query_id is None
    assert strategy.mds.last_reject_code == QueryCode.MD_INVALID_REQUEST
    # The error names the fix rather than just refusing.
    assert "1mo" in strategy.mds.last_reject_reason
    await sts.stop()


@pytest.mark.asyncio
async def test_no_md_running_reports_no_ack(broker: Broker) -> None:
    """Nothing is serving the subject, so the request rots in the list."""
    strategy = RecordingStrategy()
    sts = await _session(broker, strategy, md_ids=[FEED])
    strategy.mds._ack_timeout = 0.2

    query_id = await strategy.mds.fetch_klines(TICKER, "1h")

    assert query_id is None
    assert strategy.mds.last_reject_code == QueryCode.MD_NO_ACK
    await sts.stop()


@pytest.mark.asyncio
async def test_a_refusal_comes_back_as_none_with_its_code(broker: Broker) -> None:
    strategy = RecordingStrategy()
    sts = await _session(broker, strategy, md_ids=[FEED])
    md = FakeMd(broker)
    md.accept = False
    await md.start()

    nowhere = UniversalTicker.parse("Nowhere_Spot_BTCUSDT")
    query_id = await strategy.mds.fetch_klines(nowhere, "1h")

    assert query_id is None
    assert strategy.mds.last_reject_code == QueryCode.MD_VENUE_UNSUPPORTED_READ
    assert strategy.results == []

    await md.stop()
    await sts.stop()


@pytest.mark.asyncio
async def test_a_stale_reason_does_not_outlive_the_refusal(broker: Broker) -> None:
    strategy = RecordingStrategy()
    sts = await _session(broker, strategy, md_ids=[FEED])
    md = FakeMd(broker)
    md.accept = False
    await md.start()

    assert await strategy.mds.fetch_klines(TICKER, "1h") is None
    assert strategy.mds.last_reject_reason

    md.accept = True
    assert await strategy.mds.fetch_klines(TICKER, "1h")
    assert strategy.mds.last_reject_reason == ""
    assert strategy.mds.last_reject_code == QueryCode.NONE

    await md.stop()
    await sts.stop()


@pytest.mark.asyncio
async def test_the_interval_is_normalized_before_it_goes_on_the_wire(
    broker: Broker,
) -> None:
    strategy = RecordingStrategy()
    sts = await _session(broker, strategy, md_ids=[FEED])
    md = FakeMd(broker)
    await md.start()

    await strategy.mds.fetch_klines(TICKER, " 1H ")

    assert md.requests[0].interval == "1h"
    await md.stop()
    await sts.stop()


@pytest.mark.asyncio
async def test_query_ids_are_unique_within_a_session(broker: Broker) -> None:
    strategy = RecordingStrategy()
    sts = await _session(broker, strategy, md_ids=[FEED])
    md = FakeMd(broker)
    md.answer = False
    await md.start()

    ids = [
        await strategy.mds.fetch_klines(TICKER, "1h")
        for _ in range(3)
    ]

    assert len(set(ids)) == 3
    assert all(i is not None and i.startswith(SESSION_ID) for i in ids)
    await md.stop()
    await sts.stop()


# --- delivery ---------------------------------------------------------------


def _result_env(query_id: str, **kwargs) -> Envelope:
    return Envelope[MdKlinesResult].wrap(
        MdKlinesResult(
            query_id=query_id,
            ticker=str(TICKER),
            interval="1h",
            **kwargs,
        ),
        type=MD_KLINES_RESULT,
        source="md",
    )


@pytest.mark.asyncio
async def test_a_failed_query_still_reaches_the_hook(broker: Broker) -> None:
    """Otherwise a strategy holding a query_id cannot tell failure from delay."""
    strategy = RecordingStrategy()
    sts = await _session(broker, strategy, md_ids=[FEED])

    await broker.publish(
        Topics.md_fetch_reply(SESSION_ID),
        _result_env(
            "q9",
            ok=False,
            reason="[429] TOO_MANY_REQUESTS: slow down",
            error_code=QueryCode.VENUE_RATE_LIMITED,
        ),
    )

    await _wait_until(lambda: strategy.results)
    result = strategy.results[0]
    assert result.ok is False
    assert result.klines == []
    assert result.error_code == QueryCode.VENUE_RATE_LIMITED
    await sts.stop()


@pytest.mark.asyncio
async def test_a_query_result_is_not_a_kline_feed_push(broker: Broker) -> None:
    """``on_fetch_klines`` and ``on_kline`` are different hooks on different
    channels, and neither may leak into the other."""
    strategy = RecordingStrategy()
    sts = await _session(broker, strategy, md_ids=[FEED])

    await broker.publish(
        Topics.md_fetch_reply(SESSION_ID), _result_env("q1", klines=[_kline()])
    )
    await broker.publish(
        Topics.md_session(SESSION_ID),
        UntypedEnvelope.wrap(
            _kline().model_dump(mode="json"), type=MD_KLINE, source="md"
        ),
    )

    await _wait_until(lambda: strategy.results and strategy.klines)
    assert len(strategy.results) == 1
    assert len(strategy.klines) == 1
    assert isinstance(strategy.results[0], MdKlinesResult)
    assert isinstance(strategy.klines[0], Kline)
    await sts.stop()


@pytest.mark.asyncio
async def test_a_raising_hook_does_not_kill_the_pump(broker: Broker) -> None:
    class BrokenStrategy(RecordingStrategy):
        async def on_fetch_klines(self, result: MdKlinesResult) -> None:
            self.results.append(result)
            raise RuntimeError("boom")

    strategy = BrokenStrategy()
    sts = await _session(broker, strategy, md_ids=[FEED])

    for query_id in ("q1", "q2"):
        await broker.publish(
            Topics.md_fetch_reply(SESSION_ID), _result_env(query_id)
        )

    await _wait_until(lambda: len(strategy.results) == 2)
    assert [r.query_id for r in strategy.results] == ["q1", "q2"]
    await sts.stop()


@pytest.mark.asyncio
async def test_an_unreadable_result_is_dropped_not_fatal(broker: Broker) -> None:
    strategy = RecordingStrategy()
    sts = await _session(broker, strategy, md_ids=[FEED])

    await broker.publish(
        Topics.md_fetch_reply(SESSION_ID),
        UntypedEnvelope.wrap(
            {"not": "a result"}, type=MD_KLINES_RESULT, source="md"
        ),
    )
    await broker.publish(
        Topics.md_fetch_reply(SESSION_ID), _result_env("good")
    )

    await _wait_until(lambda: strategy.results)
    assert [r.query_id for r in strategy.results] == ["good"]
    await sts.stop()


# --- the other two reads ----------------------------------------------------


@pytest.mark.asyncio
async def test_each_kind_of_answer_reaches_its_own_hook(broker: Broker) -> None:
    """One reply channel carries all three, so the type is what routes them."""

    class Collector(RecordingStrategy):
        def __init__(self) -> None:
            super().__init__()
            self.books: list[MdOrderBookResult] = []
            self.quotes: list[MdBestQuoteResult] = []

        async def on_fetch_orderbook(self, result: MdOrderBookResult) -> None:
            self.books.append(result)

        async def on_fetch_bestquote(self, result: MdBestQuoteResult) -> None:
            self.quotes.append(result)

    strategy = Collector()
    sts = await _session(broker, strategy, md_ids=[])
    channel = Topics.md_fetch_reply(SESSION_ID)

    await broker.publish(channel, _result_env("k1", klines=[_kline()]))
    await broker.publish(
        channel,
        Envelope[MdOrderBookResult].wrap(
            MdOrderBookResult(
                query_id="b1",
                ticker=str(TICKER),
                book=OrderBook(
                    universal_ticker="Gate_Spot_BTCUSDT",
                    bids=[BookLevel(price=Decimal("59999"), qty=Decimal("3"))],
                    asks=[BookLevel(price=Decimal("60001"), qty=Decimal("1"))],
                ),
            ),
            type=MD_ORDERBOOK_RESULT,
            source="md",
        ),
    )
    await broker.publish(
        channel,
        Envelope[MdBestQuoteResult].wrap(
            MdBestQuoteResult(
                query_id="q1",
                ticker=str(TICKER),
                quote=BestQuote(
                    universal_ticker="Gate_Spot_BTCUSDT",
                    bid=Decimal("59999"),
                    bid_qty=Decimal("3"),
                    ask=Decimal("60001"),
                    ask_qty=Decimal("1"),
                ),
            ),
            type=MD_BESTQUOTE_RESULT,
            source="md",
        ),
    )

    await _wait_until(
        lambda: strategy.results and strategy.books and strategy.quotes
    )
    assert strategy.results[0].query_id == "k1"
    assert strategy.books[0].book.bids[0].price == Decimal("59999")
    assert strategy.quotes[0].quote.ask == Decimal("60001")
    await sts.stop()


@pytest.mark.asyncio
async def test_a_quote_with_nothing_resting_is_not_an_error(
    broker: Broker,
) -> None:
    """``ok`` with no quote: a side of the book was empty. A strategy checking
    whether its price can rest has nothing to check, not a quote of zero."""

    class Collector(RecordingStrategy):
        def __init__(self) -> None:
            super().__init__()
            self.quotes: list[MdBestQuoteResult] = []

        async def on_fetch_bestquote(self, result: MdBestQuoteResult) -> None:
            self.quotes.append(result)

    strategy = Collector()
    sts = await _session(broker, strategy, md_ids=[])

    await broker.publish(
        Topics.md_fetch_reply(SESSION_ID),
        Envelope[MdBestQuoteResult].wrap(
            MdBestQuoteResult(query_id="q1", ticker=str(TICKER)),
            type=MD_BESTQUOTE_RESULT,
            source="md",
        ),
    )

    await _wait_until(lambda: strategy.quotes)
    assert strategy.quotes[0].ok is True
    assert strategy.quotes[0].quote is None
    assert strategy.quotes[0].error_code == QueryCode.NONE
    await sts.stop()


@pytest.mark.asyncio
async def test_fetch_funding_history_reaches_its_own_hook(broker: Broker) -> None:
    """Ack, then the settled rows at ``on_fetch_funding_history``."""

    class Collector(RecordingStrategy):
        def __init__(self) -> None:
            super().__init__()
            self.history: list[MdFundingHistoryResult] = []

        async def on_fetch_funding_history(
            self, result: MdFundingHistoryResult
        ) -> None:
            self.history.append(result)

    class FundingMd:
        def __init__(self) -> None:
            self._stop = asyncio.Event()
            self._task: asyncio.Task[Any] | None = None
            self.requests: list[MdFetchFundingHistory] = []

        async def start(self) -> None:
            self._task = asyncio.create_task(self._serve())
            await asyncio.sleep(0.05)

        async def stop(self) -> None:
            self._stop.set()
            if self._task is not None:
                await asyncio.gather(self._task, return_exceptions=True)

        async def _serve(self) -> None:
            async for req in broker.serve(Topics.md_fetch(), stop=self._stop):
                payload = MdFetchFundingHistory.model_validate(
                    req.envelope.payload
                )
                self.requests.append(payload)
                await req.reply(
                    Envelope[MdQueryAck].wrap(
                        MdQueryAck(query_id=payload.query_id, accepted=True),
                        type=MD_QUERY_ACK,
                        source="md",
                    )
                )
                await broker.publish(
                    payload.reply_channel,
                    Envelope[MdFundingHistoryResult].wrap(
                        MdFundingHistoryResult(
                            query_id=payload.query_id,
                            ticker=payload.ticker,
                            rates=[
                                FundingRate(
                                    universal_ticker=payload.ticker,
                                    rate=Decimal("0.0001"),
                                    ts=1_700_000_000.0,
                                )
                            ],
                        ),
                        type=MD_FUNDING_HISTORY_RESULT,
                        source="md",
                    ),
                )

    strategy = Collector()
    sts = await _session(broker, strategy, md_ids=[])
    md = FundingMd()
    await md.start()

    query_id = await strategy.mds.fetch_funding_history(TICKER, limit=5)
    assert query_id is not None, strategy.mds.last_reject_reason
    await _wait_until(lambda: strategy.history)
    assert strategy.history[0].query_id == query_id
    assert strategy.history[0].rates[0].rate == Decimal("0.0001")
    assert md.requests[0].limit == 5

    await md.stop()
    await sts.stop()
