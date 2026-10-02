"""MD fetch handler — one decoded query, an ack back, the answer published.

Direct calls, no bus. The serve loop is :func:`mftik.broker.handler.serve`
and is tested with that layer. A controller roll is
``test_md_fetch_roll``.
"""

from __future__ import annotations

import ast
import asyncio
from decimal import Decimal
from pathlib import Path
from typing import Any

from mftik.exchange.errors import ExchangeError
from mftik.exchange.intervals import InvalidIntervalError
from mftik.exchange.models import (
    BestQuote,
    BookLevel,
    FundingRate,
    Kline,
    OpenInterest,
    OrderBook,
)
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import (
    MD_FETCH_BESTQUOTE,
    MD_FETCH_FUNDING_HISTORY,
    MD_FETCH_KLINES,
    MD_FETCH_OPEN_INTEREST,
    MD_FETCH_ORDERBOOK,
    Envelope,
    MdBestQuoteResult,
    MdFetchBestQuote,
    MdFetchFundingHistory,
    MdFetchKlines,
    MdFetchOpenInterest,
    MdFetchOrderBook,
    MdFundingHistoryResult,
    MdKlinesResult,
    MdOpenInterestResult,
    MdOrderBookResult,
    MdQueryAck,
    QueryCode,
    Topics,
    UntypedEnvelope,
)
from mftik_md.fetch import FetchHandler, NoReaderError
from mftik_md.fetch.readers import BinanceSpotReader, GateSpotReader

VENUE = "Gate"
SYMBOL = "BTCUSDT"
TICKER = UniversalTicker.of(VENUE, "Spot", SYMBOL)
REPLY = Topics.md_fetch_reply("caller-1")
_FETCH_SRC = Path(__file__).resolve().parents[1] / "src" / "mftik_md"


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
        self.rates: list[FundingRate] = []
        self.rate_calls: list[tuple[str, int]] = []
        self.interest: OpenInterest | None = None
        self.interest_calls: list[str] = []

    async def connect(self) -> None:
        self.connects += 1

    async def close(self) -> None:
        self.closes += 1

    async def _wait(self) -> None:
        if self.gate is not None:
            await self.gate.wait()
        if self.raises is not None:
            raise self.raises

    async def fetch_klines(
        self, ticker: UniversalTicker, interval: str, *, limit: int
    ) -> list[Kline]:
        self.calls.append((ticker.symbol, interval, limit))
        await self._wait()
        return list(self.klines)

    async def fetch_order_book(
        self, ticker: UniversalTicker, *, depth: int
    ) -> OrderBook:
        self.book_calls.append((ticker.symbol, depth))
        await self._wait()
        return self.book or OrderBook(universal_ticker=str(ticker), bids=[], asks=[])

    async def fetch_best_quote(self, ticker: UniversalTicker) -> BestQuote | None:
        await self._wait()
        return self.quote

    async def fetch_funding_history(
        self, ticker: UniversalTicker, *, limit: int
    ) -> list[FundingRate]:
        self.rate_calls.append((ticker.symbol, limit))
        await self._wait()
        return list(self.rates)

    async def fetch_open_interest(self, ticker: UniversalTicker) -> OpenInterest:
        self.interest_calls.append(ticker.symbol)
        await self._wait()
        return self.interest or OpenInterest(
            universal_ticker=str(ticker),
            qty=Decimal("1000"),
            ts=1_700_000_000.0,
        )


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


class Sink:
    """The publisher the handler was given instead of a broker."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, Envelope[Any]]] = []

    async def __call__(self, topic: str, envelope: Envelope[Any]) -> None:
        self.sent.append((topic, envelope))


def _message(type: str, payload: Any) -> UntypedEnvelope:
    wrapped = Envelope[Any].wrap(payload, type=type, source="test")
    return UntypedEnvelope.model_validate_json(wrapped.to_json())


def _ack(reply: Envelope[Any] | None) -> MdQueryAck:
    assert reply is not None
    payload = reply.payload
    if isinstance(payload, MdQueryAck):
        return payload
    return MdQueryAck.model_validate(payload)


def _klines(
    *,
    query_id: str = "q1",
    interval: str = "1h",
    limit: int = 100,
    ticker: str = str(TICKER),
    reply_channel: str = REPLY,
) -> MdFetchKlines:
    return MdFetchKlines(
        reply_channel=reply_channel,
        query_id=query_id,
        ticker=ticker,
        interval=interval,
        limit=limit,
    )


def _book_req(depth: int = 10) -> MdFetchOrderBook:
    return MdFetchOrderBook(
        reply_channel=REPLY, query_id="q1", ticker=str(TICKER), depth=depth
    )


def _quote_req() -> MdFetchBestQuote:
    return MdFetchBestQuote(
        reply_channel=REPLY, query_id="q1", ticker=str(TICKER)
    )


def _funding_req(limit: int = 100) -> MdFetchFundingHistory:
    return MdFetchFundingHistory(
        reply_channel=REPLY, query_id="q1", ticker=str(TICKER), limit=limit
    )


def _oi_req() -> MdFetchOpenInterest:
    return MdFetchOpenInterest(
        reply_channel=REPLY, query_id="q1", ticker=str(TICKER)
    )


def _handler(
    reader: FakeReader, sink: Sink, *, max_in_flight: int = 32
) -> FetchHandler:
    return FetchHandler(sink, FakeFactory(reader), max_in_flight=max_in_flight)


async def _yield_until(predicate) -> None:
    for _ in range(8):
        if predicate():
            return
        await asyncio.sleep(0)
    assert predicate()


def test_the_fetch_worker_does_not_own_the_serve_loop() -> None:
    """``serve`` is called by name. The handler never sees a request handle."""
    for name in ("worker.py", "session.py"):
        path = _FETCH_SRC / "fetch" / name
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "serve"
            ):
                raise AssertionError(f"{name}:{node.lineno} calls .serve")
            if isinstance(node, ast.ImportFrom) and any(
                alias.name == "IncomingRequest" for alias in node.names
            ):
                raise AssertionError(f"{name}:{node.lineno} imports IncomingRequest")


async def test_a_query_is_acked_then_answered_on_the_callers_channel() -> None:
    reader = FakeReader()
    sink = Sink()
    handler = _handler(reader, sink)

    ack = _ack(await handler(_message(MD_FETCH_KLINES, _klines(limit=3))))

    assert ack.accepted is True
    assert ack.error_code == QueryCode.NONE
    assert ack.query_id == "q1"
    await handler.wait_idle()
    topic, envelope = sink.sent[0]
    result = envelope.payload
    assert isinstance(result, MdKlinesResult)
    assert topic == REPLY
    assert result.ok is True
    assert result.query_id == "q1"
    assert result.klines[0].close == Decimal("60500")
    assert reader.calls == [(SYMBOL, "1h", 3)]


async def test_no_feed_subscription_is_needed() -> None:
    """Nothing was attached or subscribed, and the venue still answers."""
    reader = FakeReader()
    sink = Sink()
    handler = _handler(reader, sink)
    assert handler.venues == []

    await handler(_message(MD_FETCH_KLINES, _klines()))
    await handler.wait_idle()

    result = sink.sent[0][1].payload
    assert isinstance(result, MdKlinesResult)
    assert result.ok is True
    assert handler.venues == [VENUE]


async def test_the_answer_follows_the_request_not_the_caller() -> None:
    """Routing rides on the request, so the handler needs no idea who asked."""
    elsewhere = Topics.md_fetch_reply("somewhere-else")
    sink = Sink()
    handler = _handler(FakeReader(), sink)

    await handler(
        _message(
            MD_FETCH_KLINES,
            _klines(query_id="routed", reply_channel=elsewhere),
        )
    )
    await handler.wait_idle()

    assert [topic for topic, _envelope in sink.sent] == [elsewhere]
    result = sink.sent[0][1].payload
    assert isinstance(result, MdKlinesResult)
    assert result.query_id == "routed"


async def test_the_ack_lands_before_the_venue_answers() -> None:
    reader = FakeReader()
    reader.gate = asyncio.Event()
    sink = Sink()
    handler = _handler(reader, sink)

    ack = _ack(await handler(_message(MD_FETCH_KLINES, _klines())))
    assert ack.accepted is True
    await _yield_until(lambda: bool(reader.calls))
    assert sink.sent == []

    reader.gate.set()
    await handler.wait_idle()
    result = sink.sent[0][1].payload
    assert isinstance(result, MdKlinesResult)
    assert result.ok is True


async def test_a_slow_query_does_not_block_the_next_one() -> None:
    reader = FakeReader()
    reader.gate = asyncio.Event()
    sink = Sink()
    handler = _handler(reader, sink)

    slow = _ack(
        await handler(_message(MD_FETCH_KLINES, _klines(query_id="slow")))
    )
    fast = _ack(
        await handler(_message(MD_FETCH_KLINES, _klines(query_id="fast")))
    )
    assert slow.accepted is True
    assert fast.accepted is True
    assert sink.sent == []

    reader.gate.set()
    await handler.wait_idle()
    ids = set()
    for _topic, envelope in sink.sent:
        assert isinstance(envelope.payload, MdKlinesResult)
        ids.add(envelope.payload.query_id)
    assert ids == {"slow", "fast"}


async def test_a_venue_reader_is_built_once_and_kept() -> None:
    reader = FakeReader()
    handler = _handler(reader, Sink())

    for i in range(3):
        await handler(_message(MD_FETCH_KLINES, _klines(query_id=f"q{i}")))
        await handler.wait_idle()

    assert reader.connects == 1


async def test_concurrent_first_queries_build_one_reader() -> None:
    """Without the per-venue lock each would build a client."""
    reader = FakeReader()
    reader.gate = asyncio.Event()
    handler = _handler(reader, Sink())

    for i in range(4):
        ack = _ack(
            await handler(_message(MD_FETCH_KLINES, _klines(query_id=f"c{i}")))
        )
        assert ack.accepted is True
    await _yield_until(lambda: reader.connects == 1)
    assert reader.connects == 1

    reader.gate.set()
    await handler.wait_idle()
    assert reader.connects == 1


async def test_unsupported_request_type_is_refused() -> None:
    handler = _handler(FakeReader(), Sink())
    ack = _ack(await handler(_message("md.fetch.something_else", {})))
    assert ack.accepted is False
    assert ack.error_code == QueryCode.MD_UNSUPPORTED_REQUEST


async def test_unreadable_payload_is_refused() -> None:
    handler = _handler(FakeReader(), Sink())
    ack = _ack(await handler(_message(MD_FETCH_KLINES, {"nonsense": True})))
    assert ack.accepted is False
    assert ack.error_code == QueryCode.MD_INVALID_REQUEST


async def test_a_query_with_nowhere_to_answer_is_refused() -> None:
    handler = _handler(FakeReader(), Sink())
    ack = _ack(
        await handler(_message(MD_FETCH_KLINES, _klines(reply_channel="")))
    )
    assert ack.accepted is False
    assert ack.error_code == QueryCode.MD_INVALID_REQUEST


async def test_too_many_in_flight_is_refused_at_the_ack() -> None:
    reader = FakeReader()
    reader.gate = asyncio.Event()
    handler = _handler(reader, Sink(), max_in_flight=2)

    assert _ack(
        await handler(_message(MD_FETCH_KLINES, _klines(query_id="a")))
    ).accepted
    assert _ack(
        await handler(_message(MD_FETCH_KLINES, _klines(query_id="b")))
    ).accepted
    overflow = _ack(
        await handler(_message(MD_FETCH_KLINES, _klines(query_id="c")))
    )

    assert overflow.accepted is False
    assert overflow.error_code == QueryCode.MD_TOO_MANY_IN_FLIGHT

    reader.gate.set()
    await handler.aclose()


async def test_a_venue_that_serves_no_reads_says_so() -> None:
    sink = Sink()
    handler = _handler(FakeReader(), sink)
    ack = _ack(
        await handler(
            _message(MD_FETCH_KLINES, _klines(ticker="Paper_Spot_BTCUSDT"))
        )
    )
    assert ack.accepted is True
    await handler.wait_idle()
    result = sink.sent[0][1].payload
    assert isinstance(result, MdKlinesResult)
    assert result.ok is False
    assert result.error_code == QueryCode.MD_VENUE_UNSUPPORTED_READ


async def test_a_venue_failure_still_produces_a_result() -> None:
    reader = FakeReader()
    reader.raises = GateStyleError("TOO_MANY_REQUESTS", "slow down")
    sink = Sink()
    handler = _handler(reader, sink)

    assert _ack(await handler(_message(MD_FETCH_KLINES, _klines()))).accepted
    await handler.wait_idle()
    result = sink.sent[0][1].payload
    assert isinstance(result, MdKlinesResult)
    assert result.ok is False
    assert result.klines == []
    assert result.error_code == QueryCode.VENUE_RATE_LIMITED
    assert "slow down" in result.reason


async def test_an_unsupported_interval_maps_to_its_own_code() -> None:
    reader = FakeReader()
    reader.raises = InvalidIntervalError("Gate serves no 2w candles")
    sink = Sink()
    handler = _handler(reader, sink)

    await handler(_message(MD_FETCH_KLINES, _klines(interval="2w")))
    await handler.wait_idle()
    result = sink.sent[0][1].payload
    assert isinstance(result, MdKlinesResult)
    assert result.ok is False
    assert result.error_code == QueryCode.MD_INTERVAL_NOT_SUPPORTED


async def test_an_empty_answer_is_a_success() -> None:
    reader = FakeReader()
    reader.klines = []
    sink = Sink()
    handler = _handler(reader, sink)

    await handler(_message(MD_FETCH_KLINES, _klines()))
    await handler.wait_idle()
    result = sink.sent[0][1].payload
    assert isinstance(result, MdKlinesResult)
    assert result.ok is True
    assert result.klines == []
    assert result.error_code == QueryCode.NONE


async def test_an_unmapped_venue_label_passes_through() -> None:
    reader = FakeReader()
    reader.raises = GateStyleError("SOME_NEW_LABEL", "who knows")
    sink = Sink()
    handler = _handler(reader, sink)

    await handler(_message(MD_FETCH_KLINES, _klines()))
    await handler.wait_idle()
    result = sink.sent[0][1].payload
    assert isinstance(result, MdKlinesResult)
    assert result.error_code == "SOME_NEW_LABEL"


async def test_stopping_closes_every_reader() -> None:
    reader = FakeReader()
    handler = _handler(reader, Sink())
    await handler(_message(MD_FETCH_KLINES, _klines()))
    await handler.wait_idle()

    await handler.aclose()

    assert reader.closes == 1
    assert handler.venues == []


async def test_an_order_book_query_comes_back_as_a_book() -> None:
    reader = FakeReader()
    reader.book = OrderBook(
        universal_ticker=f"Paper_Spot_{SYMBOL}",
        bids=[BookLevel(price=Decimal("59999"), qty=Decimal("3"))],
        asks=[BookLevel(price=Decimal("60001"), qty=Decimal("1"))],
    )
    sink = Sink()
    handler = _handler(reader, sink)

    ack = _ack(await handler(_message(MD_FETCH_ORDERBOOK, _book_req(depth=5))))
    assert ack.accepted is True
    await handler.wait_idle()
    result = sink.sent[0][1].payload
    assert isinstance(result, MdOrderBookResult)
    assert result.ok is True
    assert result.book is not None
    assert result.book.bids[0].price == Decimal("59999")
    assert reader.book_calls == [(SYMBOL, 5)]


async def test_a_best_quote_query_comes_back_as_a_quote() -> None:
    reader = FakeReader()
    reader.quote = BestQuote(
        universal_ticker=f"Paper_Spot_{SYMBOL}",
        bid=Decimal("59999"),
        bid_qty=Decimal("3"),
        ask=Decimal("60001"),
        ask_qty=Decimal("1"),
    )
    sink = Sink()
    handler = _handler(reader, sink)

    await handler(_message(MD_FETCH_BESTQUOTE, _quote_req()))
    await handler.wait_idle()
    result = sink.sent[0][1].payload
    assert isinstance(result, MdBestQuoteResult)
    assert result.ok is True
    assert result.quote is not None
    assert result.quote.bid == Decimal("59999")
    assert result.quote.ask_qty == Decimal("1")


async def test_a_one_sided_book_is_a_success_with_no_quote() -> None:
    reader = FakeReader()
    reader.quote = None
    sink = Sink()
    handler = _handler(reader, sink)

    await handler(_message(MD_FETCH_BESTQUOTE, _quote_req()))
    await handler.wait_idle()
    result = sink.sent[0][1].payload
    assert isinstance(result, MdBestQuoteResult)
    assert result.ok is True
    assert result.quote is None
    assert result.error_code == QueryCode.NONE


async def test_a_one_sided_option_quote_arrives_with_its_zero_side() -> None:
    reader = FakeReader()
    reader.quote = BestQuote(
        universal_ticker="Deribit_Option_BTCUSD-260928-93000-C",
        bid=Decimal("0"),
        bid_qty=Decimal("0"),
        ask=Decimal("0.0001"),
        ask_qty=Decimal("30.2"),
    )
    sink = Sink()
    handler = _handler(reader, sink)

    await handler(_message(MD_FETCH_BESTQUOTE, _quote_req()))
    await handler.wait_idle()
    result = sink.sent[0][1].payload
    assert isinstance(result, MdBestQuoteResult)
    assert result.ok is True
    assert result.quote is not None
    assert (result.quote.bid, result.quote.bid_qty) == (0, 0)
    assert result.quote.ask == Decimal("0.0001")


async def test_a_read_the_venue_does_not_serve_is_refused_by_name() -> None:
    class KlinesOnly(FakeReader):
        fetch_order_book = None  # type: ignore[assignment]

    sink = Sink()
    handler = _handler(KlinesOnly(), sink)
    await handler(_message(MD_FETCH_ORDERBOOK, _book_req()))
    await handler.wait_idle()
    result = sink.sent[0][1].payload
    assert isinstance(result, MdOrderBookResult)
    assert result.ok is False
    assert result.error_code == QueryCode.MD_VENUE_UNSUPPORTED_READ
    assert "fetch_order_book" in result.reason


async def test_funding_history_arrives_oldest_first() -> None:
    older = FundingRate(
        universal_ticker=str(TICKER),
        rate=Decimal("0.0001"),
        ts=1_700_000_000.0,
    )
    newer = FundingRate(
        universal_ticker=str(TICKER),
        rate=Decimal("0.0002"),
        ts=1_700_028_800.0,
    )
    reader = FakeReader()
    reader.rates = [older, newer]
    sink = Sink()
    handler = _handler(reader, sink)

    await handler(_message(MD_FETCH_FUNDING_HISTORY, _funding_req(limit=5)))
    await handler.wait_idle()
    result = sink.sent[0][1].payload
    assert isinstance(result, MdFundingHistoryResult)
    assert result.ok is True
    assert [row.ts for row in result.rates] == [older.ts, newer.ts]
    assert reader.rate_calls == [(SYMBOL, 5)]


async def test_a_venue_without_funding_history_is_refused_by_name() -> None:
    class NoHistory(FakeReader):
        fetch_funding_history = None  # type: ignore[assignment]

    sink = Sink()
    handler = _handler(NoHistory(), sink)
    await handler(_message(MD_FETCH_FUNDING_HISTORY, _funding_req()))
    await handler.wait_idle()
    result = sink.sent[0][1].payload
    assert isinstance(result, MdFundingHistoryResult)
    assert result.ok is False
    assert result.error_code == QueryCode.MD_VENUE_UNSUPPORTED_READ
    assert "fetch_funding_history" in result.reason


async def test_open_interest_arrives_as_one_print() -> None:
    reader = FakeReader()
    reader.interest = OpenInterest(
        universal_ticker=str(TICKER),
        qty=Decimal("1234.5"),
        ts=1_700_000_000.0,
    )
    sink = Sink()
    handler = _handler(reader, sink)

    await handler(_message(MD_FETCH_OPEN_INTEREST, _oi_req()))
    await handler.wait_idle()
    result = sink.sent[0][1].payload
    assert isinstance(result, MdOpenInterestResult)
    assert result.ok is True
    assert result.open_interest is not None
    assert result.open_interest.qty == Decimal("1234.5")
    assert reader.interest_calls == [SYMBOL]


async def test_a_venue_without_open_interest_is_refused_by_name() -> None:
    class NoOpenInterest(FakeReader):
        fetch_open_interest = None  # type: ignore[assignment]

    sink = Sink()
    handler = _handler(NoOpenInterest(), sink)
    await handler(_message(MD_FETCH_OPEN_INTEREST, _oi_req()))
    await handler.wait_idle()
    result = sink.sent[0][1].payload
    assert isinstance(result, MdOpenInterestResult)
    assert result.ok is False
    assert result.error_code == QueryCode.MD_VENUE_UNSUPPORTED_READ
    assert result.open_interest is None
    assert "fetch_open_interest" in result.reason


def test_spot_readers_have_no_open_interest_method() -> None:
    assert not hasattr(BinanceSpotReader, "fetch_open_interest")
    assert not hasattr(GateSpotReader, "fetch_open_interest")
