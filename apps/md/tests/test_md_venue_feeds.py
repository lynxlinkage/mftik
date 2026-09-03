"""MD venue feed-topic resolution — topic → venue stream + wire type."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from decimal import Decimal

import pytest
from mftik.exchange.models import (
    AggTrade,
    BestQuote,
    BookLevel,
    FundingRate,
    Kline,
    Liquidation,
    OpenInterest,
    OrderBook,
    Ticker,
    Trade,
)
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import (
    MD_AGG_TRADE,
    MD_BEST_QUOTE,
    MD_FUNDING_RATE,
    MD_KLINE,
    MD_LIQUIDATION,
    MD_OPEN_INTEREST,
    MD_ORDERBOOK,
    MD_TICKER,
    MD_TRADE,
    UntypedEnvelope,
)
from mftik_md.session.venue import VenueSession


def _fake(symbol: str) -> UniversalTicker:
    """A ticker on the made-up venue these fakes stand in for."""
    return UniversalTicker.parse(f"Fake_Spot_{symbol}")


FAKE = _fake("BTCUSDT")


class FakePublic:
    """A venue connector: publishes one item per stream, then holds it open.

    Inherits nothing. That is the point of the shape MD declares — a connector
    satisfies it by having the methods, not by being registered anywhere.
    """

    name = "Fake"

    def __init__(self) -> None:
        self.kline_calls: list[tuple[str, str]] = []
        self.opened: list[str] = []
        self.closed: list[str] = []
        self._more: dict[str, asyncio.Queue[object]] = {}
        self.connected = False

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.connected = False

    def push(self, name: str, item: object) -> None:
        self._more[name].put_nowait(item)

    async def _once(self, name: str, item) -> AsyncIterator:  # noqa: ANN001
        self.opened.append(name)
        more: asyncio.Queue[object] = asyncio.Queue()
        self._more[name] = more
        try:
            yield item
            while True:
                yield await more.get()
        finally:
            self.closed.append(name)
            self._more.pop(name, None)

    def stream_ticker(self, ticker: UniversalTicker) -> AsyncIterator[Ticker]:
        return self._once(
            "ticker",
            Ticker(
                universal_ticker=str(ticker),
                bid=Decimal("100"),
                ask=Decimal("101"),
                last=Decimal("100.5"),
            ),
        )

    def stream_trades(self, ticker: UniversalTicker) -> AsyncIterator[Trade]:
        return self._once(
            "trades",
            Trade(
                universal_ticker=str(ticker),
                price=Decimal("100"),
                qty=Decimal("1"),
                side="buy",
            ),
        )

    def stream_order_book(self, ticker: UniversalTicker) -> AsyncIterator[OrderBook]:
        return self._once(
            "order_book",
            OrderBook(
                universal_ticker=str(ticker),
                bids=[BookLevel(price=Decimal("100"), qty=Decimal("1"))],
                asks=[BookLevel(price=Decimal("101"), qty=Decimal("1"))],
            ),
        )

    def stream_kline(
        self, ticker: UniversalTicker, interval: str
    ) -> AsyncIterator[Kline]:
        self.kline_calls.append((ticker.symbol, interval))
        return self._once(
            "kline",
            Kline(
                universal_ticker=str(ticker),
                interval=interval,
                open_time=1_700_000_000.0,
                open=Decimal("100"),
                high=Decimal("101"),
                low=Decimal("99"),
                close=Decimal("100.5"),
            ),
        )

    def stream_agg_trades(self, ticker: UniversalTicker) -> AsyncIterator[AggTrade]:
        return self._once(
            "agg_trades",
            AggTrade(
                universal_ticker=str(ticker),
                price=Decimal("100"),
                qty=Decimal("1"),
                side="buy",
                first_trade_id="10",
                last_trade_id="13",
            ),
        )

    def stream_best_quote(self, ticker: UniversalTicker) -> AsyncIterator[BestQuote]:
        return self._once(
            "best_quote",
            BestQuote(
                universal_ticker=str(ticker),
                bid=Decimal("100"),
                bid_qty=Decimal("1"),
                ask=Decimal("101"),
                ask_qty=Decimal("2"),
            ),
        )

    def stream_liquidation(self, ticker: UniversalTicker) -> AsyncIterator[Liquidation]:
        return self._once(
            "liquidation",
            Liquidation(
                universal_ticker=str(ticker),
                price=Decimal("99"),
                qty=Decimal("5"),
                side="sell",
            ),
        )

    def stream_funding_rate(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[FundingRate]:
        return self._once(
            "funding_rate",
            FundingRate(
                universal_ticker=str(ticker),
                rate=Decimal("0.0001"),
                ts=1_700_000_000.0,
            ),
        )

    def stream_open_interest(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[OpenInterest]:
        return self._once(
            "open_interest",
            OpenInterest(
                universal_ticker=str(ticker),
                qty=Decimal("1000"),
                ts=1_700_000_000.0,
            ),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("topic", "msg_type"),
    [
        ("orderbook", MD_ORDERBOOK),
        ("ticker", MD_TICKER),
        ("trade", MD_TRADE),
        ("aggtrade", MD_AGG_TRADE),
        ("bestquote", MD_BEST_QUOTE),
        ("liquidation", MD_LIQUIDATION),
        ("funding_rate", MD_FUNDING_RATE),
        ("open_interest", MD_OPEN_INTEREST),
        ("kline_1m", MD_KLINE),
    ],
)
async def test_feed_topic_publishes_its_message_type(topic: str, msg_type: str) -> None:
    seen: list[tuple[str, UniversalTicker, UntypedEnvelope]] = []

    async def _on_update(topic_, ticker, env) -> None:  # noqa: ANN001
        seen.append((topic_, ticker, env))

    sess = VenueSession(FAKE.venue, FakePublic(), on_update=_on_update)
    await sess.start()
    await sess.ensure_feed(topic, FAKE)

    await _wait_until(lambda: bool(seen))
    topic_, ticker, env = seen[0]
    assert (topic_, ticker) == (topic, FAKE)
    assert env.type == msg_type
    # The payload names the instrument, not just the symbol. A strategy routes
    # every book to one hook whatever feed it came from, and the envelope
    # carries no feed key — so this is the only thing that says which.
    assert env.payload["universal_ticker"] == "Fake_Spot_BTCUSDT"

    await sess.stop()


@pytest.mark.asyncio
async def test_kline_topic_carries_the_interval() -> None:
    public = FakePublic()
    sess = VenueSession(FAKE.venue, public, on_update=_noop_update)
    await sess.start()
    await sess.ensure_feed("kline_15m", _fake("ETHUSDT"))
    assert public.kline_calls == [("ETHUSDT", "15m")]
    await sess.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("topic", ["depth", "kline_", ""])
async def test_unknown_topic_is_rejected_at_subscribe(topic: str) -> None:
    sess = VenueSession(FAKE.venue, FakePublic(), on_update=_noop_update)
    await sess.start()
    with pytest.raises(ValueError):
        await sess.ensure_feed(topic, FAKE)
    assert sess.feed_count == 0
    await sess.stop()


@pytest.mark.asyncio
async def test_unsupported_stream_is_rejected_at_subscribe() -> None:
    """A venue that does not publish the feed fails the subscribe, not a task."""

    class NoKlines(FakePublic):
        """Does not publish candles, so it simply has no method for them."""

        stream_kline = None

    sess = VenueSession(FAKE.venue, NoKlines(), on_update=_noop_update)
    await sess.start()
    with pytest.raises(ValueError, match="does not publish stream_kline"):
        await sess.ensure_feed("kline_1m", FAKE)
    assert sess.feed_count == 0
    await sess.stop()


@pytest.mark.asyncio
async def test_a_venue_without_agg_trades_refuses_that_topic() -> None:
    """Only Binance coalesces the tape; Gate and paper have no such stream."""

    class NoAggTrades(FakePublic):
        stream_agg_trades = None

    sess = VenueSession(FAKE.venue, NoAggTrades(), on_update=_noop_update)
    await sess.start()
    with pytest.raises(ValueError, match="does not publish stream_agg_trades"):
        await sess.ensure_feed("aggtrade", FAKE)
    assert sess.feed_count == 0
    await sess.stop()


@pytest.mark.asyncio
async def test_a_venue_without_liquidations_refuses_that_topic() -> None:
    """A venue without the method refuses; Bybit/OKX/GateFutures have one."""

    class NoLiquidations(FakePublic):
        stream_liquidation = None

    sess = VenueSession(FAKE.venue, NoLiquidations(), on_update=_noop_update)
    await sess.start()
    with pytest.raises(ValueError, match="does not publish stream_liquidation"):
        await sess.ensure_feed("liquidation", FAKE)
    assert sess.feed_count == 0
    await sess.stop()


@pytest.mark.asyncio
async def test_a_venue_without_funding_rate_refuses_that_topic() -> None:
    """A venue without the method refuses; perpetual books grow one later."""

    class NoFunding(FakePublic):
        stream_funding_rate = None

    sess = VenueSession(FAKE.venue, NoFunding(), on_update=_noop_update)
    await sess.start()
    with pytest.raises(ValueError, match="does not publish stream_funding_rate"):
        await sess.ensure_feed("funding_rate", FAKE)
    assert sess.feed_count == 0
    await sess.stop()


@pytest.mark.asyncio
async def test_a_venue_without_open_interest_refuses_that_topic() -> None:
    """A venue without the method refuses; contract books grow one later."""

    class NoOpenInterest(FakePublic):
        stream_open_interest = None

    sess = VenueSession(FAKE.venue, NoOpenInterest(), on_update=_noop_update)
    await sess.start()
    with pytest.raises(ValueError, match="does not publish stream_open_interest"):
        await sess.ensure_feed("open_interest", FAKE)
    assert sess.feed_count == 0
    await sess.stop()


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_ticker_and_bestquote_each_open_their_own_stream() -> None:
    """S1 — two product pumps, two connector calls. The wire identity is MDS-1b."""
    public = FakePublic()
    seen: list[str] = []

    async def _on_update(topic, ticker, env) -> None:  # noqa: ANN001
        seen.append(topic)

    sess = VenueSession(FAKE.venue, public, on_update=_on_update)
    await sess.start()
    await sess.ensure_feed("ticker", FAKE)
    await sess.ensure_feed("bestquote", FAKE)
    await _wait_until(lambda: set(seen) == {"ticker", "bestquote"})
    assert public.opened == ["ticker", "best_quote"]
    assert sess.feed_count == 2
    await sess.stop()


async def test_a_ticker_from_another_venue_is_refused() -> None:
    """A session owns one venue's connector; a stray ticker is a routing bug."""
    sess = VenueSession(FAKE.venue, FakePublic(), on_update=_noop_update)
    await sess.start()
    with pytest.raises(ValueError, match="was handed a"):
        await sess.ensure_feed("orderbook", UniversalTicker.parse("Other_Spot_BTCUSDT"))
    assert sess.feed_count == 0
    await sess.stop()


async def _noop_update(topic, ticker, env) -> None:  # noqa: ANN001
    return None


async def _wait_until(pred, *, timeout: float = 3.0) -> None:  # noqa: ANN001
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if pred():
            return
        await asyncio.sleep(0.02)
    raise TimeoutError("condition not met")
