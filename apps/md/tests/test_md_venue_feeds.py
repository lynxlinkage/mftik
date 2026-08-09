"""MD venue feed-topic resolution — topic → venue stream + wire type."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from decimal import Decimal

import pytest
from mft.exchange.models import (
    AggTrade,
    BestQuote,
    BookLevel,
    Kline,
    OrderBook,
    Ticker,
    Trade,
)
from mft.exchange.tickers import UniversalTicker
from mft.protocol import (
    MD_AGG_TRADE,
    MD_BEST_QUOTE,
    MD_KLINE,
    MD_ORDERBOOK,
    MD_TICKER,
    MD_TRADE,
    UntypedEnvelope,
)
from mft_md.session.venue import VenueSession


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
        self.connected = False

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.connected = False

    async def _once(self, item) -> AsyncIterator:  # noqa: ANN001
        yield item
        await asyncio.Event().wait()

    def stream_ticker(self, ticker: UniversalTicker) -> AsyncIterator[Ticker]:
        return self._once(
            Ticker(
                universal_ticker=str(ticker),
                bid=Decimal("100"),
                ask=Decimal("101"),
                last=Decimal("100.5"),
            )
        )

    def stream_trades(self, ticker: UniversalTicker) -> AsyncIterator[Trade]:
        return self._once(
            Trade(
                universal_ticker=str(ticker),
                price=Decimal("100"),
                qty=Decimal("1"),
                side="buy",
            )
        )

    def stream_order_book(self, ticker: UniversalTicker) -> AsyncIterator[OrderBook]:
        return self._once(
            OrderBook(
                universal_ticker=str(ticker),
                bids=[BookLevel(price=Decimal("100"), qty=Decimal("1"))],
                asks=[BookLevel(price=Decimal("101"), qty=Decimal("1"))],
            )
        )

    def stream_kline(
        self, ticker: UniversalTicker, interval: str
    ) -> AsyncIterator[Kline]:
        self.kline_calls.append((ticker.symbol, interval))
        return self._once(
            Kline(
                universal_ticker=str(ticker),
                interval=interval,
                open_time=1_700_000_000.0,
                open=Decimal("100"),
                high=Decimal("101"),
                low=Decimal("99"),
                close=Decimal("100.5"),
            )
        )

    def stream_agg_trades(self, ticker: UniversalTicker) -> AsyncIterator[AggTrade]:
        return self._once(
            AggTrade(
                universal_ticker=str(ticker),
                price=Decimal("100"),
                qty=Decimal("1"),
                side="buy",
                first_trade_id="10",
                last_trade_id="13",
            )
        )

    def stream_best_quote(self, ticker: UniversalTicker) -> AsyncIterator[BestQuote]:
        return self._once(
            BestQuote(
                universal_ticker=str(ticker),
                bid=Decimal("100"),
                bid_qty=Decimal("1"),
                ask=Decimal("101"),
                ask_qty=Decimal("2"),
            )
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
        ("kline_1m", MD_KLINE),
    ],
)
async def test_feed_topic_publishes_its_message_type(
    topic: str, msg_type: str
) -> None:
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
