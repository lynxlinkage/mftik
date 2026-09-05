"""The futures market-data connector — feeds and on-demand reads.

Everything crossing this boundary is canonical: canonical symbols in and out,
canonical intervals in and out. What is futures-specific is that two of MD's
feeds cannot be read off one Binance stream — the ticker needs a quote the 24h
stats do not carry, and the tape only exists in its aggregated form — so those
are what the tests below spend their time on.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import pytest
from binance_stub import FakeBinanceStream
from mftik.exchange.binance.future.feed import BinanceFutureStream
from mftik.exchange.binance.future.public import (
    BINANCE_FUTURE_INTERVALS,
    BinanceFuturePublicClient,
    venue_interval,
)
from mftik.exchange.intervals import InvalidIntervalError
from mftik.exchange.models import AggTrade, Side
from mftik.exchange.tickers import UniversalTicker

TICKER = UniversalTicker.parse("BinanceUM_Perp_BTCUSDT")
NATIVE = "BTC-USDT"


class StubSymbols:
    """A symbol plane that renders Binance's spelling and reads it back.

    Deliberately *not* the identity function: Binance happens to spell
    ``BTCUSDT`` the same way we do, and a stub that leaned on that would let a
    connector doing string surgery pass.
    """

    async def exch_ticker(self, ticker: UniversalTicker) -> str:
        return NATIVE

    async def symbol_for(
        self, venue: str, exch_ticker: str, *, category: str
    ) -> UniversalTicker:
        assert exch_ticker == NATIVE, f"unexpected venue symbol {exch_ticker!r}"
        return UniversalTicker.of(venue, category, "BTCUSDT")


def _client(
    public: FakeBinanceStream, market: FakeBinanceStream, **kwargs: Any
) -> BinanceFuturePublicClient:
    return BinanceFuturePublicClient(
        symbols=StubSymbols(),
        feed=BinanceFutureStream(
            public_url=public.url,  # type: ignore[attr-defined]
            market_url=market.url,  # type: ignore[attr-defined]
            keepalive=0,
            retry_backoff=0.01,
        ),
        **kwargs,
    )


AGG_TRADE = {
    "e": "aggTrade",
    "E": 1672515782136,
    "s": NATIVE,
    "a": 12345,
    "p": "40000",
    "q": "0.5",
    "f": 100,
    "l": 139,
    "T": 1672515782136,
    "m": True,
}

BOOK_TICKER = {
    "e": "bookTicker",
    "u": 1,
    "s": NATIVE,
    "b": "39999",
    "B": "3",
    "a": "40001",
    "A": "4",
    "T": 1672515782136,
    "E": 1672515782136,
}

TICKER_STATS = {
    "e": "24hrTicker",
    "E": 1672515782136,
    "s": NATIVE,
    "c": "40000",
    "o": "39000",
    "h": "41000",
    "l": "38000",
    "v": "100",
    "q": "4000000",
}


# --- intervals -------------------------------------------------------------


def test_the_month_is_the_one_interval_binance_spells_differently() -> None:
    assert venue_interval("1mo") == "1M"
    assert BINANCE_FUTURE_INTERVALS["1mo"] == "1M"


def test_futures_serves_no_one_second_candles_though_spot_does() -> None:
    """The one window the two markets disagree on — refused before the call."""
    assert "1s" not in BINANCE_FUTURE_INTERVALS
    with pytest.raises(InvalidIntervalError, match="no 1s candles"):
        venue_interval("1s")


# --- streams ---------------------------------------------------------------


async def test_a_ticker_pairs_the_stats_with_a_real_quote(
    future_public_stream: FakeBinanceStream,
    future_market_stream: FakeBinanceStream,
) -> None:
    """The futures 24h ticker has no bid or ask, so the feed reads two streams.

    Nothing is published until a quote has arrived: a ``Ticker`` whose bid and
    ask were the last price would read as a flat, crossable book to a strategy
    comparing venues.
    """
    client = _client(future_public_stream, future_market_stream)
    async with client:
        stream = client.stream_ticker(TICKER)
        pump = asyncio.ensure_future(anext(stream))
        await asyncio.sleep(0.05)

        await future_market_stream.push(f"{NATIVE.lower()}@ticker", TICKER_STATS)
        await asyncio.sleep(0.05)
        assert not pump.done(), "no quote has arrived yet"

        await future_public_stream.push(
            f"{NATIVE.lower()}@bookTicker", BOOK_TICKER
        )
        await asyncio.sleep(0.05)
        assert not pump.done(), "a quote alone is not a ticker either"

        await future_market_stream.push(
            f"{NATIVE.lower()}@ticker", {**TICKER_STATS, "c": "40010"}
        )
        ticker = await asyncio.wait_for(pump, timeout=2.0)

    assert ticker.symbol == "BTCUSDT", "the venue spelling must not escape"
    assert ticker.last == Decimal("40010")
    assert (ticker.bid, ticker.ask) == (Decimal("39999"), Decimal("40001"))


async def test_the_tape_is_the_aggregated_one_because_there_is_no_other(
    future_public_stream: FakeBinanceStream,
    future_market_stream: FakeBinanceStream,
) -> None:
    """Futures publishes no ``@trade``; ``trade`` and ``aggtrade`` share it."""
    client = _client(future_public_stream, future_market_stream)
    async with client:
        raw = client.stream_trades(TICKER)
        agg = client.stream_agg_trades(TICKER)
        raw_pump = asyncio.ensure_future(anext(raw))
        agg_pump = asyncio.ensure_future(anext(agg))
        await asyncio.sleep(0.05)

        await future_market_stream.push(f"{NATIVE.lower()}@aggTrade", AGG_TRADE)
        trade = await asyncio.wait_for(raw_pump, timeout=2.0)
        aggregated = await asyncio.wait_for(agg_pump, timeout=2.0)

    assert trade.symbol == "BTCUSDT"
    assert trade.side is Side.SELL, "m=true means the buyer rested"
    assert not isinstance(trade, AggTrade)
    assert isinstance(aggregated, AggTrade)
    assert aggregated.match_count == 40
    assert future_market_stream.subscribed == {f"{NATIVE.lower()}@aggTrade"}


async def test_the_book_comes_off_the_public_socket_dated_by_binance(
    future_public_stream: FakeBinanceStream,
    future_market_stream: FakeBinanceStream,
) -> None:
    client = _client(future_public_stream, future_market_stream)
    async with client:
        stream = client.stream_order_book(TICKER)
        pump = asyncio.ensure_future(anext(stream))
        await asyncio.sleep(0.05)
        await future_public_stream.push(
            f"{NATIVE.lower()}@depth20@100ms",
            {
                "e": "depthUpdate",
                "E": 1672515782136,
                "T": 1672515782000,
                "s": NATIVE,
                "U": 1,
                "u": 2,
                "pu": 0,
                "b": [["39999", "3"]],
                "a": [["40001", "4"]],
            },
        )
        book = await asyncio.wait_for(pump, timeout=2.0)

    assert book.symbol == "BTCUSDT"
    assert book.bids[0].price == Decimal("39999")
    assert book.ts == 1672515782.0, "the venue's stamp, not arrival"


async def test_liquidations_are_reported_as_the_position_that_was_closed(
    future_public_stream: FakeBinanceStream,
    future_market_stream: FakeBinanceStream,
) -> None:
    """The feed spot has no concept of, and the field easiest to get backwards."""
    client = _client(future_public_stream, future_market_stream)
    async with client:
        stream = client.stream_liquidation(TICKER)
        pump = asyncio.ensure_future(anext(stream))
        await asyncio.sleep(0.05)
        await future_market_stream.push(
            f"{NATIVE.lower()}@forceOrder",
            {
                "e": "forceOrder",
                "E": 1568014460893,
                "o": {
                    "s": NATIVE,
                    "S": "SELL",
                    "o": "LIMIT",
                    "f": "IOC",
                    "q": "0.014",
                    "p": "9910",
                    "ap": "9910",
                    "X": "FILLED",
                    "l": "0.014",
                    "z": "0.014",
                    "T": 1568014460893,
                },
            },
        )
        liquidation = await asyncio.wait_for(pump, timeout=2.0)

    assert liquidation.symbol == "BTCUSDT"
    assert liquidation.side is Side.BUY, "a SELL force-order closes a long"
    assert liquidation.qty == Decimal("0.014")


async def test_mark_price_yields_a_funding_rate_and_skips_a_print_without_one(
    future_public_stream: FakeBinanceStream,
    future_market_stream: FakeBinanceStream,
) -> None:
    client = _client(future_public_stream, future_market_stream)
    async with client:
        stream = client.stream_funding_rate(TICKER)
        pump = asyncio.ensure_future(anext(stream))
        await asyncio.sleep(0.05)
        await future_market_stream.push(
            f"{NATIVE.lower()}@markPrice@1s",
            {
                "e": "markPriceUpdate",
                "E": 1562305380000,
                "s": NATIVE,
                "p": "11794.15",
            },
        )
        await future_market_stream.push(
            f"{NATIVE.lower()}@markPrice@1s",
            {
                "e": "markPriceUpdate",
                "E": 1562305381000,
                "s": NATIVE,
                "p": "11794.15",
                "r": "0.00038167",
                "T": 1562306400000,
            },
        )
        funding = await asyncio.wait_for(pump, timeout=2.0)

    assert funding.rate == Decimal("0.00038167")
    assert funding.ts == 1562305381.0
    assert not hasattr(funding, "next_funding_time")


async def test_a_dated_future_has_no_funding_rate_stream(
    future_public_stream: FakeBinanceStream,
    future_market_stream: FakeBinanceStream,
) -> None:
    """A dated future settles at expiry; ``@markPrice`` would push without
    ``r`` and the pump would never yield. Refused before any subscribe."""
    client = _client(future_public_stream, future_market_stream)
    async with client:
        with pytest.raises(ValueError, match="serves no funding rate stream"):
            client.stream_funding_rate(
                UniversalTicker.parse("BinanceUM_Future_BTCUSDT-250926")
            )


async def test_candles_answer_in_the_interval_that_was_asked_for(
    future_public_stream: FakeBinanceStream,
    future_market_stream: FakeBinanceStream,
) -> None:
    """``1mo`` down, ``1M`` on the wire, ``1mo`` back up."""
    client = _client(future_public_stream, future_market_stream)
    async with client:
        stream = client.stream_kline(TICKER, "1mo")
        pump = asyncio.ensure_future(anext(stream))
        await asyncio.sleep(0.05)
        assert future_market_stream.subscribed == {f"{NATIVE.lower()}@kline_1M"}

        await future_market_stream.push(
            f"{NATIVE.lower()}@kline_1M",
            {
                "e": "kline",
                "E": 1638747660000,
                "s": NATIVE,
                "k": {
                    "t": 1638747660000,
                    "T": 1638747719999,
                    "s": NATIVE,
                    "i": "1M",
                    "o": "41000",
                    "c": "41100",
                    "h": "41200",
                    "l": "40900",
                    "v": "12",
                    "q": "492000",
                    "n": 30,
                    "x": True,
                },
            },
        )
        kline = await asyncio.wait_for(pump, timeout=2.0)

    assert kline.interval == "1mo", "Binance's month spelling stays inside"
    assert kline.symbol == "BTCUSDT"
    assert kline.closed


async def test_best_quotes_carry_the_resting_sizes(
    future_public_stream: FakeBinanceStream,
    future_market_stream: FakeBinanceStream,
) -> None:
    client = _client(future_public_stream, future_market_stream)
    async with client:
        stream = client.stream_best_quote(TICKER)
        pump = asyncio.ensure_future(anext(stream))
        await asyncio.sleep(0.05)
        await future_public_stream.push(
            f"{NATIVE.lower()}@bookTicker", BOOK_TICKER
        )
        quote = await asyncio.wait_for(pump, timeout=2.0)

    assert (quote.bid_qty, quote.ask_qty) == (Decimal("3"), Decimal("4"))
    assert quote.ts == 1672515782.136, "futures dates its book ticker"


async def test_another_venues_ticker_is_refused(
    future_public_stream: FakeBinanceStream,
    future_market_stream: FakeBinanceStream,
) -> None:
    """An instrument on the spot venue is a different instrument."""
    client = _client(future_public_stream, future_market_stream)
    async with client:
        stream = client.stream_trades(UniversalTicker.parse("Binance_Spot_BTCUSDT"))
        with pytest.raises(ValueError, match="Binance ticker"):
            await anext(stream)
