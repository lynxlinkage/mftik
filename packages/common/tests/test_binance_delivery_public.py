"""The COIN-M market-data connector — one socket, contract-sized tape."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import pytest
from binance_stub import FakeBinanceStream
from mftik.exchange.binance.delivery.feed import BinanceDeliveryStream
from mftik.exchange.binance.delivery.public import (
    BINANCE_DELIVERY_INTERVALS,
    BinanceDeliveryPublicClient,
    venue_interval,
)
from mftik.exchange.intervals import InvalidIntervalError
from mftik.exchange.models import AggTrade, Side
from mftik.exchange.tickers import UniversalTicker

TICKER = UniversalTicker.parse("BinanceCM_Inverse_BTCUSD")
NATIVE = "BTCUSD_PERP"
SIZE = Decimal("100")


class StubSymbols:
    """A symbol plane that renders Binance's spelling and reads it back.

    Deliberately *not* the identity function: the canonical symbol is
    ``BTCUSD`` and the venue's is ``BTCUSD_PERP``.
    """

    def __init__(self, size: Decimal | None = SIZE) -> None:
        self.size = size

    async def exch_ticker(self, ticker: UniversalTicker) -> str:
        return NATIVE

    async def symbol_for(
        self, venue: str, exch_ticker: str, *, category: str
    ) -> UniversalTicker:
        assert exch_ticker == NATIVE, f"unexpected venue symbol {exch_ticker!r}"
        return UniversalTicker.of(venue, category, "BTCUSD")

    async def contract_size(self, ticker: UniversalTicker) -> Decimal | None:
        return self.size


def _client(
    stub: FakeBinanceStream, *, symbols: StubSymbols | None = None, **kwargs: Any
) -> BinanceDeliveryPublicClient:
    return BinanceDeliveryPublicClient(
        symbols=symbols or StubSymbols(),
        feed=BinanceDeliveryStream(
            url=stub.url,  # type: ignore[attr-defined]
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
    "q": "2",
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
    assert BINANCE_DELIVERY_INTERVALS["1mo"] == "1M"


def test_delivery_serves_no_one_second_candles() -> None:
    assert "1s" not in BINANCE_DELIVERY_INTERVALS
    with pytest.raises(InvalidIntervalError, match="no 1s candles"):
        venue_interval("1s")


# --- streams ---------------------------------------------------------------


async def test_a_ticker_pairs_the_stats_with_a_real_quote(
    binance_stream: FakeBinanceStream,
) -> None:
    """Both halves of the ticker share the one dstream socket."""
    client = _client(binance_stream)
    async with client:
        stream = client.stream_ticker(TICKER)
        pump = asyncio.ensure_future(anext(stream))
        await asyncio.sleep(0.05)

        await binance_stream.push(f"{NATIVE.lower()}@ticker", TICKER_STATS)
        await asyncio.sleep(0.05)
        assert not pump.done(), "no quote has arrived yet"

        await binance_stream.push(f"{NATIVE.lower()}@bookTicker", BOOK_TICKER)
        await asyncio.sleep(0.05)
        assert not pump.done(), "a quote alone is not a ticker either"

        await binance_stream.push(
            f"{NATIVE.lower()}@ticker", {**TICKER_STATS, "c": "40010"}
        )
        ticker = await asyncio.wait_for(pump, timeout=2.0)

    assert ticker.symbol == "BTCUSD", "the venue spelling must not escape"
    assert ticker.last == Decimal("40010")
    assert (ticker.bid, ticker.ask) == (Decimal("39999"), Decimal("40001"))
    assert binance_stream.subscribed == {
        f"{NATIVE.lower()}@ticker",
        f"{NATIVE.lower()}@bookTicker",
    }


async def test_the_tape_is_the_aggregated_one_because_there_is_no_other(
    binance_stream: FakeBinanceStream,
) -> None:
    client = _client(binance_stream)
    async with client:
        raw = client.stream_trades(TICKER)
        agg = client.stream_agg_trades(TICKER)
        raw_pump = asyncio.ensure_future(anext(raw))
        agg_pump = asyncio.ensure_future(anext(agg))
        await asyncio.sleep(0.05)

        await binance_stream.push(f"{NATIVE.lower()}@aggTrade", AGG_TRADE)
        trade = await asyncio.wait_for(raw_pump, timeout=2.0)
        aggregated = await asyncio.wait_for(agg_pump, timeout=2.0)

    assert trade.symbol == "BTCUSD"
    assert trade.side is Side.SELL, "m=true means the buyer rested"
    assert trade.qty == Decimal("2"), "contracts, unscaled"
    assert not isinstance(trade, AggTrade)
    assert isinstance(aggregated, AggTrade)
    assert aggregated.match_count == 40
    assert binance_stream.subscribed == {f"{NATIVE.lower()}@aggTrade"}


async def test_the_book_is_dated_by_binance_and_sized_in_contracts(
    binance_stream: FakeBinanceStream,
) -> None:
    client = _client(binance_stream)
    async with client:
        stream = client.stream_order_book(TICKER)
        pump = asyncio.ensure_future(anext(stream))
        await asyncio.sleep(0.05)
        await binance_stream.push(
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

    assert book.symbol == "BTCUSD"
    assert book.bids[0].price == Decimal("39999")
    assert book.bids[0].qty == Decimal("3"), "contracts, unscaled"
    assert book.ts == 1672515782.0


async def test_liquidations_are_reported_as_the_position_that_was_closed(
    binance_stream: FakeBinanceStream,
) -> None:
    client = _client(binance_stream)
    async with client:
        stream = client.stream_liquidation(TICKER)
        pump = asyncio.ensure_future(anext(stream))
        await asyncio.sleep(0.05)
        await binance_stream.push(
            f"{NATIVE.lower()}@forceOrder",
            {
                "e": "forceOrder",
                "E": 1568014460893,
                "o": {
                    "s": NATIVE,
                    "S": "SELL",
                    "o": "LIMIT",
                    "f": "IOC",
                    "q": "13",
                    "p": "9910",
                    "ap": "9910",
                    "X": "FILLED",
                    "l": "13",
                    "z": "13",
                    "T": 1568014460893,
                },
            },
        )
        liquidation = await asyncio.wait_for(pump, timeout=2.0)

    assert liquidation.symbol == "BTCUSD"
    assert liquidation.side is Side.BUY, "a SELL force-order closes a long"
    assert liquidation.qty == Decimal("13"), "contracts, unscaled"


async def test_mark_price_yields_a_funding_rate_and_skips_a_print_without_one(
    binance_stream: FakeBinanceStream,
) -> None:
    client = _client(binance_stream)
    async with client:
        stream = client.stream_funding_rate(TICKER)
        pump = asyncio.ensure_future(anext(stream))
        await asyncio.sleep(0.05)
        await binance_stream.push(
            f"{NATIVE.lower()}@markPrice@1s",
            {
                "e": "markPriceUpdate",
                "E": 1562305380000,
                "s": NATIVE,
                "p": "11794.15",
            },
        )
        await binance_stream.push(
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
    binance_stream: FakeBinanceStream,
) -> None:
    """A dated future settles at expiry; ``@markPrice`` would push without
    ``r`` and the pump would never yield. Refused before any subscribe."""
    client = _client(binance_stream)
    async with client:
        with pytest.raises(ValueError, match="serves no funding rate stream"):
            client.stream_funding_rate(
                UniversalTicker.parse("BinanceCM_Future_BTCUSD260925")
            )


async def test_ws_klines_swap_the_volume_columns(
    binance_stream: FakeBinanceStream,
) -> None:
    """Same numbers as the REST fixture: 206 contracts × 100 → quote 20600."""
    client = _client(binance_stream)
    async with client:
        stream = client.stream_kline(TICKER, "1mo")
        pump = asyncio.ensure_future(anext(stream))
        await asyncio.sleep(0.05)
        assert binance_stream.subscribed == {f"{NATIVE.lower()}@kline_1M"}

        await binance_stream.push(
            f"{NATIVE.lower()}@kline_1M",
            {
                "e": "kline",
                "E": 1591258380000,
                "s": NATIVE,
                "k": {
                    "t": 1591258320000,
                    "T": 1591258379999,
                    "s": NATIVE,
                    "i": "1M",
                    "o": "9640.7",
                    "c": "9642.0",
                    "h": "9642.4",
                    "l": "9640.6",
                    "v": "206",
                    "q": "2.13660389",
                    "n": 48,
                    "x": True,
                },
            },
        )
        kline = await asyncio.wait_for(pump, timeout=2.0)

    assert kline.interval == "1mo", "Binance's month spelling stays inside"
    assert kline.symbol == "BTCUSD"
    assert kline.volume == Decimal("2.13660389")
    assert kline.quote_volume == Decimal("20600")
    assert kline.closed


async def test_a_kline_subscribe_refuses_a_missing_contract_size(
    binance_stream: FakeBinanceStream,
) -> None:
    client = _client(binance_stream, symbols=StubSymbols(size=None))
    async with client:
        stream = client.stream_kline(TICKER, "1m")
        with pytest.raises(ValueError, match="no contract_size"):
            await anext(stream)
    assert not binance_stream.subscribed


async def test_best_quotes_carry_the_resting_sizes_in_contracts(
    binance_stream: FakeBinanceStream,
) -> None:
    client = _client(binance_stream)
    async with client:
        stream = client.stream_best_quote(TICKER)
        pump = asyncio.ensure_future(anext(stream))
        await asyncio.sleep(0.05)
        await binance_stream.push(f"{NATIVE.lower()}@bookTicker", BOOK_TICKER)
        quote = await asyncio.wait_for(pump, timeout=2.0)

    assert (quote.bid_qty, quote.ask_qty) == (Decimal("3"), Decimal("4"))
    assert quote.ts == 1672515782.136


async def test_another_venues_ticker_is_refused(
    binance_stream: FakeBinanceStream,
) -> None:
    client = _client(binance_stream)
    async with client:
        stream = client.stream_trades(
            UniversalTicker.parse("BinanceUM_Perp_BTCUSDT")
        )
        with pytest.raises(ValueError, match="BinanceUM ticker"):
            await anext(stream)
