"""The Binance spot market-data connector — feeds and on-demand reads.

Everything crossing this boundary is canonical: canonical symbols in and out,
canonical intervals in and out. What the tests here mostly check is that
Binance's own vocabulary — lowercase stream names, ``1M`` for a month — goes no
further than the adapter.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import pytest
from binance_stub import FakeBinanceApi, FakeBinanceStream
from mftik.exchange.binance.spot import methods as m
from mftik.exchange.binance.spot import streams as st
from mftik.exchange.binance.spot.client import BinanceSpotWsApi
from mftik.exchange.binance.spot.feed import BinanceSpotStream
from mftik.exchange.binance.spot.public import (
    BINANCE_INTERVALS,
    BinanceSpotPublicClient,
    venue_interval,
)
from mftik.exchange.errors import ExchangeNotConnectedError
from mftik.exchange.intervals import InvalidIntervalError
from mftik.exchange.models import AggTrade, Side
from mftik.exchange.tickers import Category, UniversalTicker

TICKER = UniversalTicker.parse("Binance_Spot_BTCUSDT")

AGG_TRADE = {
    "e": "aggTrade",
    "E": 1672515782136,
    "s": "BTCUSDT",
    "a": 12345,
    "p": "40000",
    "q": "0.5",
    "T": 1672515782136,
    "m": True,
}


class StubSymbols:
    """A symbol plane that renders Binance's spelling and reads it back.

    Deliberately *not* the identity function: Binance spot happens to spell
    ``BTCUSDT`` the same way we do, and a stub that leaned on that would let a
    connector doing string surgery pass.
    """

    def __init__(self, native: str = "BTC-USDT") -> None:
        self.native = native
        self.asked: list[UniversalTicker] = []

    async def exch_ticker(self, ticker: UniversalTicker) -> str:
        self.asked.append(ticker)
        return self.native

    async def symbol_for(
        self, venue: str, exch_ticker: str, *, category: str
    ) -> UniversalTicker:
        assert exch_ticker == self.native, f"unexpected venue symbol {exch_ticker!r}"
        return UniversalTicker.of(venue, category, "BTCUSDT")


def _client(
    api_stub: FakeBinanceApi | None = None,
    stream_stub: FakeBinanceStream | None = None,
    **kwargs: Any,
) -> BinanceSpotPublicClient:
    return BinanceSpotPublicClient(
        symbols=StubSymbols(),
        feed=(
            BinanceSpotStream(url=stream_stub.url, keepalive=0)  # type: ignore[attr-defined]
            if stream_stub
            else None
        ),
        api=(
            BinanceSpotWsApi(url=api_stub.url, keepalive=0)  # type: ignore[attr-defined]
            if api_stub
            else None
        ),
        **kwargs,
    )


# --- intervals -------------------------------------------------------------


def test_the_month_is_the_one_interval_binance_spells_differently() -> None:
    """``1mo`` in, ``1M`` on the wire — the whole reason the table exists."""
    assert venue_interval("1mo") == "1M"
    assert BINANCE_INTERVALS["1mo"] == "1M"


def test_an_interval_binance_does_not_serve_is_refused_before_the_call() -> None:
    # Well-formed, and Gate serves it; Binance does not.
    with pytest.raises(InvalidIntervalError, match="no 10s candles"):
        venue_interval("10s")


def test_a_malformed_interval_is_refused_by_the_canonical_parser() -> None:
    with pytest.raises(InvalidIntervalError):
        venue_interval("1M")  # month is spelled 1mo above the adapter


# --- streams ---------------------------------------------------------------


async def test_trades_arrive_canonical(
    binance_api: FakeBinanceApi, binance_stream: FakeBinanceStream
) -> None:
    """``trade`` is the raw tape: one message per match, the venue's own ids.

    Deliberately not ``@aggTrade``, which this used to read. That stream is a
    feed of its own now, and two topics reading one stream would leave ``trade``
    handing out aggregate ids under a field documented as a trade id.
    """
    client = _client(binance_api, binance_stream)
    async with client:
        stream = client.stream_trades(TICKER)
        pump = asyncio.ensure_future(anext(stream))
        await asyncio.sleep(0.05)
        await binance_stream.push(
            "btc-usdt@trade",
            {
                "e": "trade",
                "E": 1672515782136,
                "s": "BTC-USDT",
                "t": 999,
                "p": "40000",
                "q": "0.5",
                "T": 1672515782136,
                "m": True,
            },
        )
        trade = await asyncio.wait_for(pump, timeout=2.0)

    assert trade.symbol == "BTCUSDT", "the venue spelling must not escape"
    assert trade.price == Decimal("40000")
    assert trade.trade_id == "999", "the match's own id, not an aggregate's"
    assert trade.side is Side.SELL, "m=true means the buyer rested"
    assert binance_stream.frames_for(st.SUBSCRIBE)[0]["params"] == ["btc-usdt@trade"]


async def test_agg_trades_keep_the_match_range(
    binance_api: FakeBinanceApi, binance_stream: FakeBinanceStream
) -> None:
    """The one thing the raw tape cannot report without counting."""
    client = _client(binance_api, binance_stream)
    async with client:
        stream = client.stream_agg_trades(TICKER)
        pump = asyncio.ensure_future(anext(stream))
        await asyncio.sleep(0.05)
        await binance_stream.push(
            "btc-usdt@aggTrade",
            {**AGG_TRADE, "s": "BTC-USDT", "f": 100, "l": 139},
        )
        trade = await asyncio.wait_for(pump, timeout=2.0)

    assert isinstance(trade, AggTrade)
    assert trade.symbol == "BTCUSDT"
    assert trade.price == Decimal("40000")
    assert trade.trade_id == "12345", "the aggregate's id"
    assert trade.match_count == 40, "this print swept forty resting orders"
    assert binance_stream.frames_for(st.SUBSCRIBE)[0]["params"] == [
        "btc-usdt@aggTrade"
    ]


async def test_the_two_tapes_are_separate_subscriptions(
    binance_api: FakeBinanceApi, binance_stream: FakeBinanceStream
) -> None:
    """Same instrument, two feeds, two streams — neither feeds the other."""
    client = _client(binance_api, binance_stream)
    async with client:
        raw = client.stream_trades(TICKER)
        agg = client.stream_agg_trades(TICKER)
        raw_pump = asyncio.ensure_future(anext(raw))
        agg_pump = asyncio.ensure_future(anext(agg))
        await asyncio.sleep(0.05)

        await binance_stream.push(
            "btc-usdt@aggTrade", {**AGG_TRADE, "s": "BTC-USDT"}
        )
        aggregated = await asyncio.wait_for(agg_pump, timeout=2.0)
        assert not raw_pump.done(), "an aggTrade must not surface as a trade"

        await binance_stream.push(
            "btc-usdt@trade",
            {"e": "trade", "E": 1, "s": "BTC-USDT", "t": 7, "p": "1", "q": "2"},
        )
        matched = await asyncio.wait_for(raw_pump, timeout=2.0)

    assert isinstance(aggregated, AggTrade)
    assert matched.trade_id == "7"
    assert not isinstance(matched, AggTrade)


async def test_klines_are_subscribed_and_returned_in_our_own_spelling(
    binance_api: FakeBinanceApi, binance_stream: FakeBinanceStream
) -> None:
    client = _client(binance_api, binance_stream)
    async with client:
        stream = client.stream_kline(TICKER, "1mo")
        pump = asyncio.ensure_future(anext(stream))
        await asyncio.sleep(0.05)

        assert binance_stream.frames_for(st.SUBSCRIBE)[0]["params"] == [
            "btc-usdt@kline_1M"
        ]

        await binance_stream.push(
            "btc-usdt@kline_1M",
            {
                "e": "kline",
                "E": 1,
                "s": "BTC-USDT",
                "k": {
                    "t": 1672515780000,
                    "s": "BTC-USDT",
                    "i": "1M",
                    "o": "1",
                    "c": "2",
                    "h": "3",
                    "l": "0.5",
                    "v": "10",
                    "q": "20",
                    "x": True,
                },
            },
        )
        kline = await asyncio.wait_for(pump, timeout=2.0)

    assert kline.symbol == "BTCUSDT"
    assert kline.interval == "1mo", "Binance's 1M must not escape the adapter"
    assert kline.closed is True


async def test_order_book_pushes_are_named_by_their_stream(
    binance_api: FakeBinanceApi, binance_stream: FakeBinanceStream
) -> None:
    """Partial depth carries no symbol; the connector supplies the canonical one."""
    client = _client(binance_api, binance_stream, book_levels=5)
    async with client:
        stream = client.stream_order_book(TICKER)
        pump = asyncio.ensure_future(anext(stream))
        await asyncio.sleep(0.05)
        await binance_stream.push(
            "btc-usdt@depth5@100ms",
            {"lastUpdateId": 1, "bids": [["39999", "1"]], "asks": [["40001", "2"]]},
        )
        book = await asyncio.wait_for(pump, timeout=2.0)

    assert book.symbol == "BTCUSDT"
    assert book.bids[0].price == Decimal("39999")
    assert book.ts > 0, "Binance dates no book, so arrival is the timestamp"


async def test_best_quote_carries_both_sizes(
    binance_api: FakeBinanceApi, binance_stream: FakeBinanceStream
) -> None:
    client = _client(binance_api, binance_stream)
    async with client:
        stream = client.stream_best_quote(TICKER)
        pump = asyncio.ensure_future(anext(stream))
        await asyncio.sleep(0.05)
        await binance_stream.push(
            "btc-usdt@bookTicker",
            {
                "u": 1,
                "s": "BTC-USDT",
                "b": "39999",
                "B": "1.5",
                "a": "40001",
                "A": "2.5",
            },
        )
        quote = await asyncio.wait_for(pump, timeout=2.0)

    assert quote.symbol == "BTCUSDT"
    assert quote.bid_qty == Decimal("1.5")
    assert quote.ask_qty == Decimal("2.5")
    assert quote.ts > 0


async def test_ticker_stream_is_canonical(
    binance_api: FakeBinanceApi, binance_stream: FakeBinanceStream
) -> None:
    client = _client(binance_api, binance_stream)
    async with client:
        stream = client.stream_ticker(TICKER)
        pump = asyncio.ensure_future(anext(stream))
        await asyncio.sleep(0.05)
        await binance_stream.push(
            "btc-usdt@ticker",
            {
                "e": "24hrTicker",
                "E": 1672515782136,
                "s": "BTC-USDT",
                "c": "40000",
                "b": "39999",
                "a": "40001",
            },
        )
        ticker = await asyncio.wait_for(pump, timeout=2.0)

    assert ticker.symbol == "BTCUSDT"
    assert ticker.bid == Decimal("39999")


# --- snapshots -------------------------------------------------------------


async def test_fetch_klines_translates_both_spellings_each_way(
    binance_api: FakeBinanceApi, binance_stream: FakeBinanceStream
) -> None:
    binance_api.results[m.KLINES] = [
        [
            1499040000000, "1", "2", "0.5", "1.5", "10",
            1499040059999, "20", 5, "1", "1", "0",
        ]
    ]
    client = _client(binance_api, binance_stream)
    async with client:
        klines = await client.fetch_klines(TICKER, " 1MO ", limit=5)

    params = binance_api.call(m.KLINES)["params"]
    assert params["symbol"] == "BTC-USDT"
    assert params["interval"] == "1M"
    assert klines[0].symbol == "BTCUSDT"
    assert klines[0].interval == "1mo"


async def test_fetch_order_book_stamps_the_canonical_symbol(
    binance_api: FakeBinanceApi, binance_stream: FakeBinanceStream
) -> None:
    binance_api.results[m.DEPTH] = {
        "lastUpdateId": 1,
        "bids": [["39999", "1"]],
        "asks": [["40001", "2"]],
    }
    client = _client(binance_api, binance_stream)
    async with client:
        book = await client.fetch_order_book(TICKER, depth=5)

    assert book.symbol == "BTCUSDT"
    assert binance_api.call(m.DEPTH)["params"]["symbol"] == "BTC-USDT"


async def test_fetch_instruments_stays_in_the_venues_spelling(
    binance_api: FakeBinanceApi, binance_stream: FakeBinanceStream
) -> None:
    """This is what builds the canonical mapping; it cannot presume one."""
    binance_api.results[m.EXCHANGE_INFO] = {
        "symbols": [
            {
                "symbol": "BTC-USDT",
                "status": "TRADING",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
                "filters": [],
            }
        ]
    }
    client = _client(binance_api, binance_stream)
    async with client:
        instruments = await client.fetch_instruments()

    assert instruments[0].exch_ticker == "BTC-USDT"
    assert instruments[0].symbol == "BTCUSDT"


# --- guards ----------------------------------------------------------------


async def test_reads_are_refused_before_connect(
    binance_api: FakeBinanceApi, binance_stream: FakeBinanceStream
) -> None:
    client = _client(binance_api, binance_stream)
    with pytest.raises(ExchangeNotConnectedError):
        await client.fetch_ticker(TICKER)
    with pytest.raises(ExchangeNotConnectedError):
        client.stream_trades(TICKER)


async def test_another_venues_ticker_is_refused(
    binance_api: FakeBinanceApi, binance_stream: FakeBinanceStream
) -> None:
    gate_ticker = UniversalTicker.of("Gate", Category.SPOT, "BTCUSDT")
    client = _client(binance_api, binance_stream)
    async with client:
        with pytest.raises(ValueError, match="was handed a Gate ticker"):
            await client.fetch_ticker(gate_ticker)
