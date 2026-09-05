"""Deribit public client — refusals, one socket, ticker-shared V5."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import httpx
import pytest
from deribit_stub import FakeDeribit
from mftik.exchange.deribit import channels as ch
from mftik.exchange.deribit.feed import DeribitPublicStream
from mftik.exchange.deribit.protocol import (
    expiry_code_from_name,
    expiry_suffix_from_code,
)
from mftik.exchange.deribit.public import DeribitPublicClient, venue_interval
from mftik.exchange.deribit.rest import DeribitPublicRest
from mftik.exchange.intervals import InvalidIntervalError
from mftik.exchange.tickers import Category, UniversalTicker

SPOT = UniversalTicker.parse("Deribit_Spot_BTCUSDC")
PERP = UniversalTicker.parse("Deribit_Perp_BTCUSDC")
INVERSE = UniversalTicker.parse("Deribit_Inverse_BTCUSD")
DATED = UniversalTicker.parse("Deribit_Future_BTCUSD-260906")
BASE = "https://deribit.test"


def _wire(ticker: UniversalTicker) -> str:
    symbol = ticker.symbol
    code = None
    if "-" in symbol:
        pair, maybe = symbol.rsplit("-", 1)
        if len(maybe) == 6 and maybe.isdigit():
            symbol, code = pair, maybe
    for quote in ("USDC", "USDT", "USD"):
        if symbol.endswith(quote) and symbol != quote:
            base = symbol[: -len(quote)]
            if quote == "USD":
                if ticker.category is Category.FUTURE and code:
                    return f"{base}-{expiry_suffix_from_code(code)}"
                return f"{base}-PERPETUAL"
            pair = f"{base}_{quote}"
            if ticker.category is Category.PERP:
                return f"{pair}-PERPETUAL"
            if ticker.category is Category.FUTURE and code:
                return f"{pair}-{expiry_suffix_from_code(code)}"
            return pair
    return ticker.symbol


class StubSymbols:
    async def exch_ticker(self, ticker: UniversalTicker) -> str:
        return _wire(ticker)

    async def symbol_for(
        self, venue: str, exch_ticker: str, *, category: str
    ) -> UniversalTicker:
        code = expiry_code_from_name(exch_ticker)
        body = exch_ticker.replace("-PERPETUAL", "")
        if code:
            body = exch_ticker.rsplit("-", 1)[0]
        symbol = body.replace("_", "") if "_" in body else f"{body}USD"
        if code:
            symbol = f"{symbol}-{code}"
        return UniversalTicker.of(venue, category, symbol)

    async def contract_size(self, ticker: UniversalTicker) -> Decimal | None:
        return None


class FakeApi:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.results: dict[str, Any] = {}

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=BASE, transport=httpx.MockTransport(self._handle)
        )

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(
            200,
            json={"jsonrpc": "2.0", "result": self.results.get(request.url.path, {})},
        )


def test_intervals_translate_into_deribit_own_vocabulary() -> None:
    assert venue_interval("1m") == "1"
    assert venue_interval("1h") == "60"
    assert venue_interval("2h") == "120"
    assert venue_interval("1d") == "1D"
    with pytest.raises(InvalidIntervalError):
        venue_interval("4h")


def _client(
    api: FakeApi, feed: DeribitPublicStream | None = None
) -> DeribitPublicClient:
    return DeribitPublicClient(
        symbols=StubSymbols(),
        rest=DeribitPublicRest(base_url=BASE, client=api.client()),
        feed=feed,
    )


async def test_feeds_start_empty_and_refuse_missing_methods() -> None:
    api = FakeApi()
    async with _client(api) as client:
        assert client._feed is None
        assert not hasattr(client, "stream_agg_trades")
        assert not hasattr(client, "stream_liquidation")
        assert hasattr(client, "stream_funding_rate")
        assert hasattr(client, "stream_open_interest")


async def test_i6_spot_has_no_funding_or_oi() -> None:
    api = FakeApi()
    async with _client(api) as client:
        with pytest.raises(ValueError, match="funding"):
            client.stream_funding_rate(SPOT)
        with pytest.raises(ValueError, match="open interest"):
            client.stream_open_interest(SPOT)
        with pytest.raises(ValueError, match="funding"):
            client.stream_funding_rate(DATED)
        client.stream_funding_rate(PERP)
        client.stream_funding_rate(INVERSE)
        client.stream_open_interest(PERP)
        client.stream_open_interest(INVERSE)
        client.stream_open_interest(DATED)


async def test_a_spot_ticker_prints_on_the_one_public_socket(
    deribit_public: FakeDeribit,
) -> None:
    api = FakeApi()
    client = _client(
        api,
        DeribitPublicStream(deribit_public.url, ping_interval=0, heartbeat=0),
    )
    async with client:
        stream = client.stream_ticker(SPOT)
        task = asyncio.ensure_future(stream.__anext__())
        await asyncio.sleep(0.05)
        await deribit_public.push(
            ch.ticker("BTC_USDC"),
            {
                "instrument_name": "BTC_USDC",
                "last_price": "60000",
                "best_bid_price": "59999",
                "best_ask_price": "60001",
            },
        )
        ticker = await asyncio.wait_for(task, 2)
    assert ticker.last == Decimal("60000")
    assert ticker.universal_ticker == "Deribit_Spot_BTCUSDC"


async def test_v5_funding_and_oi_ride_the_ticker(
    deribit_public: FakeDeribit,
) -> None:
    api = FakeApi()
    feed = DeribitPublicStream(deribit_public.url, ping_interval=0, heartbeat=0)
    client = _client(api, feed)
    async with client:
        funding_stream = client.stream_funding_rate(PERP)
        oi_stream = client.stream_open_interest(PERP)
        funding_task = asyncio.ensure_future(funding_stream.__anext__())
        oi_task = asyncio.ensure_future(oi_stream.__anext__())
        await asyncio.sleep(0.05)
        await deribit_public.push(
            ch.ticker("BTC_USDC-PERPETUAL"),
            {
                "instrument_name": "BTC_USDC-PERPETUAL",
                "last_price": "60000",
                "current_funding": "0.0001",
                "open_interest": "487",
                "timestamp": 1700000001000,
            },
        )
        funding = await asyncio.wait_for(funding_task, 2)
        interest = await asyncio.wait_for(oi_task, 2)
    assert funding.rate == Decimal("0.0001")
    assert interest.qty == Decimal("487")
    assert deribit_public.subscribed == {ch.ticker("BTC_USDC-PERPETUAL")}


async def test_bestquote_and_trade_share_one_socket(
    deribit_public: FakeDeribit,
) -> None:
    api = FakeApi()
    client = _client(
        api,
        DeribitPublicStream(deribit_public.url, ping_interval=0, heartbeat=0),
    )
    async with client:
        quote_stream = client.stream_best_quote(SPOT)
        trade_stream = client.stream_trades(SPOT)
        quote_task = asyncio.ensure_future(quote_stream.__anext__())
        trade_task = asyncio.ensure_future(trade_stream.__anext__())
        await asyncio.sleep(0.05)
        await deribit_public.push(
            ch.quote("BTC_USDC"),
            {
                "instrument_name": "BTC_USDC",
                "best_bid_price": "79634.99",
                "best_bid_amount": "0.4",
                "best_ask_price": "79635",
                "best_ask_amount": "0.5",
            },
        )
        await deribit_public.push(
            ch.trades("BTC_USDC"),
            {
                "instrument_name": "BTC_USDC",
                "trade_id": "t-1",
                "price": "79635",
                "amount": "0.01",
                "direction": "buy",
                "timestamp": 1700000000000,
            },
        )
        quote = await asyncio.wait_for(quote_task, 2)
        trade = await asyncio.wait_for(trade_task, 2)
    assert quote.bid == Decimal("79634.99")
    assert trade.qty == Decimal("0.01")
    assert deribit_public.subscribed == {
        ch.quote("BTC_USDC"),
        ch.trades("BTC_USDC"),
    }


async def test_fetch_klines_use_the_resolved_instrument() -> None:
    api = FakeApi()
    api.results["/public/get_tradingview_chart_data"] = {
        "status": "ok",
        "ticks": [1700000000000, 1700000060000],
        "open": [1, 2],
        "high": [1, 2],
        "low": [1, 2],
        "close": [1, 2],
        "volume": [1, 1],
    }
    async with _client(api) as client:
        klines = await client.fetch_klines(PERP, "1h", limit=2)
    assert [k.interval for k in klines] == ["1h", "1h"]
    assert klines[0].open == Decimal("1")
    query = api.requests[0].url.query.decode()
    assert "instrument_name=BTC_USDC-PERPETUAL" in query
    assert "resolution=60" in query
