"""Bitget public client — product_of routing, refusals, ticker-shared V5."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import httpx
import pytest
from bitget_stub import FakeBitget
from mftik.exchange.bitget import channels as ch
from mftik.exchange.bitget.feed import BitgetPublicStream
from mftik.exchange.bitget.public import BitgetPublicClient, venue_interval
from mftik.exchange.bitget.rest import BitgetPublicRest
from mftik.exchange.intervals import InvalidIntervalError
from mftik.exchange.tickers import UniversalTicker

SPOT = UniversalTicker.parse("Bitget_Spot_BTCUSDT")
PERP = UniversalTicker.parse("Bitget_Perp_BTCUSDT")
USDC = UniversalTicker.parse("Bitget_Perp_BTCUSDC")
BASE = "https://bitget.test"


class StubSymbols:
    async def exch_ticker(self, ticker: UniversalTicker) -> str:
        if ticker.symbol.endswith("USDC"):
            return ticker.symbol.replace("USDC", "PERP")
        return ticker.symbol

    async def symbol_for(
        self, venue: str, exch_ticker: str, *, category: str
    ) -> UniversalTicker:
        symbol = "BTCUSDC" if exch_ticker.endswith("PERP") else exch_ticker
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
            json={
                "code": "00000",
                "msg": "success",
                "data": self.results.get(request.url.path, []),
            },
        )


def test_intervals_translate_into_bitget_own_vocabulary() -> None:
    assert venue_interval("1m") == "1m"
    assert venue_interval("1h") == "1H"
    assert venue_interval("4h") == "4H"
    assert venue_interval("1d") == "1D"
    assert venue_interval("1mo") == "1M"
    with pytest.raises(InvalidIntervalError):
        venue_interval("2h")


def _client(api: FakeApi) -> BitgetPublicClient:
    return BitgetPublicClient(
        symbols=StubSymbols(),
        rest=BitgetPublicRest(base_url=BASE, client=api.client()),
    )


async def test_feeds_start_empty_and_open_per_product() -> None:
    api = FakeApi()
    async with _client(api) as client:
        assert client._feeds == {}
        assert not hasattr(client, "stream_agg_trades")
        assert hasattr(client, "stream_funding_rate")
        assert hasattr(client, "stream_open_interest")
        assert hasattr(client, "stream_liquidation")


async def test_a_spot_ticker_never_opens_a_futures_socket(
    bitget_public: FakeBitget,
) -> None:
    api = FakeApi()
    client = BitgetPublicClient(
        symbols=StubSymbols(),
        rest=BitgetPublicRest(base_url=BASE, client=api.client()),
        feeds={
            "SPOT": BitgetPublicStream(
                bitget_public.url, inst_type="spot", ping_interval=0
            )
        },
    )
    async with client:
        stream = client.stream_ticker(SPOT)
        task = asyncio.ensure_future(stream.__anext__())
        await asyncio.sleep(0.05)
        await bitget_public.push(
            ch.ticker("spot", "BTCUSDT"),
            [
                {
                    "symbol": "BTCUSDT",
                    "lastPrice": "60000",
                    "bid1Price": "59999",
                    "ask1Price": "60001",
                }
            ],
        )
        ticker = await asyncio.wait_for(task, 2)
        opened = set(client._feeds)
    assert ticker.last == Decimal("60000")
    assert opened == {"SPOT"}


async def test_a_public_trade_on_btcusdc_publishes_the_usdc_perp(
    bitget_public: FakeBitget,
) -> None:
    api = FakeApi()
    client = BitgetPublicClient(
        symbols=StubSymbols(),
        rest=BitgetPublicRest(base_url=BASE, client=api.client()),
        feeds={
            "USDC-FUTURES": BitgetPublicStream(
                bitget_public.url, inst_type="usdc-futures", ping_interval=0
            )
        },
    )
    async with client:
        stream = client.stream_trades(USDC)
        task = asyncio.ensure_future(stream.__anext__())
        await asyncio.sleep(0.05)
        await bitget_public.push(
            ch.public_trade("usdc-futures", "BTCPERP"),
            [
                {
                    "symbol": "BTCPERP",
                    "p": "60000",
                    "v": "0.01",
                    "S": "buy",
                    "i": "t-1",
                    "T": "1700000000000",
                }
            ],
        )
        trade = await asyncio.wait_for(task, 2)
    assert trade.ticker == USDC
    assert trade.universal_ticker == "Bitget_Perp_BTCUSDC"
    assert trade.qty == Decimal("0.01")


async def test_i6_spot_has_no_liquidation_funding_or_oi() -> None:
    api = FakeApi()
    async with _client(api) as client:
        with pytest.raises(ValueError, match="liquidation"):
            client.stream_liquidation(SPOT)
        with pytest.raises(ValueError, match="funding"):
            client.stream_funding_rate(SPOT)
        with pytest.raises(ValueError, match="open interest"):
            client.stream_open_interest(SPOT)
        client.stream_liquidation(PERP)
        client.stream_funding_rate(USDC)
        client.stream_open_interest(USDC)


async def test_bestquote_and_trade_both_print_on_one_spot_socket(
    bitget_public: FakeBitget,
) -> None:
    """V12: two id-less ACKs on one socket must not leave either feed silent."""
    api = FakeApi()
    client = BitgetPublicClient(
        symbols=StubSymbols(),
        rest=BitgetPublicRest(base_url=BASE, client=api.client()),
        feeds={
            "SPOT": BitgetPublicStream(
                bitget_public.url, inst_type="spot", ping_interval=0
            )
        },
    )
    async with client:
        quote_stream = client.stream_best_quote(SPOT)
        trade_stream = client.stream_trades(SPOT)
        quote_task = asyncio.ensure_future(quote_stream.__anext__())
        trade_task = asyncio.ensure_future(trade_stream.__anext__())
        await asyncio.sleep(0.05)
        await bitget_public.push(
            ch.books("spot", "BTCUSDT", topic=ch.BOOKS1),
            [{"b": [["79634.99", "0.4"]], "a": [["79635", "0.5"]], "ts": "1"}],
        )
        await bitget_public.push(
            ch.public_trade("spot", "BTCUSDT"),
            [
                {
                    "symbol": "BTCUSDT",
                    "p": "79635",
                    "v": "0.01",
                    "S": "buy",
                    "i": "t-1",
                    "T": "1700000000000",
                }
            ],
        )
        quote = await asyncio.wait_for(quote_task, 2)
        trade = await asyncio.wait_for(trade_task, 2)
    assert quote.bid == Decimal("79634.99")
    assert trade.qty == Decimal("0.01")
    assert bitget_public.subscribed == {
        ("books1", "BTCUSDT", "spot", ""),
        ("publicTrade", "BTCUSDT", "spot", ""),
    }


async def test_v5_funding_and_oi_ride_the_ticker(
    bitget_public: FakeBitget,
) -> None:
    api = FakeApi()
    feed = BitgetPublicStream(
        bitget_public.url, inst_type="usdt-futures", ping_interval=0
    )
    client = BitgetPublicClient(
        symbols=StubSymbols(),
        rest=BitgetPublicRest(base_url=BASE, client=api.client()),
        feeds={"USDT-FUTURES": feed},
    )
    async with client:
        funding_stream = client.stream_funding_rate(PERP)
        oi_stream = client.stream_open_interest(PERP)
        funding_task = asyncio.ensure_future(funding_stream.__anext__())
        oi_task = asyncio.ensure_future(oi_stream.__anext__())
        await asyncio.sleep(0.05)
        await bitget_public.push(
            ch.ticker("usdt-futures", "BTCUSDT"),
            [
                {
                    "symbol": "BTCUSDT",
                    "lastPrice": "60000",
                    "fundingRate": "0.0001",
                    "openInterest": "1234",
                    "ts": "1700000001000",
                }
            ],
        )
        funding = await asyncio.wait_for(funding_task, 2)
        interest = await asyncio.wait_for(oi_task, 2)
    assert funding.rate == Decimal("0.0001")
    assert interest.qty == Decimal("1234")
    assert bitget_public.subscribed == {
        ("ticker", "BTCUSDT", "usdt-futures", ""),
    }


async def test_fetch_klines_use_the_resolved_category() -> None:
    api = FakeApi()
    api.results["/api/v3/market/candles"] = [
        ["1700000060000", "2", "2", "2", "2", "1"],
        ["1700000000000", "1", "1", "1", "1", "1"],
    ]
    async with _client(api) as client:
        klines = await client.fetch_klines(USDC, "1h", limit=2)
    assert [k.interval for k in klines] == ["1h", "1h"]
    assert klines[0].open == Decimal("1")
    query = api.requests[0].url.query.decode()
    assert "category=USDC-FUTURES" in query
    assert "symbol=BTCPERP" in query
    assert "interval=1H" in query
