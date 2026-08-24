"""OKX market-data connector — interval translation and REST snapshots."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import httpx
import pytest
from mftik.exchange.intervals import InvalidIntervalError
from mftik.exchange.okx.public import OkxPublicClient, venue_interval
from mftik.exchange.okx.rest import OkxPublicRest
from mftik.exchange.tickers import UniversalTicker

TICKER = UniversalTicker.parse("Okx_Spot_BTCUSDT")
PERP = UniversalTicker.parse("Okx_Perp_BTCUSDT")
NATIVE = "BTC-USDT"
BASE = "https://okx.test"


class StubSymbols:
    async def exch_ticker(self, ticker: UniversalTicker) -> str:
        return "BTC-USDT-SWAP" if ticker.category.value == "Perp" else NATIVE

    async def symbol_for(
        self, venue: str, exch_ticker: str, *, category: str
    ) -> UniversalTicker:
        return UniversalTicker.of(venue, category, "BTCUSDT")


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
                "code": "0",
                "msg": "",
                "data": self.results.get(request.url.path, []),
            },
        )


def test_intervals_translate_into_okx_own_vocabulary() -> None:
    assert venue_interval("1m") == "1m"
    assert venue_interval("1h") == "1H"
    assert venue_interval("4h") == "4H"
    assert venue_interval("1d") == "1D"
    assert venue_interval("1mo") == "1M"


def test_an_interval_okx_does_not_serve_is_refused_before_the_round_trip() -> None:
    with pytest.raises(InvalidIntervalError, match="serves no"):
        venue_interval("1s")


def _client(api: FakeApi) -> OkxPublicClient:
    return OkxPublicClient(
        symbols=StubSymbols(),
        rest=OkxPublicRest(base_url=BASE, client=api.client()),
    )


async def test_fetch_ticker_uses_the_venues_spelling() -> None:
    api = FakeApi()
    api.results["/api/v5/market/ticker"] = [
        {
            "instId": NATIVE,
            "last": "60000",
            "bidPx": "59999",
            "askPx": "60001",
            "ts": "1700000000000",
        }
    ]
    async with _client(api) as client:
        ticker = await client.fetch_ticker(TICKER)
    assert ticker.last == Decimal("60000")
    assert ticker.bid == Decimal("59999")
    assert str(ticker.ticker) == "Okx_Spot_BTCUSDT"
    assert api.requests[0].url.params["instId"] == NATIVE


async def test_fetch_klines_answers_in_the_callers_interval() -> None:
    api = FakeApi()
    api.results["/api/v5/market/candles"] = [
        ["1700000060000", "2", "2", "2", "2", "1", "2", "2", "1"],
        ["1700000000000", "1", "1", "1", "1", "1", "1", "1", "1"],
    ]
    async with _client(api) as client:
        klines = await client.fetch_klines(TICKER, "1m", limit=2)
    assert [k.interval for k in klines] == ["1m", "1m"]
    assert klines[0].open == Decimal("1")
    assert api.requests[0].url.params["bar"] == "1m"


async def test_a_perp_liquidation_stream_is_offered_and_spot_is_not() -> None:
    api = FakeApi()
    async with _client(api) as client:
        # Construction only — the iterator is not driven.
        client.stream_liquidation(PERP)
        with pytest.raises(ValueError, match="serves no liquidation"):
            client.stream_liquidation(TICKER)
