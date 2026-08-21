"""Gate futures on MD's read side — REST snapshots in base qty."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import httpx
import pytest
from mftik.exchange.gate.future.rest import GateFuturesPublicRest
from mftik.exchange.intervals import InvalidIntervalError
from mftik.exchange.tickers import UniversalTicker
from mftik_md.fetch.readers import GateFuturesReader, VenueReaderFactory

TICKER = UniversalTicker.parse("GateFutures_Perp_BTCUSDT")
CS = Decimal("0.0001")
BASE = "https://api.gateio.ws"


class StubSymbols:
    async def exch_ticker(self, ticker: UniversalTicker) -> str:
        return "BTC_USDT"

    async def symbol_for(
        self, venue: str, exch_ticker: str, *, category: str
    ) -> UniversalTicker:
        return UniversalTicker.of(venue, category, "BTCUSDT")

    async def contract_size(self, ticker: UniversalTicker) -> Decimal | None:
        return CS


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
        return httpx.Response(200, json=self.results.get(request.url.path, {}))


def _reader(api: FakeApi) -> GateFuturesReader:
    return GateFuturesReader(
        symbols=StubSymbols(),
        rest=GateFuturesPublicRest(base_url=BASE, client=api.client()),
    )


async def test_candles_and_book_convert_contracts_to_base() -> None:
    api = FakeApi()
    api.results["/api/v4/futures/usdt/candlesticks"] = [
        {
            "t": 1_700_000_000,
            "o": "1",
            "h": "2",
            "l": "0.5",
            "c": "1.5",
            "v": "100",
            "sum": "150",
        }
    ]
    api.results["/api/v4/futures/usdt/order_book"] = {
        "current": 1_700_000_000_500,
        "bids": [["59999", "20"]],
        "asks": [["60001", "10"]],
    }

    klines = await _reader(api).fetch_klines(TICKER, "1w", limit=5)
    assert klines[0].interval == "1w"
    assert klines[0].volume == Decimal("0.01")

    book = await _reader(api).fetch_order_book(TICKER, depth=5)
    assert book.bids[0].qty == Decimal("0.002")

    quote = await _reader(api).fetch_best_quote(TICKER)
    assert quote is not None
    assert quote.bid_qty == Decimal("0.002")


async def test_unsupported_interval_never_hits_the_wire() -> None:
    api = FakeApi()
    with pytest.raises(InvalidIntervalError, match="no 1s candles"):
        await _reader(api).fetch_klines(TICKER, "1s", limit=5)
    assert not api.requests


async def test_the_factory_builds_a_gate_futures_reader() -> None:
    factory = VenueReaderFactory(StubSymbols())
    reader = await factory.create("GateFutures")
    assert isinstance(reader, GateFuturesReader)
    assert reader.venue == "GateFutures"
