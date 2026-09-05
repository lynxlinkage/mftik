"""Bitget on MD's read side — one reader, product_of(ticker) chooses category."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import httpx
import pytest
from mftik.exchange.bitget.protocol import BitgetRestError
from mftik.exchange.bitget.rest import BitgetPublicRest
from mftik.exchange.intervals import InvalidIntervalError
from mftik.exchange.tickers import Category, UniversalTicker
from mftik.protocol.query_codes import QueryCode
from mftik_md.errors import normalize
from mftik_md.fetch.readers import BitgetReader, NoReaderError, VenueReaderFactory

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

    def query_for(self, path: str) -> str:
        for request in self.requests:
            if request.url.path == path:
                return request.url.query.decode()
        raise AssertionError(f"no request for {path}")


def _reader(api: FakeApi) -> BitgetReader:
    return BitgetReader(
        symbols=StubSymbols(),
        rest=BitgetPublicRest(base_url=BASE, client=api.client()),
    )


async def test_klines_include_the_resolved_category() -> None:
    api = FakeApi()
    api.results["/api/v3/market/candles"] = [
        ["1700000060000", "2", "2", "2", "2", "1"],
        ["1700000000000", "1", "1", "1", "1", "1"],
    ]
    klines = await _reader(api).fetch_klines(USDC, "1h", limit=2)
    assert [k.open_time for k in klines] == [1700000000.0, 1700000060.0]
    query = api.query_for("/api/v3/market/candles")
    assert "category=USDC-FUTURES" in query
    assert "symbol=BTCPERP" in query
    assert "interval=1H" in query


async def test_usdt_perp_hits_usdt_futures() -> None:
    api = FakeApi()
    api.results["/api/v3/market/orderbook"] = {
        "b": [["59999", "1"]],
        "a": [["60001", "1"]],
        "ts": "1700000000000",
    }
    book = await _reader(api).fetch_order_book(PERP, depth=5)
    assert book.universal_ticker == str(PERP)
    assert "category=USDT-FUTURES" in api.query_for("/api/v3/market/orderbook")
    assert "symbol=BTCUSDT" in api.query_for("/api/v3/market/orderbook")


async def test_an_interval_bitget_does_not_serve_is_refused() -> None:
    api = FakeApi()
    with pytest.raises(InvalidIntervalError):
        await _reader(api).fetch_klines(SPOT, "2h", limit=1)
    assert not api.requests


async def test_spot_funding_and_oi_are_unsupported_reads() -> None:
    api = FakeApi()
    with pytest.raises(NoReaderError, match="Spot"):
        await _reader(api).fetch_funding_history(SPOT, limit=5)
    with pytest.raises(NoReaderError, match="Spot"):
        await _reader(api).fetch_open_interest(SPOT)
    assert not api.requests
    assert (
        normalize(NoReaderError("Bitget Spot serves no open interest"), venue="Bitget")
        is QueryCode.MD_VENUE_UNSUPPORTED_READ
    )


async def test_perp_funding_and_oi_are_served() -> None:
    api = FakeApi()
    api.results["/api/v3/market/history-fund-rate"] = {
        "resultList": [
            {
                "symbol": "BTCUSDT",
                "fundingRate": "0.0002",
                "fundingRateTimestamp": "1700028800000",
            },
            {
                "symbol": "BTCUSDT",
                "fundingRate": "0.0001",
                "fundingRateTimestamp": "1700000000000",
            },
        ]
    }
    api.results["/api/v3/market/open-interest"] = {
        "list": [{"symbol": "BTCUSDT", "openInterest": "1234"}],
        "ts": "1700000000000",
    }
    rows = await _reader(api).fetch_funding_history(PERP, limit=5)
    assert [row.ts for row in rows] == [1_700_000_000.0, 1_700_028_800.0]
    assert "category=USDT-FUTURES" in api.query_for(
        "/api/v3/market/history-fund-rate"
    )
    interest = await _reader(api).fetch_open_interest(PERP)
    assert interest.qty == Decimal("1234")


async def test_the_factory_builds_a_bitget_reader() -> None:
    factory = VenueReaderFactory(StubSymbols())  # type: ignore[arg-type]
    reader = await factory.create("Bitget")
    assert isinstance(reader, BitgetReader)
    assert reader.venue == "Bitget"


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        (40015, QueryCode.VENUE_INVALID_PARAM),
        (40034, QueryCode.VENUE_RATE_LIMITED),
        (45110, QueryCode.VENUE_SYMBOL_NOT_FOUND),
    ],
)
def test_a_known_bitget_code_becomes_a_query_code(
    code: int, expected: QueryCode
) -> None:
    assert normalize(BitgetRestError(code, "nope"), venue="Bitget") is expected
