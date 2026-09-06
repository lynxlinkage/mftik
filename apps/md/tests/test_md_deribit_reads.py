"""Deribit on MD's read side — one reader, spot funding/OI refused."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import httpx
import pytest
from mftik.exchange.deribit.protocol import (
    DeribitRestError,
    expiry_code_from_name,
    expiry_suffix_from_code,
)
from mftik.exchange.deribit.rest import DeribitPublicRest
from mftik.exchange.intervals import InvalidIntervalError
from mftik.exchange.tickers import Category, UniversalTicker
from mftik.protocol.query_codes import QueryCode
from mftik_md.errors import normalize
from mftik_md.fetch.readers import DeribitReader, NoReaderError, VenueReaderFactory

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

    def query_for(self, path: str) -> str:
        for request in self.requests:
            if request.url.path == path:
                return request.url.query.decode()
        raise AssertionError(f"no request for {path}")


def _reader(api: FakeApi) -> DeribitReader:
    return DeribitReader(
        symbols=StubSymbols(),
        rest=DeribitPublicRest(base_url=BASE, client=api.client()),
    )


async def test_klines_include_the_resolved_instrument() -> None:
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
    klines = await _reader(api).fetch_klines(PERP, "1h", limit=2)
    assert [k.open_time for k in klines] == [1700000000.0, 1700000060.0]
    query = api.query_for("/public/get_tradingview_chart_data")
    assert "instrument_name=BTC_USDC-PERPETUAL" in query
    assert "resolution=60" in query


async def test_order_book_hits_the_perp_instrument() -> None:
    api = FakeApi()
    api.results["/public/get_order_book"] = {
        "bids": [["59999", "1"]],
        "asks": [["60001", "1"]],
        "timestamp": 1700000000000,
    }
    book = await _reader(api).fetch_order_book(PERP, depth=5)
    assert book.universal_ticker == str(PERP)
    assert "instrument_name=BTC_USDC-PERPETUAL" in api.query_for(
        "/public/get_order_book"
    )


async def test_an_interval_deribit_does_not_serve_is_refused() -> None:
    api = FakeApi()
    with pytest.raises(InvalidIntervalError):
        await _reader(api).fetch_klines(SPOT, "4h", limit=1)
    assert not api.requests


async def test_spot_funding_and_oi_are_unsupported_reads() -> None:
    api = FakeApi()
    with pytest.raises(NoReaderError, match="Spot"):
        await _reader(api).fetch_funding_history(SPOT, limit=5)
    with pytest.raises(NoReaderError, match="Spot"):
        await _reader(api).fetch_open_interest(SPOT)
    assert not api.requests
    assert (
        normalize(
            NoReaderError("Deribit Spot serves no open interest"), venue="Deribit"
        )
        is QueryCode.MD_VENUE_UNSUPPORTED_READ
    )


async def test_inverse_funding_is_served_and_dated_funding_is_refused() -> None:
    api = FakeApi()
    api.results["/public/get_funding_rate_history"] = [
        {"timestamp": 1700000000000, "interest_8h": "0.0001"},
    ]
    api.results["/public/ticker"] = {
        "instrument_name": "BTC-6SEP26",
        "open_interest": "2414140",
        "timestamp": 1700000000000,
    }
    rows = await _reader(api).fetch_funding_history(INVERSE, limit=5)
    assert rows[0].rate == Decimal("0.0001")
    assert "instrument_name=BTC-PERPETUAL" in api.query_for(
        "/public/get_funding_rate_history"
    )
    with pytest.raises(NoReaderError, match="Future"):
        await _reader(api).fetch_funding_history(DATED, limit=5)
    interest = await _reader(api).fetch_open_interest(DATED)
    assert interest.qty == Decimal("2414140")


async def test_perp_funding_and_oi_are_served() -> None:
    api = FakeApi()
    api.results["/public/get_funding_rate_history"] = [
        {"timestamp": 1700028800000, "interest_8h": "0.0002"},
        {"timestamp": 1700000000000, "interest_8h": "0.0001"},
    ]
    api.results["/public/ticker"] = {
        "instrument_name": "BTC_USDC-PERPETUAL",
        "open_interest": "487",
        "timestamp": 1700000000000,
    }
    rows = await _reader(api).fetch_funding_history(PERP, limit=5)
    assert [row.ts for row in rows] == [1_700_000_000.0, 1_700_028_800.0]
    assert "instrument_name=BTC_USDC-PERPETUAL" in api.query_for(
        "/public/get_funding_rate_history"
    )
    interest = await _reader(api).fetch_open_interest(PERP)
    assert interest.qty == Decimal("487")


async def test_the_factory_builds_a_deribit_reader() -> None:
    factory = VenueReaderFactory(StubSymbols())  # type: ignore[arg-type]
    reader = await factory.create("Deribit")
    assert isinstance(reader, DeribitReader)
    assert reader.venue == "Deribit"


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        (11050, QueryCode.VENUE_INVALID_PARAM),
        (10028, QueryCode.VENUE_RATE_LIMITED),
        (10009, QueryCode.VENUE_SYMBOL_NOT_FOUND),
        (11060, QueryCode.VENUE_INVALID_PARAM),
    ],
)
def test_a_known_deribit_code_becomes_a_query_code(
    code: int, expected: QueryCode
) -> None:
    assert normalize(DeribitRestError(code, "nope"), venue="Deribit") is expected
