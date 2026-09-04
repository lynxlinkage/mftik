"""Binance COIN-M on MD's read side — REST snapshots, klines need a size."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import httpx
import pytest
from mftik.exchange.binance.delivery.rest import (
    BinanceDeliveryPublicRest,
    BinanceDeliveryRestError,
)
from mftik.exchange.intervals import InvalidIntervalError
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol.query_codes import QueryCode
from mftik_md.errors import normalize
from mftik_md.fetch.readers import (
    BinanceDeliveryReader,
    NoReaderError,
    VenueReaderFactory,
)

TICKER = UniversalTicker.parse("BinanceDelivery_Inverse_BTCUSD")
NATIVE = "BTCUSD_PERP"
SIZE = Decimal("100")
BASE = "https://dapi.test"

#: Official dapi kline sample: ``[5]`` is contracts, ``[7]`` is base.
DAPI_KLINE = [
    1591258320000,
    "9640.7",
    "9642.4",
    "9640.6",
    "9642.0",
    "206",
    1591258379999,
    "2.13660389",
    48,
]

BOOK = {
    "lastUpdateId": 1027024,
    "E": 1589436922972,
    "T": 1589436922959,
    "bids": [["39999.00", "3"]],
    "asks": [["40001.00", "4"]],
}


class StubSymbols:
    def __init__(self, size: Decimal | None = SIZE) -> None:
        self.size = size

    async def exch_ticker(self, ticker: UniversalTicker) -> str:
        return NATIVE

    async def symbol_for(
        self, venue: str, exch_ticker: str, *, category: str
    ) -> UniversalTicker:
        return UniversalTicker.of(venue, category, "BTCUSD")

    async def contract_size(self, ticker: UniversalTicker) -> Decimal | None:
        return self.size


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

    def query(self, path: str) -> dict[str, str]:
        for request in self.requests:
            if request.url.path == path:
                return dict(request.url.params)
        raise AssertionError(f"no request to {path}")


def _reader(
    api: FakeApi, *, symbols: StubSymbols | None = None
) -> BinanceDeliveryReader:
    return BinanceDeliveryReader(
        symbols=symbols or StubSymbols(),  # type: ignore[arg-type]
        rest=BinanceDeliveryPublicRest(base_url=BASE, client=api.client()),
    )


async def test_candles_pass_quote_per_contract_and_swap_volumes() -> None:
    api = FakeApi()
    api.results["/dapi/v1/klines"] = [DAPI_KLINE]

    klines = await _reader(api).fetch_klines(TICKER, "1mo", limit=5)

    assert api.query("/dapi/v1/klines") == {
        "symbol": NATIVE,
        "interval": "1M",
        "limit": "5",
    }
    assert klines[0].interval == "1mo"
    assert klines[0].universal_ticker == str(TICKER)
    assert klines[0].volume == Decimal("2.13660389")
    assert klines[0].quote_volume == Decimal("20600")


async def test_a_missing_contract_size_never_hits_the_wire() -> None:
    api = FakeApi()
    api.results["/dapi/v1/klines"] = [DAPI_KLINE]

    with pytest.raises(ValueError, match="no contract_size"):
        await _reader(api, symbols=StubSymbols(size=None)).fetch_klines(
            TICKER, "1m", limit=5
        )
    assert not api.requests


async def test_unsupported_interval_never_hits_the_wire() -> None:
    api = FakeApi()
    with pytest.raises(InvalidIntervalError, match="no 1s candles"):
        await _reader(api).fetch_klines(TICKER, "1s", limit=5)
    assert not api.requests


async def test_the_book_stays_in_contracts() -> None:
    api = FakeApi()
    api.results["/dapi/v1/depth"] = BOOK

    book = await _reader(api).fetch_order_book(TICKER, depth=1)

    assert book.universal_ticker == str(TICKER)
    assert book.bids[0].qty == Decimal("3")
    assert book.ts == 1589436922.959

    quote = await _reader(api).fetch_best_quote(TICKER)
    assert quote is not None
    assert quote.bid_qty == Decimal("3")


async def test_funding_history_is_oldest_first() -> None:
    api = FakeApi()
    api.results["/dapi/v1/fundingRate"] = [
        {
            "symbol": NATIVE,
            "fundingTime": 1_700_000_000_000,
            "fundingRate": "0.0001",
        },
        {
            "symbol": NATIVE,
            "fundingTime": 1_700_028_800_000,
            "fundingRate": "0.0002",
        },
    ]

    rows = await _reader(api).fetch_funding_history(TICKER, limit=5)

    assert api.query("/dapi/v1/fundingRate") == {"symbol": NATIVE, "limit": "5"}
    assert [row.ts for row in rows] == [1_700_000_000.0, 1_700_028_800.0]
    assert rows[0].rate == Decimal("0.0001")

    before = len(api.requests)
    dated = UniversalTicker.parse("BinanceDelivery_Future_BTCUSD260925")
    with pytest.raises(NoReaderError, match="Future"):
        await _reader(api).fetch_funding_history(dated, limit=5)
    assert len(api.requests) == before
    assert (
        normalize(
            NoReaderError("BinanceDelivery Future serves no funding history"),
            venue="BinanceDelivery",
        )
        is QueryCode.MD_VENUE_UNSUPPORTED_READ
    )


async def test_open_interest_stays_in_contracts() -> None:
    api = FakeApi()
    api.results["/dapi/v1/openInterest"] = {
        "symbol": NATIVE,
        "openInterest": "890",
        "time": 1_700_000_000_000,
    }

    row = await _reader(api).fetch_open_interest(TICKER)

    assert api.query("/dapi/v1/openInterest") == {"symbol": NATIVE}
    assert row.qty == Decimal("890")
    assert row.ts == 1_700_000_000.0
    assert row.universal_ticker == str(TICKER)


async def test_a_body_without_open_interest_is_a_failed_read() -> None:
    """Absent is a failed read; the venue never omits the field on a 2xx.

    A refusal comes back 4xx with a ``code`` and is raised in the
    transport, so what lands here without it is a 2xx whose body did not
    parse. Zero would be indistinguishable from the real zero a newly
    listed contract has, and an ``ok`` zero is documented as a real print.
    """
    api = FakeApi()
    api.results["/dapi/v1/openInterest"] = {"symbol": NATIVE}

    with pytest.raises(BinanceDeliveryRestError, match="no openInterest"):
        await _reader(api).fetch_open_interest(TICKER)

    # A venue-sent zero still is one.
    api.results["/dapi/v1/openInterest"] = {
        "symbol": NATIVE,
        "openInterest": "0",
        "time": 1_700_000_000_000,
    }
    row = await _reader(api).fetch_open_interest(TICKER)
    assert row.qty == Decimal("0")
    assert row.ts == 1_700_000_000.0


async def test_the_factory_builds_a_binance_delivery_reader() -> None:
    factory = VenueReaderFactory(StubSymbols())  # type: ignore[arg-type]
    reader = await factory.create("BinanceDelivery")

    assert isinstance(reader, BinanceDeliveryReader)
    assert reader.venue == "BinanceDelivery"
