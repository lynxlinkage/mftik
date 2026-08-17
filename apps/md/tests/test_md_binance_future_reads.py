"""Binance futures on MD's read side — the reader, over REST.

The venue that shows the fetch plane's per-venue composition is not even per
*brand*: the spot reader holds a WebSocket API socket because Binance answers
candles there, and this one holds an HTTP client because on the futures market
it does not. Both have to answer alike, and that is what is tested here.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import httpx
import pytest
from mftik.exchange.binance.future.rest import BinanceFuturePublicRest
from mftik.exchange.intervals import InvalidIntervalError
from mftik.exchange.tickers import UniversalTicker
from mftik_md.fetch.readers import BinanceFutureReader, VenueReaderFactory

TICKER = UniversalTicker.parse("BinanceFuture_Perp_BTCUSDT")
NATIVE = "BTC-USDT"
BASE = "https://fapi.test"

KLINE_ROW = [
    1638747660000,
    "41000",
    "41200",
    "40900",
    "41100",
    "12",
    1638747719999,
    "492000",
    30,
    "6",
    "246000",
    "0",
]

BOOK = {
    "lastUpdateId": 1027024,
    "E": 1589436922972,
    "T": 1589436922959,
    "bids": [["39999.00", "3.000"]],
    "asks": [["40001.00", "4.000"]],
}


class StubSymbols:
    """A symbol plane whose venue spelling differs from the canonical one."""

    async def exch_ticker(self, ticker: UniversalTicker) -> str:
        return NATIVE

    async def symbol_for(
        self, venue: str, exch_ticker: str, *, category: str
    ) -> UniversalTicker:
        return UniversalTicker.of(venue, category, "BTCUSDT")


class FakeApi:
    """An httpx transport standing in for Binance's futures REST API."""

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


def _reader(api: FakeApi) -> BinanceFutureReader:
    return BinanceFutureReader(
        symbols=StubSymbols(),  # type: ignore[arg-type]
        rest=BinanceFuturePublicRest(base_url=BASE, client=api.client()),
    )


async def test_candles_are_asked_for_in_binances_spelling_and_answered_in_ours() -> (
    None
):
    api = FakeApi()
    api.results["/fapi/v1/klines"] = [KLINE_ROW]

    klines = await _reader(api).fetch_klines(TICKER, "1mo", limit=5)

    assert api.query("/fapi/v1/klines") == {
        "symbol": NATIVE,
        "interval": "1M",
        "limit": "5",
    }
    assert klines[0].interval == "1mo", "Binance's month spelling stays inside"
    assert klines[0].universal_ticker == str(TICKER)
    assert klines[0].close == Decimal("41100")


async def test_the_one_interval_spot_serves_and_futures_does_not() -> None:
    """Refused against the adapter's own table, before any round trip."""
    api = FakeApi()
    with pytest.raises(InvalidIntervalError, match="no 1s candles"):
        await _reader(api).fetch_klines(TICKER, "1s", limit=5)
    assert not api.requests


async def test_the_book_is_dated_by_the_venue() -> None:
    api = FakeApi()
    api.results["/fapi/v1/depth"] = BOOK

    book = await _reader(api).fetch_order_book(TICKER, depth=1)

    assert book.universal_ticker == str(TICKER)
    assert book.bids[0].price == Decimal("39999.00")
    assert book.ts == 1589436922.959


async def test_the_touch_comes_out_of_a_depth_one_book() -> None:
    api = FakeApi()
    api.results["/fapi/v1/depth"] = BOOK

    quote = await _reader(api).fetch_best_quote(TICKER)

    assert quote is not None
    assert (quote.bid, quote.ask) == (Decimal("39999.00"), Decimal("40001.00"))
    assert quote.bid_qty == Decimal("3.000")


async def test_an_empty_side_answers_none_rather_than_a_zero_quote() -> None:
    api = FakeApi()
    api.results["/fapi/v1/depth"] = {**BOOK, "bids": []}

    assert await _reader(api).fetch_best_quote(TICKER) is None


async def test_another_venues_ticker_never_reaches_the_wire() -> None:
    api = FakeApi()
    with pytest.raises(ValueError, match="was handed a Binance ticker"):
        await _reader(api).fetch_order_book(
            UniversalTicker.parse("Binance_Spot_BTCUSDT"), depth=1
        )
    assert not api.requests


async def test_the_factory_builds_a_binance_future_reader() -> None:
    factory = VenueReaderFactory(StubSymbols())  # type: ignore[arg-type]
    reader = await factory.create("BinanceFuture")

    assert isinstance(reader, BinanceFutureReader)
    assert reader.venue == "BinanceFuture"
