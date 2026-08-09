"""Bybit on MD's read side — the reader, across both of the venue's books.

The reader is where a venue difference shows through the fetch plane: Gate
answers over REST, Binance over its WebSocket API, Bybit over REST with a
``category`` on every call. All three have to answer alike, so what is tested
here is that they do — and that one Bybit reader covers spot and perps, since
the ticker names the book.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import httpx
import pytest
from mft.exchange.bybit.rest import BybitPublicRest
from mft.exchange.intervals import InvalidIntervalError
from mft.exchange.tickers import UniversalTicker
from mft_md.fetch.readers import BybitReader, NoReaderError, VenueReaderFactory

SPOT = UniversalTicker.parse("Bybit_Spot_BTCUSDT")
PERP = UniversalTicker.parse("Bybit_Perp_BTCUSDT")
NATIVE = "BTC-USDT"
BASE = "https://bybit.test"


class StubSymbols:
    """A symbol plane whose venue spelling differs from the canonical one."""

    async def exch_ticker(self, ticker: UniversalTicker) -> str:
        return NATIVE

    async def symbol_for(
        self, venue: str, exch_ticker: str, *, category: str
    ) -> UniversalTicker:
        return UniversalTicker.of(venue, category, "BTCUSDT")


class FakeApi:
    """An httpx transport standing in for Bybit's REST API."""

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
                "retCode": 0,
                "retMsg": "OK",
                "result": self.results.get(request.url.path, {}),
            },
        )

    def query_for(self, path: str) -> str:
        for request in self.requests:
            if request.url.path == path:
                return request.url.query.decode()
        raise AssertionError(f"no request for {path}")


@pytest.fixture
def api() -> FakeApi:
    return FakeApi()


def _reader(api: FakeApi) -> BybitReader:
    return BybitReader(
        symbols=StubSymbols(),  # type: ignore[arg-type]
        rest=BybitPublicRest(base_url=BASE, client=api.client()),
    )


# --- the reads --------------------------------------------------------------


async def test_klines_come_back_oldest_first_in_the_asked_for_spelling(
    api: FakeApi,
) -> None:
    """Bybit answers newest first and names windows by the minute count;
    neither escapes this method."""
    api.results["/v5/market/kline"] = {
        "list": [
            ["1700000060000", "2", "2", "2", "2", "1", "2"],
            ["1700000000000", "1", "1", "1", "1", "1", "1"],
        ]
    }
    klines = await _reader(api).fetch_klines(SPOT, "1h", limit=2)

    assert [k.open_time for k in klines] == [1700000000.0, 1700000060.0]
    assert {k.interval for k in klines} == {"1h"}
    assert {k.universal_ticker for k in klines} == {str(SPOT)}
    # Bybit's own spelling of the window went down the wire, not ours.
    assert "interval=60" in api.query_for("/v5/market/kline")


async def test_an_interval_bybit_does_not_serve_is_refused(api: FakeApi) -> None:
    with pytest.raises(InvalidIntervalError):
        await _reader(api).fetch_klines(SPOT, "1s", limit=1)
    assert not api.requests


async def test_the_book_comes_back_under_the_ticker_it_was_asked_for(
    api: FakeApi,
) -> None:
    api.results["/v5/market/orderbook"] = {
        "s": NATIVE,
        "b": [["59999", "1"]],
        "a": [["60001", "2"]],
        "ts": 1700000000000,
    }
    book = await _reader(api).fetch_order_book(SPOT, depth=1)

    assert book.universal_ticker == str(SPOT)
    assert book.symbol == "BTCUSDT"
    assert book.ts == 1700000000.0


async def test_the_touch_is_read_off_the_ticker_row(api: FakeApi) -> None:
    """One call rather than a book: Bybit's REST ticker carries the sizes on
    every category, unlike the push, which omits them on spot."""
    api.results["/v5/market/tickers"] = {
        "list": [
            {
                "symbol": NATIVE,
                "lastPrice": "60000",
                "bid1Price": "59999",
                "bid1Size": "1.5",
                "ask1Price": "60001",
                "ask1Size": "2.5",
            }
        ]
    }
    quote = await _reader(api).fetch_best_quote(SPOT)

    assert quote is not None
    assert (quote.bid, quote.bid_qty) == (Decimal("59999"), Decimal("1.5"))
    assert (quote.ask, quote.ask_qty) == (Decimal("60001"), Decimal("2.5"))
    assert quote.universal_ticker == str(SPOT)
    assert [r.url.path for r in api.requests] == ["/v5/market/tickers"]


async def test_an_empty_side_declines_to_answer_rather_than_quoting_zero(
    api: FakeApi,
) -> None:
    """A caller asking for the touch is checking whether its price can rest
    against it, and a zero bid answers that wrongly."""
    api.results["/v5/market/tickers"] = {
        "list": [{"symbol": NATIVE, "lastPrice": "60000", "ask1Price": "60001",
                  "ask1Size": "1"}]
    }
    assert await _reader(api).fetch_best_quote(SPOT) is None


# --- one reader, both books -------------------------------------------------


async def test_the_ticker_picks_the_book_not_the_reader(api: FakeApi) -> None:
    """Unlike the other venues there is no per-reader market: a unified venue
    trades both, and the instrument says which."""
    api.results["/v5/market/orderbook"] = {"s": NATIVE, "b": [], "a": []}
    reader = _reader(api)

    await reader.fetch_order_book(SPOT, depth=1)
    await reader.fetch_order_book(PERP, depth=1)

    queries = [
        r.url.query.decode()
        for r in api.requests
        if r.url.path == "/v5/market/orderbook"
    ]
    assert "category=spot" in queries[0]
    assert "category=linear" in queries[1]


async def test_a_ticker_from_another_venue_is_refused(api: FakeApi) -> None:
    with pytest.raises(ValueError, match="was handed a Binance ticker"):
        await _reader(api).fetch_order_book(
            UniversalTicker.parse("Binance_Spot_BTCUSDT"), depth=1
        )
    assert not api.requests


# --- the factory ------------------------------------------------------------


async def test_the_factory_builds_a_bybit_reader() -> None:
    factory = VenueReaderFactory(StubSymbols())  # type: ignore[arg-type]
    reader = await factory.create("Bybit")

    assert isinstance(reader, BybitReader)
    assert reader.venue == "Bybit"


async def test_a_venue_with_no_reader_still_refuses_by_name() -> None:
    factory = VenueReaderFactory(StubSymbols())  # type: ignore[arg-type]
    with pytest.raises(NoReaderError):
        await factory.create("Kraken")
