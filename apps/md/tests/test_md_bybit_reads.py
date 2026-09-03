"""Bybit on MD's read side — the reader, across both of the venue's books.

The reader is where a venue difference shows through the fetch plane: Gate
answers over REST, Binance over its WebSocket API, Bybit over REST with a
``category`` on every call. All three have to answer alike, so what is tested
here is that they do — and that one Bybit reader covers spot and perps, since
the ticker names the book.
"""

from __future__ import annotations

import time
from decimal import Decimal
from typing import Any

import httpx
import pytest
from mftik.exchange.bybit.rest import BybitPublicRest
from mftik.exchange.intervals import InvalidIntervalError
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol.query_codes import QueryCode
from mftik_md.errors import normalize
from mftik_md.fetch.readers import BybitReader, NoReaderError, VenueReaderFactory

SPOT = UniversalTicker.parse("Bybit_Spot_BTCUSDT")
PERP = UniversalTicker.parse("Bybit_Perp_BTCUSDT")
FUTURE = UniversalTicker.parse("Bybit_Future_BTCUSDT")
OPTION = UniversalTicker.parse("Bybit_Option_BTCUSDT")
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
        #: Bybit's envelope clock, in ms. None leaves it off the reply, which
        #: is the shape a caller falling back to local receive has to handle.
        self.time: int | None = None

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=BASE, transport=httpx.MockTransport(self._handle)
        )

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        body: dict[str, Any] = {
            "retCode": 0,
            "retMsg": "OK",
            "result": self.results.get(request.url.path, {}),
        }
        if self.time is not None:
            body["time"] = self.time
        return httpx.Response(200, json=body)

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


async def test_funding_history_reverses_newest_first_and_refuses_spot(
    api: FakeApi,
) -> None:
    api.results["/v5/market/funding/history"] = {
        "list": [
            {
                "symbol": NATIVE,
                "fundingRate": "0.0002",
                "fundingRateTimestamp": "1700028800000",
            },
            {
                "symbol": NATIVE,
                "fundingRate": "0.0001",
                "fundingRateTimestamp": "1700000000000",
            },
        ]
    }

    rows = await _reader(api).fetch_funding_history(PERP, limit=5)

    assert "category=linear" in api.query_for("/v5/market/funding/history")
    assert "limit=5" in api.query_for("/v5/market/funding/history")
    assert [row.ts for row in rows] == [1_700_000_000.0, 1_700_028_800.0]
    assert rows[0].rate == Decimal("0.0001")

    before = len(api.requests)
    with pytest.raises(NoReaderError, match="spot"):
        await _reader(api).fetch_funding_history(SPOT, limit=5)
    assert len(api.requests) == before
    assert (
        normalize(NoReaderError("Bybit spot serves no funding history"), venue="Bybit")
        is QueryCode.MD_VENUE_UNSUPPORTED_READ
    )


async def test_open_interest_reads_the_ticker_and_refuses_only_spot(
    api: FakeApi,
) -> None:
    api.results["/v5/market/tickers"] = {
        "list": [
            {
                "symbol": NATIVE,
                "lastPrice": "60000",
                "openInterest": "1234.5",
            }
        ]
    }

    row = await _reader(api).fetch_open_interest(PERP)

    assert [r.url.path for r in api.requests] == ["/v5/market/tickers"]
    assert "category=linear" in api.query_for("/v5/market/tickers")
    assert "/v5/market/open-interest" not in {r.url.path for r in api.requests}
    assert row.qty == Decimal("1234.5")
    assert row.universal_ticker == str(PERP)

    dated = await _reader(api).fetch_open_interest(FUTURE)
    assert dated.qty == Decimal("1234.5")
    assert "category=linear" in api.requests[-1].url.query.decode()

    # Refused on the set the stream refuses on, so an option ticker is a
    # read we do not serve rather than a venue call that failed.
    before = len(api.requests)
    for ticker, name in ((SPOT, "Spot"), (OPTION, "Option")):
        with pytest.raises(NoReaderError, match=name):
            await _reader(api).fetch_open_interest(ticker)
    assert len(api.requests) == before
    assert (
        normalize(NoReaderError("Bybit Spot serves no open interest"), venue="Bybit")
        is QueryCode.MD_VENUE_UNSUPPORTED_READ
    )


async def test_open_interest_is_stamped_by_the_venue_envelope(
    api: FakeApi,
) -> None:
    """The v5 row has no clock, so the stamp comes off the reply envelope.

    Local receive would date the print by how long we took to ask, which
    makes a staleness check read ~0 no matter how old the figure is.
    """
    api.time = 1_700_000_000_000
    api.results["/v5/market/tickers"] = {
        "list": [{"symbol": NATIVE, "lastPrice": "60000", "openInterest": "7"}]
    }

    row = await _reader(api).fetch_open_interest(PERP)
    assert row.ts == 1_700_000_000.0

    # And local receive when Bybit sends no envelope clock at all.
    api.time = None
    fresh = await _reader(api).fetch_open_interest(PERP)
    assert fresh.ts == pytest.approx(time.time(), abs=5)


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
