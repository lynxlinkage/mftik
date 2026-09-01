"""OKX on MD's read side — one reader for both books.

Same unified-account shape as Bybit: the ticker names the book, and one
reader answers for spot and perps. REST only — OKX's sockets are feeds,
not a request/reply plane.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import httpx
import pytest
from mftik.exchange.intervals import InvalidIntervalError
from mftik.exchange.okx.protocol import OkxRestError
from mftik.exchange.okx.rest import OkxPublicRest
from mftik.exchange.tickers import Category, UniversalTicker
from mftik.protocol.query_codes import QueryCode
from mftik_md.errors import normalize
from mftik_md.fetch.readers import NoReaderError, OkxReader, VenueReaderFactory

SPOT = UniversalTicker.parse("Okx_Spot_BTCUSDT")
PERP = UniversalTicker.parse("Okx_Perp_BTCUSDT")
BASE = "https://okx.test"


class StubSymbols:
    async def exch_ticker(self, ticker: UniversalTicker) -> str:
        return "BTC-USDT-SWAP" if ticker.category is Category.PERP else "BTC-USDT"

    async def symbol_for(
        self, venue: str, exch_ticker: str, *, category: str
    ) -> UniversalTicker:
        return UniversalTicker.of(venue, category, "BTCUSDT")

    async def contract_size(self, ticker: UniversalTicker) -> Decimal | None:
        if ticker.category is Category.PERP:
            return Decimal("0.01")
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
                "code": "0",
                "msg": "",
                "data": self.results.get(request.url.path, []),
            },
        )

    def query_for(self, path: str) -> str:
        for request in self.requests:
            if request.url.path == path:
                return request.url.query.decode()
        raise AssertionError(f"no request for {path}")


def _reader(api: FakeApi) -> OkxReader:
    return OkxReader(
        symbols=StubSymbols(),
        rest=OkxPublicRest(base_url=BASE, client=api.client()),
    )


# --- the reads --------------------------------------------------------------


async def test_klines_come_back_oldest_first_in_the_asked_for_spelling() -> None:
    """OKX answers newest first and capitalises hour bars; neither escapes."""
    api = FakeApi()
    api.results["/api/v5/market/candles"] = [
        ["1700000060000", "2", "2", "2", "2", "1", "2", "2", "1"],
        ["1700000000000", "1", "1", "1", "1", "1", "1", "1", "1"],
    ]
    klines = await _reader(api).fetch_klines(SPOT, "1h", limit=2)

    assert [k.open_time for k in klines] == [1700000000.0, 1700000060.0]
    assert {k.interval for k in klines} == {"1h"}
    assert {k.universal_ticker for k in klines} == {str(SPOT)}
    assert "bar=1H" in api.query_for("/api/v5/market/candles")


async def test_an_interval_okx_does_not_serve_is_refused() -> None:
    api = FakeApi()
    with pytest.raises(InvalidIntervalError):
        await _reader(api).fetch_klines(SPOT, "1s", limit=1)
    assert not api.requests


async def test_one_reader_answers_spot_and_perp() -> None:
    api = FakeApi()
    api.results["/api/v5/market/books"] = [
        {
            "bids": [["59999", "1", "0", "1"]],
            "asks": [["60001", "1", "0", "1"]],
            "ts": "1700000000000",
        }
    ]
    reader = _reader(api)
    spot = await reader.fetch_order_book(SPOT, depth=5)
    perp = await reader.fetch_order_book(PERP, depth=5)
    assert spot.bids[0].price == Decimal("59999")
    assert spot.bids[0].qty == Decimal("1")
    inst_ids = [r.url.params["instId"] for r in api.requests]
    assert inst_ids == ["BTC-USDT", "BTC-USDT-SWAP"]
    assert perp.universal_ticker == "Okx_Perp_BTCUSDT"
    # SWAP book sizes in contracts; the reader emits base.
    assert perp.bids[0].qty == Decimal("0.01")


async def test_the_touch_is_read_off_the_ticker_row() -> None:
    api = FakeApi()
    api.results["/api/v5/market/ticker"] = [
        {
            "instId": "BTC-USDT",
            "last": "60000",
            "bidPx": "59999",
            "bidSz": "1.5",
            "askPx": "60001",
            "askSz": "2.5",
        }
    ]
    quote = await _reader(api).fetch_best_quote(SPOT)

    assert quote is not None
    assert (quote.bid, quote.bid_qty) == (Decimal("59999"), Decimal("1.5"))
    assert (quote.ask, quote.ask_qty) == (Decimal("60001"), Decimal("2.5"))
    assert quote.universal_ticker == str(SPOT)
    assert [r.url.path for r in api.requests] == ["/api/v5/market/ticker"]


async def test_an_empty_side_declines_to_answer_rather_than_quoting_zero() -> None:
    api = FakeApi()
    api.results["/api/v5/market/ticker"] = [
        {"instId": "BTC-USDT", "last": "60000", "askPx": "60001", "askSz": "1"}
    ]
    assert await _reader(api).fetch_best_quote(SPOT) is None


async def test_a_perp_kline_volume_is_converted_to_base() -> None:
    api = FakeApi()
    api.results["/api/v5/market/candles"] = [
        ["1700000000000", "1", "1", "1", "1", "10", "0.1", "600", "1"],
    ]
    klines = await _reader(api).fetch_klines(PERP, "1m", limit=1)
    assert klines[0].volume == Decimal("0.1")
    assert klines[0].quote_volume == Decimal("600")


async def test_funding_history_reverses_newest_first_and_refuses_spot() -> None:
    api = FakeApi()
    api.results["/api/v5/public/funding-rate-history"] = [
        {
            "instId": "BTC-USDT-SWAP",
            "fundingRate": "0.0002",
            "fundingTime": "1700028800000",
            "realizedRate": "0.0003",
        },
        {
            "instId": "BTC-USDT-SWAP",
            "fundingRate": "0.0001",
            "fundingTime": "1700000000000",
            "realizedRate": "0.00015",
        },
    ]

    rows = await _reader(api).fetch_funding_history(PERP, limit=5)

    query = api.query_for("/api/v5/public/funding-rate-history")
    assert "instId=BTC-USDT-SWAP" in query
    assert "limit=5" in query
    assert [row.ts for row in rows] == [1_700_000_000.0, 1_700_028_800.0]
    assert rows[0].rate == Decimal("0.0001")

    before = len(api.requests)
    with pytest.raises(NoReaderError, match="Spot"):
        await _reader(api).fetch_funding_history(SPOT, limit=5)
    assert len(api.requests) == before


async def test_a_ticker_from_another_venue_is_refused() -> None:
    api = FakeApi()
    with pytest.raises(ValueError, match="was handed a Binance ticker"):
        await _reader(api).fetch_order_book(
            UniversalTicker.parse("Binance_Spot_BTCUSDT"), depth=1
        )
    assert not api.requests


# --- the factory ------------------------------------------------------------


async def test_the_factory_builds_an_okx_reader() -> None:
    factory = VenueReaderFactory(StubSymbols())  # type: ignore[arg-type]
    reader = await factory.create("Okx")
    assert isinstance(reader, OkxReader)
    assert reader.venue == "Okx"


# --- the error table -------------------------------------------------------


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        (51000, QueryCode.VENUE_INVALID_PARAM),
        (50011, QueryCode.VENUE_RATE_LIMITED),
        (50001, QueryCode.VENUE_INTERNAL_ERROR),
    ],
)
def test_a_known_okx_code_becomes_a_query_code(
    code: int, expected: QueryCode
) -> None:
    assert normalize(OkxRestError(code, "nope"), venue="Okx") is expected
