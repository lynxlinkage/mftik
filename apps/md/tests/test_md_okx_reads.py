"""OKX on MD's read side — one reader for both books."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import httpx
from mftik.exchange.okx.rest import OkxPublicRest
from mftik.exchange.tickers import UniversalTicker
from mftik_md.fetch.readers import OkxReader, VenueReaderFactory

SPOT = UniversalTicker.parse("Okx_Spot_BTCUSDT")
PERP = UniversalTicker.parse("Okx_Perp_BTCUSDT")
BASE = "https://okx.test"


class StubSymbols:
    async def exch_ticker(self, ticker: UniversalTicker) -> str:
        return "BTC-USDT-SWAP" if ticker.category.value == "Perp" else "BTC-USDT"

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


def _reader(api: FakeApi) -> OkxReader:
    return OkxReader(
        symbols=StubSymbols(),
        rest=OkxPublicRest(base_url=BASE, client=api.client()),
    )


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
    inst_ids = [r.url.params["instId"] for r in api.requests]
    assert inst_ids == ["BTC-USDT", "BTC-USDT-SWAP"]
    assert perp.universal_ticker == "Okx_Perp_BTCUSDT"


async def test_the_factory_builds_an_okx_reader() -> None:
    factory = VenueReaderFactory(StubSymbols())  # type: ignore[arg-type]
    reader = await factory.create("Okx")
    assert isinstance(reader, OkxReader)
    assert reader.venue == "Okx"
