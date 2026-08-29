"""The COIN-M public REST client — listing and candles dapi has no socket for."""

from __future__ import annotations

from decimal import Decimal

import httpx
import pytest
from mftik.exchange.binance.delivery.rest import (
    BinanceDeliveryPublicRest,
    BinanceDeliveryRestError,
)
from mftik.exchange.tickers import UniversalTicker

TICKER = UniversalTicker.parse("BinanceDelivery_Inverse_BTCUSD")
BASE = "https://dapi.test"
SIZE = Decimal("100")

EXCHANGE_INFO = {
    "symbols": [
        {
            "symbol": "BTCUSD_PERP",
            "contractType": "PERPETUAL",
            "contractStatus": "TRADING",
            "contractSize": 100,
            "baseAsset": "BTC",
            "quoteAsset": "USD",
            "marginAsset": "BTC",
            "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.1"},
                {"filterType": "LOT_SIZE", "stepSize": "1", "minQty": "1"},
            ],
        },
        {
            "symbol": "BTCUSD_260925",
            "contractType": "CURRENT_QUARTER",
            "contractStatus": "TRADING",
            "contractSize": 100,
            "baseAsset": "BTC",
            "quoteAsset": "USD",
            "marginAsset": "BTC",
            "filters": [],
        },
        {
            "symbol": "HALTUSD_PERP",
            "contractType": "PERPETUAL",
            "contractStatus": "PENDING_TRADING",
            "contractSize": 10,
            "baseAsset": "HALT",
            "quoteAsset": "USD",
            "marginAsset": "HALT",
            "filters": [],
        },
    ]
}

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


class FakeApi:
    def __init__(self, response: httpx.Response | None = None) -> None:
        self.requests: list[httpx.Request] = []
        self.results: dict[str, object] = {}
        self.response = response

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=BASE, transport=httpx.MockTransport(self._handle)
        )

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.response is not None:
            return self.response
        return httpx.Response(200, json=self.results.get(request.url.path, {}))


async def test_only_active_perpetuals_are_listed() -> None:
    api = FakeApi()
    api.results["/dapi/v1/exchangeInfo"] = EXCHANGE_INFO

    instruments = await BinanceDeliveryPublicRest(
        base_url=BASE, client=api.client()
    ).fetch_instruments()

    assert api.requests[0].url.path == "/dapi/v1/exchangeInfo"
    assert [i.symbol for i in instruments] == ["BTCUSD"]
    assert instruments[0].exch_ticker == "BTCUSD_PERP"
    assert instruments[0].contract_size == Decimal("100")
    assert instruments[0].filters["min_notional"] is None


async def test_candles_are_read_as_coin_margined() -> None:
    """``[5]`` is contracts; quote volume is that count times contractSize."""
    api = FakeApi()
    api.results["/dapi/v1/klines"] = [DAPI_KLINE]

    klines = await BinanceDeliveryPublicRest(
        base_url=BASE, client=api.client()
    ).fetch_klines(
        "BTCUSD_PERP",
        "1m",
        ticker=TICKER,
        quote_per_contract=SIZE,
    )

    assert klines[0].volume == Decimal("2.13660389")
    assert klines[0].quote_volume == Decimal("20600")


async def test_candles_are_asked_for_within_binances_ceiling() -> None:
    api = FakeApi()
    api.results["/dapi/v1/klines"] = []

    await BinanceDeliveryPublicRest(base_url=BASE, client=api.client()).fetch_klines(
        "BTCUSD_PERP",
        "1m",
        ticker=TICKER,
        quote_per_contract=SIZE,
        limit=99999,
    )

    params = dict(api.requests[0].url.params)
    assert params["limit"] == "1500", "asking for more is a 400, not a truncation"
    assert api.requests[0].url.path == "/dapi/v1/klines"


async def test_a_venue_refusal_keeps_its_code() -> None:
    api = FakeApi(
        response=httpx.Response(400, json={"code": -1121, "msg": "Invalid symbol."})
    )
    rest = BinanceDeliveryPublicRest(base_url=BASE, client=api.client())

    with pytest.raises(BinanceDeliveryRestError) as caught:
        await rest.fetch_klines(
            "NOPEUSD_PERP",
            "1m",
            ticker=TICKER,
            quote_per_contract=SIZE,
        )

    assert caught.value.code == -1121
    assert caught.value.status == 400
