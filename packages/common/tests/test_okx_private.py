"""The OKX trading connector — REST entry and an unscoped account stream."""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

import httpx
import pytest
from mftik.exchange.errors import OrderError
from mftik.exchange.models import (
    OrderStatus,
    OrderType,
    PlaceOrderRequest,
    Side,
    TimeInForce,
)
from mftik.exchange.okx.models import OkxFill, OkxOrderUpdate, OkxPosition
from mftik.exchange.okx.private import OkxPrivateClient
from mftik.exchange.okx.protocol import OkxAuthError
from mftik.exchange.okx.rest import OkxRest
from mftik.exchange.stream import EventStream
from mftik.exchange.tickers import Category, UniversalTicker

NATIVE = "BTC-USDT"
NATIVE_SWAP = "BTC-USDT-SWAP"
TICKER = UniversalTicker.parse("Okx_Spot_BTCUSDT")
PERP = UniversalTicker.parse("Okx_Perp_BTCUSDT")
BASE = "https://okx.test"
API_KEY = "key"
API_SECRET = "secret"
PASSPHRASE = "phrase"


class StubSymbols:
    async def exch_ticker(self, ticker: UniversalTicker) -> str:
        return NATIVE_SWAP if ticker.category is Category.PERP else NATIVE

    async def symbol_for(
        self, venue: str, exch_ticker: str, *, category: str
    ) -> UniversalTicker:
        symbol = "BTCUSDT"
        return UniversalTicker.of(venue, category, symbol)


class FakeApi:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.results: dict[str, Any] = {}
        self.errors: dict[str, tuple[int, str]] = {}

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=BASE, transport=httpx.MockTransport(self._handle)
        )

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        if path in self.errors:
            code, message = self.errors[path]
            return httpx.Response(
                200, json={"code": str(code), "msg": message, "data": []}
            )
        return httpx.Response(
            200,
            json={"code": "0", "msg": "", "data": self.results.get(path, [])},
        )

    def body_for(self, path: str) -> dict[str, Any]:
        for request in self.requests:
            if request.url.path == path and request.content:
                return json.loads(request.content)
        raise AssertionError(f"no body for {path}")


class FakeStream:
    """Stands in for the private socket: connectable, with pushable streams."""

    def __init__(self) -> None:
        self.orders: EventStream[OkxOrderUpdate] = EventStream()
        self.fills: EventStream[OkxFill] = EventStream()
        self.account: EventStream[Any] = EventStream()
        self.positions: EventStream[OkxPosition] = EventStream()
        self.connected = False
        self._reconnect_cbs: list[Any] = []

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.connected = False

    def on_reconnect(self, callback) -> None:
        self._reconnect_cbs.append(callback)

    async def subscribe_orders(self) -> EventStream[OkxOrderUpdate]:
        return self.orders

    async def subscribe_fills(self) -> EventStream[OkxFill]:
        return self.fills

    async def subscribe_account(self) -> EventStream[Any]:
        return self.account

    async def subscribe_positions(self) -> EventStream[OkxPosition]:
        return self.positions


def _client(api: FakeApi, stream: FakeStream | None = None) -> OkxPrivateClient:
    return OkxPrivateClient(
        api_key=API_KEY,
        api_secret=API_SECRET,
        passphrase=PASSPHRASE,
        symbols=StubSymbols(),
        rest=OkxRest(
            api_key=API_KEY,
            api_secret=API_SECRET,
            passphrase=PASSPHRASE,
            base_url=BASE,
            client=api.client(),
        ),
        stream=stream or FakeStream(),
    )


@pytest.fixture
def api() -> FakeApi:
    return FakeApi()


def test_a_missing_passphrase_fails_before_anything_is_sent() -> None:
    with pytest.raises(OkxAuthError, match="passphrase"):
        OkxPrivateClient(
            api_key="k",
            api_secret="s",
            passphrase="",
            symbols=StubSymbols(),
        )


async def test_a_limit_order_carries_the_venue_symbol_and_cash_td_mode(
    api: FakeApi,
) -> None:
    api.results["/api/v5/trade/order"] = [
        {"ordId": "ord-1", "clOrdId": "c-42", "sCode": "0"}
    ]
    async with _client(api) as client:
        order = await client.place_order(
            PlaceOrderRequest(
                universal_ticker="Okx_Spot_BTCUSDT",
                side=Side.BUY,
                type=OrderType.LIMIT,
                qty=Decimal("0.001"),
                price=Decimal("60000"),
                tif=TimeInForce.GTC,
                client_order_id="c-42",
            )
        )
    sent = api.body_for("/api/v5/trade/order")
    assert sent["instId"] == NATIVE
    assert sent["tdMode"] == "cash"
    assert sent["ordType"] == "limit"
    assert sent["sz"] == "0.001"
    assert sent["clOrdId"] == "c-42"
    assert "tgtCcy" not in sent
    assert order.status is OrderStatus.PENDING_NEW
    assert order.order_id == "ord-1"
    assert order.symbol == "BTCUSDT"


async def test_post_only_is_an_order_type_here(api: FakeApi) -> None:
    api.results["/api/v5/trade/order"] = [
        {"ordId": "ord-1", "clOrdId": "c", "sCode": "0"}
    ]
    async with _client(api) as client:
        await client.place_order(
            PlaceOrderRequest(
                universal_ticker="Okx_Spot_BTCUSDT",
                side=Side.BUY,
                type=OrderType.LIMIT,
                qty=Decimal("0.001"),
                price=Decimal("60000"),
                tif=TimeInForce.POST_ONLY,
                client_order_id="c",
            )
        )
    assert api.body_for("/api/v5/trade/order")["ordType"] == "post_only"


async def test_a_spot_market_buy_in_base_sends_tgt_ccy(api: FakeApi) -> None:
    """OKX's default for a market buy is quote; we size in base unless asked."""
    api.results["/api/v5/trade/order"] = [
        {"ordId": "ord-1", "sCode": "0"}
    ]
    async with _client(api) as client:
        await client.place_order(
            PlaceOrderRequest(
                universal_ticker="Okx_Spot_BTCUSDT",
                side=Side.BUY,
                type=OrderType.MARKET,
                qty=Decimal("0.5"),
            )
        )
    sent = api.body_for("/api/v5/trade/order")
    assert sent["ordType"] == "market"
    assert sent["tgtCcy"] == "base_ccy"
    assert sent["sz"] == "0.5"


async def test_a_perp_order_is_cross_and_net(api: FakeApi) -> None:
    api.results["/api/v5/trade/order"] = [
        {"ordId": "ord-1", "sCode": "0"}
    ]
    async with _client(api) as client:
        await client.place_order(
            PlaceOrderRequest(
                universal_ticker="Okx_Perp_BTCUSDT",
                side=Side.SELL,
                type=OrderType.LIMIT,
                qty=Decimal("1"),
                price=Decimal("60000"),
                reduce_only=True,
            )
        )
    sent = api.body_for("/api/v5/trade/order")
    assert sent["instId"] == NATIVE_SWAP
    assert sent["tdMode"] == "cross"
    assert sent["posSide"] == "net"
    assert sent["reduceOnly"] is True
    assert "tgtCcy" not in sent


async def test_a_venue_refusal_is_an_order_error(api: FakeApi) -> None:
    api.errors["/api/v5/trade/order"] = (51008, "Insufficient")
    async with _client(api) as client:
        with pytest.raises(OrderError, match="Insufficient"):
            await client.place_order(
                PlaceOrderRequest(
                    universal_ticker="Okx_Spot_BTCUSDT",
                    side=Side.BUY,
                    type=OrderType.MARKET,
                    qty=Decimal("1"),
                )
            )


async def test_open_orders_are_asked_of_both_books(api: FakeApi) -> None:
    api.results["/api/v5/trade/orders-pending"] = [
        {
            "instType": "SPOT",
            "instId": NATIVE,
            "ordId": "ord-1",
            "clOrdId": "c-1",
            "side": "buy",
            "ordType": "limit",
            "state": "live",
            "px": "60000",
            "sz": "0.001",
        }
    ]
    async with _client(api) as client:
        orders = await client.fetch_open_orders()
    paths = [r.url.path for r in api.requests]
    assert paths.count("/api/v5/trade/orders-pending") == 2
    assert orders[0].order_id == "ord-1"
    assert orders[0].status is OrderStatus.NEW


async def test_a_fill_push_resolves_the_row_own_book() -> None:
    api = FakeApi()
    stream = FakeStream()
    async with _client(api, stream) as client:
        fills = client.stream_fills()
        agen = fills.__aiter__()
        stream.fills.push(
            OkxFill.model_validate(
                {
                    "instType": "SWAP",
                    "instId": NATIVE_SWAP,
                    "ordId": "ord-1",
                    "tradeId": "t-1",
                    "side": "buy",
                    "fillPx": "60000",
                    "fillSz": "1",
                    "fillFee": "-0.1",
                    "fillFeeCcy": "USDT",
                    "ts": "1700000000000",
                }
            )
        )
        fill = await agen.__anext__()
        await agen.aclose()
    assert fill.ticker == PERP
    assert fill.qty == Decimal("1")
    assert fill.fee == Decimal("0.1")
