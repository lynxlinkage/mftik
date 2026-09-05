"""Bitget private client — UTA gate, qty units, hedge, both linear books."""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

import httpx
import pytest
from mftik.exchange.bitget.models import BitgetAccount, BitgetFill, BitgetPosition
from mftik.exchange.bitget.private import (
    ACCEPTED_ACCOUNT_MODES,
    HEDGE_MODE,
    ONE_WAY_MODE,
    BitgetPrivateClient,
)
from mftik.exchange.bitget.protocol import BitgetAuthError
from mftik.exchange.bitget.rest import BitgetRest
from mftik.exchange.errors import OrderError
from mftik.exchange.models import (
    OrderStatus,
    OrderType,
    PlaceOrderRequest,
    Side,
    TimeInForce,
)
from mftik.exchange.stream import EventStream
from mftik.exchange.tickers import Category, UniversalTicker

SPOT = UniversalTicker.parse("Bitget_Spot_BTCUSDT")
PERP = UniversalTicker.parse("Bitget_Perp_BTCUSDT")
USDC = UniversalTicker.parse("Bitget_Perp_BTCUSDC")
BASE = "https://bitget.test"
API_KEY = "key"
API_SECRET = "secret"
PASSPHRASE = "phrase"


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
        self.results: dict[str, Any] = {
            "/api/v3/account/settings": {
                "accountMode": "unified",
                "holdMode": ONE_WAY_MODE,
            }
        }
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
        data = self.results.get(path, [])
        return httpx.Response(
            200, json={"code": "00000", "msg": "success", "data": data}
        )

    def body_for(self, path: str) -> dict[str, Any]:
        for request in self.requests:
            if request.url.path == path and request.content:
                return json.loads(request.content)
        raise AssertionError(f"no body for {path}")

    def query_for(self, path: str) -> str:
        for request in self.requests:
            if request.url.path == path:
                return request.url.query.decode()
        raise AssertionError(f"no request for {path}")


class FakeStream:
    def __init__(self) -> None:
        self.orders: EventStream[Any] = EventStream()
        self.fills: EventStream[Any] = EventStream()
        self.account: EventStream[Any] = EventStream()
        self.positions: EventStream[Any] = EventStream()
        self.connected = False
        self._reconnect_cbs: list[Any] = []

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.connected = False

    def on_reconnect(self, callback) -> None:
        self._reconnect_cbs.append(callback)

    async def subscribe_orders(self) -> EventStream:
        return self.orders

    async def subscribe_fills(self) -> EventStream:
        return self.fills

    async def subscribe_account(self) -> EventStream:
        return self.account

    async def subscribe_positions(self) -> EventStream:
        return self.positions


def _client(
    api: FakeApi,
    stream: FakeStream | None = None,
) -> BitgetPrivateClient:
    return BitgetPrivateClient(
        api_key=API_KEY,
        api_secret=API_SECRET,
        passphrase=PASSPHRASE,
        symbols=StubSymbols(),
        rest=BitgetRest(
            api_key=API_KEY,
            api_secret=API_SECRET,
            passphrase=PASSPHRASE,
            base_url=BASE,
            client=api.client(),
        ),
        stream=stream or FakeStream(),
    )


def test_i2_a_missing_passphrase_fails_before_anything_is_sent() -> None:
    with pytest.raises(BitgetAuthError, match="passphrase"):
        BitgetPrivateClient(
            api_key="k",
            api_secret="s",
            passphrase="",
            symbols=StubSymbols(),
        )


async def test_i2_switching_account_mode_is_refused_before_any_order(
    api: FakeApi,
) -> None:
    api.results["/api/v3/account/settings"] = {
        "accountMode": "switching",
        "holdMode": ONE_WAY_MODE,
    }
    client = _client(api)
    with pytest.raises(BitgetAuthError, match="switching"):
        await client.connect()
    assert not client.connected


async def test_i2_upgrading_account_mode_is_refused(api: FakeApi) -> None:
    api.results["/api/v3/account/settings"] = {
        "accountMode": "upgrading",
        "holdMode": ONE_WAY_MODE,
    }
    with pytest.raises(BitgetAuthError, match="upgrading"):
        await _client(api).connect()


async def test_v8_unified_and_hybrid_are_accepted(api: FakeApi) -> None:
    for mode in sorted(ACCEPTED_ACCOUNT_MODES):
        api.results["/api/v3/account/settings"] = {
            "accountMode": mode,
            "holdMode": ONE_WAY_MODE,
        }
        api.requests.clear()
        async with _client(api) as client:
            assert client.connected
            assert client._account_mode == mode


async def test_v7_missing_hold_mode_is_refused(api: FakeApi) -> None:
    api.results["/api/v3/account/settings"] = {"accountMode": "unified"}
    with pytest.raises(BitgetAuthError, match="holdMode"):
        await _client(api).connect()


async def test_a_spot_limit_is_base_qty_on_spot_with_no_pos_side(
    api: FakeApi,
) -> None:
    api.results["/api/v3/trade/place-order"] = {
        "orderId": "ord-1",
        "clientOid": "c-42",
    }
    async with _client(api) as client:
        order = await client.place_order(
            PlaceOrderRequest(
                universal_ticker="Bitget_Spot_BTCUSDT",
                side=Side.BUY,
                type=OrderType.LIMIT,
                qty=Decimal("0.001"),
                price=Decimal("60000"),
                tif=TimeInForce.GTC,
                client_order_id="c-42",
            )
        )
    sent = api.body_for("/api/v3/trade/place-order")
    assert sent["category"] == "SPOT"
    assert sent["symbol"] == "BTCUSDT"
    assert sent["qty"] == "0.001"
    assert sent["orderType"] == "limit"
    assert "posSide" not in sent
    assert order.status is OrderStatus.PENDING_NEW
    assert order.order_id == "ord-1"


async def test_i9_a_spot_market_buy_in_base_is_refused(api: FakeApi) -> None:
    async with _client(api) as client:
        with pytest.raises(OrderError, match="quote"):
            await client.place_order(
                PlaceOrderRequest(
                    universal_ticker="Bitget_Spot_BTCUSDT",
                    side=Side.BUY,
                    type=OrderType.MARKET,
                    qty=Decimal("0.5"),
                )
            )
    assert not any(
        r.url.path == "/api/v3/trade/place-order" for r in api.requests
    )


async def test_i9_a_spot_market_buy_sends_quote_qty(api: FakeApi) -> None:
    api.results["/api/v3/trade/place-order"] = {"orderId": "ord-1"}
    async with _client(api) as client:
        await client.place_order(
            PlaceOrderRequest(
                universal_ticker="Bitget_Spot_BTCUSDT",
                side=Side.BUY,
                type=OrderType.MARKET,
                quote_qty=Decimal("100"),
            )
        )
    sent = api.body_for("/api/v3/trade/place-order")
    assert sent["qty"] == "100"
    assert sent["orderType"] == "market"
    assert sent["category"] == "SPOT"


async def test_i3_a_usdt_perp_sends_usdt_futures(api: FakeApi) -> None:
    api.results["/api/v3/trade/place-order"] = {"orderId": "ord-1"}
    async with _client(api) as client:
        await client.place_order(
            PlaceOrderRequest(
                universal_ticker="Bitget_Perp_BTCUSDT",
                side=Side.SELL,
                type=OrderType.LIMIT,
                qty=Decimal("0.01"),
                price=Decimal("60000"),
            )
        )
    sent = api.body_for("/api/v3/trade/place-order")
    assert sent["category"] == "USDT-FUTURES"
    assert sent["symbol"] == "BTCUSDT"
    assert sent["qty"] == "0.01"
    assert "posSide" not in sent


async def test_i3_a_usdc_perp_sends_usdc_futures(api: FakeApi) -> None:
    api.results["/api/v3/trade/place-order"] = {"orderId": "ord-1"}
    async with _client(api) as client:
        await client.place_order(
            PlaceOrderRequest(
                universal_ticker="Bitget_Perp_BTCUSDC",
                side=Side.BUY,
                type=OrderType.LIMIT,
                qty=Decimal("0.01"),
                price=Decimal("60000"),
            )
        )
    sent = api.body_for("/api/v3/trade/place-order")
    assert sent["category"] == "USDC-FUTURES"
    assert sent["symbol"] == "BTCPERP"
    assert sent["qty"] == "0.01"


async def test_i8_hedge_without_pos_side_is_not_sent(api: FakeApi) -> None:
    api.results["/api/v3/account/settings"] = {
        "accountMode": "unified",
        "holdMode": HEDGE_MODE,
    }
    async with _client(api) as client:
        with pytest.raises(OrderError, match="posSide"):
            await client.place_order(
                PlaceOrderRequest(
                    universal_ticker="Bitget_Perp_BTCUSDT",
                    side=Side.BUY,
                    type=OrderType.LIMIT,
                    qty=Decimal("0.01"),
                    price=Decimal("60000"),
                )
            )
    assert not any(
        r.url.path == "/api/v3/trade/place-order" for r in api.requests
    )


async def test_i8_hedge_sends_pos_side(api: FakeApi) -> None:
    api.results["/api/v3/account/settings"] = {
        "accountMode": "unified",
        "holdMode": HEDGE_MODE,
    }
    api.results["/api/v3/trade/place-order"] = {"orderId": "ord-1"}
    async with _client(api) as client:
        await client.place_order(
            PlaceOrderRequest(
                universal_ticker="Bitget_Perp_BTCUSDT",
                side=Side.BUY,
                type=OrderType.LIMIT,
                qty=Decimal("0.01"),
                price=Decimal("60000"),
                params={"posSide": "long"},
            )
        )
    assert api.body_for("/api/v3/trade/place-order")["posSide"] == "long"


async def test_i7_balances_come_from_uta_assets_only(api: FakeApi) -> None:
    api.results["/api/v3/account/assets"] = [
        {"coin": "USDT", "available": "10", "frozen": "1", "locked": "0"}
    ]
    async with _client(api) as client:
        balances = await client.fetch_balances()
    assert [r.url.path for r in api.requests].count(
        "/api/v3/account/funding-assets"
    ) == 0
    assert any(r.url.path == "/api/v3/account/assets" for r in api.requests)
    assert balances[0].asset == "USDT"
    assert balances[0].free == Decimal("10")
    assert balances[0].locked == Decimal("1")


async def test_a_funding_shaped_push_is_not_parsed_as_balance() -> None:
    """Funding-account rows use different field names; they must not become free."""
    wallet = BitgetAccount.model_validate(
        {"coin": "", "available": "", "assets": []}
    )
    assert wallet.to_balances() == []


async def test_a_usdc_fill_resolves_to_the_usdc_perp() -> None:
    api = FakeApi()
    stream = FakeStream()
    async with _client(api, stream) as client:
        fills = client.stream_fills()
        agen = fills.__aiter__()
        stream.fills.push(
            BitgetFill.model_validate(
                {
                    "category": "USDC-FUTURES",
                    "symbol": "BTCPERP",
                    "orderId": "ord-1",
                    "execId": "t-1",
                    "side": "buy",
                    "execPrice": "60000",
                    "execQty": "0.01",
                    "fee": "-0.1",
                    "feeCoin": "USDC",
                    "execTime": "1700000000000",
                }
            )
        )
        fill = await agen.__anext__()
        await agen.aclose()
    assert fill.ticker == USDC
    assert fill.qty == Decimal("0.01")


async def test_positions_are_asked_of_both_linear_books(api: FakeApi) -> None:
    api.results["/api/v3/position/current-position"] = [
        {
            "category": "USDT-FUTURES",
            "symbol": "BTCUSDT",
            "holdSide": "long",
            "total": "0.1",
            "openPriceAvg": "60000",
        }
    ]
    async with _client(api) as client:
        positions = await client.fetch_positions()
    paths = [r.url.path for r in api.requests]
    assert paths.count("/api/v3/position/current-position") == 2
    assert positions[0].universal_ticker == str(PERP)
    assert positions[0].qty == Decimal("0.1")
