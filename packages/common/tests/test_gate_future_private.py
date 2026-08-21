"""Gate futures trading connector — place/cancel, positions, leverage."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import httpx
import pytest
from gate_future_stub import API_KEY, API_SECRET, FakeGateFutures
from mftik.exchange.errors import OrderError
from mftik.exchange.gate.future import channels as ch
from mftik.exchange.gate.future.client import GateFuturesWebSocket
from mftik.exchange.gate.future.private import GateFuturesPrivateClient
from mftik.exchange.gate.future.rest import GateFuturesRest
from mftik.exchange.models import OrderType, PlaceOrderRequest, Side, TimeInForce
from mftik.exchange.tickers import UniversalTicker

TICKER = UniversalTicker.parse("GateFutures_Perp_BTCUSDT")
CS = Decimal("0.0001")


class StubResolver:
    def __init__(self, size: Decimal | None = CS) -> None:
        self.size = size
        self.native = {"BTCUSDT": "BTC_USDT"}
        self.canonical = {"BTC_USDT": "BTCUSDT"}

    async def exch_ticker(self, ticker: UniversalTicker) -> str:
        return self.native[ticker.symbol]

    async def symbol_for(
        self, venue: str, exch_ticker: str, *, category: str
    ) -> UniversalTicker:
        return UniversalTicker.of(venue, category, self.canonical[exch_ticker])

    async def contract_size(self, ticker: UniversalTicker) -> Decimal | None:
        return self.size


class FakeRest:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.routes: dict[str, Any] = {
            "/api/v4/futures/usdt/accounts": {
                "currency": "USDT",
                "total": "1000",
                "available": "800",
            },
            "/api/v4/futures/usdt/positions": [
                {
                    "contract": "BTC_USDT",
                    "size": "10",
                    "entry_price": "60000",
                    "unrealised_pnl": "1.5",
                }
            ],
            "/api/v4/futures/usdt/get_leverage/BTC_USDT": {
                "leverage": "0",
                "cross_leverage_limit": "15",
            },
            "/api/v4/futures/usdt/orders": [],
        }

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        for route, payload in self.routes.items():
            if path == route or path.startswith(route + "/"):
                if "get_leverage" in path:
                    leverage = self.routes[
                        "/api/v4/futures/usdt/get_leverage/BTC_USDT"
                    ]
                    return httpx.Response(200, json=leverage)
                return httpx.Response(200, json=payload)
        return httpx.Response(404, json={"label": "NOT_FOUND", "message": path})

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=httpx.MockTransport(self.handler),
            base_url="https://api.gateio.ws",
        )


async def _private(
    gate: FakeGateFutures,
    rest_stub: FakeRest | None = None,
    resolver: StubResolver | None = None,
) -> GateFuturesPrivateClient:
    rest_stub = rest_stub or FakeRest()
    ws = GateFuturesWebSocket(
        url=gate.url,  # type: ignore[attr-defined]
        api_key=API_KEY,
        api_secret=API_SECRET,
        ping_interval=0,
    )
    rest = GateFuturesRest(
        api_key=API_KEY, api_secret=API_SECRET, client=rest_stub.client()
    )
    return GateFuturesPrivateClient(
        api_key=API_KEY,
        api_secret=API_SECRET,
        ws=ws,
        rest=rest,
        symbols=resolver or StubResolver(),
    )


def _limit(
    *, qty: str = "0.001", side: Side = Side.BUY, **kw: Any
) -> PlaceOrderRequest:
    return PlaceOrderRequest(
        universal_ticker=str(TICKER),
        side=side,
        type=OrderType.LIMIT,
        qty=Decimal(qty),
        price=Decimal("60000"),
        **kw,
    )


async def test_place_converts_base_qty_to_signed_contracts(
    gate_futures: FakeGateFutures,
) -> None:
    gate_futures.api_data[ch.ORDER_PLACE] = {
        "result": {
            "id": "11",
            "text": "t-1",
            "contract": "BTC_USDT",
            "size": "-10",
            "left": "-10",
            "price": "60000",
            "status": "open",
        }
    }
    client = await _private(gate_futures)
    async with client:
        order = await client.place_order(
            _limit(qty="0.001", side=Side.SELL, client_order_id="1")
        )

    param = gate_futures.api_call(ch.ORDER_PLACE)["payload"]["req_param"]
    assert param["size"] == "-10"
    assert param["tif"] == "gtc"
    assert order.qty == Decimal("0.001")
    assert order.side is Side.SELL


async def test_market_order_is_price_zero_ioc(
    gate_futures: FakeGateFutures,
) -> None:
    gate_futures.api_data[ch.ORDER_PLACE] = {
        "result": {
            "id": "12",
            "contract": "BTC_USDT",
            "size": "10",
            "left": "0",
            "price": "0",
            "status": "finished",
            "finish_as": "filled",
        }
    }
    client = await _private(gate_futures)
    async with client:
        await client.place_order(
            PlaceOrderRequest(
                universal_ticker=str(TICKER),
                side=Side.BUY,
                type=OrderType.MARKET,
                qty=Decimal("0.001"),
            )
        )
    param = gate_futures.api_call(ch.ORDER_PLACE)["payload"]["req_param"]
    assert param["price"] == "0"
    assert param["tif"] == "ioc"


async def test_post_only_and_reduce_only_go_on_the_wire(
    gate_futures: FakeGateFutures,
) -> None:
    gate_futures.api_data[ch.ORDER_PLACE] = {
        "result": {
            "id": "13",
            "contract": "BTC_USDT",
            "size": "10",
            "left": "10",
            "price": "60000",
            "status": "open",
        }
    }
    client = await _private(gate_futures)
    async with client:
        await client.place_order(
            _limit(tif=TimeInForce.POST_ONLY, reduce_only=True)
        )
    param = gate_futures.api_call(ch.ORDER_PLACE)["payload"]["req_param"]
    assert param["tif"] == "poc"
    assert param["reduce_only"] is True


async def test_missing_contract_size_refuses_to_guess(
    gate_futures: FakeGateFutures,
) -> None:
    client = await _private(gate_futures, resolver=StubResolver(size=None))
    async with client:
        with pytest.raises(OrderError, match="contract_size"):
            await client.place_order(_limit())


async def test_cancel_uses_the_remembered_contract(
    gate_futures: FakeGateFutures,
) -> None:
    gate_futures.api_data[ch.ORDER_PLACE] = {
        "result": {
            "id": "11",
            "text": "t-1",
            "contract": "BTC_USDT",
            "size": "10",
            "left": "10",
            "price": "60000",
            "status": "open",
        }
    }
    gate_futures.api_data[ch.ORDER_CANCEL] = {
        "result": {
            "id": "11",
            "text": "t-1",
            "contract": "BTC_USDT",
            "size": "10",
            "left": "10",
            "price": "60000",
            "status": "cancelled",
            "finish_as": "cancelled",
        }
    }
    client = await _private(gate_futures)
    async with client:
        await client.place_order(_limit(client_order_id="1"))
        canceled = await client.cancel_order("11")

    param = gate_futures.api_call(ch.ORDER_CANCEL)["payload"]["req_param"]
    assert param["order_id"] == "11"
    assert param["contract"] == "BTC_USDT"
    assert canceled.status.value == "canceled"


async def test_fetch_positions_and_leverage(
    gate_futures: FakeGateFutures,
) -> None:
    client = await _private(gate_futures)
    async with client:
        positions = await client.fetch_positions()
        leverage = await client.fetch_leverage(TICKER)

    assert positions[0].qty == Decimal("0.001")
    assert leverage == Decimal("15")
