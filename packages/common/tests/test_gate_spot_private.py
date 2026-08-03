"""Gate spot as a PrivateClient — order entry over WS, recon over REST."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import httpx
import pytest
from gate_stub import API_KEY, API_SECRET, FakeGate
from mft.exchange.errors import OrderError
from mft.exchange.gate.spot import channels as ch
from mft.exchange.gate.spot.private import GateSpotPrivateClient
from mft.exchange.gate.spot.rest import GateRestError, GateSpotRest, sign_rest
from mft.exchange.models import (
    OrderStatus,
    OrderType,
    PlaceOrderRequest,
    Side,
)


def _filled_order():
    from mft.exchange.gate.spot.models import GateOrderAck

    return GateOrderAck.model_validate(
        dict(OPEN_ORDER, status="closed", left="0")
    ).to_order()


OPEN_ORDER = {
    "id": "1852454420",
    "text": "t-42",
    "currency_pair": "BTC_USDT",
    "type": "limit",
    "account": "spot",
    "side": "buy",
    "amount": "0.001",
    "price": "60000",
    "left": "0.001",
    "status": "open",
    "update_time_ms": 1774613210391,
}


class FakeGateRest:
    """httpx MockTransport standing in for Gate's REST v4."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.routes: dict[str, Any] = {
            "/api/v4/spot/open_orders": [
                {"currency_pair": "BTC_USDT", "orders": [OPEN_ORDER]}
            ],
            "/api/v4/spot/accounts": [
                {"currency": "USDT", "available": "1000.5", "locked": "12"},
                {"currency": "BTC", "available": "0.25", "locked": "0"},
            ],
            # Per-pair variant, used when fetch_open_orders gets a symbol.
            "/api/v4/spot/orders": [OPEN_ORDER],
        }
        self.status = 200

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        if self.status >= 400:
            return httpx.Response(
                self.status,
                json={"label": "INVALID_KEY", "message": "invalid key"},
            )
        for route, payload in self.routes.items():
            if path.startswith(route):
                return httpx.Response(200, json=payload)
        return httpx.Response(404, json={"label": "NOT_FOUND", "message": path})

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=httpx.MockTransport(self.handler),
            base_url="https://api.gateio.ws",
        )


class StubResolver:
    """Stands in for the symbol plane: an exact, two-way lookup table."""

    def __init__(self, pairs: dict[str, str] | None = None) -> None:
        self.native = pairs or {"BTCUSDT": "BTC_USDT", "ETHUSDT": "ETH_USDT"}
        self.canonical = {v: k for k, v in self.native.items()}
        self.lookups = 0

    async def exch_ticker(
        self, venue: str, symbol: str, *, category: str = "spot"
    ) -> str:
        self.lookups += 1
        return self.native[symbol]

    async def symbol_for(
        self, venue: str, exch_ticker: str, *, category: str = "spot"
    ) -> str:
        return self.canonical[exch_ticker]


@pytest.fixture
def resolver() -> StubResolver:
    return StubResolver()


@pytest.fixture
def rest_stub() -> FakeGateRest:
    return FakeGateRest()


async def _private(
    gate: FakeGate,
    rest_stub: FakeGateRest,
    resolver: StubResolver | None = None,
) -> GateSpotPrivateClient:
    from mft.exchange.gate.spot.client import GateSpotWebSocket

    ws = GateSpotWebSocket(
        url=gate.url,  # type: ignore[attr-defined]
        api_key=API_KEY,
        api_secret=API_SECRET,
        ping_interval=0,
    )
    rest = GateSpotRest(
        api_key=API_KEY, api_secret=API_SECRET, client=rest_stub.client()
    )
    return GateSpotPrivateClient(
        api_key=API_KEY,
        api_secret=API_SECRET,
        ws=ws,
        rest=rest,
        symbols=resolver or StubResolver(),
    )


# --- REST signing / parsing -------------------------------------------------


async def test_rest_requests_are_signed(rest_stub: FakeGateRest) -> None:
    rest = GateSpotRest(
        api_key=API_KEY, api_secret=API_SECRET, client=rest_stub.client()
    )
    await rest.fetch_balances()

    request = rest_stub.requests[0]
    assert request.headers["KEY"] == API_KEY
    ts = request.headers["Timestamp"]
    expected, _ = sign_rest(
        API_SECRET, "GET", "/api/v4/spot/accounts", "", "", ts=float(ts)
    )
    assert request.headers["SIGN"] == expected


async def test_rest_balances_map_available_and_locked(
    rest_stub: FakeGateRest,
) -> None:
    rest = GateSpotRest(
        api_key=API_KEY, api_secret=API_SECRET, client=rest_stub.client()
    )
    balances = {b.asset: b for b in await rest.fetch_balances()}

    assert balances["USDT"].free == Decimal("1000.5")
    assert balances["USDT"].locked == Decimal("12")
    assert balances["USDT"].total == Decimal("1012.5")


async def test_rest_flattens_grouped_open_orders(
    rest_stub: FakeGateRest,
) -> None:
    """``/spot/open_orders`` nests orders per pair; recon wants a flat list."""
    rest = GateSpotRest(
        api_key=API_KEY, api_secret=API_SECRET, client=rest_stub.client()
    )
    orders = await rest.fetch_open_orders()

    assert len(orders) == 1
    assert orders[0].order_id == "1852454420"
    # The REST layer stays venue-native; the private client translates.
    assert orders[0].symbol == "BTC_USDT"
    assert orders[0].client_order_id == "42"
    assert orders[0].status is OrderStatus.NEW


async def test_rest_errors_surface_the_label(rest_stub: FakeGateRest) -> None:
    rest_stub.status = 401
    rest = GateSpotRest(
        api_key=API_KEY, api_secret=API_SECRET, client=rest_stub.client()
    )
    with pytest.raises(GateRestError, match="INVALID_KEY") as exc:
        await rest.fetch_balances()
    assert exc.value.status == 401


# --- private client ---------------------------------------------------------


async def test_recon_reads_go_over_rest(
    gate: FakeGate, rest_stub: FakeGateRest
) -> None:
    """The WS API has no spot order-list or balance channel; recon needs REST."""
    client = await _private(gate, rest_stub)
    async with client:
        orders = await client.fetch_open_orders()
        balances = await client.fetch_balances()

    assert [o.order_id for o in orders] == ["1852454420"]
    assert {b.asset for b in balances} == {"USDT", "BTC"}
    paths = [r.url.path for r in rest_stub.requests]
    assert "/api/v4/spot/open_orders" in paths
    assert "/api/v4/spot/accounts" in paths


async def test_place_order_goes_over_the_websocket(
    gate: FakeGate, rest_stub: FakeGateRest
) -> None:
    gate.api_data[ch.ORDER_PLACE] = {"result": OPEN_ORDER}
    client = await _private(gate, rest_stub)
    async with client:
        order = await client.place_order(
            PlaceOrderRequest(
                symbol="BTCUSDT",
                side=Side.BUY,
                type=OrderType.LIMIT,
                qty=Decimal("0.001"),
                price=Decimal("60000"),
                client_order_id="42",
            )
        )

    param = gate.api_call(ch.ORDER_PLACE)["payload"]["req_param"]
    assert param["text"] == "t-42"
    assert param["account"] == "spot"
    assert order.order_id == "1852454420"
    assert order.status is OrderStatus.NEW
    # Order entry must not have touched REST.
    assert rest_stub.requests == []


async def test_market_buy_is_refused(
    gate: FakeGate, rest_stub: FakeGateRest
) -> None:
    """Gate sizes a spot market buy in quote; PlaceOrderRequest.qty is base."""
    client = await _private(gate, rest_stub)
    async with client:
        with pytest.raises(OrderError, match="quote currency"):
            await client.place_order(
                PlaceOrderRequest(
                    symbol="BTCUSDT",
                    side=Side.BUY,
                    type=OrderType.MARKET,
                    qty=Decimal("0.001"),
                )
            )
    assert not any(c["channel"] == ch.ORDER_PLACE for c in gate.api_calls)


async def test_market_sell_is_ioc(
    gate: FakeGate, rest_stub: FakeGateRest
) -> None:
    gate.api_data[ch.ORDER_PLACE] = {"result": OPEN_ORDER}
    client = await _private(gate, rest_stub)
    async with client:
        await client.place_order(
            PlaceOrderRequest(
                symbol="BTCUSDT",
                side=Side.SELL,
                type=OrderType.MARKET,
                qty=Decimal("0.001"),
            )
        )

    param = gate.api_call(ch.ORDER_PLACE)["payload"]["req_param"]
    assert param["type"] == "market"
    assert param["time_in_force"] == "ioc"
    assert "price" not in param


async def test_limit_order_without_price_is_refused(
    gate: FakeGate, rest_stub: FakeGateRest
) -> None:
    client = await _private(gate, rest_stub)
    async with client:
        with pytest.raises(OrderError, match="requires a price"):
            await client.place_order(
                PlaceOrderRequest(
                    symbol="BTCUSDT",
                    side=Side.BUY,
                    type=OrderType.LIMIT,
                    qty=Decimal("1"),
                )
            )


async def test_placing_an_order_caches_its_pair_for_cancel(
    gate: FakeGate, rest_stub: FakeGateRest
) -> None:
    """Gate needs the pair to cancel; the shared interface only passes an id.

    An order we placed is already known, so the cancel must not pay a REST
    round-trip to look the pair up.
    """
    gate.api_data[ch.ORDER_PLACE] = {"result": OPEN_ORDER}
    gate.api_data[ch.ORDER_CANCEL] = {
        "result": dict(OPEN_ORDER, status="cancelled")
    }
    client = await _private(gate, rest_stub)
    async with client:
        await client.place_order(
            PlaceOrderRequest(
                symbol="BTCUSDT",
                side=Side.BUY,
                type=OrderType.LIMIT,
                qty=Decimal("0.001"),
                price=Decimal("60000"),
                client_order_id="42",
            )
        )
        cancelled = await client.cancel_order("1852454420")

    assert cancelled.status is OrderStatus.CANCELED
    assert gate.api_call(ch.ORDER_CANCEL)["payload"]["req_param"] == {
        "order_id": "1852454420",
        "currency_pair": "BTC_USDT",
        "account": "spot",
    }
    assert rest_stub.requests == [], "cancel should not need a REST lookup"


async def test_cancel_of_an_unseen_order_falls_back_to_rest(
    gate: FakeGate, rest_stub: FakeGateRest
) -> None:
    gate.api_data[ch.ORDER_CANCEL] = {
        "result": dict(OPEN_ORDER, status="cancelled")
    }
    client = await _private(gate, rest_stub)
    async with client:
        await client.cancel_order("1852454420")

    assert [r.url.path for r in rest_stub.requests] == [
        "/api/v4/spot/open_orders"
    ]
    param = gate.api_call(ch.ORDER_CANCEL)["payload"]["req_param"]
    assert param["currency_pair"] == "BTC_USDT"


async def test_terminal_orders_are_evicted_from_the_pair_cache(
    gate: FakeGate, rest_stub: FakeGateRest
) -> None:
    """A filled order's pair must not linger and mask a later id reuse."""
    client = await _private(gate, rest_stub)
    async with client:
        await client.fetch_open_orders()
        assert client._pairs["1852454420"] == "BTC_USDT"

        client._remember(_filled_order(), "BTC_USDT")
        assert "1852454420" not in client._pairs
        assert "42" not in client._pairs
        assert "t-42" not in client._pairs


async def test_cancel_by_client_order_id_uses_gate_text_form(
    gate: FakeGate, rest_stub: FakeGateRest
) -> None:
    gate.api_data[ch.ORDER_CANCEL] = {"result": dict(OPEN_ORDER)}
    client = await _private(gate, rest_stub)
    async with client:
        await client.cancel_by_client_order_id("42")

    param = gate.api_call(ch.ORDER_CANCEL)["payload"]["req_param"]
    assert param["order_id"] == "t-42"
    assert param["currency_pair"] == "BTC_USDT"


async def test_cancel_of_unknown_order_raises_order_error(
    gate: FakeGate, rest_stub: FakeGateRest
) -> None:
    client = await _private(gate, rest_stub)
    async with client:
        with pytest.raises(OrderError, match="no open gate_spot order"):
            await client.cancel_order("does-not-exist")


async def test_venue_rejection_becomes_order_error(
    gate: FakeGate, rest_stub: FakeGateRest
) -> None:
    """TD turns OrderError into an order reject; a transport error it retries."""
    gate.api_data[ch.ORDER_PLACE] = {
        "errs": {"label": "BALANCE_NOT_ENOUGH", "message": "no funds"}
    }
    client = await _private(gate, rest_stub)
    async with client:
        with pytest.raises(OrderError, match="BALANCE_NOT_ENOUGH"):
            await client.place_order(
                PlaceOrderRequest(
                    symbol="BTCUSDT",
                    side=Side.BUY,
                    type=OrderType.LIMIT,
                    qty=Decimal("1"),
                    price=Decimal("1"),
                )
            )


async def test_streams_convert_to_shared_models(
    gate: FakeGate, rest_stub: FakeGateRest
) -> None:
    import asyncio

    client = await _private(gate, rest_stub)
    async with client:
        orders = client.stream_orders()
        fills = client.stream_fills()
        balances = client.stream_balances()
        # Async generators subscribe lazily; kick them off before pushing.
        pending = [
            asyncio.ensure_future(anext(orders)),
            asyncio.ensure_future(anext(fills)),
            asyncio.ensure_future(anext(balances)),
        ]
        await asyncio.sleep(0.15)

        await gate.push(
            ch.ORDERS,
            [dict(OPEN_ORDER, event="finish", finish_as="filled", left="0")],
        )
        await gate.push(
            ch.USER_TRADES,
            [
                {
                    "id": 7,
                    "order_id": "1852454420",
                    "currency_pair": "BTC_USDT",
                    "create_time_ms": "1774613210391",
                    "side": "buy",
                    "amount": "0.001",
                    "role": "taker",
                    "price": "60000",
                    "fee": "0.06",
                    "fee_currency": "USDT",
                    "text": "t-42",
                }
            ],
        )
        await gate.push(
            ch.BALANCES,
            [
                {
                    "currency": "USDT",
                    "total": "1000",
                    "available": "940",
                    "freeze": "60",
                    "change_type": "trade",
                }
            ],
        )
        order, fill, balance = await asyncio.wait_for(
            asyncio.gather(*pending), timeout=3.0
        )

    assert order.status is OrderStatus.FILLED
    assert order.symbol == "BTCUSDT"
    assert fill.order_id == "1852454420"
    assert fill.client_order_id == "42"
    assert fill.fee_asset == "USDT"
    assert balance.asset == "USDT"
    assert balance.free == Decimal("940")
    assert balance.locked == Decimal("60")


async def test_credentials_are_required() -> None:
    with pytest.raises(ValueError, match="required"):
        GateSpotPrivateClient(
            api_key="", api_secret="s", symbols=StubResolver()
        )


async def test_calls_before_connect_raise(
    gate: FakeGate, rest_stub: FakeGateRest
) -> None:
    from mft.exchange.errors import ExchangeNotConnectedError

    client = await _private(gate, rest_stub)
    with pytest.raises(ExchangeNotConnectedError):
        await client.fetch_balances()


# --- symbol boundary + extension params -------------------------------------


async def test_canonical_symbol_is_rendered_to_gates_spelling(
    gate: FakeGate, rest_stub: FakeGateRest
) -> None:
    gate.api_data[ch.ORDER_PLACE] = {"result": OPEN_ORDER}
    client = await _private(gate, rest_stub)
    async with client:
        order = await client.place_order(
            PlaceOrderRequest(
                symbol="BTCUSDT",
                side=Side.BUY,
                type=OrderType.LIMIT,
                qty=Decimal("0.001"),
                price=Decimal("60000"),
            )
        )

    # Canonical in, venue spelling on the wire, canonical back out.
    assert gate.api_call(ch.ORDER_PLACE)["payload"]["req_param"][
        "currency_pair"
    ] == "BTC_USDT"
    assert order.symbol == "BTCUSDT"


async def test_recon_returns_canonical_symbols(
    gate: FakeGate, rest_stub: FakeGateRest
) -> None:
    client = await _private(gate, rest_stub)
    async with client:
        orders = await client.fetch_open_orders()

    assert [o.symbol for o in orders] == ["BTCUSDT"]


async def test_symbol_filter_is_translated_on_the_way_out(
    gate: FakeGate, rest_stub: FakeGateRest
) -> None:
    client = await _private(gate, rest_stub)
    async with client:
        await client.fetch_open_orders("BTCUSDT")

    request = rest_stub.requests[-1]
    assert request.url.params["currency_pair"] == "BTC_USDT"


async def test_extension_params_reach_the_venue(
    gate: FakeGate, rest_stub: FakeGateRest
) -> None:
    """Gate-only order options ride in params; common fields stay common."""
    gate.api_data[ch.ORDER_PLACE] = {"result": OPEN_ORDER}
    client = await _private(gate, rest_stub)
    async with client:
        await client.place_order(
            PlaceOrderRequest(
                symbol="BTCUSDT",
                side=Side.BUY,
                type=OrderType.LIMIT,
                qty=Decimal("0.001"),
                price=Decimal("60000"),
                params={
                    "account": "margin",
                    "time_in_force": "poc",
                    "iceberg": "0.0001",
                    "stp_act": "cn",
                },
            )
        )

    param = gate.api_call(ch.ORDER_PLACE)["payload"]["req_param"]
    assert param["account"] == "margin"
    assert param["time_in_force"] == "poc"
    assert param["iceberg"] == "0.0001"
    assert param["stp_act"] == "cn"


async def test_params_default_to_the_clients_account(
    gate: FakeGate, rest_stub: FakeGateRest
) -> None:
    gate.api_data[ch.ORDER_PLACE] = {"result": OPEN_ORDER}
    client = await _private(gate, rest_stub)
    async with client:
        await client.place_order(
            PlaceOrderRequest(
                symbol="BTCUSDT",
                side=Side.BUY,
                type=OrderType.LIMIT,
                qty=Decimal("0.001"),
                price=Decimal("60000"),
            )
        )

    assert gate.api_call(ch.ORDER_PLACE)["payload"]["req_param"]["account"] == (
        "spot"
    )


async def test_params_cannot_shadow_a_common_field(
    gate: FakeGate, rest_stub: FakeGateRest
) -> None:
    """A params key that contradicts the request is dropped, not obeyed."""
    gate.api_data[ch.ORDER_PLACE] = {"result": OPEN_ORDER}
    client = await _private(gate, rest_stub)
    async with client:
        await client.place_order(
            PlaceOrderRequest(
                symbol="BTCUSDT",
                side=Side.BUY,
                type=OrderType.LIMIT,
                qty=Decimal("0.001"),
                price=Decimal("60000"),
                params={"currency_pair": "ETH_USDT", "side": "sell"},
            )
        )

    param = gate.api_call(ch.ORDER_PLACE)["payload"]["req_param"]
    assert param["currency_pair"] == "BTC_USDT"
    assert param["side"] == "buy"


async def test_params_override_the_market_order_tif(
    gate: FakeGate, rest_stub: FakeGateRest
) -> None:
    gate.api_data[ch.ORDER_PLACE] = {"result": OPEN_ORDER}
    client = await _private(gate, rest_stub)
    async with client:
        await client.place_order(
            PlaceOrderRequest(
                symbol="BTCUSDT",
                side=Side.SELL,
                type=OrderType.MARKET,
                qty=Decimal("0.001"),
                params={"time_in_force": "fok"},
            )
        )

    param = gate.api_call(ch.ORDER_PLACE)["payload"]["req_param"]
    assert param["time_in_force"] == "fok"


async def test_translation_is_a_lookup_not_a_string_transform(
    gate: FakeGate, rest_stub: FakeGateRest
) -> None:
    """A venue ticker that is not base+separator+quote must still resolve.

    Stripping separators would turn ``XBTUSD`` into ``XBTUSD``, not ``BTCUSD``.
    Only a lookup gets this right, which is why the adapter takes a resolver.
    """
    resolver = StubResolver({"BTCUSD": "XBTUSD"})
    gate.api_data[ch.ORDER_PLACE] = {"result": dict(OPEN_ORDER, currency_pair="XBTUSD")}
    client = await _private(gate, rest_stub, resolver)
    async with client:
        order = await client.place_order(
            PlaceOrderRequest(
                symbol="BTCUSD",
                side=Side.BUY,
                type=OrderType.LIMIT,
                qty=Decimal("0.001"),
                price=Decimal("60000"),
            )
        )

    assert gate.api_call(ch.ORDER_PLACE)["payload"]["req_param"][
        "currency_pair"
    ] == "XBTUSD"
    assert order.symbol == "BTCUSD"


async def test_adapter_asks_the_resolver_for_every_outbound_symbol(
    gate: FakeGate, rest_stub: FakeGateRest
) -> None:
    resolver = StubResolver()
    gate.api_data[ch.ORDER_PLACE] = {"result": OPEN_ORDER}
    client = await _private(gate, rest_stub, resolver)
    async with client:
        await client.place_order(
            PlaceOrderRequest(
                symbol="BTCUSDT",
                side=Side.BUY,
                type=OrderType.LIMIT,
                qty=Decimal("0.001"),
                price=Decimal("60000"),
            )
        )
    assert resolver.lookups == 1


async def test_input_spelling_is_normalized_before_lookup(
    gate: FakeGate, rest_stub: FakeGateRest
) -> None:
    """A caller passing the venue's spelling should still resolve."""
    gate.api_data[ch.ORDER_PLACE] = {"result": OPEN_ORDER}
    client = await _private(gate, rest_stub)
    async with client:
        await client.place_order(
            PlaceOrderRequest(
                symbol="BTC_USDT",  # not canonical
                side=Side.BUY,
                type=OrderType.LIMIT,
                qty=Decimal("0.001"),
                price=Decimal("60000"),
            )
        )
    assert gate.api_call(ch.ORDER_PLACE)["payload"]["req_param"][
        "currency_pair"
    ] == "BTC_USDT"
