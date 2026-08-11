"""The futures trading connector — three transports behind one session.

The order path is spot's with two differences that matter (post-only is a
time-in-force here, and open orders come over REST), and the report path is
entirely this market's own: a listen key issued on the API socket, opening a
second connection that carries orders, fills, balances and positions.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import pytest
from binance_future_stub import (
    API_KEY,
    LISTEN_KEY,
    FakeBinanceFutureApi,
    FakeBinanceFutureUser,
)
from mft.exchange.binance.future import methods as m
from mft.exchange.binance.future.client import BinanceFutureWsApi
from mft.exchange.binance.future.models import (
    BinanceFutureOrderAck,
    BinanceFutureSymbolConfig,
)
from mft.exchange.binance.future.private import BinanceFuturePrivateClient
from mft.exchange.errors import ExchangeError
from mft.exchange.binance.future.user import BinanceFutureUserStream
from mft.exchange.errors import OrderError
from mft.exchange.models import (
    OrderStatus,
    OrderType,
    PlaceOrderRequest,
    Side,
    TimeInForce,
)
from mft.exchange.tickers import UniversalTicker

NATIVE = "BTC-USDT"
TICKER = UniversalTicker.parse("BinanceFuture_Perp_BTCUSDT")

OPEN_ORDER = {
    "symbol": NATIVE,
    "orderId": 22542179,
    "clientOrderId": "c-42",
    "price": "40000",
    "avgPrice": "0",
    "origQty": "1.5",
    "executedQty": "0",
    "cumQuote": "0",
    "status": "NEW",
    "timeInForce": "GTC",
    "type": "LIMIT",
    "origType": "LIMIT",
    "side": "BUY",
    "positionSide": "BOTH",
    "updateTime": 1566818724722,
}


class StubSymbols:
    """A symbol plane whose venue spelling differs from the canonical one."""

    async def exch_ticker(self, ticker: UniversalTicker) -> str:
        return NATIVE

    async def symbol_for(
        self, venue: str, exch_ticker: str, *, category: str
    ) -> UniversalTicker:
        assert exch_ticker == NATIVE, f"unexpected venue symbol {exch_ticker!r}"
        return UniversalTicker.of(venue, category, "BTCUSDT")


class StubRest:
    """The one read futures has no WebSocket method for."""

    def __init__(
        self,
        rows: list[dict[str, Any]] | None = None,
        *,
        symbol_config: list[dict[str, Any]] | None = None,
    ) -> None:
        self.rows = rows or []
        self.symbol_config = symbol_config or []
        self.asked: list[str | None] = []
        self.config_asked: list[str | None] = []
        self.connected = False

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.connected = False

    async def fetch_open_orders(
        self, symbol: str | None = None
    ) -> list[BinanceFutureOrderAck]:
        self.asked.append(symbol)
        return [BinanceFutureOrderAck.model_validate(row) for row in self.rows]

    async def fetch_symbol_config(
        self, symbol: str | None = None
    ) -> list[BinanceFutureSymbolConfig]:
        self.config_asked.append(symbol)
        return [
            BinanceFutureSymbolConfig.model_validate(row)
            for row in self.symbol_config
        ]


def _client(
    api_stub: FakeBinanceFutureApi,
    user_stub: FakeBinanceFutureUser,
    pem: str,
    *,
    rest: StubRest | None = None,
) -> BinanceFuturePrivateClient:
    api = BinanceFutureWsApi(
        api_key=API_KEY,
        api_secret=pem,
        url=api_stub.url,  # type: ignore[attr-defined]
        keepalive=0,
        retry_backoff=0.01,
    )
    return BinanceFuturePrivateClient(
        api_key=API_KEY,
        api_secret=pem,
        symbols=StubSymbols(),
        api=api,
        user=BinanceFutureUserStream(
            start_key=api.start_user_stream,
            ping_key=api.ping_user_stream,
            base_url=user_stub.url,  # type: ignore[attr-defined]
            keepalive=0,
            retry_backoff=0.01,
        ),
        rest=rest or StubRest(),  # type: ignore[arg-type]
    )


def _order(**overrides: Any) -> PlaceOrderRequest:
    payload: dict[str, Any] = {
        "universal_ticker": str(TICKER),
        "side": Side.BUY,
        "type": OrderType.LIMIT,
        "qty": Decimal("1.5"),
        "price": Decimal("40000"),
        "client_order_id": "c-42",
    }
    payload.update(overrides)
    return PlaceOrderRequest(**payload)


# --- lifecycle -------------------------------------------------------------


async def test_the_account_socket_opens_on_the_key_the_api_issued(
    future_api: FakeBinanceFutureApi,
    future_user: FakeBinanceFutureUser,
    binance_key,
) -> None:
    """The seam between the two connections, which nothing else can supply."""
    _key, pem = binance_key
    client = _client(future_api, future_user, pem)
    async with client:
        await asyncio.sleep(0.05)
        assert future_api.listen_keys == [LISTEN_KEY]
        assert future_user.listen_keys == [LISTEN_KEY]


async def test_a_failed_account_socket_takes_the_order_socket_down_with_it(
    future_api: FakeBinanceFutureApi,
    future_user: FakeBinanceFutureUser,
    binance_key,
) -> None:
    """Half a connector would place orders it could never hear about."""
    _key, pem = binance_key
    client = _client(future_api, future_user, pem)
    future_api.errors[m.USER_DATA_STREAM_START] = {
        "code": -1002,
        "msg": "You are not authorized to execute this request.",
    }
    with pytest.raises(Exception):
        await client.connect()
    assert not client.connected
    assert not client.api.connected


# --- order entry -----------------------------------------------------------


async def test_post_only_is_a_time_in_force_here_not_an_order_type(
    future_api: FakeBinanceFutureApi,
    future_user: FakeBinanceFutureUser,
    binance_key,
) -> None:
    """Spot has to swap in ``LIMIT_MAKER``; futures spells it ``GTX``."""
    _key, pem = binance_key
    future_api.results[m.ORDER_PLACE] = OPEN_ORDER
    async with _client(future_api, future_user, pem) as client:
        await client.place_order(_order(tif=TimeInForce.POST_ONLY))
    params = future_api.call(m.ORDER_PLACE)["params"]
    assert params["type"] == "LIMIT"
    assert params["timeInForce"] == "GTX"


async def test_a_market_order_says_nothing_about_how_long_it_may_rest(
    future_api: FakeBinanceFutureApi,
    future_user: FakeBinanceFutureUser,
    binance_key,
) -> None:
    _key, pem = binance_key
    future_api.results[m.ORDER_PLACE] = {**OPEN_ORDER, "type": "MARKET"}
    async with _client(future_api, future_user, pem) as client:
        await client.place_order(_order(type=OrderType.MARKET, price=None))
    params = future_api.call(m.ORDER_PLACE)["params"]
    assert params["type"] == "MARKET"
    assert "timeInForce" not in params


async def test_the_default_time_in_force_is_stated_rather_than_left_out(
    future_api: FakeBinanceFutureApi,
    future_user: FakeBinanceFutureUser,
    binance_key,
) -> None:
    _key, pem = binance_key
    future_api.results[m.ORDER_PLACE] = OPEN_ORDER
    async with _client(future_api, future_user, pem) as client:
        order = await client.place_order(_order())
    assert future_api.call(m.ORDER_PLACE)["params"]["timeInForce"] == "GTC"
    assert order.universal_ticker == str(TICKER)
    assert order.status is OrderStatus.NEW


async def test_venue_only_options_ride_through_params(
    future_api: FakeBinanceFutureApi,
    future_user: FakeBinanceFutureUser,
    binance_key,
) -> None:
    """``reduceOnly`` and ``positionSide`` have no cross-venue meaning."""
    _key, pem = binance_key
    future_api.results[m.ORDER_PLACE] = OPEN_ORDER
    async with _client(future_api, future_user, pem) as client:
        await client.place_order(
            _order(params={"reduceOnly": True, "positionSide": "LONG"})
        )
    params = future_api.call(m.ORDER_PLACE)["params"]
    assert params["reduceOnly"] is True
    assert params["positionSide"] == "LONG"


async def test_a_param_that_shadows_a_request_field_is_dropped(
    future_api: FakeBinanceFutureApi,
    future_user: FakeBinanceFutureUser,
    binance_key,
) -> None:
    """A silent contradiction is worse than an ignored hint."""
    _key, pem = binance_key
    future_api.results[m.ORDER_PLACE] = OPEN_ORDER
    async with _client(future_api, future_user, pem) as client:
        await client.place_order(_order(params={"quantity": "999", "side": "SELL"}))
    params = future_api.call(m.ORDER_PLACE)["params"]
    assert params["quantity"] == "1.5"
    assert params["side"] == "BUY"


async def test_a_limit_order_without_a_price_never_reaches_the_venue(
    future_api: FakeBinanceFutureApi,
    future_user: FakeBinanceFutureUser,
    binance_key,
) -> None:
    _key, pem = binance_key
    async with _client(future_api, future_user, pem) as client:
        with pytest.raises(OrderError, match="requires a price"):
            await client.place_order(_order(price=None))
    assert not future_api.calls(m.ORDER_PLACE)


async def test_a_venue_rejection_surfaces_as_an_order_error(
    future_api: FakeBinanceFutureApi,
    future_user: FakeBinanceFutureUser,
    binance_key,
) -> None:
    """TD publishes an order reject for these; a transport failure is different."""
    _key, pem = binance_key
    future_api.errors[m.ORDER_PLACE] = {"code": -2019, "msg": "Margin is insufficient."}
    async with _client(future_api, future_user, pem) as client:
        with pytest.raises(OrderError, match="-2019"):
            await client.place_order(_order())


async def test_a_cancel_uses_the_symbol_the_order_was_placed_under(
    future_api: FakeBinanceFutureApi,
    future_user: FakeBinanceFutureUser,
    binance_key,
) -> None:
    """Binance cannot find an order from its id alone."""
    _key, pem = binance_key
    future_api.results[m.ORDER_PLACE] = OPEN_ORDER
    future_api.results[m.ORDER_CANCEL] = {**OPEN_ORDER, "status": "CANCELED"}
    rest = StubRest()
    async with _client(future_api, future_user, pem, rest=rest) as client:
        await client.place_order(_order())
        order = await client.cancel_by_client_order_id("c-42")
    params = future_api.call(m.ORDER_CANCEL)["params"]
    assert params["symbol"] == NATIVE
    assert params["origClientOrderId"] == "c-42"
    assert order.status is OrderStatus.CANCELED
    assert rest.asked == [], "the symbol was already known"


# --- recon reads -----------------------------------------------------------


async def test_open_orders_come_over_rest_because_there_is_no_method_for_them(
    future_api: FakeBinanceFutureApi,
    future_user: FakeBinanceFutureUser,
    binance_key,
) -> None:
    _key, pem = binance_key
    rest = StubRest([OPEN_ORDER])
    async with _client(future_api, future_user, pem, rest=rest) as client:
        orders = await client.fetch_open_orders()
    assert [o.client_order_id for o in orders] == ["c-42"]
    assert orders[0].universal_ticker == str(TICKER)
    assert rest.asked == [None], "the whole account, not one symbol"


async def test_leverage_comes_from_symbol_config_even_when_flat(
    future_api: FakeBinanceFutureApi,
    future_user: FakeBinanceFutureUser,
    binance_key,
) -> None:
    """positionRisk v3 would return [] here; symbolConfig still has leverage."""
    _key, pem = binance_key
    rest = StubRest(
        symbol_config=[
            {
                "symbol": NATIVE,
                "marginType": "CROSSED",
                "leverage": 21,
                "maxNotionalValue": "1000000",
            }
        ]
    )
    async with _client(future_api, future_user, pem, rest=rest) as client:
        assert await client.fetch_leverage(TICKER) == Decimal("21")
    assert rest.config_asked == [NATIVE]


async def test_leverage_fails_when_symbol_config_is_empty(
    future_api: FakeBinanceFutureApi,
    future_user: FakeBinanceFutureUser,
    binance_key,
) -> None:
    _key, pem = binance_key
    rest = StubRest(symbol_config=[])
    async with _client(future_api, future_user, pem, rest=rest) as client:
        with pytest.raises(ExchangeError, match="symbolConfig returned no row"):
            await client.fetch_leverage(TICKER)


async def test_an_order_the_venue_never_heard_of_answers_none(
    future_api: FakeBinanceFutureApi,
    future_user: FakeBinanceFutureUser,
    binance_key,
) -> None:
    """Which is an answer: the submit never landed."""
    _key, pem = binance_key
    future_api.errors[m.ORDER_STATUS] = {"code": -2013, "msg": "Order does not exist."}
    async with _client(future_api, future_user, pem) as client:
        assert await client.fetch_order_by_client_order_id("c-9", ticker=TICKER) is None


async def test_balances_report_what_is_still_committable(
    future_api: FakeBinanceFutureApi,
    future_user: FakeBinanceFutureUser,
    binance_key,
) -> None:
    _key, pem = binance_key
    future_api.results[m.ACCOUNT_BALANCE] = [
        {
            "asset": "USDT",
            "balance": "122624",
            "crossWalletBalance": "122624",
            "availableBalance": "100000",
        }
    ]
    async with _client(future_api, future_user, pem) as client:
        balances = await client.fetch_balances()
    assert balances[0].free == Decimal("100000")
    assert balances[0].locked == Decimal("22624")


async def test_positions_are_reported_flat_ones_included(
    future_api: FakeBinanceFutureApi,
    future_user: FakeBinanceFutureUser,
    binance_key,
) -> None:
    """A zero row is how the OMS learns to drop a position it was carrying."""
    _key, pem = binance_key
    future_api.results[m.ACCOUNT_POSITION] = [
        {"symbol": NATIVE, "positionAmt": "0", "entryPrice": "0"},
    ]
    async with _client(future_api, future_user, pem) as client:
        positions = await client.fetch_positions()
    assert positions[0].universal_ticker == str(TICKER)
    assert positions[0].flat


# --- account streams -------------------------------------------------------


ORDER_UPDATE = {
    "e": "ORDER_TRADE_UPDATE",
    "E": 1568879465651,
    "T": 1568879465650,
    "o": {
        "s": NATIVE,
        "c": "c-42",
        "S": "BUY",
        "o": "LIMIT",
        "f": "GTC",
        "q": "1.5",
        "p": "40000",
        "ap": "40000",
        "x": "TRADE",
        "X": "PARTIALLY_FILLED",
        "i": 22542179,
        "l": "0.5",
        "z": "0.5",
        "L": "40000",
        "N": "USDT",
        "n": "0.008",
        "T": 1568879465650,
        "t": 91921,
        "m": False,
        "ps": "BOTH",
        "rp": "0",
    },
}


async def test_orders_and_fills_read_the_one_account_socket(
    future_api: FakeBinanceFutureApi,
    future_user: FakeBinanceFutureUser,
    binance_key,
) -> None:
    """Two views, one connection — an execution is an order update here."""
    _key, pem = binance_key
    async with _client(future_api, future_user, pem) as client:
        orders = client.stream_orders()
        fills = client.stream_fills()
        order_pump = asyncio.ensure_future(anext(orders))
        fill_pump = asyncio.ensure_future(anext(fills))
        await asyncio.sleep(0.05)

        await future_user.push(ORDER_UPDATE)
        order = await asyncio.wait_for(order_pump, timeout=2.0)
        fill = await asyncio.wait_for(fill_pump, timeout=2.0)

    assert order.universal_ticker == str(TICKER), "resolved home, not BTC-USDT"
    assert order.status is OrderStatus.PARTIALLY_FILLED
    assert order.filled_qty == Decimal("0.5")
    assert fill.qty == Decimal("0.5"), "this execution, not a running total"
    assert fill.fee_asset == "USDT"


async def test_a_state_change_is_not_published_as_a_fill(
    future_api: FakeBinanceFutureApi,
    future_user: FakeBinanceFutureUser,
    binance_key,
) -> None:
    _key, pem = binance_key
    async with _client(future_api, future_user, pem) as client:
        fills = client.stream_fills()
        fill_pump = asyncio.ensure_future(anext(fills))
        await asyncio.sleep(0.05)
        await future_user.push(
            {
                **ORDER_UPDATE,
                "o": {**ORDER_UPDATE["o"], "x": "NEW", "X": "NEW", "l": "0"},
            }
        )
        await asyncio.sleep(0.05)
        assert not fill_pump.done()
        fill_pump.cancel()


async def test_balances_and_positions_come_off_one_account_update(
    future_api: FakeBinanceFutureApi,
    future_user: FakeBinanceFutureUser,
    binance_key,
) -> None:
    """Including a funding payment, which no order caused."""
    _key, pem = binance_key
    async with _client(future_api, future_user, pem) as client:
        balances = client.stream_balances()
        positions = client.stream_positions()
        balance_pump = asyncio.ensure_future(anext(balances))
        position_pump = asyncio.ensure_future(anext(positions))
        await asyncio.sleep(0.05)

        await future_user.push(
            {
                "e": "ACCOUNT_UPDATE",
                "E": 1,
                "T": 1,
                "a": {
                    "m": "FUNDING_FEE",
                    "B": [{"a": "USDT", "wb": "100", "cw": "90", "bc": "-1"}],
                    "P": [
                        {
                            "s": NATIVE,
                            "pa": "-1.5",
                            "ep": "40000",
                            "up": "-12.5",
                            "ps": "BOTH",
                        }
                    ],
                },
            }
        )
        balance = await asyncio.wait_for(balance_pump, timeout=2.0)
        position = await asyncio.wait_for(position_pump, timeout=2.0)

    assert balance.asset == "USDT"
    assert (balance.free, balance.locked) == (Decimal("90"), Decimal("10"))
    assert position.universal_ticker == str(TICKER)
    assert position.qty == Decimal("-1.5"), "negative is short"


async def test_an_order_for_another_venue_is_refused_before_it_is_sent(
    future_api: FakeBinanceFutureApi,
    future_user: FakeBinanceFutureUser,
    binance_key,
) -> None:
    """The one mistake that cannot be undone by noticing later."""
    _key, pem = binance_key
    async with _client(future_api, future_user, pem) as client:
        with pytest.raises(OrderError, match="Binance order"):
            await client.place_order(
                _order(universal_ticker="Binance_Spot_BTCUSDT")
            )
    assert not future_api.calls(m.ORDER_PLACE)
