"""The COIN-M trading connector — WS API, listen-key feed, signed REST."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import pytest
from binance_delivery_stub import (
    API_KEY,
    LISTEN_KEY,
    FakeBinanceDeliveryApi,
    FakeBinanceDeliveryUser,
)
from mftik.exchange.binance.delivery import methods as m
from mftik.exchange.binance.delivery.client import BinanceDeliveryWsApi
from mftik.exchange.binance.delivery.models import BinanceDeliveryOrderAck
from mftik.exchange.binance.delivery.private import BinanceDeliveryPrivateClient
from mftik.exchange.binance.delivery.user import BinanceDeliveryUserStream
from mftik.exchange.errors import OrderError
from mftik.exchange.models import (
    OrderStatus,
    OrderType,
    PlaceOrderRequest,
    Side,
    TimeInForce,
)
from mftik.exchange.tickers import Category, UniversalTicker
from mftik.symbols import SymbolNotFoundError

NATIVE = "BTCUSD_PERP"
#: A quarterly. Unlisted on a perp-only plane; listed as Future when both
#: books are in the resolver.
DATED = "BTCUSD_260626"
TICKER = UniversalTicker.parse("BinanceDelivery_Inverse_BTCUSD")
DATED_TICKER = UniversalTicker.parse("BinanceDelivery_Future_BTCUSD260626")

OPEN_ORDER = {
    "symbol": NATIVE,
    "orderId": 22542179,
    "clientOrderId": "c-42",
    "price": "40000",
    "avgPrice": "0",
    "origQty": "1.5",
    "executedQty": "0",
    "status": "NEW",
    "timeInForce": "GTC",
    "type": "LIMIT",
    "origType": "LIMIT",
    "side": "BUY",
    "positionSide": "BOTH",
    "updateTime": 1566818724722,
}


class StubSymbols:
    """A symbol plane whose venue spelling differs from the canonical one.

    Two books on one venue: the inverse perpetual and one dated future. A
    miss raises so the connector can try the other book rather than
    inventing a ticker.
    """

    async def exch_ticker(self, ticker: UniversalTicker) -> str:
        if ticker == DATED_TICKER:
            return DATED
        if ticker == TICKER:
            return NATIVE
        raise SymbolNotFoundError(f"no such instrument: {ticker}")

    async def symbol_for(
        self, venue: str, exch_ticker: str, *, category: str
    ) -> UniversalTicker:
        book = Category(category)
        if exch_ticker == DATED and book is Category.FUTURE:
            return DATED_TICKER
        if exch_ticker == NATIVE and book is Category.INVERSE:
            return TICKER
        raise SymbolNotFoundError(
            f"no {book.value} instrument spelled {exch_ticker!r} on "
            f"venue {venue!r}"
        )


class PerpOnlySymbols(StubSymbols):
    """A stale plane: perpetuals are in it and dated contracts are not."""

    async def exch_ticker(self, ticker: UniversalTicker) -> str:
        if ticker == TICKER:
            return NATIVE
        raise SymbolNotFoundError(f"no such instrument: {ticker}")

    async def symbol_for(
        self, venue: str, exch_ticker: str, *, category: str
    ) -> UniversalTicker:
        if exch_ticker != NATIVE:
            raise SymbolNotFoundError(f"no such instrument: {exch_ticker}")
        return UniversalTicker.of(venue, category, "BTCUSD")


class StubRest:
    """The one read dapi has no WebSocket method for."""

    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.rows = rows or []
        self.asked: list[str | None] = []
        self.connected = False

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.connected = False

    async def fetch_open_orders(
        self, symbol: str | None = None
    ) -> list[BinanceDeliveryOrderAck]:
        self.asked.append(symbol)
        return [BinanceDeliveryOrderAck.model_validate(row) for row in self.rows]


def _client(
    api_stub: FakeBinanceDeliveryApi,
    user_stub: FakeBinanceDeliveryUser,
    pem: str,
    *,
    rest: StubRest | None = None,
    symbols: Any | None = None,
) -> BinanceDeliveryPrivateClient:
    api = BinanceDeliveryWsApi(
        api_key=API_KEY,
        api_secret=pem,
        url=api_stub.url,  # type: ignore[attr-defined]
        keepalive=0,
        retry_backoff=0.01,
    )
    return BinanceDeliveryPrivateClient(
        api_key=API_KEY,
        api_secret=pem,
        symbols=symbols or StubSymbols(),
        api=api,
        user=BinanceDeliveryUserStream(
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


async def test_the_account_socket_opens_on_the_key_the_api_issued(
    delivery_api: FakeBinanceDeliveryApi,
    delivery_user: FakeBinanceDeliveryUser,
    binance_key,
) -> None:
    _key, pem = binance_key
    client = _client(delivery_api, delivery_user, pem)
    async with client:
        await asyncio.sleep(0.05)
        assert delivery_api.listen_keys == [LISTEN_KEY]
        assert delivery_user.listen_keys == [LISTEN_KEY]


async def test_a_failed_account_socket_takes_the_order_socket_down_with_it(
    delivery_api: FakeBinanceDeliveryApi,
    delivery_user: FakeBinanceDeliveryUser,
    binance_key,
) -> None:
    _key, pem = binance_key
    client = _client(delivery_api, delivery_user, pem)
    delivery_api.errors[m.USER_DATA_STREAM_START] = {
        "code": -1002,
        "msg": "You are not authorized to execute this request.",
    }
    with pytest.raises(Exception):
        await client.connect()
    assert not client.connected
    assert not client.api.connected


async def test_post_only_is_a_time_in_force_here_not_an_order_type(
    delivery_api: FakeBinanceDeliveryApi,
    delivery_user: FakeBinanceDeliveryUser,
    binance_key,
) -> None:
    _key, pem = binance_key
    delivery_api.results[m.ORDER_PLACE] = OPEN_ORDER
    async with _client(delivery_api, delivery_user, pem) as client:
        await client.place_order(_order(tif=TimeInForce.POST_ONLY))
    params = delivery_api.call(m.ORDER_PLACE)["params"]
    assert params["type"] == "LIMIT"
    assert params["timeInForce"] == "GTX"


async def test_a_market_order_says_nothing_about_how_long_it_may_rest(
    delivery_api: FakeBinanceDeliveryApi,
    delivery_user: FakeBinanceDeliveryUser,
    binance_key,
) -> None:
    _key, pem = binance_key
    delivery_api.results[m.ORDER_PLACE] = {**OPEN_ORDER, "type": "MARKET"}
    async with _client(delivery_api, delivery_user, pem) as client:
        await client.place_order(_order(type=OrderType.MARKET, price=None))
    params = delivery_api.call(m.ORDER_PLACE)["params"]
    assert params["type"] == "MARKET"
    assert "timeInForce" not in params


async def test_reduce_only_rides_on_the_request(
    delivery_api: FakeBinanceDeliveryApi,
    delivery_user: FakeBinanceDeliveryUser,
    binance_key,
) -> None:
    _key, pem = binance_key
    delivery_api.results[m.ORDER_PLACE] = OPEN_ORDER
    async with _client(delivery_api, delivery_user, pem) as client:
        await client.place_order(_order(reduce_only=True))
    assert delivery_api.call(m.ORDER_PLACE)["params"]["reduceOnly"] is True


async def test_an_ordinary_order_says_nothing_about_reduce_only(
    delivery_api: FakeBinanceDeliveryApi,
    delivery_user: FakeBinanceDeliveryUser,
    binance_key,
) -> None:
    _key, pem = binance_key
    delivery_api.results[m.ORDER_PLACE] = OPEN_ORDER
    async with _client(delivery_api, delivery_user, pem) as client:
        await client.place_order(_order())
    assert "reduceOnly" not in delivery_api.call(m.ORDER_PLACE)["params"]


async def test_quantity_is_contracts_unscaled(
    delivery_api: FakeBinanceDeliveryApi,
    delivery_user: FakeBinanceDeliveryUser,
    binance_key,
) -> None:
    _key, pem = binance_key
    delivery_api.results[m.ORDER_PLACE] = OPEN_ORDER
    async with _client(delivery_api, delivery_user, pem) as client:
        await client.place_order(_order())
    assert delivery_api.call(m.ORDER_PLACE)["params"]["quantity"] == "1.5"


async def test_a_param_that_shadows_a_request_field_is_dropped(
    delivery_api: FakeBinanceDeliveryApi,
    delivery_user: FakeBinanceDeliveryUser,
    binance_key,
) -> None:
    _key, pem = binance_key
    delivery_api.results[m.ORDER_PLACE] = OPEN_ORDER
    async with _client(delivery_api, delivery_user, pem) as client:
        await client.place_order(_order(params={"quantity": "999", "side": "SELL"}))
    params = delivery_api.call(m.ORDER_PLACE)["params"]
    assert params["quantity"] == "1.5"
    assert params["side"] == "BUY"


async def test_open_orders_come_over_rest_because_there_is_no_method_for_them(
    delivery_api: FakeBinanceDeliveryApi,
    delivery_user: FakeBinanceDeliveryUser,
    binance_key,
) -> None:
    _key, pem = binance_key
    rest = StubRest([OPEN_ORDER])
    async with _client(delivery_api, delivery_user, pem, rest=rest) as client:
        orders = await client.fetch_open_orders()
    assert [o.client_order_id for o in orders] == ["c-42"]
    assert orders[0].universal_ticker == str(TICKER)
    assert rest.asked == [None], "the whole account, not one symbol"


async def test_balances_report_what_is_still_committable(
    delivery_api: FakeBinanceDeliveryApi,
    delivery_user: FakeBinanceDeliveryUser,
    binance_key,
) -> None:
    _key, pem = binance_key
    delivery_api.results[m.ACCOUNT_BALANCE] = [
        {
            "asset": "BTC",
            "balance": "1.5",
            "crossWalletBalance": "1.5",
            "availableBalance": "1.0",
        }
    ]
    async with _client(delivery_api, delivery_user, pem) as client:
        balances = await client.fetch_balances()
    assert balances[0].free == Decimal("1.0")
    assert balances[0].locked == Decimal("0.5")


async def test_older_dapi_rows_spell_available_as_withdraw_available(
    delivery_api: FakeBinanceDeliveryApi,
    delivery_user: FakeBinanceDeliveryUser,
    binance_key,
) -> None:
    _key, pem = binance_key
    delivery_api.results[m.ACCOUNT_BALANCE] = [
        {
            "asset": "BTC",
            "balance": "2",
            "withdrawAvailable": "1.25",
        }
    ]
    async with _client(delivery_api, delivery_user, pem) as client:
        balances = await client.fetch_balances()
    assert balances[0].free == Decimal("1.25")
    assert balances[0].locked == Decimal("0.75")


async def test_positions_are_reported_flat_ones_included(
    delivery_api: FakeBinanceDeliveryApi,
    delivery_user: FakeBinanceDeliveryUser,
    binance_key,
) -> None:
    _key, pem = binance_key
    delivery_api.results[m.ACCOUNT_POSITION] = [
        {"symbol": NATIVE, "positionAmt": "0", "entryPrice": "0"},
    ]
    async with _client(delivery_api, delivery_user, pem) as client:
        positions = await client.fetch_positions()
    assert positions[0].universal_ticker == str(TICKER)
    assert positions[0].flat


async def test_an_order_for_another_venue_is_refused_before_it_is_sent(
    delivery_api: FakeBinanceDeliveryApi,
    delivery_user: FakeBinanceDeliveryUser,
    binance_key,
) -> None:
    _key, pem = binance_key
    async with _client(delivery_api, delivery_user, pem) as client:
        with pytest.raises(OrderError, match="BinanceFuture"):
            await client.place_order(
                _order(universal_ticker="BinanceFuture_Perp_BTCUSDT")
            )
    assert not delivery_api.calls(m.ORDER_PLACE)


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
        "N": "BTC",
        "n": "0.00008",
        "T": 1568879465650,
        "t": 91921,
        "m": False,
        "ps": "BOTH",
        "rp": "0",
    },
}


async def test_orders_and_fills_read_the_one_account_socket(
    delivery_api: FakeBinanceDeliveryApi,
    delivery_user: FakeBinanceDeliveryUser,
    binance_key,
) -> None:
    _key, pem = binance_key
    async with _client(delivery_api, delivery_user, pem) as client:
        orders = client.stream_orders()
        fills = client.stream_fills()
        order_pump = asyncio.ensure_future(anext(orders))
        fill_pump = asyncio.ensure_future(anext(fills))
        await asyncio.sleep(0.05)

        await delivery_user.push(ORDER_UPDATE)
        order = await asyncio.wait_for(order_pump, timeout=2.0)
        fill = await asyncio.wait_for(fill_pump, timeout=2.0)

    assert order.universal_ticker == str(TICKER)
    assert order.status is OrderStatus.PARTIALLY_FILLED
    assert order.filled_qty == Decimal("0.5")
    assert fill.qty == Decimal("0.5")
    assert fill.fee_asset == "BTC"


async def test_a_state_change_is_not_published_as_a_fill(
    delivery_api: FakeBinanceDeliveryApi,
    delivery_user: FakeBinanceDeliveryUser,
    binance_key,
) -> None:
    _key, pem = binance_key
    async with _client(delivery_api, delivery_user, pem) as client:
        fills = client.stream_fills()
        fill_pump = asyncio.ensure_future(anext(fills))
        await asyncio.sleep(0.05)
        await delivery_user.push(
            {
                **ORDER_UPDATE,
                "o": {**ORDER_UPDATE["o"], "x": "NEW", "X": "NEW", "l": "0"},
            }
        )
        await asyncio.sleep(0.05)
        assert not fill_pump.done()
        fill_pump.cancel()


async def test_balances_and_positions_come_off_one_account_update(
    delivery_api: FakeBinanceDeliveryApi,
    delivery_user: FakeBinanceDeliveryUser,
    binance_key,
) -> None:
    _key, pem = binance_key
    async with _client(delivery_api, delivery_user, pem) as client:
        balances = client.stream_balances()
        positions = client.stream_positions()
        balance_pump = asyncio.ensure_future(anext(balances))
        position_pump = asyncio.ensure_future(anext(positions))
        await asyncio.sleep(0.05)

        await delivery_user.push(
            {
                "e": "ACCOUNT_UPDATE",
                "E": 1,
                "T": 1,
                "a": {
                    "m": "FUNDING_FEE",
                    "B": [{"a": "BTC", "wb": "1.5", "cw": "1.0", "bc": "-0.01"}],
                    "P": [
                        {
                            "s": NATIVE,
                            "pa": "-2",
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

    assert balance.asset == "BTC"
    assert (balance.free, balance.locked) == (Decimal("1.0"), Decimal("0.5"))
    assert position.universal_ticker == str(TICKER)
    assert position.qty == Decimal("-2")


async def test_there_is_still_no_leverage_read(
    delivery_api: FakeBinanceDeliveryApi,
    delivery_user: FakeBinanceDeliveryUser,
    binance_key,
) -> None:
    _key, pem = binance_key
    async with _client(delivery_api, delivery_user, pem) as client:
        assert not hasattr(client, "fetch_leverage")


async def test_a_dated_contract_does_not_sink_the_whole_position_read(
    delivery_api: FakeBinanceDeliveryApi,
    delivery_user: FakeBinanceDeliveryUser,
    binance_key,
) -> None:
    """One unnameable row is skipped, not raised over the whole account.

    ``account.position`` answers for every contract listed on the venue, and
    the plane carries only the perpetuals — so an account holding a quarterly
    would otherwise get no positions at all, including the ones it trades.
    """
    _key, pem = binance_key
    delivery_api.results[m.ACCOUNT_POSITION] = [
        {"symbol": DATED, "positionAmt": "3", "entryPrice": "40000"},
        {"symbol": NATIVE, "positionAmt": "-2", "entryPrice": "40000"},
    ]
    async with _client(
        delivery_api, delivery_user, pem, symbols=PerpOnlySymbols()
    ) as client:
        positions = await client.fetch_positions()

    assert [p.universal_ticker for p in positions] == [str(TICKER)]


async def test_a_dated_contract_is_dropped_from_the_open_order_listing(
    delivery_api: FakeBinanceDeliveryApi,
    delivery_user: FakeBinanceDeliveryUser,
    binance_key,
) -> None:
    _key, pem = binance_key
    rest = StubRest([{**OPEN_ORDER, "symbol": DATED, "orderId": 99}, OPEN_ORDER])
    async with _client(
        delivery_api, delivery_user, pem, rest=rest, symbols=PerpOnlySymbols()
    ) as client:
        orders = await client.fetch_open_orders()

    assert [o.order_id for o in orders] == ["22542179"]


async def test_a_dated_contract_does_not_tear_down_the_order_stream(
    delivery_api: FakeBinanceDeliveryApi,
    delivery_user: FakeBinanceDeliveryUser,
    binance_key,
) -> None:
    """A quarterly's update is dropped; the socket keeps carrying the perp.

    Raising here would end the generator, and with it every order update for
    the instruments the account actually trades.
    """
    _key, pem = binance_key
    async with _client(
        delivery_api, delivery_user, pem, symbols=PerpOnlySymbols()
    ) as client:
        orders = client.stream_orders()
        pump = asyncio.ensure_future(anext(orders))
        await asyncio.sleep(0.05)

        await delivery_user.push(
            {**ORDER_UPDATE, "o": {**ORDER_UPDATE["o"], "s": DATED}}
        )
        await asyncio.sleep(0.05)
        assert not pump.done()

        await delivery_user.push(ORDER_UPDATE)
        order = await asyncio.wait_for(pump, timeout=2.0)

    assert order.universal_ticker == str(TICKER)


async def test_a_dated_future_order_reaches_the_wire(
    delivery_api: FakeBinanceDeliveryApi,
    delivery_user: FakeBinanceDeliveryUser,
    binance_key,
) -> None:
    """Same credential, different book — the ticker says which."""
    _key, pem = binance_key
    delivery_api.results[m.ORDER_PLACE] = {**OPEN_ORDER, "symbol": DATED}
    async with _client(delivery_api, delivery_user, pem) as client:
        order = await client.place_order(
            _order(universal_ticker=str(DATED_TICKER))
        )
    assert delivery_api.call(m.ORDER_PLACE)["params"]["symbol"] == DATED
    assert order.universal_ticker == str(DATED_TICKER)


async def test_a_dated_future_position_does_not_land_on_the_inverse(
    delivery_api: FakeBinanceDeliveryApi,
    delivery_user: FakeBinanceDeliveryUser,
    binance_key,
) -> None:
    """One account holds both books; the native spelling says which."""
    _key, pem = binance_key
    delivery_api.results[m.ACCOUNT_POSITION] = [
        {"symbol": DATED, "positionAmt": "3", "entryPrice": "40000"},
        {"symbol": NATIVE, "positionAmt": "-2", "entryPrice": "40000"},
    ]
    async with _client(delivery_api, delivery_user, pem) as client:
        positions = await client.fetch_positions()
    by_ticker = {row.universal_ticker: row.qty for row in positions}
    assert by_ticker[str(DATED_TICKER)] == Decimal("3")
    assert by_ticker[str(TICKER)] == Decimal("-2")


async def test_a_dated_open_order_resolves_home(
    delivery_api: FakeBinanceDeliveryApi,
    delivery_user: FakeBinanceDeliveryUser,
    binance_key,
) -> None:
    _key, pem = binance_key
    rest = StubRest([{**OPEN_ORDER, "symbol": DATED}])
    async with _client(delivery_api, delivery_user, pem, rest=rest) as client:
        orders = await client.fetch_open_orders()
    assert orders[0].universal_ticker == str(DATED_TICKER)
    assert rest.asked == [None]


async def test_a_dated_future_stream_resolves_home(
    delivery_api: FakeBinanceDeliveryApi,
    delivery_user: FakeBinanceDeliveryUser,
    binance_key,
) -> None:
    _key, pem = binance_key
    async with _client(delivery_api, delivery_user, pem) as client:
        orders = client.stream_orders()
        pump = asyncio.ensure_future(anext(orders))
        await asyncio.sleep(0.05)
        await delivery_user.push(
            {**ORDER_UPDATE, "o": {**ORDER_UPDATE["o"], "s": DATED}}
        )
        order = await asyncio.wait_for(pump, timeout=2.0)
    assert order.universal_ticker == str(DATED_TICKER)
