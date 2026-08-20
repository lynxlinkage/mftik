"""The Binance spot trading connector — order entry and recon on one socket."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import pytest
from binance_stub import API_KEY, FakeBinanceApi
from mftik.exchange.binance.spot import methods as m
from mftik.exchange.binance.spot.client import BinanceSpotWsApi
from mftik.exchange.binance.spot.private import BinanceSpotPrivateClient
from mftik.exchange.binance.spot.protocol import BinanceWsError
from mftik.exchange.errors import OrderError
from mftik.exchange.models import (
    OrderStatus,
    OrderType,
    PlaceOrderRequest,
    Side,
    TimeInForce,
)
from mftik.exchange.tickers import UniversalTicker

NATIVE = "BTC-USDT"
#: The instrument every order in this module is for.
TICKER = UniversalTicker.parse("Binance_Spot_BTCUSDT")

OPEN_ORDER = {
    "symbol": NATIVE,
    "orderId": 12569099453,
    "clientOrderId": "c-42",
    "price": "60000.00",
    "origQty": "0.00100000",
    "executedQty": "0.00000000",
    "cummulativeQuoteQty": "0.00000000",
    "status": "NEW",
    "timeInForce": "GTC",
    "type": "LIMIT",
    "side": "BUY",
    "transactTime": 1660801715639,
}

EXECUTION_REPORT = {
    "e": "executionReport",
    "E": 1499405658658,
    "s": NATIVE,
    "c": "c-42",
    "S": "BUY",
    "o": "LIMIT",
    "f": "GTC",
    "q": "0.001",
    "p": "60000",
    "x": "NEW",
    "X": "NEW",
    "i": 12569099453,
    "l": "0",
    "z": "0",
    "L": "0",
    "n": "0",
    "N": None,
    "T": 1499405658657,
    "t": -1,
    "Z": "0",
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


def _client(
    stub: FakeBinanceApi, pem: str, **kwargs: Any
) -> BinanceSpotPrivateClient:
    return BinanceSpotPrivateClient(
        api_key=API_KEY,
        api_secret=pem,
        symbols=StubSymbols(),
        api=BinanceSpotWsApi(
            api_key=API_KEY,
            api_secret=pem,
            url=stub.url,  # type: ignore[attr-defined]
            keepalive=0,
        ),
        **kwargs,
    )


def _limit(**overrides: Any) -> PlaceOrderRequest:
    return PlaceOrderRequest(
        **{
            "universal_ticker": str(TICKER),
            "side": Side.BUY,
            "type": OrderType.LIMIT,
            "qty": Decimal("0.001"),
            "price": Decimal("60000"),
            "client_order_id": "c-42",
            **overrides,
        }
    )


# --- order entry -----------------------------------------------------------


async def test_place_order_translates_the_symbol_both_ways(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    _key, pem = binance_key
    binance_api.results[m.ORDER_PLACE] = OPEN_ORDER
    async with _client(binance_api, pem) as client:
        order = await client.place_order(_limit())

    params = binance_api.call(m.ORDER_PLACE)["params"]
    assert params["symbol"] == NATIVE
    assert params["side"] == "BUY"
    assert params["type"] == "LIMIT"
    assert params["timeInForce"] == "GTC"
    assert params["quantity"] == "0.001"
    assert params["price"] == "60000"
    assert params["newClientOrderId"] == "c-42"

    assert order.symbol == "BTCUSDT", "the venue spelling must not escape"
    assert order.status is OrderStatus.NEW
    assert order.order_id == "12569099453"


async def test_a_market_order_sends_no_time_in_force(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    """It cannot rest, so there is nothing to say about how long it may."""
    _key, pem = binance_key
    binance_api.results[m.ORDER_PLACE] = {**OPEN_ORDER, "type": "MARKET"}
    async with _client(binance_api, pem) as client:
        await client.place_order(
            _limit(type=OrderType.MARKET, price=None)
        )

    params = binance_api.call(m.ORDER_PLACE)["params"]
    assert params["type"] == "MARKET"
    assert "timeInForce" not in params
    assert "price" not in params


async def test_a_market_buy_sizes_in_base_currency(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    """The thing Gate cannot do: ``quantity`` means base for every type."""
    _key, pem = binance_key
    binance_api.results[m.ORDER_PLACE] = {**OPEN_ORDER, "type": "MARKET"}
    async with _client(binance_api, pem) as client:
        await client.place_order(_limit(type=OrderType.MARKET, price=None))

    params = binance_api.call(m.ORDER_PLACE)["params"]
    assert params["quantity"] == "0.001"
    assert "quoteOrderQty" not in params


async def test_a_market_order_sized_in_quote_sends_quote_order_qty(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    _key, pem = binance_key
    binance_api.results[m.ORDER_PLACE] = {**OPEN_ORDER, "type": "MARKET"}
    async with _client(binance_api, pem) as client:
        await client.place_order(
            _limit(
                type=OrderType.MARKET,
                price=None,
                qty=None,
                quote_qty=Decimal("100"),
            )
        )

    params = binance_api.call(m.ORDER_PLACE)["params"]
    assert params["quoteOrderQty"] == "100"
    assert "quantity" not in params


async def test_post_only_becomes_an_order_type_not_a_time_in_force(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    """Binance spells post-only ``LIMIT_MAKER`` and refuses a TIF beside it."""
    _key, pem = binance_key
    binance_api.results[m.ORDER_PLACE] = {**OPEN_ORDER, "type": "LIMIT_MAKER"}
    async with _client(binance_api, pem) as client:
        order = await client.place_order(_limit(tif=TimeInForce.POST_ONLY))

    params = binance_api.call(m.ORDER_PLACE)["params"]
    assert params["type"] == "LIMIT_MAKER"
    assert "timeInForce" not in params
    # And it comes back as a plain limit order, which is what it is to us.
    assert order.type is OrderType.LIMIT


@pytest.mark.parametrize(
    ("tif", "expected"),
    [(TimeInForce.GTC, "GTC"), (TimeInForce.IOC, "IOC"), (TimeInForce.FOK, "FOK")],
)
async def test_time_in_force_mapping(
    binance_api: FakeBinanceApi, binance_key, tif: TimeInForce, expected: str
) -> None:
    _key, pem = binance_key
    binance_api.results[m.ORDER_PLACE] = OPEN_ORDER
    async with _client(binance_api, pem) as client:
        await client.place_order(_limit(tif=tif))
    assert binance_api.call(m.ORDER_PLACE)["params"]["timeInForce"] == expected


def test_a_limit_order_without_a_price_is_refused_locally() -> None:
    with pytest.raises(ValueError, match="requires a price"):
        _limit(price=None)


async def test_params_that_shadow_a_request_field_are_dropped(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    _key, pem = binance_key
    binance_api.results[m.ORDER_PLACE] = OPEN_ORDER
    async with _client(binance_api, pem) as client:
        await client.place_order(
            _limit(params={"symbol": "ETHUSDT", "quantity": "999", "icebergQty": "1"})
        )

    params = binance_api.call(m.ORDER_PLACE)["params"]
    assert params["symbol"] == NATIVE
    assert params["quantity"] == "0.001"
    # A venue-only extra that shadows nothing still rides along.
    assert params["icebergQty"] == "1"


async def test_a_venue_rejection_becomes_an_order_error(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    """TD publishes an order reject; a transport failure would be different."""
    _key, pem = binance_key
    binance_api.errors[m.ORDER_PLACE] = {
        "code": -2010,
        "msg": "Account has insufficient balance for requested action.",
    }
    async with _client(binance_api, pem) as client:
        with pytest.raises(OrderError, match="-2010"):
            await client.place_order(_limit())


async def test_the_wrapped_rejection_keeps_the_venues_code_reachable(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    """``OrderError`` carries no code, so the original must stay chained.

    TD normalizes on what this raises. Without the ``from exc`` the venue's
    ``-1111`` is unrecoverable and every rejection reads as a generic one.
    """
    _key, pem = binance_key
    binance_api.errors[m.ORDER_PLACE] = {
        "code": -1111,
        "msg": "Parameter 'quantity' has too much precision.",
    }
    async with _client(binance_api, pem) as client:
        with pytest.raises(OrderError) as exc:
            await client.place_order(_limit())

    assert isinstance(exc.value.__cause__, BinanceWsError)
    assert exc.value.__cause__.code == -1111


async def test_a_quantity_goes_out_without_trailing_zeros(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    """``0.00780000`` and ``0.0078`` are one number and not one parameter.

    Binance checks the *written* precision against the symbol's lot step and
    answers ``-1111`` for the first. A Decimal keeps its scale through
    arithmetic, so a size floored against a step written ``0.00010000`` arrives
    here already over-scaled unless something has stripped it.
    """
    _key, pem = binance_key
    binance_api.results[m.ORDER_PLACE] = OPEN_ORDER
    async with _client(binance_api, pem) as client:
        await client.place_order(_limit(qty=Decimal("0.00780000")))

    assert binance_api.call(m.ORDER_PLACE)["params"]["quantity"] == "0.0078"


# --- cancels ---------------------------------------------------------------


async def test_cancel_by_venue_id_uses_the_symbol_seen_on_the_ack(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    """Binance needs the symbol; the shared interface cancels by id alone."""
    _key, pem = binance_key
    binance_api.results[m.ORDER_PLACE] = OPEN_ORDER
    binance_api.results[m.ORDER_CANCEL] = {
        **OPEN_ORDER,
        "status": "CANCELED",
        "clientOrderId": "cancel-request-1",
        "origClientOrderId": "c-42",
    }
    async with _client(binance_api, pem) as client:
        placed = await client.place_order(_limit())
        canceled = await client.cancel_order(placed.order_id)

    params = binance_api.call(m.ORDER_CANCEL)["params"]
    assert params["symbol"] == NATIVE
    assert params["orderId"] == 12569099453
    assert canceled.status is OrderStatus.CANCELED
    assert canceled.client_order_id == "c-42", "the order's id, not the cancel's"
    assert binance_api.calls(m.OPEN_ORDERS_STATUS) == [], "no lookup was needed"


async def test_cancel_by_client_order_id_sends_orig_client_order_id(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    _key, pem = binance_key
    binance_api.results[m.ORDER_PLACE] = OPEN_ORDER
    binance_api.results[m.ORDER_CANCEL] = {**OPEN_ORDER, "status": "CANCELED"}
    async with _client(binance_api, pem) as client:
        await client.place_order(_limit())
        await client.cancel_by_client_order_id("c-42")

    params = binance_api.call(m.ORDER_CANCEL)["params"]
    assert params["origClientOrderId"] == "c-42"
    assert "orderId" not in params


async def test_an_unseen_id_is_resolved_from_open_orders_once(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    _key, pem = binance_key
    binance_api.results[m.OPEN_ORDERS_STATUS] = [OPEN_ORDER]
    binance_api.results[m.ORDER_CANCEL] = {**OPEN_ORDER, "status": "CANCELED"}
    async with _client(binance_api, pem) as client:
        await client.cancel_order("12569099453")

    assert len(binance_api.calls(m.OPEN_ORDERS_STATUS)) == 1
    assert binance_api.call(m.ORDER_CANCEL)["params"]["symbol"] == NATIVE


async def test_an_id_no_open_order_owns_is_refused(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    _key, pem = binance_key
    binance_api.results[m.OPEN_ORDERS_STATUS] = []
    async with _client(binance_api, pem) as client:
        with pytest.raises(OrderError, match="no open Binance order"):
            await client.cancel_order("does-not-exist")


async def test_a_terminal_order_stops_being_indexed(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    """Its reservation is released, and so is our note of where it lived."""
    _key, pem = binance_key
    binance_api.results[m.ORDER_PLACE] = {**OPEN_ORDER, "status": "FILLED"}
    binance_api.results[m.OPEN_ORDERS_STATUS] = []
    async with _client(binance_api, pem) as client:
        placed = await client.place_order(_limit())
        assert placed.status is OrderStatus.FILLED
        with pytest.raises(OrderError, match="no open Binance order"):
            await client.cancel_order(placed.order_id)


# --- recon reads -----------------------------------------------------------


async def test_fetch_open_orders_comes_back_canonical(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    _key, pem = binance_key
    binance_api.results[m.OPEN_ORDERS_STATUS] = [OPEN_ORDER]
    async with _client(binance_api, pem) as client:
        orders = await client.fetch_open_orders("BTCUSDT")

    assert binance_api.call(m.OPEN_ORDERS_STATUS)["params"]["symbol"] == NATIVE
    assert [o.symbol for o in orders] == ["BTCUSDT"]
    assert orders[0].qty == Decimal("0.001")


async def test_fetch_balances_omits_the_hundreds_of_empty_assets(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    _key, pem = binance_key
    binance_api.results[m.ACCOUNT_STATUS] = {
        "accountType": "SPOT",
        "canTrade": True,
        "balances": [
            {"asset": "USDT", "free": "1000.5", "locked": "12"},
            {"asset": "BTC", "free": "0.25", "locked": "0"},
        ],
    }
    async with _client(binance_api, pem) as client:
        balances = await client.fetch_balances()

    assert binance_api.call(m.ACCOUNT_STATUS)["params"]["omitZeroBalances"] is True
    assert [b.asset for b in balances] == ["USDT", "BTC"]
    assert balances[0].free == Decimal("1000.5")
    assert balances[0].locked == Decimal("12")


async def test_an_order_the_venue_never_saw_reads_as_none(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    """``-2013`` is an answer — the submit never landed — not a failure."""
    _key, pem = binance_key
    binance_api.errors[m.ORDER_STATUS] = {
        "code": -2013,
        "msg": "Order does not exist.",
    }
    async with _client(binance_api, pem) as client:
        found = await client.fetch_order_by_client_order_id("c-42", ticker=TICKER)

    assert found is None


async def test_a_real_failure_on_that_lookup_still_raises(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    _key, pem = binance_key
    binance_api.errors[m.ORDER_STATUS] = {"code": -1021, "msg": "Timestamp ahead."}
    async with _client(binance_api, pem) as client:
        with pytest.raises(OrderError, match="-1021"):
            await client.fetch_order_by_client_order_id("c-42", ticker=TICKER)


async def test_that_lookup_needs_a_symbol_it_has_never_seen(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    _key, pem = binance_key
    async with _client(binance_api, pem) as client:
        with pytest.raises(OrderError, match="without its symbol"):
            await client.fetch_order_by_client_order_id("never-sent")


# --- account streams -------------------------------------------------------


async def test_orders_stream_yields_canonical_orders(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    _key, pem = binance_key
    async with _client(binance_api, pem) as client:
        stream = client.stream_orders()
        pump = asyncio.ensure_future(anext(stream))
        await asyncio.sleep(0.05)
        await binance_api.push_event(EXECUTION_REPORT)
        order = await asyncio.wait_for(pump, timeout=2.0)

    assert order.symbol == "BTCUSDT"
    assert order.client_order_id == "c-42"
    assert order.status is OrderStatus.NEW


async def test_fills_are_filtered_out_of_the_order_stream(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    """Binance has no user-trade channel; a fill is a report with x=TRADE."""
    _key, pem = binance_key
    async with _client(binance_api, pem) as client:
        stream = client.stream_fills()
        pump = asyncio.ensure_future(anext(stream))
        await asyncio.sleep(0.05)

        # A state change with no execution must not surface as a fill.
        await binance_api.push_event(EXECUTION_REPORT)
        await binance_api.push_event(
            {
                **EXECUTION_REPORT,
                "x": "TRADE",
                "X": "PARTIALLY_FILLED",
                "l": "0.0004",
                "L": "59000",
                "z": "0.0004",
                "Z": "23.6",
                "n": "0.00001",
                "N": "BNB",
                "t": 555,
            }
        )
        fill = await asyncio.wait_for(pump, timeout=2.0)

    assert fill.symbol == "BTCUSDT"
    assert fill.fill_id == "555"
    assert fill.qty == Decimal("0.0004"), "this execution, not the running total"
    assert fill.price == Decimal("59000")
    assert fill.fee_asset == "BNB"


async def test_balances_stream_flattens_one_push_into_one_per_asset(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    _key, pem = binance_key
    async with _client(binance_api, pem) as client:
        stream = client.stream_balances()
        pump = asyncio.ensure_future(anext(stream))
        await asyncio.sleep(0.05)
        await binance_api.push_event(
            {
                "e": "outboundAccountPosition",
                "E": 1,
                "u": 1,
                "B": [
                    {"a": "USDT", "f": "940", "l": "60"},
                    {"a": "BTC", "f": "0.001", "l": "0"},
                ],
            }
        )
        first = await asyncio.wait_for(pump, timeout=2.0)
        second = await asyncio.wait_for(anext(stream), timeout=2.0)

    assert (first.asset, first.free) == ("USDT", Decimal("940"))
    assert (second.asset, second.free) == ("BTC", Decimal("0.001"))


# --- construction ----------------------------------------------------------


def test_credentials_are_required() -> None:
    with pytest.raises(ValueError, match="api_key and api_secret"):
        BinanceSpotPrivateClient(
            api_key="", api_secret="x", symbols=StubSymbols()
        )
