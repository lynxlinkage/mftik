"""Gate spot wire models — parsed from real v4 payload samples.

Samples are taken from Gate's official SDK struct definitions (gateio/gatews
``response_spot.go``) and the message examples in its docs.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from mftik.exchange.gate.spot import (
    GateBalance,
    GateBookTicker,
    GateCandlestick,
    GateOrderAck,
    GateOrderBook,
    GateOrderBookUpdate,
    GateOrderUpdate,
    GateTicker,
    GateTrade,
    GateUserTrade,
    from_text,
    to_text,
)
from mftik.exchange.models import OrderStatus, OrderType, Side
from mftik.exchange.tickers import UniversalTicker

#: The instrument every payload in this module is stamped with.
TICKER = UniversalTicker.parse("Gate_Spot_BTCUSDT")


def test_ticker() -> None:
    t = GateTicker.model_validate(
        {
            "currency_pair": "BTC_USDT",
            "last": "43444.82",
            "lowest_ask": "43444.82",
            "highest_bid": "43444.81",
            "change_percentage": "-4.0036",
            "base_volume": "5182.5412425462",
            "quote_volume": "227267634.93123952",
            "high_24h": "47698",
            "low_24h": "42721.03",
        }
    )
    assert t.currency_pair == "BTC_USDT"
    assert t.last == Decimal("43444.82")

    ticker = t.to_ticker(TICKER)
    assert ticker.universal_ticker == str(TICKER)
    assert ticker.bid == Decimal("43444.81")
    assert ticker.ask == Decimal("43444.82")


def test_ticker_without_quotes_falls_back_to_last() -> None:
    t = GateTicker.model_validate({"currency_pair": "GT_USDT", "last": "7.5"})
    ticker = t.to_ticker(TICKER)
    assert ticker.bid == ticker.ask == Decimal("7.5")


def test_public_trade() -> None:
    tr = GateTrade.model_validate(
        {
            "id": 3130257995,
            "create_time": 1648725035,
            "create_time_ms": "1648725035923.0",
            "side": "sell",
            "currency_pair": "LTC_USDT",
            "amount": "0.0116",
            "price": "130.11",
        }
    )
    trade = tr.to_trade(TICKER)
    assert trade.trade_id == "3130257995"
    assert trade.universal_ticker == str(TICKER)
    assert trade.side is Side.SELL
    assert trade.qty == Decimal("0.0116")
    assert trade.ts == pytest.approx(1648725035.923)


def test_candlestick_splits_name_and_marks_window_close() -> None:
    c = GateCandlestick.model_validate(
        {
            "t": "1606292580",
            "v": "2362.32035",
            "c": "19128.1",
            "h": "19128.5",
            "l": "19127.0",
            "o": "19128.1",
            "n": "1m_BTC_USDT",
            "a": "45188.3",
            "w": True,
        }
    )
    assert c.interval == "1m"
    assert c.currency_pair == "BTC_USDT"
    assert c.open_time == 1606292580.0
    assert c.low == Decimal("19127.0")
    assert c.window_closed is True
    # No shared analogue — deliberately not convertible.
    assert not hasattr(c, "to_ticker")


def test_book_ticker() -> None:
    b = GateBookTicker.model_validate(
        {
            "t": 1671363004228,
            "u": 9793320464,
            "s": "BTC_USDT",
            "b": "16716.8",
            "B": "0.0134",
            "a": "16716.9",
            "A": "0.0353",
        }
    )
    assert b.currency_pair == "BTC_USDT"
    assert b.bid == Decimal("16716.8")
    assert b.bid_size == Decimal("0.0134")
    assert b.ask == Decimal("16716.9")
    assert b.ask_size == Decimal("0.0353")
    assert b.ts == pytest.approx(1671363004.228)


def test_order_book_snapshot() -> None:
    ob = GateOrderBook.model_validate(
        {
            "t": 1606295412123,
            "lastUpdateId": 48791820,
            "s": "BTC_USDT",
            "bids": [["19137.74", "0.0001"], ["19137.73", "0.02"]],
            "asks": [["19137.75", "0.6135"]],
        }
    )
    assert ob.last_update_id == 48791820

    book = ob.to_order_book(TICKER)
    assert book.universal_ticker == str(TICKER)
    assert len(book.bids) == 2
    assert book.bids[0].price == Decimal("19137.74")
    assert book.asks[0].qty == Decimal("0.6135")


def test_order_book_update_is_a_diff_not_a_book() -> None:
    d = GateOrderBookUpdate.model_validate(
        {
            "t": 1650189272515,
            "e": "depthUpdate",
            "E": 1650189272,
            "s": "GMT_USDT",
            "U": 140595902,
            "u": 140595910,
            "b": [["2.51518", "228.119"], ["2.50587", "0"]],
            "a": [["2.5182", "4.199"]],
        }
    )
    assert d.currency_pair == "GMT_USDT"
    assert d.first_id == 140595902
    assert d.last_id == 140595910
    # A zero qty is a level delete, which is why this is not an OrderBook.
    assert d.bid_levels()[1].qty == Decimal("0")
    assert not hasattr(d, "to_order_book")


def test_order_book_update_sequencing() -> None:
    d = GateOrderBookUpdate.model_validate(
        {"t": 1, "s": "BTC_USDT", "U": 100, "u": 110, "b": [], "a": []}
    )
    assert d.follows(99)  # next expected id is 100, inside [100, 110]
    assert d.follows(105)  # overlapping range still applies
    assert not d.follows(98)  # gap — need a fresh snapshot
    assert not d.follows(200)  # stale


def test_client_order_id_text_round_trip() -> None:
    assert to_text("289865223110657") == "t-289865223110657"
    assert to_text("t-already") == "t-already"
    assert from_text("t-289865223110657") == "289865223110657"
    # Gate's own markers are not client order ids.
    assert from_text("apiv4") is None
    assert from_text("-") is None
    assert from_text(None) is None


def _order(**overrides: object) -> dict[str, object]:
    base = {
        "id": "1036717689726",
        "text": "t-289865223110657",
        "create_time": "1774613210",
        "update_time": "1774613210",
        "currency_pair": "BTC_USDT",
        "type": "limit",
        "account": "spot",
        "side": "buy",
        "amount": "0.1",
        "price": "200",
        "time_in_force": "gtc",
        "left": "0.1",
        "filled_amount": "0",
        "filled_total": "0",
        "avg_deal_price": "0",
        "fee": "0",
        "fee_currency": "BTC",
        "create_time_ms": "1774613210391",
        "update_time_ms": "1774613210391",
        "user": 10406147,
        "event": "put",
        "finish_as": "open",
    }
    base.update(overrides)
    return base


def test_order_put_is_open() -> None:
    o = GateOrderUpdate.model_validate(_order())
    assert o.status is OrderStatus.NEW
    assert o.client_order_id == "289865223110657"

    order = o.to_order(TICKER)
    assert order.order_id == "1036717689726"
    assert order.universal_ticker == str(TICKER)
    assert order.side is Side.BUY
    assert order.type is OrderType.LIMIT
    assert order.qty == Decimal("0.1")
    assert order.filled_qty == Decimal("0")
    assert order.ts == pytest.approx(1774613210.391)


def test_order_partial_fill() -> None:
    o = GateOrderUpdate.model_validate(
        _order(event="update", left="0.04", filled_amount="0.06",
               avg_deal_price="199.5")
    )
    assert o.status is OrderStatus.PARTIALLY_FILLED
    assert o.filled_qty == Decimal("0.06")
    assert o.to_order(TICKER).avg_price == Decimal("199.5")


@pytest.mark.parametrize(
    ("finish_as", "left", "expected"),
    [
        ("filled", "0", OrderStatus.FILLED),
        ("cancelled", "0.1", OrderStatus.CANCELED),
        ("ioc", "0.05", OrderStatus.CANCELED),
        ("stp", "0.1", OrderStatus.CANCELED),
        (None, "0", OrderStatus.FILLED),
        (None, "0.1", OrderStatus.CANCELED),
    ],
)
def test_order_finish_states(
    finish_as: str | None, left: str, expected: OrderStatus
) -> None:
    o = GateOrderUpdate.model_validate(
        _order(event="finish", finish_as=finish_as, left=left)
    )
    assert o.status is expected


def test_order_filled_qty_falls_back_to_amount_minus_left() -> None:
    payload = _order(event="update", left="0.03")
    del payload["filled_amount"]
    o = GateOrderUpdate.model_validate(payload)
    assert o.filled_qty == Decimal("0.07")


def test_user_trade() -> None:
    ut = GateUserTrade.model_validate(
        {
            "id": 5736713,
            "user_id": 1000001,
            "order_id": "30784428",
            "currency_pair": "BTC_USDT",
            "create_time": 1605176741,
            "create_time_ms": "1605176741123.456",
            "side": "sell",
            "amount": "1.00000000",
            "role": "taker",
            "price": "10000.00000000",
            "fee": "0.00200000000000",
            "fee_currency": "USDT",
            "point_fee": "0",
            "gt_fee": "0",
            "text": "t-99",
        }
    )
    assert ut.role == "taker"

    fill = ut.to_fill(TICKER)
    assert fill.fill_id == "5736713"
    assert fill.order_id == "30784428"
    assert fill.client_order_id == "99"
    assert fill.side is Side.SELL
    assert fill.price == Decimal("10000.00000000")
    assert fill.fee_asset == "USDT"
    assert fill.ts == pytest.approx(1605176741.123456)


def test_balance() -> None:
    b = GateBalance.model_validate(
        {
            "timestamp": "1667556323",
            "timestamp_ms": "1667556323730",
            "user": "1000001",
            "currency": "USDT",
            "change": "0",
            "total": "222244.3827652",
            "available": "222244.3827",
            "freeze": "5",
            "freeze_change": "5.000000",
            "change_type": "order-create",
        }
    )
    assert b.change_type == "order-create"
    assert b.ts == pytest.approx(1667556323.730)

    bal = b.to_balance()
    assert bal.asset == "USDT"
    assert bal.free == Decimal("222244.3827")
    assert bal.locked == Decimal("5")


def test_models_tolerate_unknown_fields() -> None:
    """Gate adds fields over time; that must not break parsing."""
    t = GateTrade.model_validate(
        {
            "id": 1,
            "create_time": 1,
            "create_time_ms": "1000",
            "side": "buy",
            "currency_pair": "BTC_USDT",
            "amount": "1",
            "price": "2",
            "some_new_field_gate_added": "whatever",
        }
    )
    assert t.price == Decimal("2")


# --- trading call replies ---------------------------------------------------


def test_api_ack_with_data_is_still_preliminary() -> None:
    """Gate's ack frame echoes ``req_param`` under ``data`` — not the order."""
    from mftik.exchange.gate.spot.protocol import GateResponse

    resp = GateResponse(
        {
            "request_id": "r1",
            "ack": True,
            "header": {
                "status": "200",
                "channel": "spot.order_place",
                "event": "api",
            },
            "data": {
                "result": {
                    "req_id": "r1",
                    "req_param": {"currency_pair": "ETH_USDT"},
                }
            },
        }
    )
    assert resp.ack is True


def test_order_ack_uses_status_not_event() -> None:
    """A call reply reports ``status``; the push channel reports ``event``."""
    ack = GateOrderAck.model_validate(
        {
            "id": "1852454420",
            "text": "t-42",
            "currency_pair": "BTC_USDT",
            "type": "limit",
            "side": "buy",
            "amount": "0.001",
            "price": "60000",
            "left": "0.001",
            "status": "open",
            "finish_as": "open",
            "update_time_ms": 1774613210391,
        }
    )
    assert ack.client_order_id == "42"
    assert ack.order_status is OrderStatus.NEW

    order = ack.to_order(TICKER)
    assert order.order_id == "1852454420"
    assert order.qty == Decimal("0.001")
    assert order.filled_qty == Decimal("0")
    assert order.ts == pytest.approx(1774613210.391)


@pytest.mark.parametrize(
    ("status", "left", "expected"),
    [
        ("open", "0.001", OrderStatus.NEW),
        ("open", "0.0004", OrderStatus.PARTIALLY_FILLED),
        ("closed", "0", OrderStatus.FILLED),
        ("cancelled", "0.001", OrderStatus.CANCELED),
    ],
)
def test_order_ack_status_mapping(
    status: str, left: str, expected: OrderStatus
) -> None:
    ack = GateOrderAck.model_validate(
        {
            "id": "1",
            "currency_pair": "BTC_USDT",
            "side": "buy",
            "amount": "0.001",
            "left": left,
            "status": status,
        }
    )
    assert ack.order_status is expected


def test_batch_cancel_leg_carries_succeeded() -> None:
    leg = GateOrderAck.model_validate(
        {
            "id": "2",
            "currency_pair": "BTC_USDT",
            "succeeded": False,
            "label": "ORDER_NOT_FOUND",
            "message": "order not found",
        }
    )
    assert leg.succeeded is False
    assert leg.label == "ORDER_NOT_FOUND"


def test_order_ack_defaults_succeeded_to_none_for_single_calls() -> None:
    """Only batch cancels carry ``succeeded``; absence must not read as False."""
    ack = GateOrderAck.model_validate(
        {"id": "1", "currency_pair": "BTC_USDT", "side": "buy", "amount": "1"}
    )
    assert ack.succeeded is None
