"""Gate futures wire models — contracts ↔ base, signed size, ``t-`` text."""

from __future__ import annotations

from decimal import Decimal

from mftik.exchange.gate.future.models import (
    GateFuturesBalance,
    GateFuturesLiquidation,
    GateFuturesOrder,
    GateFuturesPosition,
    GateFuturesTrade,
    GateFuturesUserTrade,
    base_to_contracts,
    contracts_to_base,
    from_text,
    signed_contracts,
    to_text,
)
from mftik.exchange.models import OrderStatus, OrderType, Side
from mftik.exchange.tickers import UniversalTicker

TICKER = UniversalTicker.parse("GateFutures_Perp_BTCUSDT")
CS = Decimal("0.0001")


def test_contracts_round_trip_to_base() -> None:
    assert contracts_to_base(Decimal("10"), CS) == Decimal("0.001")
    assert base_to_contracts(Decimal("0.001"), CS) == Decimal("10")


def test_sell_is_a_negative_size() -> None:
    assert signed_contracts(Side.BUY, Decimal("10")) == Decimal("10")
    assert signed_contracts(Side.SELL, Decimal("10")) == Decimal("-10")


def test_wire_size_is_not_scientific() -> None:
    from mftik.exchange.gate.future.models import format_size

    assert format_size(Decimal("-10")) == "-10"
    assert format_size(Decimal("10.50")) == "10.5"


def test_text_wraps_and_unwraps() -> None:
    assert to_text("42") == "t-42"
    assert to_text("t-42") == "t-42"
    assert from_text("t-42") == "42"
    assert from_text("apiv4") is None
    assert from_text("-") is None


def test_public_trade_size_is_base_and_signed() -> None:
    trade = GateFuturesTrade.model_validate(
        {
            "id": 7,
            "contract": "BTC_USDT",
            "size": "-10.5",
            "price": "60000",
            "create_time_ms": 1_700_000_000_500,
        }
    ).to_trade(TICKER, CS)
    assert trade.qty == Decimal("0.00105")
    assert trade.side is Side.SELL
    assert trade.ts == 1_700_000_000.5


def test_order_maps_signed_size_and_partial_fill() -> None:
    order = GateFuturesOrder.model_validate(
        {
            "id": "9",
            "text": "t-abc",
            "contract": "BTC_USDT",
            "size": "-20",
            "left": "-8",
            "price": "60000",
            "fill_price": "59990",
            "status": "open",
        }
    ).to_order(TICKER, CS)
    assert order.client_order_id == "abc"
    assert order.side is Side.SELL
    assert order.type is OrderType.LIMIT
    assert order.status is OrderStatus.PARTIALLY_FILLED
    assert order.qty == Decimal("0.002")
    assert order.filled_qty == Decimal("0.0012")
    assert order.avg_price == Decimal("59990")


def test_price_zero_is_a_market_order() -> None:
    order = GateFuturesOrder.model_validate(
        {
            "id": "1",
            "contract": "BTC_USDT",
            "size": "5",
            "left": "0",
            "price": "0",
            "status": "finished",
            "finish_as": "filled",
        }
    ).to_order(TICKER, CS)
    assert order.type is OrderType.MARKET
    assert order.price is None
    assert order.status is OrderStatus.FILLED


def test_position_qty_keeps_the_sign() -> None:
    long_pos = GateFuturesPosition.model_validate(
        {"contract": "BTC_USDT", "size": "10", "entry_price": "60000"}
    ).to_position(TICKER, CS)
    short_pos = GateFuturesPosition.model_validate(
        {"contract": "BTC_USDT", "size": "-10", "entry_price": "60000"}
    ).to_position(TICKER, CS)
    assert long_pos.qty == Decimal("0.001")
    assert short_pos.qty == Decimal("-0.001")


def test_user_trade_is_attributable_via_text() -> None:
    fill = GateFuturesUserTrade.model_validate(
        {
            "id": 3,
            "order_id": 9,
            "contract": "BTC_USDT",
            "size": "4",
            "price": "60000",
            "text": "t-abc",
            "fee": "0.01",
            "create_time": 1_700_000_000,
        }
    ).to_fill(TICKER, CS)
    assert fill.client_order_id == "abc"
    assert fill.qty == Decimal("0.0004")
    assert fill.side is Side.BUY


def test_liquidation_side_is_the_closed_position() -> None:
    """A sell-to-close (negative size) liquidated a long."""
    liq = GateFuturesLiquidation.model_validate(
        {
            "contract": "BTC_USDT",
            "price": "215.1",
            "size": "-124.5",
            "time": 1_541_486_601_123,
        }
    ).to_liquidation(TICKER, CS)
    assert liq.side is Side.BUY
    assert liq.qty == Decimal("0.01245")


def test_balance_locks_the_unavailable() -> None:
    bal = GateFuturesBalance.model_validate(
        {"currency": "USDT", "total": "100", "available": "40"}
    ).to_balance()
    assert bal.free == Decimal("40")
    assert bal.locked == Decimal("60")
