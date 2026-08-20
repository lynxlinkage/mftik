"""Offline PlaceOrderRequest shape and venue-capability checks."""

from __future__ import annotations

from decimal import Decimal

import pytest
from mftik.exchange import venues
from mftik.exchange.models import (
    OrderType,
    PlaceOrderRequest,
    Side,
    market_order,
)
from mftik.exchange.order_check import (
    _MARKET_SIZE,
    REDUCE_ONLY,
    SHAPE,
    VENUE,
    check_rules,
    classify,
    refusal_reason,
)

GATE_SPOT = "Gate_Spot_BTCUSDT"
BYBIT_PERP = "Bybit_Perp_BTCUSDT"
BINANCE_SPOT = "Binance_Spot_BTCUSDT"
PAPER_SPOT = "Paper_Spot_BTCUSDT"


def _req(**overrides: object) -> PlaceOrderRequest:
    payload: dict[str, object] = {
        "universal_ticker": GATE_SPOT,
        "side": Side.SELL,
        "type": OrderType.MARKET,
        "qty": Decimal("1"),
    }
    payload.update(overrides)
    return PlaceOrderRequest.model_validate(payload)


def test_market_requires_exactly_one_size() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        PlaceOrderRequest(
            universal_ticker=GATE_SPOT,
            side=Side.SELL,
            type=OrderType.MARKET,
        )
    with pytest.raises(ValueError, match="exactly one"):
        PlaceOrderRequest(
            universal_ticker=GATE_SPOT,
            side=Side.SELL,
            type=OrderType.MARKET,
            qty=Decimal("1"),
            quote_qty=Decimal("100"),
        )


def test_limit_rejects_quote_qty() -> None:
    with pytest.raises(ValueError, match="market-order size"):
        PlaceOrderRequest(
            universal_ticker=GATE_SPOT,
            side=Side.BUY,
            type=OrderType.LIMIT,
            qty=Decimal("1"),
            quote_qty=Decimal("100"),
            price=Decimal("1"),
        )


def test_gate_market_buy_requires_quote_qty() -> None:
    request = _req(side=Side.BUY, qty=Decimal("0.001"))
    kind, reason = classify(request) or (None, None)
    assert kind == VENUE
    assert reason is not None and "quote_qty" in reason
    with pytest.raises(ValueError, match="quote_qty"):
        request.check()
    assert request.refusal_reason() == reason


def test_gate_market_buy_with_quote_qty_is_legal() -> None:
    request = market_order(
        ticker=GATE_SPOT, side=Side.BUY, quote_qty=Decimal("100")
    )
    assert classify(request) is None
    request.check()
    assert refusal_reason(request) is None


def test_gate_market_sell_rejects_quote_qty() -> None:
    request = _req(side=Side.SELL, qty=None, quote_qty=Decimal("100"))
    kind, reason = classify(request) or (None, None)
    assert kind == VENUE
    assert reason is not None and "base" in reason


def test_perp_market_rejects_quote_qty() -> None:
    request = _req(
        universal_ticker=BYBIT_PERP,
        qty=None,
        quote_qty=Decimal("100"),
    )
    kind, reason = classify(request) or (None, None)
    assert kind == VENUE
    assert reason is not None and "quote_qty is not expressible" in reason


def test_binance_and_paper_accept_either_unit() -> None:
    for ticker in (BINANCE_SPOT, PAPER_SPOT):
        assert (
            classify(
                market_order(ticker=ticker, side=Side.BUY, qty=Decimal("1"))
            )
            is None
        )
        assert (
            classify(
                market_order(
                    ticker=ticker, side=Side.BUY, quote_qty=Decimal("100")
                )
            )
            is None
        )


def test_reduce_only_on_spot_is_refused() -> None:
    request = _req(reduce_only=True)
    kind, reason = classify(request) or (None, None)
    assert kind == REDUCE_ONLY
    assert reason is not None and "spot" in reason


def test_reduce_only_on_perp_is_legal() -> None:
    request = _req(universal_ticker=BYBIT_PERP, reduce_only=True)
    assert classify(request) is None


def test_unknown_venue_skips_capability_rules() -> None:
    # Shape still holds; we do not invent a table for a name we do not know.
    request = PlaceOrderRequest.model_construct(
        universal_ticker="Nowhere_Spot_BTCUSDT",
        side=Side.BUY,
        type=OrderType.MARKET,
        qty=Decimal("1"),
        quote_qty=None,
        price=None,
        reduce_only=False,
        params={},
    )
    found = classify(request)
    assert found is None or found[0] == SHAPE


def test_every_registered_book_says_how_its_market_orders_are_sized() -> None:
    """A missing row reads as permission, so it must not be possible to omit.

    Without this, adding a venue to the registry silently signs this module
    up to approve a size unit nobody confirmed its adapter can send.
    """
    for name, venue in venues.VENUES.items():
        for category in venue.categories:
            assert (name, category) in _MARKET_SIZE, f"{name}/{category.value}"


def test_a_registry_entry_with_no_size_rule_is_an_error() -> None:
    added = venues.Venue(name="Nowhere", label="Nowhere")
    venues.VENUES[added.name] = added
    try:
        with pytest.raises(ValueError, match="Nowhere/Spot"):
            check_rules()
    finally:
        del venues.VENUES[added.name]
