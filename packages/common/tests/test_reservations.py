"""What an order commits — the figure TD pre-locks and STS predicts.

Lives beside the arithmetic rather than beside either caller, because the
whole point of the module is that there is one answer for both of them.
"""

from __future__ import annotations

from decimal import Decimal

from mftik.exchange.models import (
    OrderType,
    PlaceOrderRequest,
    Side,
)
from mftik.exchange.reservations import commitment_for, reservation_for
from mftik.exchange.tickers import Category

BASE = "BTC"
QUOTE = "USDT"


def _request(**overrides: object) -> PlaceOrderRequest:
    payload: dict[str, object] = {
        "universal_ticker": "Paper_Spot_BTCUSDT",
        "side": Side.BUY,
        "type": OrderType.LIMIT,
        "qty": Decimal("0.01"),
        "price": Decimal("50000"),
        "client_order_id": "cid-1",
    }
    payload.update(overrides)
    return PlaceOrderRequest.model_validate(payload)


def _perp(**overrides: object) -> PlaceOrderRequest:
    payload: dict[str, object] = {
        "universal_ticker": "BinanceFuture_Perp_BTCUSDT",
        "side": Side.BUY,
        "type": OrderType.LIMIT,
        "qty": Decimal("0.01"),
        "price": Decimal("50000"),
        "client_order_id": "cid-1",
    }
    payload.update(overrides)
    return PlaceOrderRequest.model_validate(payload)


def _held(
    request: PlaceOrderRequest, *, leverage: Decimal | None = None
) -> tuple[str, Decimal] | None:
    return reservation_for(request, base=BASE, quote=QUOTE, leverage=leverage)


# --- what an order commits ------------------------------------------------


def test_a_buy_commits_quote_currency() -> None:
    assert _held(_request()) == ("USDT", Decimal("500"))


def test_a_sell_commits_base_currency() -> None:
    held = _held(_request(side=Side.SELL))
    assert held == ("BTC", Decimal("0.01"))


def test_a_market_buy_cannot_be_priced() -> None:
    """No price means no notional; the caller decides what to do about it."""
    assert _held(_request(type=OrderType.MARKET, price=None)) is None


def test_a_market_buy_sized_in_quote_commits_that_amount() -> None:
    held = _held(
        _request(
            type=OrderType.MARKET,
            price=None,
            qty=None,
            quote_qty=Decimal("100"),
        )
    )
    assert held == ("USDT", Decimal("100"))


def test_a_spot_sell_sized_in_quote_commits_nothing_knowable() -> None:
    """Quote-sized, but base is what leaves the account.

    Reserving the quote would hold the asset the sell is about to *receive*:
    the order would be refused on an account that can perfectly well afford
    it, and the base actually going out would never be pre-locked at all.
    """
    held = _held(
        _request(
            side=Side.SELL,
            type=OrderType.MARKET,
            price=None,
            qty=None,
            quote_qty=Decimal("100"),
        )
    )
    assert held is None


def test_a_delivery_future_commits_nothing_knowable() -> None:
    """Coin-m dated futures settle in the coin and size in contracts.

    ``Category.FUTURE`` on BinanceFuture is linear; the same category on
    BinanceDelivery is not. A USDT reading would lock the wrong asset.
    """
    request = _request(universal_ticker="BinanceDelivery_Future_BTCUSD260925")
    assert (
        commitment_for(
            category=Category.FUTURE,
            side=request.side,
            order_type=request.type,
            base="BTC",
            quote="USD",
            qty=request.qty,
            price=request.price,
            leverage=Decimal("10"),
            venue="BinanceDelivery",
        )
        is None
    )
    assert (
        reservation_for(request, base="BTC", quote="USD", leverage=Decimal("10"))
        is None
    )


def test_an_inverse_order_commits_nothing_knowable() -> None:
    """Margin is in the coin and size is a contract count.

    A linear or spot reading would lock USD or BTC at the wrong figure.
    ``contract_size`` is not an argument here, so the answer is None until
    the inverse formula lands beside it.
    """
    request = _request(universal_ticker="BinanceDelivery_Inverse_BTCUSD")
    assert (
        commitment_for(
            category=Category.INVERSE,
            side=request.side,
            order_type=request.type,
            base="BTC",
            quote="USD",
            qty=request.qty,
            price=request.price,
            leverage=Decimal("10"),
        )
        is None
    )
    assert (
        reservation_for(request, base="BTC", quote="USD", leverage=Decimal("10"))
        is None
    )


def test_a_perp_market_sized_in_quote_commits_margin() -> None:
    held = _held(
        _perp(
            type=OrderType.MARKET,
            price=None,
            qty=None,
            quote_qty=Decimal("100"),
        ),
        leverage=Decimal("10"),
    )
    assert held == ("USDT", Decimal("10"))


def test_a_market_sell_still_commits_base() -> None:
    """Selling commits quantity, which a market order does know."""
    held = _held(_request(side=Side.SELL, type=OrderType.MARKET, price=None))
    assert held == ("BTC", Decimal("0.01"))


def test_a_perp_sell_sized_in_quote_still_commits_margin() -> None:
    """Unlike spot, both perp sides commit margin in the settle asset."""
    held = _held(
        _perp(
            side=Side.SELL,
            type=OrderType.MARKET,
            price=None,
            qty=None,
            quote_qty=Decimal("100"),
        ),
        leverage=Decimal("10"),
    )
    assert held == ("USDT", Decimal("10"))


def test_a_perp_buy_commits_quote_margin_over_leverage() -> None:
    held = _held(_perp(), leverage=Decimal("10"))
    assert held == ("USDT", Decimal("50"))  # 500 / 10


def test_a_perp_sell_also_commits_quote_margin() -> None:
    """Perp sells do not lock base inventory — both sides need margin."""
    held = _held(_perp(side=Side.SELL), leverage=Decimal("5"))
    assert held == ("USDT", Decimal("100"))  # 500 / 5


def test_a_perp_without_leverage_defaults_to_one() -> None:
    """Conservative until ensure_leverage has filled the cache."""
    held = _held(_perp(), leverage=None)
    assert held == ("USDT", Decimal("500"))


def test_a_perp_market_order_cannot_be_priced() -> None:
    assert (
        _held(
            _perp(type=OrderType.MARKET, price=None),
            leverage=Decimal("10"),
        )
        is None
    )


# --- the two entry points are one answer -----------------------------------


def test_the_scalar_form_answers_exactly_what_the_request_form_does() -> None:
    """The reason the module exists, stated as a test.

    TD reads the figure off a built request; a strategy asks before it has
    one. If these two could disagree, a strategy would size against a number
    TD does not enforce — which is the drift the single copy is here to make
    impossible.
    """
    cases = [
        (_request(), Category.SPOT, None),
        (_request(side=Side.SELL), Category.SPOT, None),
        (_request(type=OrderType.MARKET, price=None), Category.SPOT, None),
        (_perp(), Category.PERP, Decimal("10")),
        (_perp(side=Side.SELL), Category.PERP, Decimal("5")),
        (_perp(), Category.PERP, None),
    ]
    for request, category, leverage in cases:
        assert commitment_for(
            category=category,
            side=request.side,
            order_type=request.type,
            base=BASE,
            quote=QUOTE,
            qty=request.qty,
            quote_qty=request.quote_qty,
            price=request.price,
            leverage=leverage,
        ) == reservation_for(
            request, base=BASE, quote=QUOTE, leverage=leverage
        )
