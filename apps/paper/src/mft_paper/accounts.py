"""Seeded paper accounts (matches scripts/seed_paper_apis.py keys)."""

from __future__ import annotations

from decimal import Decimal

from mft.exchange.models import OrderType, PlaceOrderRequest, Side

# api_key, api_secret, balances
SEEDED_ACCOUNTS: tuple[tuple[str, str, dict[str, Decimal]], ...] = (
    (
        "paper-key-1",
        "paper-secret-1",
        {"BTC": Decimal("1"), "USDT": Decimal("100000")},
    ),
    (
        "paper-key-2",
        "paper-secret-2",
        {"BTC": Decimal("10"), "USDT": Decimal("500000")},
    ),
)

# Resting liquidity placed by paper-key-2 (counterparty for matching).
# bid [[49999, 10]], ask [[50001, 10]]
LIQUIDITY_ORDERS: tuple[tuple[str, PlaceOrderRequest], ...] = (
    (
        "paper-key-2",
        PlaceOrderRequest(
            universal_ticker="Paper_Spot_BTCUSDT",
            side=Side.BUY,
            type=OrderType.LIMIT,
            qty=Decimal("10"),
            price=Decimal("49999"),
            client_order_id="seed-bid-49999",
        ),
    ),
    (
        "paper-key-2",
        PlaceOrderRequest(
            universal_ticker="Paper_Spot_BTCUSDT",
            side=Side.SELL,
            type=OrderType.LIMIT,
            qty=Decimal("10"),
            price=Decimal("50001"),
            client_order_id="seed-ask-50001",
        ),
    ),
)
