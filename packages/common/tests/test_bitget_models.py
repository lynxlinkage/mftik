"""Bitget listing rows and ticker-shared funding / OI (V2 / V3 / V5)."""

from __future__ import annotations

from decimal import Decimal

from mftik.exchange.bitget.listing import to_listed
from mftik.exchange.bitget.models import BitgetTicker
from mftik.exchange.tickers import Category, UniversalTicker

USDT_PERP = {
    "symbol": "BTCUSDT",
    "category": "USDT-FUTURES",
    "baseCoin": "BTC",
    "quoteCoin": "USDT",
    "type": "perpetual",
    "status": "online",
    "minOrderQty": "0.001",
    "maxOrderQty": "100",
    "minOrderAmount": "5",
    "pricePrecision": "1",
    "quantityPrecision": "3",
    "priceMultiplier": "0.1",
    "quantityMultiplier": "0.001",
}

USDC_PERP = {
    "symbol": "BTCPERP",
    "category": "USDC-FUTURES",
    "baseCoin": "BTC",
    "quoteCoin": "USDC",
    "type": "perpetual",
    "status": "online",
    "minOrderQty": "0.001",
    "maxOrderQty": "100",
    "minOrderAmount": "5",
    "pricePrecision": "1",
    "quantityPrecision": "3",
    "priceMultiplier": "0.1",
    "quantityMultiplier": "0.001",
}

DELIVERY = {
    **USDT_PERP,
    "symbol": "BTCUSDT-260327",
    "type": "delivery",
}


def test_i5_usdt_and_usdc_perps_are_both_named() -> None:
    usdt = to_listed(USDT_PERP, category=Category.PERP)
    usdc = to_listed(USDC_PERP, category=Category.PERP)
    assert usdt is not None and usdc is not None
    assert str(usdt.ticker) == "Bitget_Perp_BTCUSDT"
    assert str(usdc.ticker) == "Bitget_Perp_BTCUSDC"
    assert usdt.exch_ticker == "BTCUSDT"
    assert usdc.exch_ticker == "BTCPERP"
    assert usdt.quote == "USDT" and usdt.settlement_asset == "USDT"
    assert usdc.quote == "USDC" and usdc.settlement_asset == "USDC"


def test_v3_delivery_rows_are_dropped_from_perp() -> None:
    assert to_listed(DELIVERY, category=Category.PERP) is None


def test_a_malformed_row_is_skipped() -> None:
    assert to_listed({"symbol": "BTCUSDT"}, category=Category.SPOT) is None


def test_spot_listing_does_not_emit_a_perp_ticker() -> None:
    listed = to_listed(
        {
            "symbol": "BTCUSDT",
            "baseCoin": "BTC",
            "quoteCoin": "USDT",
            "status": "online",
            "type": "spot",
        },
        category=Category.SPOT,
    )
    assert listed is not None
    assert listed.category is Category.SPOT
    assert listed.settlement_asset is None
    assert str(listed.ticker) == "Bitget_Spot_BTCUSDT"


def test_v5_ticker_carries_funding_and_open_interest() -> None:
    ticker = UniversalTicker.parse("Bitget_Perp_BTCUSDT")
    row = BitgetTicker.model_validate(
        {
            "symbol": "BTCUSDT",
            "lastPrice": "60000",
            "fundingRate": "0.0001",
            "openInterest": "1234",
            "ts": "1700000001000",
        }
    )
    funding = row.to_funding_rate(ticker)
    interest = row.to_open_interest(ticker)
    assert funding is not None and funding.rate == Decimal("0.0001")
    assert interest is not None and interest.qty == Decimal("1234")
