"""dapi ``exchangeInfo`` row → :class:`ListedInstrument`."""

from __future__ import annotations

from decimal import Decimal

from mftik.exchange.binance.delivery.listing import to_listed
from mftik.exchange.tickers import Category

#: Pinned to a live ``BTCUSD_PERP`` row: ``contractStatus``, int ``contractSize``.
PERP = {
    "symbol": "BTCUSD_PERP",
    "pair": "BTCUSD",
    "contractType": "PERPETUAL",
    "contractStatus": "TRADING",
    "contractSize": 100,
    "baseAsset": "BTC",
    "quoteAsset": "USD",
    "marginAsset": "BTC",
    "filters": [
        {
            "filterType": "PRICE_FILTER",
            "tickSize": "0.1",
            "minPrice": "1000",
            "maxPrice": "4520958",
        },
        {
            "filterType": "LOT_SIZE",
            "stepSize": "1",
            "minQty": "1",
            "maxQty": "1000000",
        },
    ],
}


def test_a_perp_row_keeps_quote_per_contract_and_lot_in_contracts() -> None:
    listed = to_listed(PERP)

    assert listed is not None
    assert str(listed.ticker) == "BinanceDelivery_Perp_BTCUSD"
    assert listed.exch_ticker == "BTCUSD_PERP"
    assert listed.category is Category.PERP
    assert listed.contract_size == Decimal("100")
    assert listed.settlement_asset == "BTC"
    assert listed.is_active
    # Lot stays in contracts — multiplying by contractSize would invent BTC.
    assert listed.filters["qty_step"] == Decimal("1")
    assert listed.filters["min_qty"] == Decimal("1")
    assert listed.filters["max_qty"] == Decimal("1000000")
    assert listed.filters["price_tick"] == Decimal("0.1")
    assert listed.filters["min_price"] == Decimal("1000")
    assert listed.filters["max_price"] == Decimal("4520958")
    assert listed.filters["min_notional"] is None


def test_a_dated_contract_is_dropped() -> None:
    """``BTCUSD_260925`` canonicalizes to ``BTCUSD`` and would collide."""
    listed = to_listed(
        {
            **PERP,
            "symbol": "BTCUSD_260925",
            "contractType": "CURRENT_QUARTER",
        }
    )
    assert listed is None


def test_missing_contract_size_is_skipped() -> None:
    row = {k: v for k, v in PERP.items() if k != "contractSize"}
    assert to_listed(row) is None


def test_contract_status_falls_back_to_status() -> None:
    """A mixed fixture that still spells ``status`` must not abort the row."""
    row = {k: v for k, v in PERP.items() if k != "contractStatus"}
    row["status"] = "TRADING"
    listed = to_listed(row)
    assert listed is not None
    assert listed.is_active


def test_pending_trading_is_listed_inactive() -> None:
    listed = to_listed({**PERP, "contractStatus": "PENDING_TRADING"})
    assert listed is not None
    assert not listed.is_active
