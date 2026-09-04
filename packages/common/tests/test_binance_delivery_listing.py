"""dapi ``exchangeInfo`` row → :class:`ListedInstrument`."""

from __future__ import annotations

from datetime import UTC, datetime
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
    assert str(listed.ticker) == "BinanceCM_Inverse_BTCUSD"
    assert listed.exch_ticker == "BTCUSD_PERP"
    assert listed.category is Category.INVERSE
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


def test_a_dated_contract_is_dropped_from_the_inverse_book() -> None:
    """``BTCUSD_260925`` canonicalizes to ``BTCUSD`` and would steal the perp."""
    listed = to_listed(
        {
            **PERP,
            "symbol": "BTCUSD_260925",
            "contractType": "CURRENT_QUARTER",
        }
    )
    assert listed is None


def test_a_dated_future_glues_yymmdd_onto_the_symbol() -> None:
    """So it cannot collide with ``BinanceCM_Inverse_BTCUSD``."""
    listed = to_listed(
        {
            **PERP,
            "symbol": "BTCUSD_260925",
            "contractType": "CURRENT_QUARTER",
        },
        category=Category.FUTURE,
    )
    assert listed is not None
    assert listed.exch_ticker == "BTCUSD_260925"
    assert listed.symbol == "BTCUSD260925"
    assert str(listed.ticker) == "BinanceCM_Future_BTCUSD260925"
    assert listed.category is Category.FUTURE
    assert listed.contract_size == Decimal("100")
    assert listed.settlement_asset == "BTC"
    assert listed.expiry == datetime(2026, 9, 25, 8, tzinfo=UTC)


def test_a_dated_row_without_delivery_date_still_gets_an_expiry() -> None:
    listed = to_listed(
        {
            **PERP,
            "symbol": "ETHUSD_251226",
            "contractType": "NEXT_QUARTER",
            "baseAsset": "ETH",
            "marginAsset": "ETH",
        },
        category=Category.FUTURE,
    )
    assert listed is not None
    assert listed.symbol == "ETHUSD251226"
    assert listed.expiry == datetime(2025, 12, 26, 8, tzinfo=UTC)


def test_a_perp_is_not_stored_as_a_future() -> None:
    assert to_listed(PERP, category=Category.FUTURE) is None


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
