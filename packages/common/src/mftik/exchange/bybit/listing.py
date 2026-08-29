"""Bybit ``instruments-info`` row → :class:`ListedInstrument`."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from mftik.exchange import venues
from mftik.exchange.tickers import Category
from mftik.symbols.listed import (
    MAX_NOTIONAL,
    MAX_PRICE,
    MAX_QTY,
    MIN_NOTIONAL,
    MIN_PRICE,
    MIN_QTY,
    PRICE_TICK,
    QTY_STEP,
    ListedInstrument,
    listing_decimal,
)

VENUE = venues.BYBIT.name
TRADING = "Trading"
PERPETUAL = frozenset({"LinearPerpetual", "InversePerpetual"})


class BybitInstrumentRow(BaseModel):
    """One ``result.list[]`` row of ``GET /v5/market/instruments-info``."""

    model_config = ConfigDict(extra="ignore")

    symbol: str = ""
    base_coin: str = Field(default="", alias="baseCoin")
    quote_coin: str = Field(default="", alias="quoteCoin")
    settle_coin: str = Field(default="", alias="settleCoin")
    status: str = ""
    contract_type: str = Field(default="", alias="contractType")
    lot_size_filter: dict[str, Any] = Field(
        default_factory=dict, alias="lotSizeFilter"
    )
    price_filter: dict[str, Any] = Field(
        default_factory=dict, alias="priceFilter"
    )


def to_listed(
    row: dict[str, Any] | BybitInstrumentRow,
    *,
    venue: str = VENUE,
    category: Category = Category.SPOT,
) -> ListedInstrument | None:
    parsed = (
        row
        if isinstance(row, BybitInstrumentRow)
        else BybitInstrumentRow.model_validate(row)
    )
    base = parsed.base_coin.upper()
    quote = parsed.quote_coin.upper()
    exch_ticker = parsed.symbol
    if not base or not quote or not exch_ticker:
        return None
    if category is Category.PERP and parsed.contract_type not in PERPETUAL:
        return None

    lot = parsed.lot_size_filter
    price = parsed.price_filter
    return ListedInstrument(
        venue=venue,
        base=base,
        quote=quote,
        exch_ticker=exch_ticker,
        category=category,
        settlement_asset=parsed.settle_coin or None,
        is_active=parsed.status == TRADING,
        filters={
            PRICE_TICK: listing_decimal(price.get("tickSize")),
            QTY_STEP: listing_decimal(
                lot.get("basePrecision") or lot.get("qtyStep")
            ),
            MIN_QTY: listing_decimal(lot.get("minOrderQty")),
            MAX_QTY: listing_decimal(lot.get("maxOrderQty")),
            MIN_NOTIONAL: listing_decimal(
                lot.get("minOrderAmt") or lot.get("minNotionalValue")
            ),
            MAX_NOTIONAL: listing_decimal(lot.get("maxOrderAmt")),
            MIN_PRICE: listing_decimal(price.get("minPrice")),
            MAX_PRICE: listing_decimal(price.get("maxPrice")),
        },
    )


__all__ = ["BybitInstrumentRow", "PERPETUAL", "TRADING", "VENUE", "to_listed"]
