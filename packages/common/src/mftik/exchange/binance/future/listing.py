"""Binance USD-M ``exchangeInfo`` row → :class:`ListedInstrument`."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from mftik.exchange import venues
from mftik.exchange.binance.listing import bound, filters_by_type
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
)

PERPETUAL = "PERPETUAL"
TRADING = "TRADING"

VENUE = venues.BINANCE_FUTURE.name


class BinanceFutureExchangeSymbol(BaseModel):
    """One ``symbols[]`` row of ``GET /fapi/v1/exchangeInfo``."""

    model_config = ConfigDict(extra="ignore")

    symbol: str = ""
    base_asset: str = Field(default="", alias="baseAsset")
    quote_asset: str = Field(default="", alias="quoteAsset")
    margin_asset: str = Field(default="", alias="marginAsset")
    status: str = ""
    contract_type: str = Field(default="", alias="contractType")
    filters: list[dict[str, Any]] = Field(default_factory=list)


def to_listed(
    row: dict[str, Any] | BinanceFutureExchangeSymbol,
    *,
    venue: str = VENUE,
    category: Category = Category.PERP,
) -> ListedInstrument | None:
    parsed = (
        row
        if isinstance(row, BinanceFutureExchangeSymbol)
        else BinanceFutureExchangeSymbol.model_validate(row)
    )
    if parsed.contract_type != PERPETUAL:
        return None
    base = parsed.base_asset.upper()
    quote = parsed.quote_asset.upper()
    exch_ticker = parsed.symbol
    if not base or not quote or not exch_ticker:
        return None

    filters = filters_by_type(parsed.filters)
    price = filters.get("PRICE_FILTER", {})
    lot = filters.get("LOT_SIZE", {})
    notional = filters.get("MIN_NOTIONAL", {})

    return ListedInstrument(
        venue=venue,
        base=base,
        quote=quote,
        exch_ticker=exch_ticker,
        category=category,
        settlement_asset=parsed.margin_asset or None,
        is_active=parsed.status == TRADING,
        filters={
            PRICE_TICK: bound(price.get("tickSize")),
            QTY_STEP: bound(lot.get("stepSize")),
            MIN_QTY: bound(lot.get("minQty")),
            MAX_QTY: bound(lot.get("maxQty")),
            MIN_NOTIONAL: bound(notional.get("notional")),
            MAX_NOTIONAL: None,
            MIN_PRICE: bound(price.get("minPrice")),
            MAX_PRICE: bound(price.get("maxPrice")),
        },
    )


__all__ = [
    "BinanceFutureExchangeSymbol",
    "PERPETUAL",
    "TRADING",
    "VENUE",
    "to_listed",
]
