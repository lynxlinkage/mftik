"""Binance spot ``exchangeInfo`` row → :class:`ListedInstrument`."""

from __future__ import annotations

import logging
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
    WireList,
    WireStr,
    parse_listing_row,
)

logger = logging.getLogger(__name__)

VENUE = venues.BINANCE.name
TRADING = "TRADING"


class BinanceSpotExchangeSymbol(BaseModel):
    """One ``symbols[]`` row of ``GET /api/v3/exchangeInfo``."""

    model_config = ConfigDict(extra="ignore")

    symbol: WireStr = ""
    base_asset: WireStr = Field(default="", alias="baseAsset")
    quote_asset: WireStr = Field(default="", alias="quoteAsset")
    status: WireStr = ""
    filters: WireList = Field(default_factory=list)


def to_listed(
    row: dict[str, Any] | BinanceSpotExchangeSymbol,
    *,
    venue: str = VENUE,
    category: Category = Category.SPOT,
) -> ListedInstrument | None:
    parsed = parse_listing_row(BinanceSpotExchangeSymbol, row)
    if parsed is None:
        logger.warning("%s skipping malformed symbol: %r", venue, row)
        return None
    base = parsed.base_asset.upper()
    quote = parsed.quote_asset.upper()
    exch_ticker = parsed.symbol
    if not base or not quote or not exch_ticker:
        logger.warning("%s skipping malformed symbol: %r", venue, row)
        return None

    filters = filters_by_type(parsed.filters)
    price = filters.get("PRICE_FILTER", {})
    lot = filters.get("LOT_SIZE", {})
    notional = filters.get("NOTIONAL") or filters.get("MIN_NOTIONAL") or {}

    return ListedInstrument(
        venue=venue,
        base=base,
        quote=quote,
        exch_ticker=exch_ticker,
        category=category,
        is_active=parsed.status == TRADING,
        filters={
            PRICE_TICK: bound(price.get("tickSize")),
            QTY_STEP: bound(lot.get("stepSize")),
            MIN_QTY: bound(lot.get("minQty")),
            MAX_QTY: bound(lot.get("maxQty")),
            MIN_NOTIONAL: bound(notional.get("minNotional")),
            MAX_NOTIONAL: bound(notional.get("maxNotional")),
            MIN_PRICE: bound(price.get("minPrice")),
            MAX_PRICE: bound(price.get("maxPrice")),
        },
    )


__all__ = ["BinanceSpotExchangeSymbol", "TRADING", "VENUE", "to_listed"]
