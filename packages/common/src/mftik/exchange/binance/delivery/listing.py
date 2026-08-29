"""Binance COIN-M ``exchangeInfo`` row → :class:`ListedInstrument`."""

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
    listing_decimal,
    parse_listing_row,
)

logger = logging.getLogger(__name__)

PERPETUAL = "PERPETUAL"
TRADING = "TRADING"

VENUE = venues.BINANCE_DELIVERY.name


class BinanceDeliveryExchangeSymbol(BaseModel):
    """One ``symbols[]`` row of ``GET /dapi/v1/exchangeInfo``.

    dapi spells the live flag ``contractStatus`` (fapi uses ``status``) and
    publishes ``contractSize`` as an unquoted int — USD per contract, not
    base per contract. :func:`~mftik.symbols.listed.listing_decimal` accepts
    the int; lot filters stay in contracts and are not scaled by it.
    """

    model_config = ConfigDict(extra="ignore")

    symbol: WireStr = ""
    pair: WireStr = ""
    base_asset: WireStr = Field(default="", alias="baseAsset")
    quote_asset: WireStr = Field(default="", alias="quoteAsset")
    margin_asset: WireStr = Field(default="", alias="marginAsset")
    contract_status: WireStr = Field(default="", alias="contractStatus")
    status: WireStr = ""
    contract_type: WireStr = Field(default="", alias="contractType")
    contract_size: Any = Field(default=None, alias="contractSize")
    filters: WireList = Field(default_factory=list)


def to_listed(
    row: dict[str, Any] | BinanceDeliveryExchangeSymbol,
    *,
    venue: str = VENUE,
    category: Category = Category.PERP,
) -> ListedInstrument | None:
    parsed = parse_listing_row(BinanceDeliveryExchangeSymbol, row)
    if parsed is None:
        logger.warning("%s skipping malformed symbol: %r", venue, row)
        return None
    if parsed.contract_type != PERPETUAL:
        return None
    size = listing_decimal(parsed.contract_size)
    if size is None:
        logger.warning("%s skipping symbol with no contractSize: %r", venue, row)
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
    status = parsed.contract_status or parsed.status

    return ListedInstrument(
        venue=venue,
        base=base,
        quote=quote,
        exch_ticker=exch_ticker,
        category=category,
        contract_size=size,
        settlement_asset=parsed.margin_asset or None,
        is_active=status == TRADING,
        filters={
            PRICE_TICK: bound(price.get("tickSize")),
            QTY_STEP: bound(lot.get("stepSize")),
            MIN_QTY: bound(lot.get("minQty")),
            MAX_QTY: bound(lot.get("maxQty")),
            MIN_NOTIONAL: None,
            MAX_NOTIONAL: None,
            MIN_PRICE: bound(price.get("minPrice")),
            MAX_PRICE: bound(price.get("maxPrice")),
        },
    )


__all__ = [
    "BinanceDeliveryExchangeSymbol",
    "PERPETUAL",
    "TRADING",
    "VENUE",
    "to_listed",
]
