"""Binance USD-M ``exchangeInfo`` row → :class:`ListedInstrument`."""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime
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

PERPETUAL = "PERPETUAL"
TRADING = "TRADING"

#: Binance writes the expiry onto the wire ticker as ``BTCUSDT_250926``.
#: The six digits are identity — ``deliveryDate`` is the clock, not the name.
_DATE_SUFFIX = re.compile(r"_(\d{6})\Z")

VENUE = venues.BINANCE_UM.name


class BinanceFutureExchangeSymbol(BaseModel):
    """One ``symbols[]`` row of ``GET /fapi/v1/exchangeInfo``."""

    model_config = ConfigDict(extra="ignore")

    symbol: WireStr = ""
    base_asset: WireStr = Field(default="", alias="baseAsset")
    quote_asset: WireStr = Field(default="", alias="quoteAsset")
    margin_asset: WireStr = Field(default="", alias="marginAsset")
    status: WireStr = ""
    contract_type: WireStr = Field(default="", alias="contractType")
    delivery_date: Any = Field(default=None, alias="deliveryDate")
    filters: WireList = Field(default_factory=list)


def to_listed(
    row: dict[str, Any] | BinanceFutureExchangeSymbol,
    *,
    venue: str = VENUE,
    category: Category = Category.PERP,
) -> ListedInstrument | None:
    parsed = parse_listing_row(BinanceFutureExchangeSymbol, row)
    if parsed is None:
        logger.warning("%s skipping malformed symbol: %r", venue, row)
        return None
    expiry_code = _date_code(parsed.symbol)
    if category is Category.PERP:
        if parsed.contract_type != PERPETUAL:
            return None
        expiry_code = None
        expiry = None
    elif category is Category.FUTURE:
        if parsed.contract_type == PERPETUAL or expiry_code is None:
            return None
        expiry = _delivery_expiry(parsed.delivery_date) or _expiry_from_code(
            expiry_code
        )
    else:
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
    notional = filters.get("MIN_NOTIONAL", {})

    return ListedInstrument(
        venue=venue,
        base=base,
        quote=quote,
        exch_ticker=exch_ticker,
        category=category,
        settlement_asset=parsed.margin_asset or None,
        expiry=expiry,
        expiry_code=expiry_code,
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


def _date_code(exch_ticker: str) -> str | None:
    found = _DATE_SUFFIX.search(exch_ticker)
    return found.group(1) if found else None


def _delivery_expiry(value: Any) -> datetime | None:
    if value in (None, "", 0, "0"):
        return None
    try:
        ms = int(value)
    except (TypeError, ValueError):
        return None
    if ms <= 0:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


def _expiry_from_code(code: str) -> datetime:
    """Last-resort clock when ``deliveryDate`` is missing: 08:00 UTC that day.

    Binance settles dated USD-M at 08:00 UTC. The code is the identity; this
    is only so the ``expiry`` column is not blank on a well-formed ticker.
    """
    return datetime.strptime(code, "%y%m%d").replace(
        tzinfo=UTC, hour=8, minute=0, second=0, microsecond=0
    )


__all__ = [
    "BinanceFutureExchangeSymbol",
    "PERPETUAL",
    "TRADING",
    "VENUE",
    "to_listed",
]
