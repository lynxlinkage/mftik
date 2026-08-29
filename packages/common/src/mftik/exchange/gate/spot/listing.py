"""Gate spot ``currency_pairs`` row → :class:`ListedInstrument`."""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from mftik.exchange import venues
from mftik.exchange.tickers import Category
from mftik.symbols.listed import (
    MAX_NOTIONAL,
    MAX_QTY,
    MIN_NOTIONAL,
    MIN_QTY,
    PRICE_TICK,
    QTY_STEP,
    ListedInstrument,
    WireStr,
    listing_decimal,
    parse_listing_row,
    tick_from_precision,
)

logger = logging.getLogger(__name__)

VENUE = venues.GATE.name
TRADABLE = frozenset({"tradable", "buyable", "sellable"})


class GateSpotCurrencyPair(BaseModel):
    """One row of ``GET /api/v4/spot/currency_pairs``."""

    model_config = ConfigDict(extra="ignore")

    id: WireStr = ""
    base: WireStr = ""
    quote: WireStr = ""
    trade_status: WireStr = ""
    precision: Any = None
    amount_precision: Any = Field(default=None, alias="amount_precision")
    min_base_amount: Any = None
    max_base_amount: Any = None
    min_quote_amount: Any = None
    max_quote_amount: Any = None


def to_listed(
    row: dict[str, Any] | GateSpotCurrencyPair,
    *,
    venue: str = VENUE,
    category: Category = Category.SPOT,
) -> ListedInstrument | None:
    parsed = parse_listing_row(GateSpotCurrencyPair, row)
    if parsed is None:
        logger.warning("%s skipping malformed pair: %r", venue, row)
        return None
    base = parsed.base.upper()
    quote = parsed.quote.upper()
    exch_ticker = parsed.id
    if not base or not quote or not exch_ticker:
        logger.warning("%s skipping malformed pair: %r", venue, row)
        return None

    return ListedInstrument(
        venue=venue,
        base=base,
        quote=quote,
        exch_ticker=exch_ticker,
        category=category,
        is_active=parsed.trade_status in TRADABLE,
        filters={
            PRICE_TICK: tick_from_precision(parsed.precision),
            QTY_STEP: tick_from_precision(parsed.amount_precision),
            MIN_QTY: listing_decimal(parsed.min_base_amount),
            MAX_QTY: listing_decimal(parsed.max_base_amount),
            MIN_NOTIONAL: listing_decimal(parsed.min_quote_amount),
            MAX_NOTIONAL: listing_decimal(parsed.max_quote_amount),
        },
    )


__all__ = ["GateSpotCurrencyPair", "TRADABLE", "VENUE", "to_listed"]
