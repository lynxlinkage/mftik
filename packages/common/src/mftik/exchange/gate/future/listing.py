"""Gate USDT-perp ``contracts`` row → :class:`ListedInstrument`."""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel, ConfigDict

from mftik.exchange import venues
from mftik.exchange.gate.future.protocol import SETTLE
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
    WireStr,
    listing_decimal,
    parse_listing_row,
)

logger = logging.getLogger(__name__)

VENUE = venues.GATE_FUTURES.name


class GateFuturesContract(BaseModel):
    """One row of ``GET /api/v4/futures/{settle}/contracts``.

    Size and price fields are untyped: Gate publishes them as JSON numbers
    (int64 order sizes, decimal strings on some older rows).
    :func:`~mftik.symbols.listed.listing_decimal` accepts either.
    """

    model_config = ConfigDict(extra="ignore")

    name: WireStr = ""
    in_delisting: Any = False
    expire_time: Any = None
    quanto_multiplier: Any = None
    order_size_min: Any = None
    order_size_max: Any = None
    order_price_round: Any = None
    settle: WireStr = ""
    settle_currency: WireStr = ""
    settlement_currency: WireStr = ""


def to_listed(
    row: dict[str, Any] | GateFuturesContract,
    *,
    venue: str = VENUE,
    category: Category = Category.PERP,
) -> ListedInstrument | None:
    parsed = parse_listing_row(GateFuturesContract, row)
    if parsed is None:
        logger.warning("%s skipping malformed contract: %r", venue, row)
        return None
    if parsed.in_delisting:
        return None
    if parsed.expire_time not in (None, "", 0, "0"):
        return None
    exch_ticker = parsed.name
    if not exch_ticker or "_" not in exch_ticker:
        logger.warning("%s skipping malformed contract: %r", venue, row)
        return None
    base, _, quote = exch_ticker.rpartition("_")
    base = base.upper()
    quote = quote.upper()
    if not base or not quote:
        logger.warning("%s skipping malformed contract: %r", venue, row)
        return None

    multiplier = listing_decimal(parsed.quanto_multiplier)
    if multiplier is None:
        return None

    min_size = listing_decimal(parsed.order_size_min)
    max_size = listing_decimal(parsed.order_size_max)
    settle = (
        parsed.settle
        or parsed.settle_currency
        or parsed.settlement_currency
        or SETTLE
    ).upper()

    return ListedInstrument(
        venue=venue,
        base=base,
        quote=quote,
        exch_ticker=exch_ticker,
        category=category,
        contract_size=multiplier,
        settlement_asset=settle or None,
        is_active=True,
        filters={
            PRICE_TICK: listing_decimal(parsed.order_price_round),
            QTY_STEP: multiplier,
            MIN_QTY: None if min_size is None else min_size * multiplier,
            MAX_QTY: None if max_size is None else max_size * multiplier,
            MIN_NOTIONAL: None,
            MAX_NOTIONAL: None,
            MIN_PRICE: None,
            MAX_PRICE: None,
        },
    )


__all__ = ["GateFuturesContract", "VENUE", "to_listed"]
