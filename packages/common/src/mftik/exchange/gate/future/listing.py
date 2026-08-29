"""Gate USDT-perp ``contracts`` row → :class:`ListedInstrument`."""

from __future__ import annotations

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
    listing_decimal,
)

VENUE = venues.GATE_FUTURES.name


class GateFuturesContract(BaseModel):
    """One row of ``GET /api/v4/futures/{settle}/contracts``."""

    model_config = ConfigDict(extra="ignore")

    name: str = ""
    in_delisting: bool = False
    expire_time: Any = None
    quanto_multiplier: str | None = None
    order_size_min: str | None = None
    order_size_max: str | None = None
    order_price_round: str | None = None
    settle: str = ""
    settle_currency: str = ""
    settlement_currency: str = ""


def to_listed(
    row: dict[str, Any] | GateFuturesContract,
    *,
    venue: str = VENUE,
    category: Category = Category.PERP,
) -> ListedInstrument | None:
    parsed = (
        row
        if isinstance(row, GateFuturesContract)
        else GateFuturesContract.model_validate(row)
    )
    if parsed.in_delisting:
        return None
    if parsed.expire_time not in (None, "", 0, "0"):
        return None
    exch_ticker = parsed.name
    if not exch_ticker or "_" not in exch_ticker:
        return None
    base, _, quote = exch_ticker.rpartition("_")
    base = base.upper()
    quote = quote.upper()
    if not base or not quote:
        return None

    multiplier = listing_decimal(parsed.quanto_multiplier)
    if multiplier is None:
        return None

    min_size = listing_decimal(parsed.order_size_min)
    max_size = listing_decimal(parsed.order_size_max)
    settle = (
        parsed.settle or parsed.settle_currency or parsed.settlement_currency or SETTLE
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
