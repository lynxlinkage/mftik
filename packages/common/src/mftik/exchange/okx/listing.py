"""OKX ``/public/instruments`` row → :class:`ListedInstrument`."""

from __future__ import annotations

import logging
from decimal import Decimal
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
    WireStr,
    listing_decimal,
    parse_listing_row,
)

logger = logging.getLogger(__name__)

VENUE = venues.OKX.name
LIVE = "live"
LINEAR = "linear"


class OkxInstrumentRow(BaseModel):
    """One ``data[]`` row of ``GET /api/v5/public/instruments``."""

    model_config = ConfigDict(extra="ignore")

    inst_id: WireStr = Field(default="", alias="instId")
    base_ccy: WireStr = Field(default="", alias="baseCcy")
    quote_ccy: WireStr = Field(default="", alias="quoteCcy")
    ct_val_ccy: WireStr = Field(default="", alias="ctValCcy")
    settle_ccy: WireStr = Field(default="", alias="settleCcy")
    ct_type: WireStr = Field(default="", alias="ctType")
    ct_val: WireStr = Field(default="", alias="ctVal")
    ct_mult: WireStr = Field(default="", alias="ctMult")
    state: WireStr = ""
    exp_time: WireStr = Field(default="", alias="expTime")
    tick_sz: WireStr = Field(default="", alias="tickSz")
    lot_sz: WireStr = Field(default="", alias="lotSz")
    min_sz: WireStr = Field(default="", alias="minSz")
    max_lmt_sz: WireStr = Field(default="", alias="maxLmtSz")


def to_listed(
    row: dict[str, Any] | OkxInstrumentRow,
    *,
    venue: str = VENUE,
    category: Category = Category.SPOT,
) -> ListedInstrument | None:
    parsed = parse_listing_row(OkxInstrumentRow, row)
    if parsed is None:
        logger.warning("%s skipping malformed instrument: %r", venue, row)
        return None
    if category is Category.PERP:
        if parsed.ct_type != LINEAR:
            return None
        if parsed.exp_time not in ("", "0"):
            return None

    base = (parsed.base_ccy or parsed.ct_val_ccy).upper()
    quote = (parsed.quote_ccy or parsed.settle_ccy).upper()
    exch_ticker = parsed.inst_id
    if not base or not quote or not exch_ticker:
        logger.warning("%s skipping malformed instrument: %r", venue, row)
        return None

    contract_size = _contract_size(parsed, category)
    if category is Category.PERP and contract_size is None:
        return None

    scale = contract_size if contract_size is not None else Decimal("1")
    lot = listing_decimal(parsed.lot_sz)
    minimum = listing_decimal(parsed.min_sz)
    maximum = listing_decimal(parsed.max_lmt_sz)
    settle = parsed.settle_ccy.upper()

    return ListedInstrument(
        venue=venue,
        base=base,
        quote=quote,
        exch_ticker=exch_ticker,
        category=category,
        contract_size=contract_size,
        settlement_asset=settle or None,
        is_active=parsed.state == LIVE,
        filters={
            PRICE_TICK: listing_decimal(parsed.tick_sz),
            QTY_STEP: None if lot is None else lot * scale,
            MIN_QTY: None if minimum is None else minimum * scale,
            MAX_QTY: None if maximum is None else maximum * scale,
            MIN_NOTIONAL: None,
            MAX_NOTIONAL: None,
            MIN_PRICE: None,
            MAX_PRICE: None,
        },
    )


def _contract_size(
    parsed: OkxInstrumentRow, category: Category
) -> Decimal | None:
    if category is not Category.PERP:
        return None
    value = listing_decimal(parsed.ct_val)
    if value is None:
        return None
    mult = listing_decimal(parsed.ct_mult) or Decimal("1")
    return value * mult


__all__ = ["LINEAR", "LIVE", "OkxInstrumentRow", "VENUE", "to_listed"]
