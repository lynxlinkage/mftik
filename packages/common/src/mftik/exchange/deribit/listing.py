"""Deribit ``public/get_instruments`` row → :class:`ListedInstrument`.

**V2 / V3 (live public pull, 2026-09-06):**

* Spot ``instrument_name`` is ``BTC_USDC``. Platform symbol is
  ``base+quote`` (``BTCUSDC``); the underscore stays on ``exch_ticker``.
* Linear perpetual names are ``BTC_USDC-PERPETUAL``. Same platform
  symbol as the spot pair; category separates them.
* Inverse ``BTC-PERPETUAL`` is ``instrument_type=reversed``. Dated
  futures have ``settlement_period != perpetual``. Both are dropped.
* CBE-routed spots set ``is_cbe_routed`` / ``is_csr``; native spots omit
  both. Presence, not ``== false`` (V12).
"""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel, ConfigDict

from mftik.exchange.deribit.protocol import (
    KIND_FUTURE,
    KIND_SPOT,
    is_cbe_routed,
    is_linear_perp,
)
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

VENUE = "Deribit"


class DeribitInstrumentRow(BaseModel):
    """One ``result[]`` row of ``public/get_instruments``."""

    model_config = ConfigDict(extra="ignore")

    instrument_name: WireStr = ""
    kind: WireStr = ""
    instrument_type: WireStr = ""
    future_type: WireStr = ""
    settlement_period: WireStr = ""
    base_currency: WireStr = ""
    quote_currency: WireStr = ""
    settlement_currency: WireStr = ""
    tick_size: WireStr = ""
    min_trade_amount: WireStr = ""
    contract_size: WireStr = ""
    is_active: bool = False
    state: WireStr = ""
    is_cbe_routed: bool | None = None
    is_csr: bool | None = None


def to_listed(
    row: dict[str, Any] | DeribitInstrumentRow,
    *,
    venue: str = VENUE,
    category: Category = Category.SPOT,
) -> ListedInstrument | None:
    parsed = parse_listing_row(DeribitInstrumentRow, row)
    if parsed is None:
        logger.warning("%s skipping malformed instrument: %r", venue, row)
        return None

    kind = parsed.kind.strip().casefold()
    if category is Category.SPOT:
        if kind != KIND_SPOT:
            return None
    elif category is Category.PERP:
        if kind != KIND_FUTURE or not is_linear_perp(
            instrument_type=parsed.instrument_type,
            future_type=parsed.future_type,
            settlement_period=parsed.settlement_period,
            kind=parsed.kind,
        ):
            return None
    else:
        return None

    base = parsed.base_currency.upper()
    quote = parsed.quote_currency.upper()
    exch_ticker = parsed.instrument_name
    if not base or not quote or not exch_ticker:
        logger.warning("%s skipping malformed instrument: %r", venue, row)
        return None

    raw = row if isinstance(row, dict) else parsed.model_dump()
    _ = is_cbe_routed(raw)  # presence is the fact; listing still includes it

    settle = (parsed.settlement_currency or quote).upper() or None
    return ListedInstrument(
        venue=venue,
        base=base,
        quote=quote,
        exch_ticker=exch_ticker,
        category=category,
        settlement_asset=settle if category is Category.PERP else None,
        contract_size=listing_decimal(parsed.contract_size),
        is_active=bool(parsed.is_active) or parsed.state.strip().casefold() == "open",
        filters={
            PRICE_TICK: listing_decimal(parsed.tick_size),
            QTY_STEP: listing_decimal(parsed.contract_size)
            or listing_decimal(parsed.min_trade_amount),
            MIN_QTY: listing_decimal(parsed.min_trade_amount),
            MAX_QTY: None,
            MIN_NOTIONAL: None,
            MAX_NOTIONAL: None,
            MIN_PRICE: None,
            MAX_PRICE: None,
        },
    )


__all__ = [
    "DeribitInstrumentRow",
    "VENUE",
    "is_cbe_routed",
    "to_listed",
]
