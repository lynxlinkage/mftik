"""Bitget ``/api/v3/market/instruments`` row → :class:`ListedInstrument`.

**V2 / V3 (live public pull, 2026-09-05):**

* ``USDC-FUTURES`` perpetual symbols are ``BTCPERP``, ``ETHPERP``, … —
  not a second ``BTCUSDT`` and not ``BTCUSDC``. ``quoteCoin`` is
  ``USDC``. Zero overlap with ``USDT-FUTURES`` symbols, so
  ``Bitget_Perp_BTCUSDT`` (exch ``BTCUSDT``) and ``Bitget_Perp_BTCUSDC``
  (exch ``BTCPERP``) can both be named. The platform symbol is
  ``base + quote``; the venue spelling stays on ``exch_ticker``.
* ``type`` is ``perpetual`` on every live USDT-FUTURES (778) and
  USDC-FUTURES (49) row. No delivery rows currently exist on those
  categories. The Perp source still drops ``type != "perpetual"``.
"""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

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
    tick_from_precision,
)

logger = logging.getLogger(__name__)

VENUE = "Bitget"
ONLINE = "online"
PERPETUAL = "perpetual"


class BitgetInstrumentRow(BaseModel):
    """One ``data[]`` row of ``GET /api/v3/market/instruments``."""

    model_config = ConfigDict(extra="ignore")

    symbol: WireStr = ""
    category: WireStr = ""
    base_coin: WireStr = Field(default="", alias="baseCoin")
    quote_coin: WireStr = Field(default="", alias="quoteCoin")
    type: WireStr = ""
    status: WireStr = ""
    min_order_qty: WireStr = Field(default="", alias="minOrderQty")
    max_order_qty: WireStr = Field(default="", alias="maxOrderQty")
    min_order_amount: WireStr = Field(default="", alias="minOrderAmount")
    price_precision: WireStr = Field(default="", alias="pricePrecision")
    quantity_precision: WireStr = Field(default="", alias="quantityPrecision")
    price_multiplier: WireStr = Field(default="", alias="priceMultiplier")
    quantity_multiplier: WireStr = Field(default="", alias="quantityMultiplier")


def to_listed(
    row: dict[str, Any] | BitgetInstrumentRow,
    *,
    venue: str = VENUE,
    category: Category = Category.SPOT,
) -> ListedInstrument | None:
    parsed = parse_listing_row(BitgetInstrumentRow, row)
    if parsed is None:
        logger.warning("%s skipping malformed instrument: %r", venue, row)
        return None
    if category is Category.PERP and parsed.type != PERPETUAL:
        return None

    base = parsed.base_coin.upper()
    quote = parsed.quote_coin.upper()
    exch_ticker = parsed.symbol
    if not base or not quote or not exch_ticker:
        logger.warning("%s skipping malformed instrument: %r", venue, row)
        return None

    price_tick = listing_decimal(parsed.price_multiplier)
    if price_tick is None:
        try:
            price_tick = tick_from_precision(int(parsed.price_precision))
        except (TypeError, ValueError):
            price_tick = None
    qty_step = listing_decimal(parsed.quantity_multiplier)
    if qty_step is None:
        try:
            qty_step = tick_from_precision(int(parsed.quantity_precision))
        except (TypeError, ValueError):
            qty_step = None

    return ListedInstrument(
        venue=venue,
        base=base,
        quote=quote,
        exch_ticker=exch_ticker,
        category=category,
        settlement_asset=quote if category is Category.PERP else None,
        is_active=parsed.status == ONLINE,
        filters={
            PRICE_TICK: price_tick,
            QTY_STEP: qty_step,
            MIN_QTY: listing_decimal(parsed.min_order_qty),
            MAX_QTY: listing_decimal(parsed.max_order_qty),
            MIN_NOTIONAL: listing_decimal(parsed.min_order_amount),
            MAX_NOTIONAL: None,
            MIN_PRICE: None,
            MAX_PRICE: None,
        },
    )


__all__ = [
    "ONLINE",
    "PERPETUAL",
    "BitgetInstrumentRow",
    "VENUE",
    "to_listed",
]
