"""Deribit ``public/get_instruments`` row → :class:`ListedInstrument`.

**V2 / V3 (live public pull, 2026-09-06):**

* Spot ``instrument_name`` is ``BTC_USDC``. Platform symbol is
  ``base+quote`` (``BTCUSDC``); the underscore stays on ``exch_ticker``.
* Linear perpetual names are ``BTC_USDC-PERPETUAL``. Same platform
  symbol as the spot pair; category separates them.
* Inverse perpetual names are ``BTC-PERPETUAL``. Platform symbol is
  ``BTCUSD``; category is ``Inverse``, never a second Perp.
* Dated names keep Deribit's day-month-year suffix on the wire
  (``BTC-6SEP26``, ``BTC_USDC-6SEP26``). Platform ``expiry_code`` is
  ``YYMMDD`` (``260906``), so identity is ``Deribit_Future_BTCUSD-260906``
  / ``Deribit_Future_BTCUSDC-260906``. Linear dated quote USDC; inverse
  dated quote USD and settle the coin.
* CBE-routed spots set ``is_cbe_routed`` / ``is_csr``; native spots omit
  both. Presence, not ``== false`` (V12).
* Options (V13) are one ``Option`` book. Platform quote is
  ``counter_currency`` (inverse ``USD``, linear ``USDC``), not
  ``quote_currency`` (inverse options quote the coin). Identity is
  ``Deribit_Option_BTCUSD-260913-70000-C`` /
  ``Deribit_Option_BTCUSDC-260913-70000-C``. Combos stay unlisted.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict

from mftik.exchange.deribit.protocol import (
    KIND_FUTURE,
    KIND_OPTION,
    KIND_SPOT,
    expiry_code_from_name,
    expiry_code_from_option_name,
    expiry_from_code,
    expiry_from_timestamp,
    is_cbe_routed,
    is_dated_future,
    is_inverse_perp,
    is_linear_perp,
)
from mftik.exchange.symbols import spell_strike
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

_DERIVS = frozenset(
    {Category.PERP, Category.INVERSE, Category.FUTURE, Category.OPTION}
)


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
    counter_currency: WireStr = ""
    settlement_currency: WireStr = ""
    option_type: WireStr = ""
    strike: Any = None
    tick_size: WireStr = ""
    min_trade_amount: WireStr = ""
    contract_size: WireStr = ""
    expiration_timestamp: Any = None
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
    strike = None
    option_type = None
    if category is Category.SPOT:
        if kind != KIND_SPOT:
            return None
        expiry_code = None
        expiry = None
    elif category is Category.PERP:
        if kind != KIND_FUTURE or not is_linear_perp(
            instrument_type=parsed.instrument_type,
            future_type=parsed.future_type,
            settlement_period=parsed.settlement_period,
            kind=parsed.kind,
        ):
            return None
        expiry_code = None
        expiry = None
    elif category is Category.INVERSE:
        if kind != KIND_FUTURE or not is_inverse_perp(
            instrument_type=parsed.instrument_type,
            future_type=parsed.future_type,
            settlement_period=parsed.settlement_period,
            kind=parsed.kind,
        ):
            return None
        expiry_code = None
        expiry = None
    elif category is Category.FUTURE:
        if kind != KIND_FUTURE or not is_dated_future(
            settlement_period=parsed.settlement_period,
            kind=parsed.kind,
        ):
            return None
        expiry_code = expiry_code_from_name(parsed.instrument_name)
        if expiry_code is None:
            return None
        expiry = expiry_from_timestamp(parsed.expiration_timestamp) or expiry_from_code(
            expiry_code
        )
    elif category is Category.OPTION:
        if kind != KIND_OPTION:
            return None
        expiry_code = expiry_code_from_option_name(parsed.instrument_name)
        if expiry_code is None:
            return None
        expiry = expiry_from_timestamp(parsed.expiration_timestamp) or expiry_from_code(
            expiry_code
        )
        strike = _option_strike(parsed.strike)
        option_type = _option_flag(parsed.option_type)
        if strike is None or option_type is None:
            return None
    else:
        return None

    base = parsed.base_currency.upper()
    quote = (
        parsed.counter_currency.upper()
        if category is Category.OPTION
        else parsed.quote_currency.upper()
    )
    exch_ticker = parsed.instrument_name
    if not base or not quote or not exch_ticker:
        logger.warning("%s skipping malformed instrument: %r", venue, row)
        return None
    if category is Category.OPTION and spell_strike(strike) is None:
        return None

    raw = row if isinstance(row, dict) else parsed.model_dump()
    _ = is_cbe_routed(raw)  # presence is the fact; listing still includes it

    settle = (parsed.settlement_currency or quote).upper() or None
    qty_step = listing_decimal(parsed.min_trade_amount)
    if category is not Category.OPTION:
        qty_step = listing_decimal(parsed.contract_size) or qty_step
    return ListedInstrument(
        venue=venue,
        base=base,
        quote=quote,
        exch_ticker=exch_ticker,
        category=category,
        settlement_asset=settle if category in _DERIVS else None,
        contract_size=listing_decimal(parsed.contract_size),
        expiry=expiry,
        expiry_code=expiry_code,
        strike=strike if category is Category.OPTION else None,
        option_type=option_type if category is Category.OPTION else None,
        is_active=bool(parsed.is_active) or parsed.state.strip().casefold() == "open",
        filters={
            PRICE_TICK: listing_decimal(parsed.tick_size),
            QTY_STEP: qty_step,
            MIN_QTY: listing_decimal(parsed.min_trade_amount),
            MAX_QTY: None,
            MIN_NOTIONAL: None,
            MAX_NOTIONAL: None,
            MIN_PRICE: None,
            MAX_PRICE: None,
        },
    )


def _option_flag(value: str) -> str | None:
    folded = (value or "").strip().casefold()
    if folded in {"c", "call"}:
        return "C"
    if folded in {"p", "put"}:
        return "P"
    return None


def _option_strike(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        parsed = Decimal(str(value))
    except Exception:
        return None
    if parsed <= 0:
        return None
    return parsed


__all__ = [
    "DeribitInstrumentRow",
    "VENUE",
    "is_cbe_routed",
    "to_listed",
]
