"""One instrument as a venue listing, already canonicalized for the plane.

Adapters parse a venue's own exchangeInfo shape and map it here. The symbol
plane persists these rows and is the only thing that serves
:class:`~mftik.protocol.messages.SymbolInfo` to TD / MD / STS.

``symbol`` is derived from ``base`` + ``quote`` rather than by splitting the
venue's ticker — the venue tells us both, so no guessing is involved.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

from mftik.exchange.tickers import Category, UniversalTicker

#: Filter keys the plane stores. Same strings as ``mftik_db`` ``FilterName``.
PRICE_TICK = "price_tick"
QTY_STEP = "qty_step"
MIN_QTY = "min_qty"
MAX_QTY = "max_qty"
MIN_NOTIONAL = "min_notional"
MAX_NOTIONAL = "max_notional"
MIN_PRICE = "min_price"
MAX_PRICE = "max_price"


@dataclass(frozen=True)
class ListedInstrument:
    venue: str
    base: str
    quote: str
    exch_ticker: str
    category: Category = Category.SPOT
    contract_size: Decimal | None = None
    settlement_asset: str | None = None
    expiry: datetime | None = None
    is_active: bool = True
    #: name → bound. A key with a ``None`` value means the venue publishes the
    #: restriction but sets no limit, which is not the same as omitting it.
    filters: dict[str, Decimal | None] = field(default_factory=dict)

    @property
    def symbol(self) -> str:
        """Canonical symbol — exact, because base and quote came from the venue."""
        return f"{self.base.upper()}{self.quote.upper()}"

    @property
    def ticker(self) -> UniversalTicker:
        """The row's identity in the golden tables."""
        return UniversalTicker.of(self.venue, self.category, self.symbol)


def listing_decimal(value: Any) -> Decimal | None:
    """A published bound, or ``None`` where the venue enforces none.

    ``0`` and the empty string mean the filter is present but unbounded.
    Trailing zeros are stripped: on a ``Decimal`` they are the scale, and a
    size floored against ``0.00010000`` comes out written to eight decimals
    where the venue allows four.
    """
    if value is None or value == "":
        return None
    try:
        parsed = Decimal(str(value))
    except Exception:
        return None
    if parsed <= 0:
        return None
    stripped = parsed.normalize()
    if stripped.as_tuple().exponent > 0:
        return stripped.quantize(Decimal(1))
    return stripped


def tick_from_precision(precision: int | None) -> Decimal | None:
    """Turn a decimal-places count into a step size."""
    if precision is None:
        return None
    try:
        places = int(precision)
    except (TypeError, ValueError):
        return None
    if places < 0:
        return None
    return Decimal(1).scaleb(-places)


__all__ = [
    "ListedInstrument",
    "MAX_NOTIONAL",
    "MAX_PRICE",
    "MAX_QTY",
    "MIN_NOTIONAL",
    "MIN_PRICE",
    "MIN_QTY",
    "PRICE_TICK",
    "QTY_STEP",
    "listing_decimal",
    "tick_from_precision",
]
