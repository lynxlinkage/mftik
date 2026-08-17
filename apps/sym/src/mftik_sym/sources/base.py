"""Instrument sources — one per venue, fetched and normalized here.

A source's only job is to turn a venue's instrument-info endpoint into
:class:`Instrument` records with canonical symbols. Everything downstream —
persistence, RPC, refresh scheduling — is venue-agnostic.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Protocol

from mftik.exchange.tickers import Category, UniversalTicker


@dataclass(frozen=True)
class Instrument:
    """One instrument as a venue describes it, already canonicalized.

    ``symbol`` is derived from ``base`` + ``quote`` rather than by splitting
    the venue's ticker — the venue tells us both, so no guessing is involved.
    That is the whole point of this plane.
    """

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


class InstrumentSource(Protocol):
    """Fetches every instrument a venue currently lists.

    One source is one ``(venue, category)`` — the unit a venue's instrument
    endpoint actually serves, and the unit a refresh can safely delist within.
    A unified-account venue therefore has one source per market, not one for
    the venue.
    """

    venue: str
    category: Category

    async def fetch(self) -> list[Instrument]:
        """Return the venue's current listing. Raises on transport failure."""

    async def close(self) -> None:
        """Release any connection held by the source."""


def tick_from_precision(precision: int | None) -> Decimal | None:
    """Turn a decimal-places count into a step size.

    Venues express granularity either way; the plane stores steps, since that
    is what an order has to be a multiple of.
    """
    if precision is None:
        return None
    if precision < 0:
        return None
    return Decimal(1).scaleb(-int(precision))
