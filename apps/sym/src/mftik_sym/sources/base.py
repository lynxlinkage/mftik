"""Instrument sources — one per venue, fetched and normalized here.

A source's only job is to turn a venue's instrument-info endpoint into
:class:`~mftik.symbols.listed.ListedInstrument` records. The parse lives in
each adapter; sources fetch and call it. Everything downstream —
persistence, RPC, refresh scheduling — is venue-agnostic.
"""

from __future__ import annotations

from typing import Protocol

from mftik.exchange.tickers import Category
from mftik.symbols.listed import ListedInstrument, tick_from_precision

#: Plane-facing name for a listing row. The class lives in common so adapters
#: can produce it without importing this app.
Instrument = ListedInstrument


class InstrumentSource(Protocol):
    """Fetches every instrument a venue currently lists.

    One source is one ``(venue, category)`` — the unit a venue's instrument
    endpoint actually serves, and the unit a refresh can safely delist within.
    A unified-account venue therefore has one source per market, not one for
    the venue.
    """

    venue: str
    category: Category

    async def fetch(self) -> list[ListedInstrument]:
        """Return the venue's current listing. Raises on transport failure."""

    async def close(self) -> None:
        """Release any connection held by the source."""


__all__ = [
    "Instrument",
    "InstrumentSource",
    "ListedInstrument",
    "tick_from_precision",
]
