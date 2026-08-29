"""Paper-local listing — the simulated venue's own catalog row.

Field names stay compatible with the ``paper.fetch_instruments`` payload so
the wire does not need a contract bump. The symbol plane maps this to
:class:`~mftik.symbols.listed.ListedInstrument`; it is not a shared venue
Instrument.
"""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict


class PaperListed(BaseModel):
    """One paper pair and the restrictions the engine matches against."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    base: str
    quote: str
    tick_size: Decimal = Decimal("0.01")
    lot_size: Decimal = Decimal("0.0001")
    min_qty: Decimal | None = None
    min_notional: Decimal | None = None


__all__ = ["PaperListed"]
