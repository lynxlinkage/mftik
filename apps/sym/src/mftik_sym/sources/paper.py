"""Paper instrument source — asks the paper adapter, same as any venue.

The simulated venue has no HTTP endpoint, but it does have an instrument list,
reachable over the broker exactly like a real venue's is over its API. Pulling
it rather than restating it here means paper cannot drift out of step with the
engine, and it keeps paper on the same code path as everything else.
"""

from __future__ import annotations

import logging

from mftik.broker import Broker
from mftik.exchange import venues
from mftik.exchange.paper.models import PaperListed
from mftik.exchange.paper.remote_public import PaperRemotePublicClient
from mftik.exchange.tickers import Category
from mftik.symbols.listed import MIN_NOTIONAL, MIN_QTY, PRICE_TICK, QTY_STEP

from mftik_sym.sources.base import Instrument

logger = logging.getLogger(__name__)

VENUE = venues.PAPER.name


class PaperInstrumentSource:
    """Instruments the paper engine currently lists."""

    venue = VENUE
    category = Category.SPOT

    def __init__(
        self, broker: Broker, *, public: PaperRemotePublicClient | None = None
    ) -> None:
        self._broker = broker
        self._public = public
        self._owns_public = public is None

    async def fetch(self) -> list[Instrument]:
        public = self._public
        if public is None:
            public = PaperRemotePublicClient(self._broker)
            await public.connect()
            self._public = public
            self._owns_public = True

        rows = await public.fetch_instruments()
        out = [self._to_listed(row) for row in rows]
        logger.info("%s instruments fetched=%s", VENUE, len(out))
        return out

    def _to_listed(self, row: PaperListed) -> Instrument:
        return Instrument(
            venue=self.venue,
            base=row.base,
            quote=row.quote,
            # Paper already spells pairs the canonical way.
            exch_ticker=row.symbol,
            category=self.category,
            filters={
                PRICE_TICK: row.tick_size,
                QTY_STEP: row.lot_size,
                MIN_QTY: row.min_qty,
                MIN_NOTIONAL: row.min_notional,
            },
        )

    async def close(self) -> None:
        if self._public is not None and self._owns_public:
            await self._public.close()
            self._public = None


__all__ = ["VENUE", "PaperInstrumentSource"]
