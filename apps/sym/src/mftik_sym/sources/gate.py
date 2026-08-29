"""Gate spot instrument source — ``GET /api/v4/spot/currency_pairs``.

Public endpoint, no signing. Gate reports granularity as decimal-place counts;
the adapter converts those to step sizes.
"""

from __future__ import annotations

import logging

import httpx
from mftik.exchange.gate.spot.listing import TRADABLE, VENUE, to_listed
from mftik.exchange.gate.spot.rest import API_PREFIX, GATE_SPOT_REST_URL
from mftik.exchange.tickers import Category

from mftik_sym.sources.base import Instrument

logger = logging.getLogger(__name__)


class GateSpotInstrumentSource:
    """Every spot pair Gate lists, as canonical instruments."""

    venue = VENUE
    category = Category.SPOT

    def __init__(
        self,
        *,
        base_url: str = GATE_SPOT_REST_URL,
        timeout: float = 20.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._client = client
        self._owns_client = client is None

    async def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url, timeout=self.timeout
            )
            self._owns_client = True
        return self._client

    async def close(self) -> None:
        if self._client is not None and self._owns_client:
            await self._client.aclose()
            self._client = None

    async def fetch(self) -> list[Instrument]:
        client = await self._http()
        response = await client.get(f"{API_PREFIX}/spot/currency_pairs")
        response.raise_for_status()
        rows = response.json()
        out: list[Instrument] = []
        for row in rows or []:
            instrument = to_listed(row, venue=self.venue, category=self.category)
            if instrument is not None:
                out.append(instrument)
        logger.info("%s instruments fetched=%s", VENUE, len(out))
        return out


__all__ = ["TRADABLE", "VENUE", "GateSpotInstrumentSource"]
