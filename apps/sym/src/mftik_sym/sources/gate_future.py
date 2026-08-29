"""Gate USDT-perpetual instrument source — ``GET /api/v4/futures/usdt/contracts``.

Public endpoint, no signing. Delivery lives on a different path
(``/delivery/...``); this listing is perpetuals. The adapter drops rows that
still carry an expiry or are delisting so they cannot collide with
``GateFutures_Perp_BTCUSDT``.
"""

from __future__ import annotations

import logging

import httpx
from mftik.exchange.gate.future.listing import VENUE, to_listed
from mftik.exchange.gate.future.protocol import (
    API_PREFIX,
    GATE_FUTURES_REST_URL,
    SETTLE,
)
from mftik.exchange.tickers import Category

from mftik_sym.sources.base import Instrument

logger = logging.getLogger(__name__)


class GateFuturesInstrumentSource:
    """Every USDT perpetual Gate lists, as canonical instruments."""

    venue = VENUE
    category = Category.PERP

    def __init__(
        self,
        *,
        base_url: str = GATE_FUTURES_REST_URL,
        timeout: float = 30.0,
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
        response = await client.get(f"{API_PREFIX}/futures/{SETTLE}/contracts")
        response.raise_for_status()
        out: list[Instrument] = []
        for row in response.json() or []:
            instrument = to_listed(row, venue=self.venue, category=self.category)
            if instrument is not None:
                out.append(instrument)
        logger.info("%s instruments fetched=%s", VENUE, len(out))
        return out


__all__ = ["VENUE", "GateFuturesInstrumentSource"]
