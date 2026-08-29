"""Binance spot instrument source — ``GET /api/v3/exchangeInfo``.

Public endpoint, no signing.

HTTP rather than the WebSocket API the rest of the Binance adapter uses. The
plane refreshes a whole venue's listing on a timer and then goes quiet, so a
socket here would be opened, used once and left idle between refreshes — and
:class:`InstrumentSource` is deliberately a batch fetcher, not a connector.
The connector's own ``fetch_instruments`` covers the same endpoint for callers
that already hold a socket.
"""

from __future__ import annotations

import logging

import httpx
from mftik.exchange.binance.spot.listing import TRADING, VENUE, to_listed
from mftik.exchange.binance.spot.protocol import BINANCE_SPOT_REST_URL
from mftik.exchange.tickers import Category

from mftik_sym.sources.base import Instrument

logger = logging.getLogger(__name__)

API_PREFIX = "/api/v3"


class BinanceSpotInstrumentSource:
    """Every spot symbol Binance lists, as canonical instruments."""

    venue = VENUE
    category = Category.SPOT

    def __init__(
        self,
        *,
        base_url: str = BINANCE_SPOT_REST_URL,
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
        response = await client.get(f"{API_PREFIX}/exchangeInfo")
        response.raise_for_status()
        payload = response.json()
        out: list[Instrument] = []
        for row in payload.get("symbols") or []:
            instrument = to_listed(row, venue=self.venue, category=self.category)
            if instrument is not None:
                out.append(instrument)
        logger.info("%s instruments fetched=%s", VENUE, len(out))
        return out


__all__ = [
    "API_PREFIX",
    "BINANCE_SPOT_REST_URL",
    "TRADING",
    "VENUE",
    "BinanceSpotInstrumentSource",
]
