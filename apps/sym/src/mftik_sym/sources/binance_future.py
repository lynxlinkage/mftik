"""Binance USDⓈ-M futures instrument source — ``GET /fapi/v1/exchangeInfo``.

Public endpoint, no signing. HTTP for the same reason the spot source uses it:
the plane refreshes a whole venue's listing on a timer and then goes quiet, and
:class:`InstrumentSource` is a batch fetcher rather than a connector. Here it is
also the only option — futures serves no ``exchangeInfo`` over its WebSocket
API at all.

**Only perpetuals.** The endpoint lists dated futures beside them
(``BTCUSDT_250926``, ``ETHUSDT_251226``) whose ``baseAsset`` and ``quoteAsset``
are the perpetual's, so both canonicalize to ``BTCUSDT`` and the upsert would
keep whichever was written last. The adapter's ``to_listed`` keeps
``contractType == "PERPETUAL"``; a ``Future`` source that does not exist yet
would take the rest.
"""

from __future__ import annotations

import logging

import httpx
from mftik.exchange.binance.future.listing import VENUE, to_listed
from mftik.exchange.binance.future.protocol import BINANCE_FUTURE_REST_URL
from mftik.exchange.binance.future.rest import API_PREFIX
from mftik.exchange.tickers import Category

from mftik_sym.sources.base import Instrument

logger = logging.getLogger(__name__)


class BinanceFutureInstrumentSource:
    """Every perpetual Binance's USDⓈ-M plane lists, as canonical instruments."""

    venue = VENUE
    category = Category.PERP

    def __init__(
        self,
        *,
        base_url: str = BINANCE_FUTURE_REST_URL,
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


__all__ = ["VENUE", "BinanceFutureInstrumentSource"]
