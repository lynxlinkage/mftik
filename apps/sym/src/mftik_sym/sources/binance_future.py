"""Binance USDⓈ-M futures instrument source — ``GET /fapi/v1/exchangeInfo``.

Public endpoint, no signing. HTTP for the same reason the spot source uses it:
the plane refreshes a whole venue's listing on a timer and then goes quiet, and
:class:`InstrumentSource` is a batch fetcher rather than a connector. Here it
is also the only option — futures serves no ``exchangeInfo`` over its
WebSocket API at all.

**Two sources, one endpoint.** The listing mixes perpetuals with dated
futures (``BTCUSDT_250926``) that share the perpetual's ``baseAsset`` and
``quoteAsset``. Each source stamps its own category so a refresh can delist
one book without touching the other, and ``to_listed`` glues ``YYMMDD`` onto
the dated symbol so the two cannot collide as ``BinanceFuture_Perp_BTCUSDT``.
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

_BOOKS = frozenset({Category.PERP, Category.FUTURE})


class BinanceFutureInstrumentSource:
    """Every instrument Binance's USDⓈ-M plane lists on one of its books.

    ``category`` is ``Perp`` or ``Future``; the endpoint is the same, the
    filter and the stored ticker are not.
    """

    venue = VENUE

    def __init__(
        self,
        *,
        category: Category = Category.PERP,
        base_url: str = BINANCE_FUTURE_REST_URL,
        timeout: float = 30.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if category not in _BOOKS:
            raise ValueError(
                f"{VENUE} source trades Perp and Future; got {category.value}"
            )
        self.category = category
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
        logger.info(
            "%s instruments category=%s fetched=%s",
            VENUE,
            self.category.value,
            len(out),
        )
        return out


__all__ = ["VENUE", "BinanceFutureInstrumentSource"]
