"""Binance COIN-M instrument source — ``GET /dapi/v1/exchangeInfo``.

Public endpoint, no signing. HTTP for the same reason the USD-M source uses
it: the plane refreshes a whole venue's listing on a timer, and dapi serves
no ``exchangeInfo`` over its WebSocket API.

**Two sources, one endpoint.** The listing mixes inverse perpetuals with
dated futures (``BTCUSD_260925``) that share the perpetual's ``baseAsset``
and ``quoteAsset``. Each source stamps its own category so a refresh can
delist one book without touching the other, and ``to_listed`` glues
``YYMMDD`` onto the dated symbol so the two cannot collide as
``BinanceDelivery_Inverse_BTCUSD``. Inverse stays the coin-margined perp;
dated contracts are ``Future``, not a second Inverse.
"""

from __future__ import annotations

import logging

import httpx
from mftik.exchange.binance.delivery.listing import VENUE, to_listed
from mftik.exchange.binance.delivery.protocol import BINANCE_DELIVERY_REST_URL
from mftik.exchange.binance.delivery.rest import API_PREFIX
from mftik.exchange.tickers import Category

from mftik_sym.sources.base import Instrument

logger = logging.getLogger(__name__)

_BOOKS = frozenset({Category.INVERSE, Category.FUTURE})


class BinanceDeliveryInstrumentSource:
    """Every instrument Binance's COIN-M plane lists on one of its books.

    ``category`` is ``Inverse`` or ``Future``; the endpoint is the same, the
    filter and the stored ticker are not.
    """

    venue = VENUE

    def __init__(
        self,
        *,
        category: Category = Category.INVERSE,
        base_url: str = BINANCE_DELIVERY_REST_URL,
        timeout: float = 30.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if category not in _BOOKS:
            raise ValueError(
                f"{VENUE} source trades Inverse and Future; got {category.value}"
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


__all__ = ["VENUE", "BinanceDeliveryInstrumentSource"]
