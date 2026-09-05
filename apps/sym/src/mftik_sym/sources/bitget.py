"""Bitget instrument source — ``GET /api/v3/market/instruments``.

Public endpoint, no signing.

**One source per book, not per venue.** Bitget is a unified account: one
credential (plus a passphrase) trades spot and both linear perpetual
books, but the listing endpoint still answers one wire ``category`` at a
time, and that is also the unit
:meth:`~mftik_sym.plane.SymbolPlane.refresh` can safely delist within.
So ``Bitget`` contributes exactly two sources, ``Spot`` and ``Perp``.

**The Perp source is the union.** Bitget's wire splits USDT-M and USDC-M
(``USDT-FUTURES`` vs ``USDC-FUTURES``). Identity is one ``Perp``.
``deactivate_missing`` is ``(venue, category)``, so two Perp sources
would deactivate each other. ``product_of`` is an adapter function, not
the delist key — a refresh of this source hits both listing endpoints
and unions the perpetual rows.

**V2 / V3:** USDC-FUTURES symbols are ``BTCPERP`` (quote ``USDC``), not
a second ``BTCUSDT``. Delivery rows (``type != perpetual``) are dropped.
"""

from __future__ import annotations

import logging

import httpx
from mftik.exchange.bitget import channels as ch
from mftik.exchange.bitget.listing import VENUE, to_listed
from mftik.exchange.bitget.protocol import (
    BITGET_REST_URL,
    RET_OK,
    SPOT,
    USDC_FUTURES,
    USDT_FUTURES,
)
from mftik.exchange.tickers import Category

from mftik_sym.sources.base import Instrument

logger = logging.getLogger(__name__)

_PERP_PRODUCTS = (USDT_FUTURES, USDC_FUTURES)


class BitgetInstrumentSource:
    """Every instrument Bitget lists on one of its books.

    ``category`` picks the book in the platform's vocabulary and is what
    the stored tickers carry. Spot fetches ``category=SPOT``. Perp
    fetches ``USDT-FUTURES`` and ``USDC-FUTURES`` and unions them.
    """

    venue = VENUE

    def __init__(
        self,
        *,
        category: Category = Category.SPOT,
        base_url: str = BITGET_REST_URL,
        timeout: float = 30.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.category = category
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._client = client
        self._owns_client = client is None

    @property
    def products(self) -> tuple[str, ...]:
        if self.category is Category.SPOT:
            return (SPOT,)
        if self.category is Category.PERP:
            return _PERP_PRODUCTS
        raise ValueError(f"Bitget source has no listing for {self.category}")

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
        out: list[Instrument] = []
        seen: set[str] = set()
        for product in self.products:
            response = await client.get(
                ch.MARKET_INSTRUMENTS, params={"category": product}
            )
            response.raise_for_status()
            payload = response.json() or {}
            code = str(payload.get("code") or "")
            if code != RET_OK:
                raise RuntimeError(
                    f"Bitget instruments {code}: {payload.get('msg') or 'refused'}"
                )
            for row in payload.get("data") or []:
                instrument = to_listed(
                    row, venue=self.venue, category=self.category
                )
                if instrument is None:
                    continue
                ticker = str(instrument.ticker)
                if ticker in seen:
                    continue
                seen.add(ticker)
                out.append(instrument)
        logger.info(
            "%s instruments category=%s fetched=%s products=%s",
            VENUE,
            self.category,
            len(out),
            ",".join(self.products),
        )
        return out


__all__ = [
    "VENUE",
    "BitgetInstrumentSource",
]
