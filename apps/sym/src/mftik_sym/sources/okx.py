"""OKX instrument source — ``GET /api/v5/public/instruments``.

Public endpoint, no signing.

**One source per book, not per venue.** OKX is a unified account: one
credential (plus a passphrase) trades spot and USDT-margined swaps, but the
listing endpoint still answers one ``instType`` at a time, and that is also
the unit :meth:`~mftik_sym.plane.SymbolPlane.refresh` can safely delist
within. So ``Okx`` contributes two sources, ``Spot`` and ``Perp``.

The adapter's ``to_listed`` keeps only live linear swaps on the perp book and
scales contract sizes to base. The endpoint is not paginated: one
``instType`` is one response.
"""

from __future__ import annotations

import logging

import httpx
from mftik.exchange.okx import channels as ch
from mftik.exchange.okx.listing import LINEAR, LIVE, VENUE, to_listed
from mftik.exchange.okx.protocol import OKX_REST_URL, product_of
from mftik.exchange.tickers import Category

from mftik_sym.sources.base import Instrument

logger = logging.getLogger(__name__)


class OkxInstrumentSource:
    """Every instrument OKX lists on one of its books.

    ``category`` picks the book in the platform's vocabulary and is what the
    stored tickers carry; the OKX ``instType`` it maps to comes from
    :func:`~mftik.exchange.okx.protocol.product_of`.
    """

    venue = VENUE

    def __init__(
        self,
        *,
        category: Category = Category.SPOT,
        base_url: str = OKX_REST_URL,
        timeout: float = 30.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.category = category
        self.product = product_of(category)
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
        response = await client.get(
            ch.MARKET_INSTRUMENTS, params={"instType": self.product}
        )
        response.raise_for_status()
        payload = response.json() or {}
        code = str(payload.get("code") or "0")
        if code != "0":
            raise RuntimeError(
                f"OKX instruments {code}: {payload.get('msg') or 'refused'}"
            )
        out: list[Instrument] = []
        for row in payload.get("data") or []:
            instrument = to_listed(
                row, venue=self.venue, category=self.category
            )
            if instrument is not None:
                out.append(instrument)
        logger.info(
            "%s instruments category=%s fetched=%s",
            VENUE,
            self.product,
            len(out),
        )
        return out


__all__ = [
    "LINEAR",
    "LIVE",
    "VENUE",
    "OkxInstrumentSource",
]
