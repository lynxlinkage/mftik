"""Bybit instrument source — ``GET /v5/market/instruments-info``.

Public endpoint, no signing.

**One source per book, not per venue.** Bybit is a unified account: one
credential trades spot and perps, but the listing endpoint still answers one
``category`` at a time, and that is also the unit
:meth:`~mftik_sym.plane.SymbolPlane.refresh` can safely delist within — a spot
refresh must not deactivate perp rows just because they were absent from a spot
response. So ``Bybit`` contributes two sources, ``Spot`` and ``Perp``.

**The linear book is not only perpetuals.** ``category=linear`` lists dated
futures alongside them. The adapter's ``to_listed`` keeps only
``contractType`` ending in ``Perpetual`` when the source is a perp book.

**It is paginated behind a cursor**, with no total. A refresh that read only the
first page would quietly publish a fraction of the venue and then deactivate
everything it did not see — which is worse than not refreshing at all.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx
from mftik.exchange.bybit import channels as ch
from mftik.exchange.bybit.listing import PERPETUAL, TRADING, VENUE, to_listed
from mftik.exchange.bybit.protocol import BYBIT_REST_URL, product_of
from mftik.exchange.tickers import Category

from mftik_sym.sources.base import Instrument

logger = logging.getLogger(__name__)

#: Most rows one page returns. Bybit caps it here and answers a cursor for the
#: rest.
PAGE_LIMIT = 1000

#: How many pages a single fetch will follow before giving up. Bybit's spot
#: book is one page and its linear book two or three; a cursor that never
#: empties is a bug at one end or the other, and looping forever on it would
#: hang the whole refresh cycle rather than fail one venue.
MAX_PAGES = 50


class BybitInstrumentSource:
    """Every instrument Bybit lists on one of its books.

    ``category`` picks the book in the platform's vocabulary and is what the
    stored tickers carry; the Bybit ``category`` parameter it maps to comes
    from :func:`~mftik.exchange.bybit.protocol.product_of`.
    """

    venue = VENUE

    def __init__(
        self,
        *,
        category: Category = Category.SPOT,
        base_url: str = BYBIT_REST_URL,
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
        out: list[Instrument] = []
        cursor = ""
        for _ in range(MAX_PAGES):
            params: dict[str, Any] = {
                "category": self.product,
                "limit": PAGE_LIMIT,
            }
            if cursor:
                params["cursor"] = cursor
            response = await client.get(ch.MARKET_INSTRUMENTS, params=params)
            response.raise_for_status()
            result = (response.json() or {}).get("result") or {}
            for row in result.get("list") or []:
                instrument = to_listed(
                    row, venue=self.venue, category=self.category
                )
                if instrument is not None:
                    out.append(instrument)
            cursor = str(result.get("nextPageCursor") or "")
            if not cursor:
                break
        else:
            logger.warning(
                "%s %s listing still had a cursor after %s pages",
                VENUE,
                self.product,
                MAX_PAGES,
            )
        logger.info(
            "%s instruments category=%s fetched=%s", VENUE, self.product, len(out)
        )
        return out


__all__ = [
    "MAX_PAGES",
    "PAGE_LIMIT",
    "PERPETUAL",
    "TRADING",
    "VENUE",
    "BybitInstrumentSource",
]
