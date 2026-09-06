"""Deribit instrument source — ``public/get_instruments``.

Public JSON-RPC, no signing.

**One source per book, not per venue.** Deribit is a unified account: one
HMAC credential trades spot, linear perps, inverse perps and dated
futures, but listing is still one filter at a time, and that is the unit
:meth:`~mftik_sym.plane.SymbolPlane.refresh` can safely delist within.
So ``Deribit`` contributes exactly four sources.

**The Perp source is the linear union.** ``kind=future`` also returns
inverse and dated rows. :func:`~mftik.exchange.deribit.listing.to_listed`
keeps linear perpetuals on Perp, inverse perpetuals on Inverse, and
every dated row (linear USDC and inverse USD) on Future. A second source
of the same category would deactivate the first.

**V2 / V3:** platform symbol is ``base+quote``. The underscore stays on
``exch_ticker``. Dated identity hyphenates ``YYMMDD``
(``Deribit_Future_BTCUSD-260906``); the wire keeps ``BTC-6SEP26``.
"""

from __future__ import annotations

import logging

import httpx
from mftik.exchange.deribit.listing import VENUE, to_listed
from mftik.exchange.deribit.protocol import (
    DERIBIT_REST_URL,
    KIND_FUTURE,
    KIND_SPOT,
)
from mftik.exchange.tickers import Category

from mftik_sym.sources.base import Instrument

logger = logging.getLogger(__name__)

_BOOKS = frozenset(
    {Category.SPOT, Category.PERP, Category.INVERSE, Category.FUTURE}
)


class DeribitInstrumentSource:
    """Every instrument Deribit lists on one of its books.

    ``category`` picks the book in the platform's vocabulary. Spot fetches
    ``kind=spot``. Perp / Inverse / Future all fetch ``kind=future`` and
    keep their own rows.
    """

    venue = VENUE

    def __init__(
        self,
        *,
        category: Category = Category.SPOT,
        base_url: str = DERIBIT_REST_URL,
        timeout: float = 30.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if category not in _BOOKS:
            raise ValueError(
                f"{VENUE} source trades Spot, Perp, Inverse and Future; "
                f"got {category.value}"
            )
        self.category = category
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._client = client
        self._owns_client = client is None

    @property
    def kind(self) -> str:
        return KIND_SPOT if self.category is Category.SPOT else KIND_FUTURE

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
            "public/get_instruments",
            params={"currency": "any", "kind": self.kind},
        )
        response.raise_for_status()
        payload = response.json() or {}
        error = payload.get("error")
        if isinstance(error, dict):
            raise RuntimeError(
                f"Deribit instruments {error.get('code')}: "
                f"{error.get('message') or 'refused'}"
            )
        out: list[Instrument] = []
        seen: set[str] = set()
        for row in payload.get("result") or []:
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
            "%s instruments category=%s fetched=%s kind=%s",
            VENUE,
            self.category,
            len(out),
            self.kind,
        )
        return out


__all__ = ["VENUE", "DeribitInstrumentSource"]
