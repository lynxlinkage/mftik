"""Binance spot instrument source — ``GET /api/v3/exchangeInfo``.

Public endpoint, no signing.

HTTP rather than the WebSocket API the rest of the Binance adapter uses. The
plane refreshes a whole venue's listing on a timer and then goes quiet, so a
socket here would be opened, used once and left idle between refreshes — and
:class:`InstrumentSource` is deliberately a batch fetcher, not a connector.
The connector's own ``fetch_instruments`` covers the same endpoint for callers
that already hold a socket.

Binance publishes granularity as step sizes already (``tickSize``,
``stepSize``), so unlike Gate there is no precision-count to convert — but they
live inside a list of typed filter objects rather than as fields, and a step
the venue does not enforce is published as ``0`` rather than omitted.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

import httpx
from mftik.exchange import venues
from mftik.exchange.tickers import Category
from mftik_db.models.symbol import FilterName

from mftik_sym.sources.base import Instrument

logger = logging.getLogger(__name__)

VENUE = venues.BINANCE.name

BINANCE_SPOT_REST_URL = "https://api.binance.com"
API_PREFIX = "/api/v3"

#: The only ``status`` that means the symbol can be traded right now. Binance
#: also lists ``HALT``, ``BREAK`` and pre-launch symbols on this endpoint.
TRADING = "TRADING"


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
            instrument = self._to_instrument(row)
            if instrument is not None:
                out.append(instrument)
        logger.info("%s instruments fetched=%s", VENUE, len(out))
        return out

    def _to_instrument(self, row: dict[str, Any]) -> Instrument | None:
        base = str(row.get("baseAsset") or "").upper()
        quote = str(row.get("quoteAsset") or "").upper()
        exch_ticker = str(row.get("symbol") or "")
        if not base or not quote or not exch_ticker:
            logger.warning("%s skipping malformed symbol: %r", VENUE, row)
            return None

        filters = _by_type(row.get("filters"))
        price = filters.get("PRICE_FILTER", {})
        lot = filters.get("LOT_SIZE", {})
        # ``NOTIONAL`` superseded ``MIN_NOTIONAL``; older symbols still carry
        # the latter and a few carry both.
        notional = filters.get("NOTIONAL") or filters.get("MIN_NOTIONAL") or {}

        bounds: dict[str, Decimal | None] = {
            FilterName.PRICE_TICK.value: _step(price.get("tickSize")),
            FilterName.QTY_STEP.value: _step(lot.get("stepSize")),
            FilterName.MIN_QTY.value: _step(lot.get("minQty")),
            FilterName.MAX_QTY.value: _step(lot.get("maxQty")),
            FilterName.MIN_NOTIONAL.value: _step(notional.get("minNotional")),
            FilterName.MAX_NOTIONAL.value: _step(notional.get("maxNotional")),
            FilterName.MIN_PRICE.value: _step(price.get("minPrice")),
            FilterName.MAX_PRICE.value: _step(price.get("maxPrice")),
        }

        return Instrument(
            venue=self.venue,
            base=base,
            quote=quote,
            exch_ticker=exch_ticker,
            category=self.category,
            is_active=str(row.get("status", "")) == TRADING,
            filters=bounds,
        )


def _by_type(filters: Any) -> dict[str, dict[str, Any]]:
    """Binance's filter list, keyed by ``filterType``."""
    return {
        str(f.get("filterType", "")): f
        for f in filters or []
        if isinstance(f, dict)
    }


def _step(value: Any) -> Decimal | None:
    """A published bound, or ``None`` where Binance enforces none.

    Binance writes ``0.00000000`` for a filter that is present but unbounded,
    which is not the same as the bound being zero — an order cannot be a
    multiple of a zero step, and a zero minimum is no minimum. Both read as
    "the venue publishes the restriction but sets no limit", which is exactly
    what a ``None`` value on a present key means to this plane.

    Trailing zeros are stripped, because on a ``Decimal`` they are not
    decoration — they are the scale, and the scale propagates. Binance
    publishes ETHUSDT's lot step as ``"0.00010000"``, and a size floored
    against that comes out as ``0.00780000``: eight decimals where the venue
    allows four, which it then rejects as ``-1111``. Gate never showed this
    because its steps are built from a precision count and are already exact.
    The golden record stores granularity, not the venue's formatting.
    """
    if value is None or value == "":
        return None
    try:
        parsed = Decimal(str(value))
    except Exception:
        return None
    if parsed <= 0:
        return None
    stripped = parsed.normalize()
    # ``normalize`` renders a whole number exponentially (10 → 1E+1); keep it
    # written the way it was published.
    if stripped.as_tuple().exponent > 0:
        return stripped.quantize(Decimal(1))
    return stripped


__all__ = [
    "API_PREFIX",
    "BINANCE_SPOT_REST_URL",
    "TRADING",
    "VENUE",
    "BinanceSpotInstrumentSource",
]
