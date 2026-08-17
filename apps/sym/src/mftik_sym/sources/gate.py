"""Gate spot instrument source — ``GET /api/v4/spot/currency_pairs``.

Public endpoint, no signing. Gate reports granularity as decimal-place counts
(``precision`` for price, ``amount_precision`` for quantity), which are
converted to step sizes here.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

import httpx
from mftik.exchange import venues
from mftik.exchange.gate.spot.rest import API_PREFIX, GATE_SPOT_REST_URL
from mftik.exchange.tickers import Category
from mftik_db.models.symbol import FilterName

from mftik_sym.sources.base import Instrument, tick_from_precision

logger = logging.getLogger(__name__)

VENUE = venues.GATE.name

#: Gate's trade_status values that mean the pair can actually be traded.
TRADABLE = frozenset({"tradable", "buyable", "sellable"})


class GateSpotInstrumentSource:
    """Every spot pair Gate lists, as canonical instruments."""

    venue = VENUE
    category = Category.SPOT

    def __init__(
        self,
        *,
        base_url: str = GATE_SPOT_REST_URL,
        timeout: float = 20.0,
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
        response = await client.get(f"{API_PREFIX}/spot/currency_pairs")
        response.raise_for_status()
        rows = response.json()
        out: list[Instrument] = []
        for row in rows or []:
            instrument = self._to_instrument(row)
            if instrument is not None:
                out.append(instrument)
        logger.info("%s instruments fetched=%s", VENUE, len(out))
        return out

    def _to_instrument(self, row: dict[str, Any]) -> Instrument | None:
        base = str(row.get("base") or "").upper()
        quote = str(row.get("quote") or "").upper()
        exch_ticker = str(row.get("id") or "")
        if not base or not quote or not exch_ticker:
            logger.warning("%s skipping malformed pair: %r", VENUE, row)
            return None

        filters: dict[str, Decimal | None] = {
            FilterName.PRICE_TICK.value: tick_from_precision(
                row.get("precision")
            ),
            FilterName.QTY_STEP.value: tick_from_precision(
                row.get("amount_precision")
            ),
            # Gate publishes these with null meaning "no limit"; keep the key
            # so a caller can tell "unbounded" from "not published".
            FilterName.MIN_QTY.value: _dec(row.get("min_base_amount")),
            FilterName.MAX_QTY.value: _dec(row.get("max_base_amount")),
            FilterName.MIN_NOTIONAL.value: _dec(row.get("min_quote_amount")),
            FilterName.MAX_NOTIONAL.value: _dec(row.get("max_quote_amount")),
        }

        return Instrument(
            venue=self.venue,
            base=base,
            quote=quote,
            exch_ticker=exch_ticker,
            category=self.category,
            is_active=str(row.get("trade_status", "")) in TRADABLE,
            filters=filters,
        )


def _dec(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


__all__ = ["TRADABLE", "VENUE", "GateSpotInstrumentSource"]
