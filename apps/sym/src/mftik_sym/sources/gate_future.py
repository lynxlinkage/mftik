"""Gate USDT-perpetual instrument source — ``GET /api/v4/futures/usdt/contracts``.

Public endpoint, no signing. Delivery lives on a different path
(``/delivery/...``); this listing is perpetuals. Rows that still carry an
expiry or are delisting are dropped so they cannot collide with
``GateFutures_Perp_BTCUSDT``.

Sizes on the wire are contracts. Filters are stored in **base** so STS and
the ledger never have to know about a quanto multiplier:
``min_qty = order_size_min * quanto_multiplier``.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

import httpx
from mftik.exchange import venues
from mftik.exchange.gate.future.protocol import (
    API_PREFIX,
    GATE_FUTURES_REST_URL,
    SETTLE,
)
from mftik.exchange.tickers import Category
from mftik_db.models.symbol import FilterName

from mftik_sym.sources.base import Instrument

logger = logging.getLogger(__name__)

VENUE = venues.GATE_FUTURES.name


class GateFuturesInstrumentSource:
    """Every USDT perpetual Gate lists, as canonical instruments."""

    venue = VENUE
    category = Category.PERP

    def __init__(
        self,
        *,
        base_url: str = GATE_FUTURES_REST_URL,
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
        response = await client.get(f"{API_PREFIX}/futures/{SETTLE}/contracts")
        response.raise_for_status()
        out: list[Instrument] = []
        for row in response.json() or []:
            instrument = self._to_instrument(row)
            if instrument is not None:
                out.append(instrument)
        logger.info("%s instruments fetched=%s", VENUE, len(out))
        return out

    def _to_instrument(self, row: dict[str, Any]) -> Instrument | None:
        if row.get("in_delisting"):
            return None
        expire = row.get("expire_time")
        if expire not in (None, "", 0, "0"):
            return None
        exch_ticker = str(row.get("name") or "")
        if not exch_ticker or "_" not in exch_ticker:
            logger.warning("%s skipping malformed contract: %r", VENUE, row)
            return None
        base, _, quote = exch_ticker.rpartition("_")
        base = base.upper()
        quote = quote.upper()
        if not base or not quote:
            logger.warning("%s skipping malformed contract: %r", VENUE, row)
            return None

        multiplier = _dec(row.get("quanto_multiplier"))
        if multiplier is None or multiplier <= 0:
            logger.warning(
                "%s skipping %s: no quanto_multiplier", VENUE, exch_ticker
            )
            return None

        min_size = _dec(row.get("order_size_min"))
        max_size = _dec(row.get("order_size_max"))
        filters: dict[str, Decimal | None] = {
            FilterName.PRICE_TICK.value: _dec(row.get("order_price_round")),
            FilterName.QTY_STEP.value: multiplier,
            FilterName.MIN_QTY.value: (
                min_size * multiplier if min_size is not None else None
            ),
            FilterName.MAX_QTY.value: (
                max_size * multiplier if max_size is not None else None
            ),
            FilterName.MIN_NOTIONAL.value: None,
            FilterName.MAX_NOTIONAL.value: None,
            FilterName.MIN_PRICE.value: None,
            FilterName.MAX_PRICE.value: None,
        }

        settle = str(
            row.get("settle")
            or row.get("settle_currency")
            or row.get("settlement_currency")
            or SETTLE
        ).upper()

        return Instrument(
            venue=self.venue,
            base=base,
            quote=quote,
            exch_ticker=exch_ticker,
            category=self.category,
            contract_size=multiplier,
            settlement_asset=settle or None,
            is_active=True,
            filters=filters,
        )


def _dec(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        parsed = Decimal(str(value))
    except Exception:
        return None
    return parsed


__all__ = ["VENUE", "GateFuturesInstrumentSource"]
