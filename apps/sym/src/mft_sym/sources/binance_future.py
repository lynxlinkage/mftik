"""Binance USDⓈ-M futures instrument source — ``GET /fapi/v1/exchangeInfo``.

Public endpoint, no signing. HTTP for the same reason the spot source uses it:
the plane refreshes a whole venue's listing on a timer and then goes quiet, and
:class:`InstrumentSource` is a batch fetcher rather than a connector. Here it is
also the only option — futures serves no ``exchangeInfo`` over its WebSocket
API at all.

**Only perpetuals.** The endpoint lists dated futures beside them
(``BTCUSDT_250926``, ``ETHUSDT_251226``) whose ``baseAsset`` and ``quoteAsset``
are the perpetual's, so both canonicalize to ``BTCUSDT`` and the upsert would
keep whichever was written last. ``BinanceFuture_Perp_BTCUSDT`` would then hold
a September future's ``exch_ticker`` and every perp order would route to a
contract that expires — the same collision Bybit's linear book has, and the
same fix: this source keeps ``contractType == "PERPETUAL"`` and a ``Future``
source that does not exist yet would take the rest.

Two payload details differ from spot's and both are silent if missed: futures
spells the notional floor ``notional`` inside a ``MIN_NOTIONAL`` filter (spot
says ``minNotional``), and it publishes a ``marginAsset`` — the currency the
contract settles in, which on spot has no meaning at all.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

import httpx
from mft.exchange import venues
from mft.exchange.binance.future.rest import (
    API_PREFIX,
    BINANCE_FUTURE_REST_URL,
    PERPETUAL,
    TRADING,
)
from mft.exchange.tickers import Category
from mft_db.models.symbol import FilterName

from mft_sym.sources.base import Instrument

logger = logging.getLogger(__name__)

VENUE = venues.BINANCE_FUTURE.name


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
            instrument = self._to_instrument(row)
            if instrument is not None:
                out.append(instrument)
        logger.info("%s instruments fetched=%s", VENUE, len(out))
        return out

    def _to_instrument(self, row: dict[str, Any]) -> Instrument | None:
        if str(row.get("contractType") or "") != PERPETUAL:
            return None
        base = str(row.get("baseAsset") or "").upper()
        quote = str(row.get("quoteAsset") or "").upper()
        exch_ticker = str(row.get("symbol") or "")
        if not base or not quote or not exch_ticker:
            logger.warning("%s skipping malformed symbol: %r", VENUE, row)
            return None

        filters = _by_type(row.get("filters"))
        price = filters.get("PRICE_FILTER", {})
        lot = filters.get("LOT_SIZE", {})
        # ``notional``, not ``minNotional`` — the spot key returns nothing here
        # and the floor would disappear without a word.
        notional = filters.get("MIN_NOTIONAL", {})

        bounds: dict[str, Decimal | None] = {
            FilterName.PRICE_TICK.value: _step(price.get("tickSize")),
            FilterName.QTY_STEP.value: _step(lot.get("stepSize")),
            FilterName.MIN_QTY.value: _step(lot.get("minQty")),
            FilterName.MAX_QTY.value: _step(lot.get("maxQty")),
            FilterName.MIN_NOTIONAL.value: _step(notional.get("notional")),
            FilterName.MAX_NOTIONAL.value: None,
            FilterName.MIN_PRICE.value: _step(price.get("minPrice")),
            FilterName.MAX_PRICE.value: _step(price.get("maxPrice")),
        }

        return Instrument(
            venue=self.venue,
            base=base,
            quote=quote,
            exch_ticker=exch_ticker,
            category=self.category,
            # What the contract is margined and settled in — USDT or USDC on
            # this plane. Spot has no such field because the quote currency
            # *is* the settlement.
            settlement_asset=str(row.get("marginAsset") or "") or None,
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

    Same reading as the spot source's: ``0`` means the filter is present but
    unbounded, and trailing zeros are stripped because on a ``Decimal`` they
    are the scale rather than decoration — a size floored against a step stored
    as ``0.00100000`` comes out written to eight decimals, which Binance
    rejects as ``-1111`` where the same quantity written to three is taken.
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


__all__ = ["VENUE", "BinanceFutureInstrumentSource"]
