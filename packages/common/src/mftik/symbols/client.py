"""Symbol plane client — how TD / MD / STS read the golden record.

The plane is authoritative for two things every domain needs and none should
derive on its own: how a venue spells an instrument, and what its trading
restrictions are. Guessing either one is how orders land on the wrong
instrument or get rejected for a tick-size violation.

Everything here is keyed by a :class:`~mftik.exchange.tickers.UniversalTicker`,
never a bare symbol — on a unified-account venue ``BTCUSDT`` names both the
spot pair and the perp, and they have different tick sizes.

Reads are cached in-process. Listings are near-static by definition, so a
process refetches on a miss or when its TTL lapses, not per order.
"""

from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal

from mftik.broker import Broker
from mftik.exchange.tickers import Category, UniversalTicker
from mftik.protocol import (
    SYM_LIST,
    SYM_REFRESH,
    SYM_VENUES,
    Envelope,
    SymbolInfo,
    SymListRequest,
    SymListResult,
    SymRefreshRequest,
    SymVenuesResult,
    Topics,
)

logger = logging.getLogger(__name__)

DEFAULT_TTL = 600.0

#: Cache bucket: one venue's one market. That is also the unit the plane
#: refreshes, so a bucket is never half stale.
TableKey = tuple[str, Category]


class SymbolNotFoundError(LookupError):
    """The plane has no such instrument (wrong venue, category, or symbol)."""


class SymbolClient:
    """Cached reads against the ``sym`` service."""

    def __init__(
        self,
        broker: Broker,
        *,
        ttl: float = DEFAULT_TTL,
        timeout: float | None = None,
    ) -> None:
        self._broker = broker
        self.ttl = ttl
        self.timeout = timeout
        # (venue, category) → {symbol: SymbolInfo}
        self._cache: dict[TableKey, dict[str, SymbolInfo]] = {}
        # (venue, category) → {exch_ticker: symbol}, for the inbound direction
        self._reverse: dict[TableKey, dict[str, str]] = {}
        self._fetched_at: dict[TableKey, float] = {}
        self._lock = asyncio.Lock()

    # --- reads -------------------------------------------------------------

    async def get(self, ticker: UniversalTicker) -> SymbolInfo:
        """One instrument, or :class:`SymbolNotFoundError`."""
        key = _key(ticker)
        table = await self._table(key)
        info = table.get(ticker.symbol)
        if info is None:
            # Could be newly listed; one forced refresh before giving up.
            table = await self._table(key, force=True)
            info = table.get(ticker.symbol)
        if info is None:
            raise SymbolNotFoundError(f"no such instrument: {ticker}")
        return info

    async def list(
        self, venue: str, *, category: Category | str = Category.SPOT
    ) -> list[SymbolInfo]:
        table = await self._table((venue, Category(category)))
        return sorted(table.values(), key=lambda i: i.universal_ticker)

    async def exch_ticker(self, ticker: UniversalTicker) -> str:
        """Universal → the venue's spelling. The authoritative translation."""
        return (await self.get(ticker)).exch_ticker

    async def contract_size(self, ticker: UniversalTicker) -> Decimal | None:
        """How much base one venue-native size unit is, or ``None``."""
        return (await self.get(ticker)).contract_size

    async def symbol_for(
        self, venue: str, exch_ticker: str, *, category: Category | str
    ) -> UniversalTicker:
        """The venue's spelling → the universal ticker.

        Looked up rather than derived: a venue whose ticker is not simply
        ``base + separator + quote`` (``XBTUSD`` for BTC/USD, say) would not
        survive a string transform.
        """
        key = (venue, Category(category))
        await self._table(key)
        found = self._reverse.get(key, {}).get(exch_ticker)
        if found is None:
            await self._table(key, force=True)
            found = self._reverse.get(key, {}).get(exch_ticker)
        if found is None:
            raise SymbolNotFoundError(
                f"no {key[1].value} instrument spelled {exch_ticker!r} on "
                f"venue {venue!r}"
            )
        return UniversalTicker(venue=venue, category=key[1], symbol=found)

    async def filter(self, ticker: UniversalTicker, name: str) -> Decimal | None:
        """One restriction, e.g. ``price_tick`` or ``min_notional``."""
        return (await self.get(ticker)).filter(name)

    async def venues(self) -> SymVenuesResult:
        reply = await self._request(SYM_VENUES, SymListRequest())
        return SymVenuesResult.model_validate(reply)

    async def refresh(self, venue: str | None = None) -> dict[str, object]:
        """Ask the plane to re-pull, then drop our cache for that venue."""
        reply = await self._request(SYM_REFRESH, SymRefreshRequest(venue=venue))
        self.invalidate(venue)
        return dict(reply)

    def invalidate(self, venue: str | None = None) -> None:
        if venue is None:
            self._cache.clear()
            self._reverse.clear()
            self._fetched_at.clear()
            return
        for key in [k for k in self._cache if k[0] == venue]:
            self._cache.pop(key, None)
            self._reverse.pop(key, None)
            self._fetched_at.pop(key, None)

    # --- internals ---------------------------------------------------------

    async def _table(
        self, key: TableKey, *, force: bool = False
    ) -> dict[str, SymbolInfo]:
        venue, category = key
        async with self._lock:
            fresh = time.monotonic() - self._fetched_at.get(key, 0.0) < self.ttl
            if not force and fresh and key in self._cache:
                return self._cache[key]

            reply = await self._request(
                SYM_LIST,
                SymListRequest(venue=venue, category=category.value),
            )
            result = SymListResult.model_validate(reply)
            self._cache[key] = {info.symbol: info for info in result.symbols}
            self._reverse[key] = {
                info.exch_ticker: info.symbol for info in result.symbols
            }
            self._fetched_at[key] = time.monotonic()
            return self._cache[key]

    async def _request(self, type_: str, payload: object) -> dict:
        envelope = Envelope[type(payload)].wrap(  # type: ignore[misc]
            payload, type=type_, source="sym.client"
        )
        reply = await self._broker.request(
            Topics.SYM, envelope, timeout=self.timeout
        )
        if reply.type.endswith(".error"):
            raise LookupError(
                f"sym error: {reply.payload.get('code')}: "
                f"{reply.payload.get('message')}"
            )
        return reply.payload


def _key(ticker: UniversalTicker) -> TableKey:
    return (ticker.venue, ticker.category)


__all__ = ["DEFAULT_TTL", "SymbolClient", "SymbolNotFoundError"]
