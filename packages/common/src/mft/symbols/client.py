"""Symbol plane client — how TD / MD / STS read the golden record.

The plane is authoritative for two things every domain needs and none should
derive on its own: how a venue spells an instrument, and what its trading
restrictions are. Guessing either one is how orders land on the wrong
instrument or get rejected for a tick-size violation.

Reads are cached in-process. Listings are near-static by definition, so a
process refetches on a miss or when its TTL lapses, not per order.
"""

from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal

from mft.broker import Broker
from mft.protocol import (
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


class SymbolNotFoundError(LookupError):
    """The plane has no such instrument (wrong venue, symbol, or category)."""


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
        self._cache: dict[tuple[str, str], dict[str, SymbolInfo]] = {}
        # (venue, category) → {exch_ticker: symbol}, for the inbound direction
        self._reverse: dict[tuple[str, str], dict[str, str]] = {}
        self._fetched_at: dict[tuple[str, str], float] = {}
        self._lock = asyncio.Lock()

    # --- reads -------------------------------------------------------------

    async def get(
        self, venue: str, symbol: str, *, category: str = "spot"
    ) -> SymbolInfo:
        """One instrument, or :class:`SymbolNotFoundError`."""
        table = await self._table(venue, category)
        info = table.get(symbol)
        if info is None:
            # Could be newly listed; one forced refresh before giving up.
            table = await self._table(venue, category, force=True)
            info = table.get(symbol)
        if info is None:
            raise SymbolNotFoundError(
                f"no {category} instrument {symbol!r} on venue {venue!r}"
            )
        return info

    async def list(
        self, venue: str, *, category: str = "spot"
    ) -> list[SymbolInfo]:
        table = await self._table(venue, category)
        return sorted(table.values(), key=lambda i: i.symbol)

    async def exch_ticker(
        self, venue: str, symbol: str, *, category: str = "spot"
    ) -> str:
        """Canonical → the venue's spelling. The authoritative translation."""
        return (await self.get(venue, symbol, category=category)).exch_ticker

    async def symbol_for(
        self, venue: str, exch_ticker: str, *, category: str = "spot"
    ) -> str:
        """The venue's spelling → canonical.

        Looked up rather than derived: a venue whose ticker is not simply
        ``base + separator + quote`` (``XBTUSD`` for BTC/USD, say) would not
        survive a string transform.
        """
        await self._table(venue, category)
        found = self._reverse.get((venue, category), {}).get(exch_ticker)
        if found is None:
            await self._table(venue, category, force=True)
            found = self._reverse.get((venue, category), {}).get(exch_ticker)
        if found is None:
            raise SymbolNotFoundError(
                f"no {category} instrument spelled {exch_ticker!r} on "
                f"venue {venue!r}"
            )
        return found

    async def filter(
        self, venue: str, symbol: str, name: str, *, category: str = "spot"
    ) -> Decimal | None:
        """One restriction, e.g. ``price_tick`` or ``min_notional``."""
        return (await self.get(venue, symbol, category=category)).filter(name)

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
        self, venue: str, category: str, *, force: bool = False
    ) -> dict[str, SymbolInfo]:
        key = (venue, category)
        async with self._lock:
            fresh = time.monotonic() - self._fetched_at.get(key, 0.0) < self.ttl
            if not force and fresh and key in self._cache:
                return self._cache[key]

            reply = await self._request(
                SYM_LIST, SymListRequest(venue=venue, category=category)
            )
            result = SymListResult.model_validate(reply)
            table = {info.symbol: info for info in result.symbols}
            self._cache[key] = table
            self._reverse[key] = {
                info.exch_ticker: info.symbol for info in result.symbols
            }
            self._fetched_at[key] = time.monotonic()
            return table

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


__all__ = ["DEFAULT_TTL", "SymbolClient", "SymbolNotFoundError"]
