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

A single-instrument read asks the plane for that ticker. Loading a whole
venue table to resolve one pair is how Gate became silent on MD: 2200+
rows with filters miss the 5s RPC budget (and NATS's 1 MiB payload), the
feed pump dies after attach has already returned, and STS sees a live
session with no prints. ``list`` / reverse lookup still load the table,
but they page it.
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

#: One ``SYM_LIST`` page. Matches :class:`SymListRequest.limit`'s ceiling —
#: large enough that a venue is a handful of round trips, small enough that
#: a Gate-sized page with every filter still fits a 1 MiB NATS payload.
LIST_PAGE = 500

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
        #: Per-instrument cache for :meth:`get`. Independent of a full table
        #: load, so resolving ``Gate_Spot_ETHUSDT`` does not pull Gate's 2200
        #: other pairs.
        self._singles: dict[UniversalTicker, tuple[float, SymbolInfo]] = {}
        self._lock = asyncio.Lock()

    # --- reads -------------------------------------------------------------

    async def get(self, ticker: UniversalTicker) -> SymbolInfo:
        """One instrument, or :class:`SymbolNotFoundError`.

        Hits the plane by exact ticker. A venue-wide ``SYM_LIST`` is how a
        Gate feed attached, stayed ``live``, and never printed.
        """
        async with self._lock:
            cached = self._cached_one(ticker)
            if cached is not None:
                return cached
        info = await self._fetch_one(ticker)
        if info is None:
            raise SymbolNotFoundError(f"no such instrument: {ticker}")
        async with self._lock:
            self._store_one(ticker, info)
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
            self._singles.clear()
            return
        for key in [k for k in self._cache if k[0] == venue]:
            self._cache.pop(key, None)
            self._reverse.pop(key, None)
            self._fetched_at.pop(key, None)
        self._singles = {
            ticker: item
            for ticker, item in self._singles.items()
            if ticker.venue != venue
        }

    # --- internals ---------------------------------------------------------

    def _table_fresh(self, key: TableKey) -> bool:
        """Whether a *whole* table was loaded and has not lapsed.

        Keyed on ``_fetched_at`` having an entry rather than defaulting a
        missing one to ``0.0``: ``time.monotonic`` counts from boot on Linux,
        so on a node that has just started ``now - 0.0`` is a few seconds and
        a table nobody ever fetched would read as fresh.
        """
        fetched_at = self._fetched_at.get(key)
        return fetched_at is not None and time.monotonic() - fetched_at < self.ttl

    def _cached_one(self, ticker: UniversalTicker) -> SymbolInfo | None:
        """A hit from the single-instrument cache or a still-fresh table."""
        single = self._singles.get(ticker)
        if single is not None:
            fetched_at, info = single
            if time.monotonic() - fetched_at < self.ttl:
                return info
        key = _key(ticker)
        if self._table_fresh(key):
            return self._cache[key].get(ticker.symbol)
        return None

    def _store_one(self, ticker: UniversalTicker, info: SymbolInfo) -> None:
        """Remember one instrument, and only that.

        Deliberately does not seed ``_cache`` / ``_reverse``: those hold whole
        venue tables, and a handful of separately resolved rows sitting in
        them is a table that looks loaded and is missing almost everything.
        ``list`` and ``symbol_for`` answer from a real load or not at all.
        """
        self._singles[ticker] = (time.monotonic(), info)

    def _install_table(
        self, key: TableKey, symbols: dict[str, SymbolInfo]
    ) -> None:
        now = time.monotonic()
        self._cache[key] = symbols
        self._reverse[key] = {
            info.exch_ticker: info.symbol for info in symbols.values()
        }
        self._fetched_at[key] = now
        for info in symbols.values():
            self._singles[info.ticker] = (now, info)

    async def _fetch_one(self, ticker: UniversalTicker) -> SymbolInfo | None:
        result = SymListResult.model_validate(
            await self._request(
                SYM_LIST,
                SymListRequest(universal_ticker=str(ticker)),
            )
        )
        return result.symbols[0] if result.symbols else None

    async def _fetch_pages(self, key: TableKey) -> dict[str, SymbolInfo]:
        """Walk ``SYM_LIST`` in :data:`LIST_PAGE` chunks.

        One unpaged Gate Spot reply is 2200+ instruments and does not come
        back inside :data:`mftik.broker.config.BrokerConfig.request_timeout`.
        """
        venue, category = key
        out: dict[str, SymbolInfo] = {}
        offset = 0
        while True:
            result = SymListResult.model_validate(
                await self._request(
                    SYM_LIST,
                    SymListRequest(
                        venue=venue,
                        category=category.value,
                        limit=LIST_PAGE,
                        offset=offset,
                    ),
                )
            )
            for info in result.symbols:
                out[info.symbol] = info
            if not result.symbols:
                break
            offset += len(result.symbols)
            if offset >= result.total:
                break
        return out

    async def _table(
        self, key: TableKey, *, force: bool = False
    ) -> dict[str, SymbolInfo]:
        async with self._lock:
            if not force and self._table_fresh(key):
                return self._cache[key]
        symbols = await self._fetch_pages(key)
        async with self._lock:
            self._install_table(key, symbols)
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


__all__ = ["DEFAULT_TTL", "LIST_PAGE", "SymbolClient", "SymbolNotFoundError"]
