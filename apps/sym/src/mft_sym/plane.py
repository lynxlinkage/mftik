"""The symbol plane — refreshes venue listings into the golden tables.

Unlike TD/MD/STS there are no sessions and no leases here. The plane starts,
keeps ``symbol_ticker`` / ``symbol_filter`` current, answers queries, and
closes. Nothing about its lifetime is tied to a strategy deployment, which is
the point: a strategy asking "what is the tick size" must get an answer whether
or not anything is currently trading.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from mft.protocol import SymbolFilterInfo, SymbolInfo

from mft_sym.sources import InstrumentSource

logger = logging.getLogger(__name__)

Upsert = Callable[..., Awaitable[Any]]
Deactivate = Callable[..., Awaitable[int]]
ListTickers = Callable[..., Awaitable[Sequence[Any]]]
ListFilters = Callable[[int], Awaitable[Sequence[Any]]]


class SymbolPlane:
    """Owns the refresh cycle and the read path over the symbol tables."""

    def __init__(
        self,
        sources: Sequence[InstrumentSource],
        *,
        upsert: Upsert,
        deactivate_missing: Deactivate,
        list_tickers: ListTickers,
        list_filters: ListFilters,
        refresh_interval: float = 3600.0,
    ) -> None:
        self._sources = list(sources)
        self._upsert = upsert
        self._deactivate = deactivate_missing
        self._list_tickers = list_tickers
        self._list_filters = list_filters
        self.refresh_interval = refresh_interval
        self._lock = asyncio.Lock()

    @property
    def venues(self) -> list[str]:
        return sorted({s.venue for s in self._sources})

    # --- refresh -----------------------------------------------------------

    async def refresh(self, venue: str | None = None) -> dict[str, Any]:
        """Pull each source and reconcile it into the tables.

        Sources are refreshed independently: one venue's endpoint being down
        must not stop the others from updating, so failures are collected and
        reported rather than raised.
        """
        refreshed: dict[str, int] = {}
        deactivated: dict[str, int] = {}
        failed: dict[str, str] = {}

        async with self._lock:
            for source in self._sources:
                if venue is not None and source.venue != venue:
                    continue
                try:
                    instruments = await source.fetch()
                except Exception as exc:
                    logger.exception(
                        "sym refresh failed venue=%s", source.venue
                    )
                    failed[source.venue] = f"{type(exc).__name__}: {exc}"
                    continue

                seen: set[str] = set()
                for inst in instruments:
                    await self._upsert(
                        venue=inst.venue,
                        symbol=inst.symbol,
                        category=inst.category,
                        base=inst.base,
                        quote=inst.quote,
                        exch_ticker=inst.exch_ticker,
                        filters=dict(inst.filters),
                        contract_size=inst.contract_size,
                        settlement_asset=inst.settlement_asset,
                        expiry=inst.expiry,
                        is_active=inst.is_active,
                    )
                    if inst.is_active:
                        seen.add(inst.symbol)

                gone = await self._deactivate(
                    venue=source.venue, category=source.category, keep=seen
                )
                refreshed[source.venue] = len(instruments)
                deactivated[source.venue] = gone
                logger.info(
                    "sym refreshed venue=%s instruments=%s deactivated=%s",
                    source.venue,
                    len(instruments),
                    gone,
                )

        return {
            "refreshed": refreshed,
            "deactivated": deactivated,
            "failed": failed,
        }

    async def refresh_loop(self, stop: asyncio.Event) -> None:
        """Re-pull on an interval. Listings are near-static, so this is slow."""
        while not stop.is_set():
            try:
                await asyncio.wait_for(
                    stop.wait(), timeout=self.refresh_interval
                )
                return
            except TimeoutError:
                pass
            try:
                await self.refresh()
            except Exception:
                logger.exception("sym periodic refresh failed")

    # --- reads -------------------------------------------------------------

    async def list_symbols(
        self,
        *,
        venue: str | None = None,
        category: str | None = None,
        symbol: str | None = None,
        active_only: bool = True,
    ) -> list[SymbolInfo]:
        rows = await self._list_tickers(
            venue=venue, category=category, active_only=active_only
        )
        out: list[SymbolInfo] = []
        for row in rows:
            if symbol is not None and row.symbol != symbol:
                continue
            out.append(await self._to_info(row))
        return out

    async def counts(self) -> dict[str, int]:
        """Active instrument count per venue, for a health read."""
        rows = await self._list_tickers(active_only=True)
        totals: dict[str, int] = {venue: 0 for venue in self.venues}
        for row in rows:
            totals[row.venue] = totals.get(row.venue, 0) + 1
        return totals

    async def _to_info(self, row: Any) -> SymbolInfo:
        filters = [
            SymbolFilterInfo(name=f.name, value=f.value)
            for f in await self._list_filters(row.id)
        ]
        return SymbolInfo(
            venue=row.venue,
            symbol=row.symbol,
            category=row.category,
            base=row.base,
            quote=row.quote,
            exch_ticker=row.exch_ticker,
            contract_size=row.contract_size,
            settlement_asset=row.settlement_asset,
            expiry=row.expiry.timestamp() if row.expiry else None,
            is_active=row.is_active,
            filters=filters,
            updated_at=row.updated_at.timestamp() if row.updated_at else None,
        )

    async def close(self) -> None:
        for source in self._sources:
            try:
                await source.close()
            except Exception:
                logger.exception("sym source close failed venue=%s", source.venue)


__all__ = ["SymbolPlane"]
