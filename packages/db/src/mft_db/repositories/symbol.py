"""Reads and upserts for the symbol plane."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from mft_db.models.symbol import SymbolCategory, SymbolFilter, SymbolTicker
from mft_db.repositories.base import BaseRepository


class SymbolRepository(BaseRepository[SymbolTicker]):
    """The ``sym`` service writes through this; everyone else reads."""

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, SymbolTicker)

    # --- reads -------------------------------------------------------------

    async def get_ticker(
        self,
        *,
        venue: str,
        symbol: str,
        category: str = SymbolCategory.SPOT.value,
    ) -> SymbolTicker | None:
        result = await self.session.execute(
            select(SymbolTicker).where(
                SymbolTicker.venue == venue,
                SymbolTicker.symbol == symbol,
                SymbolTicker.category == category,
            )
        )
        return result.scalar_one_or_none()

    async def list_tickers(
        self,
        *,
        venue: str | None = None,
        category: str | None = None,
        active_only: bool = True,
    ) -> Sequence[SymbolTicker]:
        stmt = select(SymbolTicker)
        if venue is not None:
            stmt = stmt.where(SymbolTicker.venue == venue)
        if category is not None:
            stmt = stmt.where(SymbolTicker.category == category)
        if active_only:
            stmt = stmt.where(SymbolTicker.is_active.is_(True))
        stmt = stmt.order_by(
            SymbolTicker.venue.asc(),
            SymbolTicker.category.asc(),
            SymbolTicker.symbol.asc(),
        )
        result = await self.session.execute(stmt)
        return result.scalars().unique().all()

    async def list_filters(self, ticker_id: int) -> Sequence[SymbolFilter]:
        """Filter rows for one instrument, ordered by name."""
        result = await self.session.execute(
            select(SymbolFilter)
            .where(SymbolFilter.ticker_id == ticker_id)
            .order_by(SymbolFilter.name.asc())
        )
        return result.scalars().all()

    async def venues(self) -> list[str]:
        result = await self.session.execute(
            select(SymbolTicker.venue).distinct().order_by(SymbolTicker.venue)
        )
        return list(result.scalars().all())

    # --- writes ------------------------------------------------------------

    async def upsert(
        self,
        *,
        venue: str,
        symbol: str,
        category: str,
        base: str,
        quote: str,
        exch_ticker: str,
        filters: dict[str, Decimal | None],
        contract_size: Decimal | None = None,
        settlement_asset: str | None = None,
        expiry: object = None,
        is_active: bool = True,
    ) -> SymbolTicker:
        """Insert or update one instrument and reconcile its filter rows.

        Filters are reconciled rather than replaced: a venue dropping a filter
        removes that row, and one whose bound changed is updated in place, so
        ``symbol_filter.id`` stays stable for anything referencing it.
        """
        ticker = await self.get_ticker(
            venue=venue, symbol=symbol, category=category
        )
        if ticker is None:
            ticker = SymbolTicker(
                venue=venue, symbol=symbol, category=category
            )
            self.session.add(ticker)

        ticker.base = base
        ticker.quote = quote
        ticker.exch_ticker = exch_ticker
        ticker.contract_size = contract_size
        ticker.settlement_asset = settlement_asset
        ticker.expiry = expiry  # type: ignore[assignment]
        ticker.is_active = is_active
        await self.session.flush()

        # Queried, not traversed: on a freshly inserted ticker the relationship
        # would lazy-load, which async SQLAlchemy cannot do mid-transaction.
        existing = {f.name: f for f in await self.list_filters(ticker.id)}
        for name, value in filters.items():
            row = existing.pop(name, None)
            if row is None:
                self.session.add(
                    SymbolFilter(ticker_id=ticker.id, name=name, value=value)
                )
            elif row.value != value:
                row.value = value
        for stale in existing.values():
            await self.session.delete(stale)

        await self.session.flush()
        return ticker

    async def deactivate_missing(
        self, *, venue: str, category: str, keep: set[str]
    ) -> int:
        """Flag instruments the venue no longer lists. Returns how many.

        Delisted rows stay: sessions, orders and audit history still reference
        them, and a symbol can come back.
        """
        result = await self.session.execute(
            select(SymbolTicker).where(
                SymbolTicker.venue == venue,
                SymbolTicker.category == category,
                SymbolTicker.is_active.is_(True),
            )
        )
        count = 0
        for ticker in result.scalars().unique().all():
            if ticker.symbol not in keep:
                ticker.is_active = False
                count += 1
        await self.session.flush()
        return count
