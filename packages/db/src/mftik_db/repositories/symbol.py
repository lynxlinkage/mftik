"""Reads and upserts for the symbol plane."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import noload

from mftik_db.models.symbol import SymbolFilter, SymbolTicker
from mftik_db.repositories.base import BaseRepository

#: What separates the parts of a universal ticker — and, awkwardly, also the
#: single-character wildcard in SQL ``LIKE``. Every pattern built from it goes
#: through ``autoescape=True`` so ``Gate_Spot_`` cannot match ``GateXSpotY``.
SEPARATOR = "_"


class SymbolRepository(BaseRepository[SymbolTicker]):
    """The ``sym`` service writes through this; everyone else reads."""

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, SymbolTicker)

    # --- reads -------------------------------------------------------------

    async def get_ticker(self, universal_ticker: str) -> SymbolTicker | None:
        result = await self.session.execute(
            select(SymbolTicker).where(
                SymbolTicker.universal_ticker == universal_ticker
            )
        )
        return result.scalar_one_or_none()

    async def list_tickers(
        self,
        *,
        universal_ticker: str | None = None,
        venue: str | None = None,
        category: str | None = None,
        symbol: str | None = None,
        active_only: bool = True,
        q: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> Sequence[SymbolTicker]:
        """Instruments matching the given parts of a ticker.

        Every filter is a pattern over the one identity column. Naming a venue
        (with or without a category) gives a left-anchored prefix, which the
        index serves; the rest are unanchored and scan, which is fine because
        nothing on a hot path asks for "every Perp everywhere".

        ``universal_ticker`` is an exact match — used when the caller already
        has the identity and must not scan the whole table to find one row.

        ``q`` is a case-insensitive substring over the ticker, the venue's
        spelling, and the base/quote legs. ``limit``/``offset`` page the
        ordered result; omit ``limit`` for the whole match.

        Filter rows are deliberately not loaded here: the relationship is
        ``lazy="selectin"``, which would pull every filter for every ticker
        in the result before the caller can ask for a slim subset. Callers
        that need filters use :meth:`list_filters_for`.
        """
        # noload: see docstring. Without it, selectin eager-loads every
        # symbol_filter row for the page and makes slim lists pointless.
        stmt = select(SymbolTicker).options(noload(SymbolTicker.filters))
        if universal_ticker is not None:
            stmt = stmt.where(SymbolTicker.universal_ticker == universal_ticker)
        for clause in _match(venue, category, symbol):
            stmt = stmt.where(clause)
        if active_only:
            stmt = stmt.where(SymbolTicker.is_active.is_(True))
        for clause in _search(q):
            stmt = stmt.where(clause)
        stmt = stmt.order_by(SymbolTicker.universal_ticker.asc())
        if offset:
            stmt = stmt.offset(offset)
        if limit is not None:
            stmt = stmt.limit(limit)
        result = await self.session.execute(stmt)
        return result.scalars().unique().all()

    async def count_tickers(
        self,
        *,
        universal_ticker: str | None = None,
        venue: str | None = None,
        category: str | None = None,
        symbol: str | None = None,
        active_only: bool = True,
        q: str | None = None,
    ) -> int:
        """How many instruments match, ignoring ``limit``/``offset``."""
        stmt = select(func.count()).select_from(SymbolTicker)
        if universal_ticker is not None:
            stmt = stmt.where(SymbolTicker.universal_ticker == universal_ticker)
        for clause in _match(venue, category, symbol):
            stmt = stmt.where(clause)
        if active_only:
            stmt = stmt.where(SymbolTicker.is_active.is_(True))
        for clause in _search(q):
            stmt = stmt.where(clause)
        result = await self.session.execute(stmt)
        return int(result.scalar_one())

    async def list_filters(self, ticker_id: int) -> Sequence[SymbolFilter]:
        """Filter rows for one instrument, ordered by name."""
        result = await self.session.execute(
            select(SymbolFilter)
            .where(SymbolFilter.ticker_id == ticker_id)
            .order_by(SymbolFilter.name.asc())
        )
        return result.scalars().all()

    async def list_filters_for(
        self,
        ticker_ids: Sequence[int],
        *,
        names: Sequence[str] | None = None,
    ) -> dict[int, list[SymbolFilter]]:
        """Filter rows for many instruments at once, grouped by ticker id.

        Building a whole venue table must use this rather than looping over
        ``list_filters``: Gate lists 2200+ pairs, and one round trip per
        instrument to a database on another host takes tens of seconds — well
        past the caller's RPC timeout. Ids absent from the result simply have
        no filters.

        ``names`` restricts which filter keys come back (the browse UI only
        needs tick / lot / minimums).
        """
        if not ticker_ids:
            return {}
        stmt = (
            select(SymbolFilter)
            .where(SymbolFilter.ticker_id.in_(ticker_ids))
            .order_by(SymbolFilter.ticker_id.asc(), SymbolFilter.name.asc())
        )
        if names is not None:
            stmt = stmt.where(SymbolFilter.name.in_(list(names)))
        result = await self.session.execute(stmt)
        grouped: dict[int, list[SymbolFilter]] = {}
        for row in result.scalars().all():
            grouped.setdefault(row.ticker_id, []).append(row)
        return grouped

    async def venues(self) -> list[str]:
        """Distinct venues present in the table.

        Split in Python rather than in SQL: the identity is one column now, and
        the string functions that would carve a venue out of it differ between
        PostgreSQL and the SQLite the tests run on. This is not on a hot path.
        """
        result = await self.session.execute(
            select(SymbolTicker.universal_ticker).distinct()
        )
        found = {
            value.split(SEPARATOR, 1)[0]
            for value in result.scalars().all()
            if SEPARATOR in value
        }
        return sorted(found)

    # --- writes ------------------------------------------------------------

    async def upsert(
        self,
        *,
        universal_ticker: str,
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
        ticker = await self.get_ticker(universal_ticker)
        if ticker is None:
            ticker = SymbolTicker(universal_ticker=universal_ticker)
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

        Scoped to one ``venue``/``category`` because that is what a source
        refreshes — a Bybit spot pull says nothing about Bybit perps, and
        must not delist them. ``keep`` holds universal tickers.

        Delisted rows stay: sessions, orders and audit history still reference
        them, and a symbol can come back.
        """
        stmt = select(SymbolTicker).where(SymbolTicker.is_active.is_(True))
        for clause in _match(venue, category, None):
            stmt = stmt.where(clause)
        result = await self.session.execute(stmt)
        count = 0
        for ticker in result.scalars().unique().all():
            if ticker.universal_ticker not in keep:
                ticker.is_active = False
                count += 1
        await self.session.flush()
        return count


def _match(venue: str | None, category: str | None, symbol: str | None) -> list:
    """LIKE clauses over ``universal_ticker`` for whichever parts were named."""
    column = SymbolTicker.universal_ticker
    clauses = []
    if venue is not None and category is not None:
        prefix = f"{venue}{SEPARATOR}{category}{SEPARATOR}"
        clauses.append(column.startswith(prefix, autoescape=True))
    elif venue is not None:
        clauses.append(column.startswith(f"{venue}{SEPARATOR}", autoescape=True))
    elif category is not None:
        clauses.append(
            column.contains(f"{SEPARATOR}{category}{SEPARATOR}", autoescape=True)
        )
    if symbol is not None:
        clauses.append(column.endswith(f"{SEPARATOR}{symbol}", autoescape=True))
    return clauses


def _search(q: str | None) -> list:
    """Case-insensitive substring match across the columns the UI searches."""
    needle = (q or "").strip()
    if not needle:
        return []
    pattern = _like_pattern(needle)
    return [
        or_(
            SymbolTicker.universal_ticker.ilike(pattern, escape="\\"),
            SymbolTicker.exch_ticker.ilike(pattern, escape="\\"),
            SymbolTicker.base.ilike(pattern, escape="\\"),
            SymbolTicker.quote.ilike(pattern, escape="\\"),
        )
    ]


def _like_pattern(value: str) -> str:
    """``%value%`` with LIKE metacharacters escaped for ``escape='\\'``."""
    escaped = (
        value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    )
    return f"%{escaped}%"
