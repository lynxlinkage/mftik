"""Sym persistence helpers over the symbol plane tables."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from mft_db.models.symbol import SymbolFilter, SymbolTicker
from mft_db.repositories import SymbolRepository
from mft_db.session import session_scope


async def upsert_instrument(**kwargs: Any) -> SymbolTicker:
    async with session_scope() as db:
        return await SymbolRepository(db).upsert(**kwargs)


async def deactivate_missing(
    *, venue: str, category: str, keep: set[str]
) -> int:
    async with session_scope() as db:
        return await SymbolRepository(db).deactivate_missing(
            venue=venue, category=category, keep=keep
        )


async def list_tickers(
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
    async with session_scope() as db:
        return list(
            await SymbolRepository(db).list_tickers(
                universal_ticker=universal_ticker,
                venue=venue,
                category=category,
                symbol=symbol,
                active_only=active_only,
                q=q,
                limit=limit,
                offset=offset,
            )
        )


async def count_tickers(
    *,
    universal_ticker: str | None = None,
    venue: str | None = None,
    category: str | None = None,
    symbol: str | None = None,
    active_only: bool = True,
    q: str | None = None,
) -> int:
    async with session_scope() as db:
        return await SymbolRepository(db).count_tickers(
            universal_ticker=universal_ticker,
            venue=venue,
            category=category,
            symbol=symbol,
            active_only=active_only,
            q=q,
        )


async def list_filters(ticker_id: int) -> Sequence[SymbolFilter]:
    async with session_scope() as db:
        return list(await SymbolRepository(db).list_filters(ticker_id))


async def list_filters_for(
    ticker_ids: Sequence[int],
    *,
    names: Sequence[str] | None = None,
) -> dict[int, list[SymbolFilter]]:
    async with session_scope() as db:
        return await SymbolRepository(db).list_filters_for(
            ticker_ids, names=names
        )


__all__ = [
    "count_tickers",
    "deactivate_missing",
    "list_filters",
    "list_filters_for",
    "list_tickers",
    "upsert_instrument",
]
