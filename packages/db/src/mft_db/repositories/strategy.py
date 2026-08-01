"""Repository for deployed strategy.yml rows."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from mft_db.models.strategy import StrategyRow
from mft_db.repositories.base import BaseRepository


class StrategyRepository(BaseRepository[StrategyRow]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, StrategyRow)

    async def create(
        self,
        *,
        type: str,
        config: dict[str, Any] | None = None,
        created_by: int,
        sts_session: str,
    ) -> StrategyRow:
        row = StrategyRow(
            type=type,
            config=dict(config or {}),
            created_by=created_by,
            sts_session=sts_session,
        )
        return await self.add(row)

    async def list_with_session(
        self,
        *,
        limit: int = 100,
    ) -> Sequence[StrategyRow]:
        stmt = (
            select(StrategyRow)
            .options(joinedload(StrategyRow.session))
            .order_by(StrategyRow.created_at.desc())
            .limit(limit)
        )
        result = await self.session.execute(stmt)
        return result.scalars().unique().all()

    async def get_by_sts_session(self, sts_session: str) -> StrategyRow | None:
        result = await self.session.execute(
            select(StrategyRow)
            .options(joinedload(StrategyRow.session))
            .where(StrategyRow.sts_session == sts_session)
        )
        return result.scalars().unique().one_or_none()
