from __future__ import annotations

from collections.abc import Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from mftik_db.models.audit import Audit
from mftik_db.repositories.base import BaseRepository


class AuditRepository(BaseRepository[Audit]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, Audit)

    async def record(
        self,
        *,
        user_id: int,
        operation: str,
        result: str,
        via: str | None = None,
        key_id: int | None = None,
        key_kind: str | None = None,
    ) -> Audit:
        entry = Audit(
            user_id=user_id,
            operation=operation,
            result=result,
            via=via,
            key_id=key_id,
            key_kind=key_kind,
        )
        return await self.add(entry)

    async def list_by_user(
        self,
        user_id: int,
        *,
        limit: int = 100,
    ) -> Sequence[Audit]:
        result = await self.session.execute(
            select(Audit)
            .where(Audit.user_id == user_id)
            .order_by(Audit.created_at.desc())
            .limit(limit)
        )
        return result.scalars().all()

    async def list_recent(self, *, limit: int = 100) -> Sequence[Audit]:
        result = await self.session.execute(
            select(Audit).order_by(Audit.created_at.desc()).limit(limit)
        )
        return result.scalars().all()
