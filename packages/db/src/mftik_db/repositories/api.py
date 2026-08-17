from __future__ import annotations

from collections.abc import Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from mftik_db.models.api import Api
from mftik_db.repositories.base import BaseRepository


class ApiRepository(BaseRepository[Api]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, Api)

    async def get_by_api_key(self, api_key: str) -> Api | None:
        result = await self.session.execute(
            select(Api).where(Api.api_key == api_key)
        )
        return result.scalar_one_or_none()

    async def list_all(self) -> Sequence[Api]:
        result = await self.session.execute(select(Api).order_by(Api.id.asc()))
        return result.scalars().all()

    async def list_by_owner(self, owner_id: int) -> Sequence[Api]:
        result = await self.session.execute(
            select(Api)
            .where(Api.owner_id == owner_id)
            .order_by(Api.created_at.desc())
        )
        return result.scalars().all()

    async def list_by_owner_and_venue(
        self, owner_id: int, venue: str
    ) -> Sequence[Api]:
        result = await self.session.execute(
            select(Api)
            .where(Api.owner_id == owner_id, Api.venue == venue)
            .order_by(Api.created_at.desc())
        )
        return result.scalars().all()

    async def delete(self, api: Api) -> None:
        await self.session.delete(api)
        await self.session.flush()
