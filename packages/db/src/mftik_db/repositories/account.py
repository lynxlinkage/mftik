"""Repository for trading accounts (1-1 with apis)."""

from __future__ import annotations

from collections.abc import Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from mftik_db.models.account import Account
from mftik_db.repositories.base import BaseRepository


class AccountRepository(BaseRepository[Account]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, Account)

    async def get_by_api_id(self, api_id: int) -> Account | None:
        result = await self.session.execute(
            select(Account)
            .options(joinedload(Account.api))
            .where(Account.api_id == api_id)
        )
        return result.scalars().unique().one_or_none()

    async def get_by_name(self, name: str) -> Account | None:
        result = await self.session.execute(
            select(Account)
            .options(joinedload(Account.api))
            .where(Account.name == name)
        )
        return result.scalars().unique().one_or_none()

    async def list_with_api(self, *, limit: int = 200) -> Sequence[Account]:
        stmt = (
            select(Account)
            .options(joinedload(Account.api))
            .order_by(Account.id.asc())
            .limit(limit)
        )
        result = await self.session.execute(stmt)
        return result.scalars().unique().all()

    async def create(
        self,
        *,
        name: str,
        api_id: int,
        created_by: int,
    ) -> Account:
        row = Account(name=name, api_id=api_id, created_by=created_by)
        return await self.add(row)

    async def delete(self, account: Account) -> None:
        await self.session.delete(account)
        await self.session.flush()
