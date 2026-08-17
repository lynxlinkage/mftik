from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from mftik_db.models.user import User
from mftik_db.repositories.base import BaseRepository


class UserRepository(BaseRepository[User]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, User)

    async def get_by_email(self, email: str) -> User | None:
        result = await self.session.execute(
            select(User).where(User.email == email)
        )
        return result.scalar_one_or_none()

    async def get_by_username(self, username: str) -> User | None:
        result = await self.session.execute(
            select(User).where(User.username == username)
        )
        return result.scalar_one_or_none()

    async def get_owner(self) -> User | None:
        """The instance's single user, whatever it is called.

        Lowest id rather than "the only row": a database that somehow grew a
        second user must still resolve to one Owner deterministically instead
        of raising, and the first one is the one every existing foreign key
        already points at.
        """
        result = await self.session.execute(
            select(User).order_by(User.id).limit(1)
        )
        return result.scalar_one_or_none()
