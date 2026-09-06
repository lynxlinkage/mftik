"""Repository for declared plane instances."""

from __future__ import annotations

from collections.abc import Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from mftik_db.models.instance import Instance
from mftik_db.repositories.base import BaseRepository


class InstanceRepository(BaseRepository[Instance]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, Instance)

    async def get_by_name(self, name: str) -> Instance | None:
        result = await self.session.execute(
            select(Instance).where(Instance.name == name)
        )
        return result.scalars().one_or_none()

    async def list_all(
        self, *, domain: str | None = None, limit: int = 200
    ) -> Sequence[Instance]:
        stmt = select(Instance).order_by(Instance.domain.asc(), Instance.name.asc())
        if domain is not None:
            stmt = stmt.where(Instance.domain == domain)
        result = await self.session.execute(stmt.limit(limit))
        return result.scalars().all()

    async def create(
        self,
        *,
        name: str,
        domain: str,
        region: str | None = None,
        created_by: int | None = None,
    ) -> Instance:
        return await self.add(
            Instance(
                name=name,
                domain=domain,
                region=region,
                created_by=created_by,
            )
        )

    async def update(
        self,
        instance: Instance,
        *,
        region: str | None = None,
        enabled: bool | None = None,
    ) -> Instance:
        """Change what nothing routes on.

        Deliberately cannot touch ``name`` or ``domain``. A process learns its
        name from ``MFTIK_INSTANCE`` in an environment this service cannot
        write, so a rename here would leave the row and the process disagreeing
        with nothing to reconcile them — see :class:`Instance`.
        """
        if region is not None:
            instance.region = region
        if enabled is not None:
            instance.enabled = enabled
        await self.session.flush()
        return instance

    async def delete(self, instance: Instance) -> None:
        """Retire an instance.

        ``apis.instance_id`` is ``ON DELETE RESTRICT``, so a credential still
        pointing here refuses this at the database. Rows that merely *record*
        an instance — ``md_sessions.instance``, ``sts_sessions.instance`` — are
        plain strings and do not, by design: they are history, and retiring an
        instance must not break the record of what it did.
        """
        await self.session.delete(instance)
        await self.session.flush()
