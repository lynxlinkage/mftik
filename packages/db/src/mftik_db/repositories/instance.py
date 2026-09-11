"""Repository for declared plane instances."""

from __future__ import annotations

from collections.abc import Sequence

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from mftik_db.models.api import Api
from mftik_db.models.instance import Instance
from mftik_db.models.session import (
    MdSessionRow,
    SessionDomain,
    SessionStatus,
    StsSessionRow,
)
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

    async def derived_sts(self, api_ids: Sequence[int]) -> str | None:
        """The unique enabled STS in the region those credentials share.

        ``None`` when there is no unique answer: no accounts, a missing
        credential, a missing or empty region, mixed regions, or that
        region not having exactly one enabled STS. Callers must not coin
        flip — a create names an instance, an existing null row stays
        interrupted.
        """
        ids = list(dict.fromkeys(int(i) for i in api_ids))
        if not ids:
            return None
        result = await self.session.execute(
            select(Instance.region)
            .join(Api, Api.instance_id == Instance.id)
            .where(Api.id.in_(ids))
        )
        regions = [row[0] for row in result.all()]
        if len(regions) != len(ids) or any(not region for region in regions):
            return None
        unique = set(regions)
        if len(unique) != 1:
            return None
        region = next(iter(unique))
        sts = await self.session.execute(
            select(Instance.name).where(
                Instance.domain == SessionDomain.STS.value,
                Instance.region == region,
                Instance.enabled.is_(True),
            )
        )
        names = list(sts.scalars().all())
        if len(names) != 1:
            return None
        return names[0]

    async def update(
        self,
        instance: Instance,
        *,
        region: str | None = None,
        enabled: bool | None = None,
    ) -> Instance:
        """Change the label and the drain flag.

        Deliberately cannot touch ``name`` or ``domain``. A process learns its
        name from ``MFTIK_INSTANCE`` in an environment this service cannot
        write, so a rename here would leave the row and the process disagreeing
        with nothing to reconcile them — see :class:`Instance`. ``region``
        places unnamed STS sessions, so editing a TD's region moves where
        those sessions come back.
        """
        if region is not None:
            instance.region = region
        if enabled is not None:
            instance.enabled = enabled
        await self.session.flush()
        return instance

    async def live_sessions_naming(self, instance: Instance) -> int:
        """How many live sessions still name this instance.

        The half the database cannot enforce. ``apis.instance_id`` is a
        foreign key and refuses a delete on its own, but
        ``md_sessions.instance`` and ``sts_sessions.instance`` are plain
        strings by design — they are history, and retiring an instance must
        not break the rows describing what it did. History is exactly what
        must not block a delete; a session that is *still running* is not.

        Which table depends on the domain, because a name belongs to one
        plane. Counting the other one would refuse a delete for a reason that
        is not true — an ``md_sessions`` row naming ``sts-tw`` describes some
        MD instance that happened to be called that, not this STS.

        TD has no row of its own to check: a TD attach is named by
        ``apis.instance_id``, and that foreign key already refuses.
        """
        if instance.domain == SessionDomain.MD.value:
            stmt = (
                select(func.count())
                .select_from(MdSessionRow)
                .where(
                    MdSessionRow.instance == instance.name,
                    MdSessionRow.status == SessionStatus.LIVE.value,
                )
            )
        elif instance.domain == SessionDomain.STS.value:
            stmt = (
                select(func.count())
                .select_from(StsSessionRow)
                .where(
                    StsSessionRow.instance == instance.name,
                    StsSessionRow.status == SessionStatus.LIVE.value,
                )
            )
        else:
            return 0
        return int((await self.session.execute(stmt)).scalar_one())

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
