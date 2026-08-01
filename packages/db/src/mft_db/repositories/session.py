"""Repositories for per-domain session tables."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any, Generic, TypeVar

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from mft_db.models.session import (
    MdSessionRow,
    SessionStatus,
    StsSessionRow,
    TdSessionRow,
)
from mft_db.repositories.base import BaseRepository

RowT = TypeVar("RowT")


class _SessionListMixin(BaseRepository[RowT], Generic[RowT]):
    async def mark_done(self, *args: Any, **kwargs: Any) -> RowT | None:
        raise NotImplementedError

    async def list_sessions(
        self,
        *,
        status: str | None = SessionStatus.LIVE.value,
        created_by: int | None = None,
        limit: int = 100,
    ) -> Sequence[RowT]:
        stmt = select(self.model).order_by(self.model.created_at.desc())  # type: ignore[attr-defined]
        if status is not None:
            stmt = stmt.where(self.model.status == status)  # type: ignore[attr-defined]
        if created_by is not None:
            stmt = stmt.where(self.model.created_by == created_by)  # type: ignore[attr-defined]
        stmt = stmt.limit(limit)
        result = await self.session.execute(stmt)
        return result.scalars().all()

    async def count(self, *, status: str | None = None) -> int:
        stmt = select(func.count()).select_from(self.model)
        if status is not None:
            stmt = stmt.where(self.model.status == status)  # type: ignore[attr-defined]
        result = await self.session.execute(stmt)
        return int(result.scalar_one())


class StsSessionRepository(_SessionListMixin[StsSessionRow]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, StsSessionRow)

    async def get_by_session_id(self, session_id: str) -> StsSessionRow | None:
        return await self.session.get(StsSessionRow, session_id)

    async def create_live(
        self,
        *,
        session_id: str,
        created_by: int,
        strategy: str | None = None,
        td_api_ids: list[int] | None = None,
        md_ids: list[str] | None = None,
        st_paras: dict[str, Any] | None = None,
    ) -> StsSessionRow:
        row = StsSessionRow(
            session_id=session_id,
            created_by=created_by,
            strategy=strategy,
            td_api_ids=list(td_api_ids or []),
            md_ids=list(md_ids or []),
            st_paras=dict(st_paras or {}),
            status=SessionStatus.LIVE.value,
        )
        return await self.add(row)

    async def mark_done(self, session_id: str) -> StsSessionRow | None:
        row = await self.get_by_session_id(session_id)
        if row is None:
            return None
        row.status = SessionStatus.DONE.value
        row.finished_at = datetime.now(UTC)
        await self.session.flush()
        return row


class TdSessionRepository(_SessionListMixin[TdSessionRow]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, TdSessionRow)

    async def get_live(
        self, *, session_id: str, api_id: int
    ) -> TdSessionRow | None:
        result = await self.session.execute(
            select(TdSessionRow).where(
                TdSessionRow.session_id == session_id,
                TdSessionRow.api_id == api_id,
                TdSessionRow.status == SessionStatus.LIVE.value,
            )
        )
        return result.scalar_one_or_none()

    async def create_live(
        self,
        *,
        session_id: str,
        created_by: int,
        api_id: int,
    ) -> TdSessionRow:
        row = TdSessionRow(
            session_id=session_id,
            created_by=created_by,
            api_id=api_id,
            status=SessionStatus.LIVE.value,
        )
        return await self.add(row)

    async def mark_done(self, *, session_id: str, api_id: int) -> TdSessionRow | None:
        row = await self.get_live(session_id=session_id, api_id=api_id)
        if row is None:
            return None
        row.status = SessionStatus.DONE.value
        row.finished_at = datetime.now(UTC)
        await self.session.flush()
        return row

    async def count_live_for_api(self, api_id: int) -> int:
        result = await self.session.execute(
            select(func.count()).where(
                TdSessionRow.api_id == api_id,
                TdSessionRow.status == SessionStatus.LIVE.value,
            )
        )
        return int(result.scalar_one())


class MdSessionRepository(_SessionListMixin[MdSessionRow]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, MdSessionRow)

    async def get_live(
        self, *, venue: str, session_id: str
    ) -> MdSessionRow | None:
        result = await self.session.execute(
            select(MdSessionRow).where(
                MdSessionRow.venue == venue,
                MdSessionRow.session_id == session_id,
                MdSessionRow.status == SessionStatus.LIVE.value,
            )
        )
        return result.scalar_one_or_none()

    async def create_live(
        self,
        *,
        venue: str,
        session_id: str,
        created_by: int,
    ) -> MdSessionRow:
        row = MdSessionRow(
            venue=venue,
            session_id=session_id,
            created_by=created_by,
            status=SessionStatus.LIVE.value,
        )
        return await self.add(row)

    async def mark_done(
        self, *, venue: str, session_id: str
    ) -> MdSessionRow | None:
        row = await self.get_live(venue=venue, session_id=session_id)
        if row is None:
            return None
        row.status = SessionStatus.DONE.value
        row.finished_at = datetime.now(UTC)
        await self.session.flush()
        return row

    async def mark_done_session(self, session_id: str) -> list[MdSessionRow]:
        result = await self.session.execute(
            select(MdSessionRow).where(
                MdSessionRow.session_id == session_id,
                MdSessionRow.status == SessionStatus.LIVE.value,
            )
        )
        rows = list(result.scalars().all())
        now = datetime.now(UTC)
        for row in rows:
            row.status = SessionStatus.DONE.value
            row.finished_at = now
        if rows:
            await self.session.flush()
        return rows
