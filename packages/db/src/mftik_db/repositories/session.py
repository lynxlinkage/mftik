"""Repositories for per-domain session tables."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any, Generic, TypeVar

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from mftik_db.models.session import (
    MdSessionRow,
    SessionStatus,
    StsSessionRow,
    TdSessionRow,
)
from mftik_db.repositories.base import BaseRepository

RowT = TypeVar("RowT")


class _SessionListMixin(BaseRepository[RowT], Generic[RowT]):
    async def mark_done(self, *args: Any, **kwargs: Any) -> RowT | None:
        raise NotImplementedError

    async def list_sessions(
        self,
        *,
        status: str | Sequence[str] | None = SessionStatus.LIVE.value,
        created_by: int | None = None,
        limit: int = 100,
    ) -> Sequence[RowT]:
        # Callers that need to see everything in a status must pass a limit
        # large enough to say so — the default silently truncates.

        stmt = select(self.model).order_by(self.model.created_at.desc())  # type: ignore[attr-defined]
        if status is not None:
            # ``str`` is a Sequence of characters — check it first or
            # ``status="done"`` becomes ``IN ('d','o','n','e')``.
            if isinstance(status, str):
                stmt = stmt.where(self.model.status == status)  # type: ignore[attr-defined]
            else:
                values = list(status)
                if len(values) == 1:
                    stmt = stmt.where(self.model.status == values[0])  # type: ignore[attr-defined]
                elif values:
                    stmt = stmt.where(self.model.status.in_(values))  # type: ignore[attr-defined]
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

    async def list_sessions(
        self,
        *,
        status: str | Sequence[str] | None = SessionStatus.LIVE.value,
        created_by: int | None = None,
        limit: int = 100,
        before_session: str | None = None,
    ) -> Sequence[StsSessionRow]:
        """STS list, newest first, optionally older than ``before_session``.

        Overrides the mixin: ``session_id`` is unique here, so it is a total
        order with ``created_at``. ``td_sessions`` is one row per
        ``(session_id, api_id)`` — the same cursor would not be.

        An unknown ``before_session`` matches nothing (the subquery is
        NULL). That is not the first page. The handler turns it into 422
        so a deleted user is not read as the end of history.
        """
        stmt = select(StsSessionRow).order_by(
            StsSessionRow.created_at.desc(),
            StsSessionRow.session_id.desc(),
        )
        if status is not None:
            if isinstance(status, str):
                stmt = stmt.where(StsSessionRow.status == status)
            else:
                values = list(status)
                if not values:
                    # An empty union is "none of these", not "skip the filter".
                    return []
                if len(values) == 1:
                    stmt = stmt.where(StsSessionRow.status == values[0])
                else:
                    stmt = stmt.where(StsSessionRow.status.in_(values))
        if created_by is not None:
            stmt = stmt.where(StsSessionRow.created_by == created_by)
        if before_session is not None:
            anchor = (
                select(StsSessionRow.created_at)
                .where(StsSessionRow.session_id == before_session)
                .scalar_subquery()
            )
            stmt = stmt.where(
                (StsSessionRow.created_at < anchor)
                | (
                    (StsSessionRow.created_at == anchor)
                    & (StsSessionRow.session_id < before_session)
                )
            )
        stmt = stmt.limit(limit)
        result = await self.session.execute(stmt)
        return result.scalars().all()

    async def create_live(
        self,
        *,
        session_id: str,
        created_by: int,
        strategy: str | None = None,
        type: str | None = None,
        yaml_text: str | None = None,
        td_api_ids: list[int] | None = None,
        md_ids: list[str] | None = None,
        st_paras: dict[str, Any] | None = None,
        cid_slot: int | None = None,
        restart: str = "always",
    ) -> StsSessionRow:
        row = StsSessionRow(
            session_id=session_id,
            created_by=created_by,
            strategy=strategy,
            type=type,
            yaml_text=yaml_text,
            td_api_ids=list(td_api_ids or []),
            md_ids=list(md_ids or []),
            st_paras=dict(st_paras or {}),
            cid_slot=cid_slot,
            restart=restart,
            rebuild_count=0,
            st_facts={},
            status=SessionStatus.LIVE.value,
        )
        return await self.add(row)

    async def mark_finished(
        self,
        session_id: str,
        *,
        status: str = SessionStatus.DONE.value,
        reason: str | None = None,
    ) -> StsSessionRow | None:
        """Move a session to a terminal status, recording why it ended.

        ``reason`` is kept for any terminal status, but only ``failed`` is
        expected to carry one — a natural exit has nothing to explain.
        """
        row = await self.get_by_session_id(session_id)
        if row is None:
            return None
        row.status = status
        row.reason = reason[:256] if reason else None
        row.finished_at = datetime.now(UTC)
        await self.session.flush()
        return row

    async def mark_done(self, session_id: str) -> StsSessionRow | None:
        """Natural end — see :meth:`mark_finished` for the failed path."""
        return await self.mark_finished(
            session_id, status=SessionStatus.DONE.value
        )

    async def mark_live(self, session_id: str) -> StsSessionRow | None:
        """Put a terminal session back to ``live`` — the rebuild path.

        Clears ``finished_at`` and ``reason`` along with the status: a session
        that is running again has no end and no reason for one, and leaving
        either behind would describe a row that ended and is also live.
        """
        row = await self.get_by_session_id(session_id)
        if row is None:
            return None
        row.status = SessionStatus.LIVE.value
        row.finished_at = None
        row.reason = None
        await self.session.flush()
        return row

    async def bump_rebuild_count(self, session_id: str) -> int:
        """Count one rebuild attempt and return the new total.

        Written before the attempt, not after: a rebuild that takes the
        process down with it has to count, because that is precisely the loop
        the cap exists to break.
        """
        row = await self.get_by_session_id(session_id)
        if row is None:
            return 0
        row.rebuild_count = int(row.rebuild_count or 0) + 1
        await self.session.flush()
        return row.rebuild_count

    async def remember(
        self, session_id: str, key: str, value: str
    ) -> StsSessionRow | None:
        """Record one fact a strategy established while running.

        Reassigns the dict rather than mutating it: a plain JSON column does
        not track in-place changes, so an update written through the existing
        object would be silently dropped.
        """
        row = await self.get_by_session_id(session_id)
        if row is None:
            return None
        row.st_facts = {**(row.st_facts or {}), key: value}
        await self.session.flush()
        return row

    async def mark_failed(
        self, session_id: str, reason: str
    ) -> StsSessionRow | None:
        """Terminal end that was not a natural one."""
        return await self.mark_finished(
            session_id, status=SessionStatus.FAILED.value, reason=reason
        )

    _ACKABLE = frozenset(
        {SessionStatus.FAILED.value, SessionStatus.INTERRUPTED.value}
    )

    async def mark_ack(self, session_id: str) -> StsSessionRow | None:
        """Operator acknowledgement of a failed or interrupted session.

        Turns an abnormal stop into a normal one without rewriting why it
        ended or when. Returns ``None`` when the row is missing or is not
        in a status that can be acked — the caller distinguishes those.
        """
        row = await self.get_by_session_id(session_id)
        if row is None:
            return None
        if row.status not in self._ACKABLE:
            return None
        row.status = SessionStatus.ACK.value
        await self.session.flush()
        return row

    async def list_live_for_origin(self, origin: str) -> Sequence[StsSessionRow]:
        """Sessions whose type is ``{origin}::…`` and that are still live.

        ``type`` is the qualified key (``node1::Tiny``). Matching on
        ``{origin}::`` so ``node1`` does not catch ``node10``. Paused sessions
        stay ``live``.
        """
        prefix = f"{origin}::"
        stmt = (
            select(StsSessionRow)
            .where(StsSessionRow.status == SessionStatus.LIVE.value)
            .where(StsSessionRow.type.startswith(prefix))
            .order_by(StsSessionRow.created_at.desc())
        )
        result = await self.session.execute(stmt)
        rows = result.scalars().all()
        # ``startswith`` is the SQL prefilter; refuse a type with extra ``::``.
        return [
            row
            for row in rows
            if row.type is not None
            and row.type.startswith(prefix)
            and "::" not in row.type[len(prefix) :]
        ]


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

    async def attach_live(
        self, *, session_id: str, created_by: int, api_id: int
    ) -> TdSessionRow:
        """Record this attach as live, reusing the row if the pair had one.

        ``(session_id, api_id)`` is unique and detaching only marks the row
        done, so a pair that attaches, detaches and attaches again cannot
        insert a second row. That sequence never came up while every deploy
        minted a fresh session id; rebuilding one reuses it, and the insert
        fails on the unique constraint.
        """
        result = await self.session.execute(
            select(TdSessionRow).where(
                TdSessionRow.session_id == session_id,
                TdSessionRow.api_id == api_id,
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return await self.create_live(
                session_id=session_id, created_by=created_by, api_id=api_id
            )
        row.status = SessionStatus.LIVE.value
        row.finished_at = None
        await self.session.flush()
        return row

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

    async def attach_live(
        self, *, venue: str, session_id: str, created_by: int
    ) -> MdSessionRow:
        """Record this attach as live, reusing the row if the pair had one.

        Same reason as :meth:`TdSessionRepository.attach_live` — ``(venue,
        session_id)`` is unique and a detach only marks the row done.
        """
        result = await self.session.execute(
            select(MdSessionRow).where(
                MdSessionRow.venue == venue,
                MdSessionRow.session_id == session_id,
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return await self.create_live(
                venue=venue, session_id=session_id, created_by=created_by
            )
        row.status = SessionStatus.LIVE.value
        row.finished_at = None
        await self.session.flush()
        return row

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
