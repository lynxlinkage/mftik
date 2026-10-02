"""Repositories for per-domain session tables."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Generic, TypeVar

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from mftik_db.models.api import Api
from mftik_db.models.instance import Instance
from mftik_db.models.session import (
    MdSessionRow,
    SessionStatus,
    StsSessionRow,
    TdSessionRow,
)
from mftik_db.repositories.base import BaseRepository

RowT = TypeVar("RowT")


def _fold_instance_counts(rows: Sequence[Any]) -> dict[str, dict[str, int]]:
    """``(instance, status, n)`` rows → ``{instance: {status: n}}``.

    Drops a NULL instance rather than inventing a key: Home cards only show
    work that named one.
    """
    out: dict[str, dict[str, int]] = {}
    for instance, status, n in rows:
        if instance is None:
            continue
        out.setdefault(instance, {})[status] = int(n)
    return out


class _SessionListMixin(BaseRepository[RowT], Generic[RowT]):
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

    def _list_filters(
        self,
        stmt: Any,
        *,
        status: str | Sequence[str] | None,
        created_by: int | None,
    ) -> Any | None:
        """Apply the list's status/owner filters. ``None`` means match nothing."""
        if status is not None:
            if isinstance(status, str):
                stmt = stmt.where(StsSessionRow.status == status)
            else:
                values = list(status)
                if not values:
                    # An empty union is "none of these", not "skip the filter".
                    return None
                if len(values) == 1:
                    stmt = stmt.where(StsSessionRow.status == values[0])
                else:
                    stmt = stmt.where(StsSessionRow.status.in_(values))
        if created_by is not None:
            stmt = stmt.where(StsSessionRow.created_by == created_by)
        return stmt

    async def count_sessions(
        self,
        *,
        status: str | Sequence[str] | None = SessionStatus.LIVE.value,
        created_by: int | None = None,
    ) -> int:
        """How many STS rows match the list filter, ignoring limit/offset."""
        stmt = self._list_filters(
            select(func.count()).select_from(StsSessionRow),
            status=status,
            created_by=created_by,
        )
        if stmt is None:
            return 0
        result = await self.session.execute(stmt)
        return int(result.scalar_one())

    async def count_by_instance(self) -> dict[str, dict[str, int]]:
        """How many sessions each named instance has, by status.

        Unpinned rows (``instance`` is NULL) are omitted — a Home card only
        shows work that asked for that instance. A pin to a name that is no
        longer declared still lands under that name; the route does not turn
        it into a card.
        """
        stmt = (
            select(StsSessionRow.instance, StsSessionRow.status, func.count())
            .where(StsSessionRow.instance.is_not(None))
            .group_by(StsSessionRow.instance, StsSessionRow.status)
        )
        result = await self.session.execute(stmt)
        return _fold_instance_counts(result.all())

    async def list_sessions(
        self,
        *,
        status: str | Sequence[str] | None = SessionStatus.LIVE.value,
        created_by: int | None = None,
        limit: int = 100,
        offset: int = 0,
        before_session: str | None = None,
    ) -> Sequence[StsSessionRow]:
        """STS list, newest first.

        ``offset`` pages a numbered browse — the only paging any caller in
        the tree uses today. ``before_session`` is the keyset cursor kept
        for a caller that cannot tolerate rows shifting under it: unknown
        matches nothing (the subquery is NULL) rather than falling back to
        the first page, so whoever revives it must say what that means.

        Overrides the mixin: ``session_id`` is unique here, so it is a total
        order with ``created_at``. ``td_sessions`` is one row per
        ``(session_id, api_id)`` — the same cursor would not be.
        """
        stmt = self._list_filters(
            select(StsSessionRow).order_by(
                StsSessionRow.created_at.desc(),
                StsSessionRow.session_id.desc(),
            ),
            status=status,
            created_by=created_by,
        )
        if stmt is None:
            return []
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
        if offset:
            stmt = stmt.offset(offset)
        stmt = stmt.limit(limit)
        result = await self.session.execute(stmt)
        return result.scalars().all()

    async def create_live(
        self,
        *,
        session_id: str,
        created_by: int,
        type: str | None = None,
        yaml_text: str | None = None,
        td: dict[str, Any] | None = None,
        md_ids: list[str] | dict[str, list[str]] | None = None,
        st_paras: dict[str, Any] | None = None,
        restart: str = "never",
        instance: str | None = None,
        strategy_digest: str | None = None,
        env_generation: int | None = None,
    ) -> StsSessionRow:
        """Insert a live session.

        ``restart`` defaults to ``never`` (F11). The column also stores
        ``on_failure``, and a historical ``always`` if a caller still
        passes one. ``strategy_digest`` and ``env_generation`` default
        to null so a caller that has not resolved a pin leaves the
        columns empty.
        """
        row = StsSessionRow(
            session_id=session_id,
            created_by=created_by,
            type=type,
            yaml_text=yaml_text,
            instance=instance,
            td=dict(td or {}),
            md_ids=md_ids if md_ids is not None else [],
            st_paras=dict(st_paras or {}),
            restart=restart,
            status=SessionStatus.LIVE.value,
            strategy_digest=strategy_digest,
            env_generation=env_generation,
        )
        return await self.add(row)

    async def list_nonterminal(self) -> Sequence[StsSessionRow]:
        """Rows whose status is not :meth:`SessionStatus.terminal`.

        ``done``, ``failed``, ``interrupted`` and ``ack`` are terminal.
        ``live`` and any later non-terminal word stay. Order is
        ``session_id``.
        """
        terminal = tuple(SessionStatus.terminal())
        stmt = (
            select(StsSessionRow)
            .where(StsSessionRow.status.notin_(terminal))
            .order_by(StsSessionRow.session_id)
        )
        result = await self.session.execute(stmt)
        return result.scalars().all()

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
        """Put a terminal session back to ``live``.

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

    async def record_status(
        self,
        session_id: str,
        *,
        status: str,
        reason: str | None,
        finished_at: float | None,
        observed_generation: int | None,
        worker_incarnation: int | None,
        conditions: Mapping[str, str],
        restart_count: int,
    ) -> StsSessionRow | None:
        """Write the Status columns. A missing row is left alone.

        The API inserts the row. This method does not. ``finished_at`` of
        ``0`` is a real timestamp. Live writes do not clear ``reason`` or
        ``finished_at``: a session that is still running has not ended,
        and a later terminal write is what sets them.
        """
        row = await self.get_by_session_id(session_id)
        if row is None:
            return None
        row.status = status
        row.observed_generation = observed_generation
        row.worker_incarnation = worker_incarnation
        row.conditions = dict(conditions)
        row.restart_count = restart_count
        if status in (SessionStatus.DONE.value, SessionStatus.FAILED.value):
            row.reason = reason[:256] if reason else None
            if finished_at is None:
                row.finished_at = datetime.now(UTC)
            else:
                row.finished_at = datetime.fromtimestamp(finished_at, UTC)
        await self.session.flush()
        return row

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
        ``{origin}::`` so ``node1`` does not catch ``node10``.
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
    """Reads of ``td_sessions``.

    B10-01 stopped writing this table (F38). The rows are history from
    before intents. ``list_sessions``, ``count``, ``count_by_instance``
    and ``count_live_for_api`` stay; nothing here inserts or updates.
    """

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, TdSessionRow)

    async def count_live_for_api(self, api_id: int) -> int:
        result = await self.session.execute(
            select(func.count()).where(
                TdSessionRow.api_id == api_id,
                TdSessionRow.status == SessionStatus.LIVE.value,
            )
        )
        return int(result.scalar_one())

    async def count_by_instance(self) -> dict[str, dict[str, int]]:
        """How many attaches each TD instance has, by status.

        ``td_sessions`` has no instance column. The credential's
        ``apis.instance_id`` is the same fact rebuild routing reads — a
        snapshot taken at deploy time would send a later count to the
        instance that used to be allowed to use it.
        """
        stmt = (
            select(Instance.name, TdSessionRow.status, func.count())
            .select_from(TdSessionRow)
            .join(Api, Api.id == TdSessionRow.api_id)
            .join(Instance, Instance.id == Api.instance_id)
            .group_by(Instance.name, TdSessionRow.status)
        )
        result = await self.session.execute(stmt)
        return _fold_instance_counts(result.all())


class MdSessionRepository(_SessionListMixin[MdSessionRow]):
    """Reads of ``md_sessions``.

    B10-01 stopped writing this table (F38), same as
    :class:`TdSessionRepository`. ``list_sessions``, ``count`` and
    ``count_by_instance`` stay.
    """

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, MdSessionRow)

    async def count_by_instance(self) -> dict[str, dict[str, int]]:
        """How many attaches each MD instance has, by status."""
        stmt = select(
            MdSessionRow.instance, MdSessionRow.status, func.count()
        ).group_by(MdSessionRow.instance, MdSessionRow.status)
        result = await self.session.execute(stmt)
        return _fold_instance_counts(result.all())
