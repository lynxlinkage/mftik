"""STS session manager — independent strategy sessions."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from mft.broker import Broker
from mft.protocol import (
    ListSessionsRequest,
    SessionInfo,
    StsCreateSessionRequest,
    StsCreateSessionResult,
    StsSessionControlResult,
)
from mft_db.models.session import SessionDomain, SessionStatus

from mft_sts.impl import resolve as resolve_strategy
from mft_sts.session.session import StsSession
from mft_sts.strategy import Strategy

logger = logging.getLogger(__name__)

PersistLive = Callable[..., Awaitable[Any]]
MarkDone = Callable[[str], Awaitable[Any]]
ListDbSessions = Callable[..., Awaitable[Sequence[Any]]]
StrategyFactory = Callable[[str | None], Strategy]


class SessionManager:
    """Owns STS sessions. Each binds exactly one Strategy (1-1)."""

    def __init__(
        self,
        broker: Broker,
        *,
        persist_live: PersistLive | None = None,
        mark_done: MarkDone | None = None,
        list_db_sessions: ListDbSessions | None = None,
        heartbeat_interval: float = 1.0,
        strategy_factory: StrategyFactory | None = None,
    ) -> None:
        self._broker = broker
        self._persist_live = persist_live
        self._mark_done = mark_done
        self._list_db_sessions = list_db_sessions
        self._heartbeat_interval = heartbeat_interval
        self._strategy_factory = strategy_factory or resolve_strategy
        self._sessions: dict[str, StsSession] = {}

    def get(self, session_id: str) -> StsSession | None:
        return self._sessions.get(session_id)

    @property
    def active_session_ids(self) -> list[str]:
        return list(self._sessions)

    async def create_session(
        self, request: StsCreateSessionRequest
    ) -> StsCreateSessionResult:
        if request.session_id in self._sessions:
            raise KeyError(f"sts session already exists: {request.session_id}")

        strategy = self._strategy_factory(request.strategy)
        session = StsSession(
            session_id=request.session_id,
            broker=self._broker,
            created_by=request.created_by,
            strategy=strategy,
            td_api_ids=list(request.td),
            md_ids=list(request.md),
            st_paras=dict(request.st_paras),
            heartbeat_interval=self._heartbeat_interval,
        )
        await session.start()

        if self._persist_live is not None:
            await self._persist_live(
                session_id=request.session_id,
                created_by=request.created_by,
                strategy=strategy.name,
                td_api_ids=list(request.td),
                md_ids=list(request.md),
                st_paras=dict(request.st_paras),
            )

        self._sessions[request.session_id] = session
        logger.info(
            "STS session created id=%s strategy=%s td=%s",
            request.session_id,
            strategy.name,
            request.td,
        )
        return StsCreateSessionResult(
            session_id=request.session_id,
            strategy=strategy.name,
            td=list(request.td),
        )

    async def list_sessions(
        self, request: ListSessionsRequest
    ) -> list[SessionInfo]:
        if request.domain not in (None, SessionDomain.STS.value, "sts"):
            return []

        if self._list_db_sessions is not None:
            db_rows = await self._list_db_sessions(
                status=request.status,
                created_by=request.created_by,
            )
            out: list[SessionInfo] = []
            for row in db_rows:
                live = self._sessions.get(row.session_id)
                out.append(
                    SessionInfo(
                        session_id=row.session_id,
                        domain=SessionDomain.STS.value,
                        created_by=row.created_by,
                        created_at=(
                            row.created_at.timestamp() if row.created_at else 0.0
                        ),
                        finished_at=(
                            row.finished_at.timestamp() if row.finished_at else None
                        ),
                        status=row.status,
                        api_id=None,
                        sts_session_id=row.session_id,
                        strategy=(
                            live.strategy_name
                            if live is not None
                            else getattr(row, "strategy", None)
                        ),
                        paused=live.strategy.paused if live is not None else None,
                    )
                )
            return out

        rows: list[SessionInfo] = []
        for session in self._sessions.values():
            if (
                request.created_by is not None
                and session.created_by != request.created_by
            ):
                continue
            if request.status not in (None, SessionStatus.LIVE.value, "live"):
                continue
            rows.append(
                SessionInfo(
                    session_id=session.session_id,
                    domain=SessionDomain.STS.value,
                    created_by=session.created_by,
                    created_at=0.0,
                    finished_at=None,
                    status=SessionStatus.LIVE.value,
                    sts_session_id=session.session_id,
                    strategy=session.strategy_name,
                    paused=session.strategy.paused,
                )
            )
        return rows

    async def pause(self, session_id: str) -> StsSessionControlResult:
        session = self._sessions.get(session_id)
        if session is None:
            raise KeyError(f"no active sts session {session_id}")
        await session.pause()
        return StsSessionControlResult(
            session_id=session_id,
            status=SessionStatus.LIVE.value,
            paused=session.strategy.paused,
            strategy=session.strategy_name,
        )

    async def resume(self, session_id: str) -> StsSessionControlResult:
        session = self._sessions.get(session_id)
        if session is None:
            raise KeyError(f"no active sts session {session_id}")
        await session.resume()
        return StsSessionControlResult(
            session_id=session_id,
            status=SessionStatus.LIVE.value,
            paused=session.strategy.paused,
            strategy=session.strategy_name,
        )

    async def stop_session(self, session_id: str) -> StsSessionControlResult:
        session = self._sessions.get(session_id)
        if session is None:
            raise KeyError(f"no active sts session {session_id}")
        strategy = session.strategy_name
        await self.close(session_id)
        return StsSessionControlResult(
            session_id=session_id,
            status=SessionStatus.DONE.value,
            paused=False,
            strategy=strategy,
        )

    async def close(self, session_id: str) -> None:
        session = self._sessions.pop(session_id, None)
        if session is None:
            return
        await session.stop()
        if self._mark_done is not None:
            await self._mark_done(session_id)
        logger.info("STS session closed id=%s", session_id)

    async def close_all(self) -> None:
        for session_id in list(self._sessions):
            await self.close(session_id)
