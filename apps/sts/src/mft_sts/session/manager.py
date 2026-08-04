"""STS session manager — independent strategy sessions."""

from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from mft.broker import Broker
from mft.protocol import (
    STS_SESSION_STATUS,
    ListSessionsRequest,
    SessionInfo,
    StsCreateSessionRequest,
    StsCreateSessionResult,
    StsSessionControlResult,
    StsSessionStatus,
    StsSessionStatusEnvelope,
    Topics,
    publish_sts_log,
)
from mft_db.models.session import SessionDomain, SessionStatus

from mft_sts.client_order_id import SLOT_SPACE
from mft_sts.impl import resolve as resolve_strategy
from mft_sts.liveness import clear_alive, is_alive, mark_alive
from mft_sts.session.session import StsSession
from mft_sts.strategy import Strategy

logger = logging.getLogger(__name__)

#: Redis counter backing cid slot allocation (suffixed onto the key prefix).
CID_SLOT_KEY = "cid:slot"

#: How many status events the replay buffer keeps, and for how long. Sized for
#: "what happened recently", not history — the DB is the record, this only has
#: to cover the gap between a page loading and its socket being live.
_STATUS_BUFFER = 200
_STATUS_TTL_SECONDS = 3600

PersistLive = Callable[..., Awaitable[Any]]
#: ``(session_id, *, status, reason)`` — move the row to a terminal status.
MarkDone = Callable[..., Awaitable[Any]]
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

    async def _publish_status(
        self,
        session_id: str,
        *,
        status: str,
        paused: bool = False,
        strategy: str | None = None,
        reason: str | None = None,
        created_by: int | None = None,
    ) -> None:
        """Announce a session's state on the shared status channel.

        Always called *after* the DB write, never before: a UI that reacts to
        the event by re-reading REST must not be able to read the old row.

        Published through ``publish_log`` for its ring buffer — plain pub/sub
        drops everything sent while no browser is connected, and a page that
        loads a second after a session failed would never hear about it. The
        bridge replays the buffer on connect.
        """
        terminal = status != SessionStatus.LIVE.value
        payload = StsSessionStatus(
            session_id=session_id,
            status=status,
            paused=paused,
            strategy=strategy,
            reason=reason,
            created_by=created_by,
            finished_at=time.time() if terminal else None,
        )
        envelope = StsSessionStatusEnvelope.wrap(
            payload,
            type=STS_SESSION_STATUS,
            source="sts",
            session_id=session_id,
        )
        try:
            await self._broker.publish_log(
                Topics.status_sts(),
                envelope,
                maxlen=_STATUS_BUFFER,
                ttl_seconds=_STATUS_TTL_SECONDS,
            )
        except Exception:
            # The row is already written, so the UI recovers on its next load.
            # Never let a status announcement take the session down with it.
            logger.exception(
                "STS status publish failed session=%s status=%s",
                session_id,
                status,
            )

    @property
    def active_session_ids(self) -> list[str]:
        return list(self._sessions)

    async def _allocate_cid_slot(self) -> int:
        """Reserve the 16-bit ``client_order_id`` slot for a new session.

        Allocation goes through Redis, not a process-local counter: STS serves
        RPC via ``BLPOP`` competing consumers, so several STS processes may be
        creating sessions at once.

        The counter is monotonic and wraps at :data:`SLOT_SPACE`, so two live
        sessions share a slot only if 65536 sessions were created while one of
        them was still running. Reuse after that is harmless — TD keys order
        ownership on the whole cid, whose ``ts_ms`` differs.
        """
        key = f"{self._broker.config.key_prefix}:{CID_SLOT_KEY}"
        return int(await self._broker.redis.incr(key)) % SLOT_SPACE

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
            cid_slot=await self._allocate_cid_slot(),
            td_api_ids=list(request.td),
            md_ids=list(request.md),
            st_paras=dict(request.st_paras),
            heartbeat_interval=self._heartbeat_interval,
            on_exit=self._on_session_exit,
        )
        # Register before start so Strategy.exit() during on_start/on_ready works.
        self._sessions[request.session_id] = session
        # Claim liveness before the row exists, not after: a reaper that saw a
        # live row with no key would read it as an orphan and fail a session
        # that is only a moment old.
        await mark_alive(self._broker, request.session_id)
        # Persist before start, not after: a strategy that ends inside
        # on_start / on_ready reaches close() before start() returns, and a
        # row written afterwards would resurrect it as live forever.
        if self._persist_live is not None:
            await self._persist_live(
                session_id=request.session_id,
                created_by=request.created_by,
                strategy=strategy.name,
                td_api_ids=list(request.td),
                md_ids=list(request.md),
                st_paras=dict(request.st_paras),
            )
        try:
            await session.start()
        except Exception as exc:
            self._sessions.pop(request.session_id, None)
            reason = f"start failed: {exc}"
            if self._mark_done is not None:
                await self._mark_done(
                    request.session_id,
                    status=SessionStatus.FAILED.value,
                    reason=reason,
                )
            await clear_alive(self._broker, request.session_id)
            await self._publish_status(
                request.session_id,
                status=SessionStatus.FAILED.value,
                strategy=strategy.name,
                created_by=request.created_by,
                reason=reason,
            )
            raise

        # A strategy that ended inside on_start / on_ready is already gone and
        # has announced its own terminal status — do not follow it with "live".
        if request.session_id in self._sessions:
            await self._publish_status(
                request.session_id,
                status=SessionStatus.LIVE.value,
                strategy=strategy.name,
                created_by=request.created_by,
            )
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
                        reason=getattr(row, "reason", None),
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
        await self._publish_status(
            session_id,
            status=SessionStatus.LIVE.value,
            paused=session.strategy.paused,
            strategy=session.strategy_name,
            created_by=session.created_by,
        )
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
        await self._publish_status(
            session_id,
            status=SessionStatus.LIVE.value,
            paused=session.strategy.paused,
            strategy=session.strategy_name,
            created_by=session.created_by,
        )
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

    async def close(
        self,
        session_id: str,
        *,
        status: str = SessionStatus.DONE.value,
        reason: str | None = None,
    ) -> None:
        session = self._sessions.pop(session_id, None)
        if session is None:
            return
        broker = session.broker
        strategy_name = session.strategy_name
        created_by = session.created_by
        await session.stop()
        if self._mark_done is not None:
            await self._mark_done(session_id, status=status, reason=reason)
        try:
            await clear_alive(broker, session_id)
        except Exception:
            logger.exception(
                "STS liveness release failed session=%s", session_id
            )
        await self._publish_status(
            session_id,
            status=status,
            strategy=strategy_name,
            reason=reason,
            created_by=created_by,
        )
        if status == SessionStatus.FAILED.value:
            # The row carries the reason for later, but the operator watching
            # the live stream should not have to reload the page to see it.
            try:
                await publish_sts_log(
                    broker,
                    session_id,
                    f"session failed: {reason or 'no reason given'}",
                    source="sts",
                    level="error",
                )
            except Exception:
                logger.exception(
                    "STS failure log publish failed session=%s", session_id
                )
        logger.info(
            "STS session closed id=%s status=%s reason=%s",
            session_id,
            status,
            reason or "—",
        )

    async def _on_session_exit(
        self, session_id: str, reason: str, failed: bool = False
    ) -> None:
        """Handle :meth:`Strategy.exit` / :meth:`Strategy.fail` — tear down."""
        logger.info(
            "STS strategy exit id=%s failed=%s reason=%s",
            session_id,
            failed,
            reason or "—",
        )
        if failed:
            await self.close(
                session_id, status=SessionStatus.FAILED.value, reason=reason
            )
        else:
            await self.close(session_id)

    async def reap_orphans(self) -> list[str]:
        """Fail rows left ``live`` by a process that died without a word.

        Every other ending writes its own row. This covers only the one that
        cannot: the process vanishing outright, where the row keeps claiming a
        session is running and nothing else will ever say otherwise.

        A row is an orphan when nobody holds its liveness key. That test is
        what makes this safe to run in every STS process at once — several
        serve the same RPC subject, so "not mine" says nothing about whether a
        peer is running it, while "no key" is true for all of them at once.

        Returns the session ids reaped, for logging and tests.
        """
        if self._list_db_sessions is None or self._mark_done is None:
            return []
        try:
            rows = await self._list_db_sessions(
                status=SessionStatus.LIVE.value, created_by=None
            )
        except Exception:
            logger.exception("STS orphan scan failed to list sessions")
            return []

        reaped: list[str] = []
        for row in rows:
            session_id = getattr(row, "session_id", None)
            if session_id is None or session_id in self._sessions:
                continue
            try:
                if await is_alive(self._broker, session_id):
                    continue
            except Exception:
                # Unreadable liveness is not evidence of death. Leaving a
                # stale row is recoverable; failing a running strategy is not.
                logger.exception(
                    "STS liveness check failed session=%s", session_id
                )
                continue

            reason = "process died: no session heartbeat"
            try:
                await self._mark_done(
                    session_id,
                    status=SessionStatus.FAILED.value,
                    reason=reason,
                )
            except Exception:
                logger.exception("STS orphan reap failed session=%s", session_id)
                continue
            await self._publish_status(
                session_id,
                status=SessionStatus.FAILED.value,
                strategy=getattr(row, "strategy", None),
                reason=reason,
                created_by=getattr(row, "created_by", None),
            )
            reaped.append(session_id)
            logger.warning(
                "STS reaped orphaned session id=%s strategy=%s",
                session_id,
                getattr(row, "strategy", None),
            )
        return reaped

    async def close_all(self) -> None:
        """Shut every session down, recording the terminal status first.

        Order matters here in a way it does not for a single ``close``. A
        shutdown runs against a deadline — Docker sends SIGKILL ten seconds
        after SIGTERM — while the teardown it has to finish first can block on
        TD acking the cancels of orders that must not outlive the session. When
        the teardown outlives the deadline, the process dies *after* the
        session stopped and *before* its row was written, leaving it marked
        live with no process left to ever correct it: a session the UI shows
        as running that nobody can stop.

        Writing the row up front costs one redundant update per session (the
        ``close`` below repeats it) and is the difference between a wrong row
        and a slow one.
        """
        for session_id in list(self._sessions):
            if self._mark_done is not None:
                try:
                    await self._mark_done(
                        session_id, status=SessionStatus.DONE.value, reason=None
                    )
                except Exception:
                    logger.exception(
                        "STS shutdown pre-mark failed session=%s", session_id
                    )
        for session_id in list(self._sessions):
            await self.close(session_id)
