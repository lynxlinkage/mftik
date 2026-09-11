"""STS session manager — independent strategy sessions."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from typing import Any

from mftik.broker import Broker
from mftik.broker.errors import RequestTimeoutError
from mftik.protocol import (
    ANY_INSTANCE,
    MD_ERROR,
    MD_SESSION_ATTACH,
    STS_REASON_OPERATOR_STOP,
    STS_SESSION_STATUS,
    TD_ERROR,
    TD_SESSION_ATTACH,
    ListSessionsRequest,
    MdAttachRequest,
    MdAttachRequestEnvelope,
    MdAttachResult,
    RpcError,
    SessionInfo,
    StsCreateSessionRequest,
    StsCreateSessionResult,
    StsSessionControlResult,
    StsSessionStatus,
    StsSessionStatusEnvelope,
    TdAttachRequest,
    TdAttachRequestEnvelope,
    Topics,
    attached_api_ids,
    dump_td,
    load_md,
    load_td,
    md_feeds_of,
    md_instances_of,
    publish_sts_log,
    td_api_ids_of,
)
from mftik.strategy import Strategy
from mftik.strategy.client_order_id import slot_for_session
from mftik_db.models.session import SessionDomain, SessionStatus

from mftik_sts.impl import resolve as resolve_strategy
from mftik_sts.runtime_env import IncompatibleEnvironment, ensure_deployable
from mftik_sts.session.session import StsSession

logger = logging.getLogger(__name__)

#: Why a session in ``interrupted`` stopped. A constant because it is the
#: same event for every session in the process, not a per-session diagnosis.
#: How long a shutdown waits for control loops to notice they were asked to
#: stop. A little over the test broker's poll and well under production's, so
#: this never becomes the thing that runs a container into SIGKILL.
_CONTROL_RETIRE_S = 2.0

_SHUTDOWN_REASON = "STS shut down while this was running"

#: How long a rebuild keeps trying to attach one domain. TD and MD may still be
#: starting — nothing makes them come up before STS — so waiting is the whole
#: strategy, and attach is idempotent on both sides, which is what makes
#: re-sending safe.
#:
#: A budget rather than a number of attempts, because how long one attempt takes
#: is the transport's business and not this loop's. Under Redis an unserved
#: request waits on its subject and the first attempt alone spends the whole
#: twenty second timeout on it; under NATS the same request comes back in
#: milliseconds saying nobody is listening. Counting attempts would have made
#: this window a minute and a half on one transport and twenty seconds on the
#: other without anything saying so.
_ATTACH_BUDGET_S = 100.0
_ATTACH_BACKOFF_S = 2.0
_ATTACH_TIMEOUT_S = 20.0

#: How long after being interrupted a session may still be rebuilt.
#: Restoring is for a restart, where the gap is seconds to minutes. A session
#: interrupted days ago would come back to a market that has moved on and to
#: orders the venue may have expired, which is a decision for a person, not
#: something to do to them at boot. Anything older is left alone.
_REBUILD_MAX_AGE_S = 1800.0

#: How many times one session may be rebuilt before it is left alone. A
#: strategy that takes the process down with it would otherwise be restored
#: into the same crash on every boot, for as long as the age window holds.
_REBUILD_MAX_ATTEMPTS = 3

#: How long a rebuilt session must keep running before its attempt count is
#: forgiven. The count exists to break a restore-into-crash loop, and only
#: time can tell that loop from a session that simply lives through deploys:
#: the rebuild itself returns successfully in both cases. Long enough that a
#: strategy which dies on the way back has died before this fires, short
#: enough that a session which is plainly fine is not carrying attempts from
#: yesterday into tonight's restart.
_REBUILD_SETTLE_S = 300.0

#: How many interrupted rows one scan will consider. Well above any plausible
#: number of sessions running at once — the point is that hitting it is
#: reported rather than silently dropping the rest.
_REBUILD_SCAN_LIMIT = 1000

#: How many consecutive scans must agree a live row is ours and not running
#: before it is interrupted. Covers the window between persist and the
#: session landing in ``_sessions`` on a peer, and a broker blip.
_ORPHAN_STRIKES = 2

def _age_seconds(finished_at: Any, now: datetime) -> float | None:
    """Seconds since a session ended, or None when that cannot be told.

    An unknown age is treated as too old by the caller: a row that says it is
    interrupted without saying when is not evidence that it stopped recently.
    """
    if finished_at is None:
        return None
    stamped = (
        finished_at
        if finished_at.tzinfo is not None
        else finished_at.replace(tzinfo=UTC)
    )
    return (now - stamped).total_seconds()


PersistLive = Callable[..., Awaitable[Any]]
#: ``(session_id, key, value)`` — persist one fact a strategy established.
RememberFact = Callable[..., Awaitable[Any]]
#: ``(session_id)`` — put a terminal row back to live when rebuilding it.
MarkLive = Callable[..., Awaitable[Any]]
#: ``(session_id)`` — count one rebuild attempt, returning the new total.
BumpRebuildCount = Callable[..., Awaitable[Any]]
#: ``(session_id)`` — clear the attempt count of a rebuild that has settled.
ResetRebuildCount = Callable[..., Awaitable[Any]]
TdInstanceLookup = Callable[[int], Awaitable[str | None]]
#: ``api_ids`` → the unique enabled STS in those credentials' TD region.
DeriveSts = Callable[[list[int]], Awaitable[str | None]]
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
        remember_fact: RememberFact | None = None,
        mark_live: MarkLive | None = None,
        bump_rebuild_count: BumpRebuildCount | None = None,
        reset_rebuild_count: ResetRebuildCount | None = None,
        rebuild_max_age_s: float = _REBUILD_MAX_AGE_S,
        rebuild_max_attempts: int = _REBUILD_MAX_ATTEMPTS,
        rebuild_settle_s: float = _REBUILD_SETTLE_S,
        heartbeat_interval: float = 1.0,
        strategy_factory: StrategyFactory | None = None,
        td_instance: TdInstanceLookup | None = None,
        derive_sts: DeriveSts | None = None,
        instance: str = SessionDomain.STS.value,
    ) -> None:
        self._broker = broker
        self._persist_live = persist_live
        self._mark_done = mark_done
        self._list_db_sessions = list_db_sessions
        self._remember = remember_fact
        self._mark_live = mark_live
        self._bump_rebuild_count = bump_rebuild_count
        self._reset_rebuild_count = reset_rebuild_count
        self._rebuild_max_age_s = rebuild_max_age_s
        self._rebuild_max_attempts = rebuild_max_attempts
        self._rebuild_settle_s = rebuild_settle_s
        self._heartbeat_interval = heartbeat_interval
        self._strategy_factory = strategy_factory or resolve_strategy
        #: ``api_id`` → the TD instance allowed to use that credential.
        #: Injected like every other database reach here, so a test can drive
        #: a rebuild without one.
        self._td_instance_lookup = td_instance
        #: ``api_ids`` → derived STS instance, or None when that is not unique.
        self._derive_sts = derive_sts
        #: Which STS this is. Only the rebuild scan reads it: a session is
        #: addressed by the subject it was created on, not by this.
        self._instance = instance
        self._orphan_strikes: dict[str, int] = {}
        #: ``session_id`` → the loop serving that session's control subject,
        #: and the event that ends it. One per live session: stop and fail are
        #: answered from ``self._sessions``, so only the process holding a
        #: session may answer for it.
        self._control: dict[str, tuple[asyncio.Event, asyncio.Task[Any]]] = {}
        #: Control loops asked to stop and not yet retired. Held so shutdown
        #: can wait for them rather than leaving tasks pending at loop close,
        #: and so one is not garbage-collected before it notices.
        self._retiring: set[asyncio.Task[Any]] = set()
        self._sessions: dict[str, StsSession] = {}
        # Held so shutdown can cancel them: each outlives the rebuild scan
        # that started it, and a pending task at loop close is a warning
        # nobody can act on.
        self._settle_tasks: set[asyncio.Task[None]] = set()

    @property
    def instance(self) -> str:
        """Which STS this is. Read by the event log, which has to say whose
        disk a part is on so a read can be addressed there."""
        return self._instance

    def get(self, session_id: str) -> StsSession | None:
        return self._sessions.get(session_id)

    async def _remember_fact(self, session_id: str, key: str, value: str) -> None:
        """Persist one fact for ``session_id``, or drop it if nothing can.

        Losing a fact must not take the strategy down with it: everything
        written here is a nicety for a rebuild that may never happen, while
        the strategy calling it is in the middle of trading.
        """
        if self._remember is None:
            return
        try:
            await self._remember(session_id, key, value)
        except Exception:
            logger.exception(
                "STS remember failed session=%s key=%s", session_id, key
            )

    async def _publish_status(
        self,
        session_id: str,
        *,
        status: str,
        strategy: str | None = None,
        reason: str | None = None,
        created_by: int | None = None,
        type: str | None = None,
    ) -> None:
        """Announce a session's state on the shared status channel.

        Always called *after* the DB write, never before: a UI that reacts to
        the event by re-reading REST must not be able to read the old row.

        Published on the live channel. A socket that opens late reads the
        current rows (the same source as REST) and then this subject — the
        row is written first, so that read cannot see the previous status.
        """
        terminal = status != SessionStatus.LIVE.value
        payload = StsSessionStatus(
            session_id=session_id,
            status=status,
            strategy=strategy,
            reason=reason,
            created_by=created_by,
            finished_at=time.time() if terminal else None,
            type=type,
        )
        envelope = StsSessionStatusEnvelope.wrap(
            payload,
            type=STS_SESSION_STATUS,
            source="sts",
            session_id=session_id,
        )
        try:
            await self._broker.publish(Topics.status_sts(), envelope)
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

    async def create_session(
        self, request: StsCreateSessionRequest
    ) -> StsCreateSessionResult:
        if request.session_id in self._sessions:
            raise KeyError(f"sts session already exists: {request.session_id}")

        ensure_deployable(request.type or request.strategy)
        strategy = self._strategy_factory(request.strategy)
        # Derived from the session id, so two STS instances need not agree on
        # anything to keep their live sessions apart, and a rebuild lands on
        # the slot its resting orders already carry. Still written to the row:
        # sessions older than this derivation carry a slot it cannot reproduce.
        cid_slot = slot_for_session(request.session_id)
        session = StsSession(
            session_id=request.session_id,
            broker=self._broker,
            created_by=request.created_by,
            strategy=strategy,
            td_instance=self._td_instance_lookup,
            cid_slot=cid_slot,
            remember=self._remember_fact,
            td=dict(request.td),
            md=dict(request.md),
            st_paras=dict(request.st_paras),
            heartbeat_interval=self._heartbeat_interval,
            on_exit=self._on_session_exit,
            strategy_type=request.type,
        )
        # Register before start so Strategy.exit() during on_start/on_ready works.
        # Also before persist: the reaper treats "names me and not in
        # ``_sessions``" as an orphan, and strikes only cover a short window.
        self._sessions[request.session_id] = session
        self._serve_control(request.session_id)
        # Persist before start, not after: a strategy that ends inside
        # on_start / on_ready reaches close() before start() returns, and a
        # row written afterwards would resurrect it as live forever.
        if self._persist_live is not None:
            await self._persist_live(
                session_id=request.session_id,
                created_by=request.created_by,
                strategy=strategy.name,
                type=request.type,
                yaml_text=request.yaml_text,
                td=dump_td(dict(request.td)),
                md_ids=dict(request.md),
                st_paras=dict(request.st_paras),
                cid_slot=cid_slot,
                restart=request.restart,
                instance=request.instance,
            )
        try:
            await session.start()
        except Exception as exc:
            self._sessions.pop(request.session_id, None)
            self._stop_serving_control(request.session_id)
            reason = f"start failed: {exc}"
            if self._mark_done is not None:
                await self._mark_done(
                    request.session_id,
                    status=SessionStatus.FAILED.value,
                    reason=reason,
                )
            await self._publish_status(
                request.session_id,
                status=SessionStatus.FAILED.value,
                strategy=strategy.name,
                created_by=request.created_by,
                reason=reason,
                type=request.type,
            )
            raise

        # A strategy that ended inside on_start / on_ready is already gone and
        # has announced its own terminal status — do not follow it with "live".
        #
        # Asked of the session rather than of this dict, because the two do not
        # answer at the same moment: the teardown that removes the entry is a
        # task, and it may not have run yet, while the flag is set the instant
        # the strategy calls exit() or fail().
        session_exited = session.exit_requested
        if request.session_id in self._sessions and not session_exited:
            await self._publish_status(
                request.session_id,
                status=SessionStatus.LIVE.value,
                strategy=strategy.name,
                created_by=request.created_by,
                type=request.type,
            )
        if session_exited:
            status = (
                SessionStatus.FAILED.value
                if session.exit_failed
                else SessionStatus.DONE.value
            )
            # Not an exception: the session was created, and everything that
            # follows a create — the row, the rollback, the audit line — still
            # has to happen. The caller is being told what it created, which
            # is a session that is already over.
            logger.warning(
                "STS session ended during start id=%s strategy=%s status=%s "
                "reason=%s",
                request.session_id,
                strategy.name,
                status,
                session.exit_reason,
            )
            return StsCreateSessionResult(
                session_id=request.session_id,
                strategy=strategy.name,
                status=status,
                reason=session.exit_reason,
            )
        logger.info(
            "STS session created id=%s strategy=%s td=%s",
            request.session_id,
            strategy.name,
            list(request.td),
        )
        return StsCreateSessionResult(
            session_id=request.session_id,
            strategy=strategy.name,
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
                        reason=getattr(row, "reason", None),
                        type=(
                            (live.type if live is not None else None)
                            or getattr(row, "type", None)
                        ),
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
                    type=session.type,
                )
            )
        return rows

    async def stop_session(self, session_id: str) -> StsSessionControlResult:
        session = self._sessions.get(session_id)
        if session is None:
            raise KeyError(f"no active sts session {session_id}")
        strategy = session.strategy_name
        # Still `done` — a deliberate stop is not a failure and not an
        # interruption. The reason is what separates it from a strategy that
        # reached its own end, which the status alone cannot say.
        await self.close(session_id, reason=STS_REASON_OPERATOR_STOP)
        return StsSessionControlResult(
            session_id=session_id,
            status=SessionStatus.DONE.value,
            strategy=strategy,
            reason=STS_REASON_OPERATOR_STOP,
        )

    async def fail_session(
        self, session_id: str, *, reason: str
    ) -> StsSessionControlResult:
        """Tear down a live session as a failure — attach-rollback, not stop."""
        session = self._sessions.get(session_id)
        if session is None:
            raise KeyError(f"no active sts session {session_id}")
        strategy = session.strategy_name
        await self.close(
            session_id, status=SessionStatus.FAILED.value, reason=reason
        )
        return StsSessionControlResult(
            session_id=session_id,
            status=SessionStatus.FAILED.value,
            strategy=strategy,
            reason=reason,
        )

    async def close(
        self,
        session_id: str,
        *,
        status: str = SessionStatus.DONE.value,
        reason: str | None = None,
    ) -> None:
        session = self._sessions.pop(session_id, None)
        self._stop_serving_control(session_id)
        if session is None:
            return
        broker = session.broker
        strategy_name = session.strategy_name
        created_by = session.created_by
        session_type = session.type
        await session.stop()
        if self._mark_done is not None:
            await self._mark_done(session_id, status=status, reason=reason)
        await self._publish_status(
            session_id,
            status=status,
            strategy=strategy_name,
            reason=reason,
            created_by=created_by,
            type=session_type,
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
                    type=session_type,
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
            # Kept rather than dropped: `done` says a strategy reached its own
            # end, not which end. `oco_filled` and `chase_expired` are both
            # done and mean very different things to whoever reads the row.
            await self.close(session_id, reason=reason)

    async def reap_orphans(self) -> list[str]:
        """Fail rows left ``live`` by a process that died without a word.

        Every other ending writes its own row. This covers only the one that
        cannot: the process vanishing outright, where the row keeps claiming a
        session is running and nothing else will ever say otherwise.

        A row is an orphan when it belongs to this instance — named, or
        derived from its TD region — and this process does not have it
        locally. Strikes cover the window between persist and
        ``_sessions``. A row that derives to nobody, or to someone else,
        is left alone.

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
                self._orphan_strikes.pop(session_id, None)
                continue
            if not await self._placement_is_mine(row):
                self._orphan_strikes.pop(session_id, None)
                continue
            strikes = self._orphan_strikes.get(session_id, 0) + 1
            self._orphan_strikes[session_id] = strikes
            if strikes < _ORPHAN_STRIKES:
                continue

            # `interrupted`, not `failed`: nothing was wrong with the
            # strategy and it did not choose to stop — the same category as a
            # shutdown, which is what makes the rebuild candidate set exactly
            # `status = interrupted` rather than a reason-string match.
            reason = "process died: no session heartbeat"
            try:
                await self._mark_done(
                    session_id,
                    status=SessionStatus.INTERRUPTED.value,
                    reason=reason,
                )
            except Exception:
                logger.exception("STS orphan reap failed session=%s", session_id)
                continue
            await self._publish_status(
                session_id,
                status=SessionStatus.INTERRUPTED.value,
                strategy=getattr(row, "strategy", None),
                reason=reason,
                created_by=getattr(row, "created_by", None),
                type=getattr(row, "type", None),
            )
            self._orphan_strikes.pop(session_id, None)
            reaped.append(session_id)
            logger.warning(
                "STS reaped orphaned session id=%s strategy=%s",
                session_id,
                getattr(row, "strategy", None),
            )
        return reaped

    # --- rebuild -----------------------------------------------------------

    async def rebuild_interrupted(self) -> list[str]:
        """Restore sessions that were running when STS last went away.

        Candidates are exactly ``status = interrupted``: a shutdown or a
        process killed outright, neither of which is the strategy deciding to
        stop. Returns the session ids restored.

        Placement is the whole guard: a named row is rebuilt only by that
        instance, a null row only by the STS its TD region derives to.
        A derivation that is not unique waits on the Attention list.
        """
        if self._list_db_sessions is None:
            return []
        try:
            rows = await self._list_db_sessions(
                status=SessionStatus.INTERRUPTED.value,
                created_by=None,
                limit=_REBUILD_SCAN_LIMIT,
            )
        except Exception:
            logger.exception("STS rebuild scan failed to list sessions")
            return []
        if len(rows) >= _REBUILD_SCAN_LIMIT:
            # Truncation is the one thing a scan must not do quietly: the
            # sessions past the limit look exactly like sessions nobody asked
            # to restore.
            logger.warning(
                "STS rebuild scan hit its %d-row limit — there may be "
                "interrupted sessions it did not consider",
                _REBUILD_SCAN_LIMIT,
            )

        rebuilt: list[str] = []
        now = datetime.now(UTC)
        for row in rows:
            session_id = getattr(row, "session_id", None)
            if session_id is None or session_id in self._sessions:
                continue
            age = _age_seconds(getattr(row, "finished_at", None), now)
            if age is None or age > self._rebuild_max_age_s:
                # Not an error — nothing failed, this is the policy. The row
                # keeps its status and its reason, so it stays visible and a
                # person can still decide to do something with it.
                logger.warning(
                    "STS not rebuilding session=%s: interrupted %s ago, "
                    "past the %.0fs window",
                    session_id,
                    "an unknown time" if age is None else f"{age:.0f}s",
                    self._rebuild_max_age_s,
                )
                continue
            if not await self._placement_is_mine(row):
                # Somebody else's run, or a null row whose derivation is
                # not this process — including "not unique", which waits
                # on the Attention list rather than a coin flip.
                logger.debug(
                    "STS not rebuilding session=%s: placement is not %s",
                    session_id,
                    self._instance,
                )
                continue
            if str(getattr(row, "restart", "always")) != "always":
                # This run said it would rather stay ended. Nothing to warn
                # about — it is doing what it was deployed to do.
                logger.info(
                    "STS not rebuilding session=%s: deployed with restart=%s",
                    session_id,
                    getattr(row, "restart", None),
                )
                continue
            attempts = int(getattr(row, "rebuild_count", 0) or 0)
            if attempts >= self._rebuild_max_attempts:
                logger.warning(
                    "STS not rebuilding session=%s: already tried %d times",
                    session_id,
                    attempts,
                )
                continue
            if getattr(row, "cid_slot", None) is None:
                # Predates the slot being recorded. Rebuilding would mint
                # order ids in a different slot, leaving the strategy unable
                # to recognise anything it placed before — worse than leaving
                # the session where it is.
                logger.warning(
                    "STS cannot rebuild session=%s: no recorded cid_slot",
                    session_id,
                )
                continue
            try:
                ensure_deployable(
                    getattr(row, "type", None) or getattr(row, "strategy", None)
                )
                strategy = self._strategy_factory(getattr(row, "strategy", None))
            except IncompatibleEnvironment as exc:
                logger.warning(
                    "STS not rebuilding session=%s: incompatible environment "
                    "(%s)",
                    session_id,
                    exc,
                )
                if self._bump_rebuild_count is not None:
                    try:
                        await self._bump_rebuild_count(session_id)
                    except Exception:
                        logger.exception(
                            "STS rebuild count bump failed session=%s",
                            session_id,
                        )
                continue
            except KeyError:
                # A row naming a strategy this build does not have. Expected —
                # a strategy can be renamed or withdrawn while a session that
                # ran it is still on file — so it reads like its neighbours
                # here rather than like a fault. A stack trace every boot for
                # a row that will never resolve teaches the operator to skip
                # the tracebacks.
                logger.warning(
                    "STS not rebuilding session=%s: no strategy named %r in "
                    "this build",
                    session_id,
                    getattr(row, "strategy", None),
                )
                continue
            except Exception:
                # Anything else is the class failing to construct, which is a
                # fault and keeps its traceback.
                logger.exception(
                    "STS cannot rebuild session=%s: strategy %r would not "
                    "build",
                    session_id,
                    getattr(row, "strategy", None),
                )
                continue
            if not strategy.rebuildable:
                # Readiness is the class flag, not the env var. Without it a
                # rebuilt instance would treat recon as a clean account and
                # place beside the orders this session left resting.
                logger.warning(
                    "STS not rebuilding session=%s: %s does not support it",
                    session_id,
                    strategy.name,
                )
                continue
            if self._bump_rebuild_count is not None:
                try:
                    await self._bump_rebuild_count(session_id)
                except Exception:
                    logger.exception(
                        "STS rebuild count bump failed session=%s", session_id
                    )
            try:
                await self._rebuild_one(row, strategy)
            except Exception:
                logger.exception("STS rebuild failed session=%s", session_id)
                await self._abandon_rebuild(session_id)
                continue
            rebuilt.append(session_id)
            self._watch_rebuild_settle(session_id)
            logger.info(
                "STS rebuilt session=%s strategy=%s",
                session_id,
                getattr(row, "strategy", None),
            )
        return rebuilt

    def _watch_rebuild_settle(self, session_id: str) -> None:
        """Start the timer that forgives this session's attempt count.

        The session is read here rather than inside the task: a task does not
        run until the loop next yields, by which time the session it was
        started for may already have been replaced by another one under the
        same id.
        """
        if self._reset_rebuild_count is None or self._rebuild_settle_s <= 0:
            return
        session = self._sessions.get(session_id)
        if session is None:
            return
        task = asyncio.create_task(
            self._settle_rebuild(session_id, session),
            name=f"sts-rebuild-settle-{session_id}",
        )
        self._settle_tasks.add(task)
        task.add_done_callback(self._settle_tasks.discard)

    async def _settle_rebuild(self, session_id: str, session: StsSession) -> None:
        """Clear the attempt count once a rebuilt session has kept running.

        The session is compared by identity, not by id: a session that stopped
        and was deployed again under the same id is a different run, and
        clearing the count on its behalf would credit it for surviving
        something it was never part of.
        """
        await asyncio.sleep(self._rebuild_settle_s)
        if self._sessions.get(session_id) is not session:
            # Gone, or replaced. Either way the rebuild did not hold, and the
            # count it was carrying is exactly what the next boot should see.
            return
        if self._reset_rebuild_count is None:
            return
        try:
            await self._reset_rebuild_count(session_id)
        except Exception:
            # Nothing to recover here: the count stays where it was, which
            # costs this session one of its future attempts rather than
            # anything it is doing now.
            logger.exception(
                "STS rebuild count reset failed session=%s", session_id
            )
            return
        logger.info(
            "STS rebuild settled session=%s — attempt count cleared after "
            "%.0fs",
            session_id,
            self._rebuild_settle_s,
        )

    async def _placement_is_mine(self, row: Any) -> bool:
        """Whether this process is the one instance that may run ``row``.

        A name on the row is what the deploy asked for. Null means derive
        from the credentials' TD region. Missing or non-unique derivation
        is nobody's.
        """
        pinned = getattr(row, "instance", None)
        if pinned is not None:
            return pinned == self._instance
        if self._derive_sts is None:
            return False
        try:
            derived = await self._derive_sts(attached_api_ids(row))
        except Exception:
            logger.exception(
                "STS placement derive failed session=%s",
                getattr(row, "session_id", None),
            )
            return False
        return derived == self._instance

    async def _rebuild_one(self, row: Any, strategy: Strategy) -> None:
        """Restore one session, in the order a deploy uses and for the reason.

        TD blocks its attach until it sees the session's lease heartbeat, so
        the session has to be running before anything can be attached to it.
        """
        session_id = row.session_id
        td = load_td(getattr(row, "td", None))
        td_api_ids = td_api_ids_of(td)
        # The compat shim. A row written before instances stored a flat list
        # meaning "any MD"; ``load_md`` reads either shape, so a session
        # interrupted by the deploy that introduced this still rebuilds.
        md = load_md(getattr(row, "md_ids", None))
        md_ids = md_feeds_of(md)
        created_by = int(getattr(row, "created_by", 0) or 0)

        session = StsSession(
            session_id=session_id,
            broker=self._broker,
            created_by=created_by,
            strategy=strategy,
            td_instance=self._td_instance_lookup,
            cid_slot=int(row.cid_slot),
            td=td,
            md=md,
            st_paras=dict(getattr(row, "st_paras", None) or {}),
            heartbeat_interval=self._heartbeat_interval,
            on_exit=self._on_session_exit,
            remember=self._remember_fact,
            strategy_type=getattr(row, "type", None),
        )
        self._sessions[session_id] = session
        self._serve_control(session_id)

        # Before on_start, so every hook that follows already sees whatever
        # the strategy restored — including on_recon_done, which is where a
        # strategy has to know these orders are its own.
        remembered = {
            str(k): str(v)
            for k, v in (getattr(row, "st_facts", None) or {}).items()
        }
        await strategy.on_rebuild(remembered)

        if self._mark_live is not None:
            await self._mark_live(session_id)
        await publish_sts_log(
            self._broker,
            session_id,
            f"rebuilding session strategy={session.strategy_name} "
            f"td={td_api_ids} md={md_ids}",
            source="sts",
            type=session.type,
        )
        await session.start()

        try:
            if md:
                await self._attach_md(session_id, created_by, md)
            for api_id in td_api_ids:
                await self._attach_td(session_id, created_by, api_id)
        except Exception:
            # A session with half its attaches is worse than one still marked
            # interrupted: it heartbeats and looks alive while blind to a feed
            # or an account. Put it back for the next boot to try.
            await self.close(
                session_id,
                status=SessionStatus.INTERRUPTED.value,
                reason="rebuild failed to attach",
            )
            raise

        await self._publish_status(
            session_id,
            status=SessionStatus.LIVE.value,
            strategy=session.strategy_name,
            created_by=created_by,
            type=session.type,
        )

    async def _abandon_rebuild(self, session_id: str) -> None:
        """Drop a half-built session so the next boot may retry."""
        self._sessions.pop(session_id, None)
        self._stop_serving_control(session_id)

    async def _attach_md(
        self, session_id: str, created_by: int, md: dict[str, list[str]]
    ) -> None:
        """One attach per instance the document names.

        Sequential rather than gathered: ``_attach_with_retry`` gives up by
        failing the session, and a second failure racing the first would
        report a rebuild as failing for whichever reason arrived last.
        """
        for instance in md_instances_of(md):
            feeds = md.get(instance) or []
            if not feeds:
                continue
            reply = await self._attach_with_retry(
                what=f"md instance={instance} feeds={feeds}",
                subject=(
                    Topics.MD
                    if instance == ANY_INSTANCE
                    else Topics.md(instance)
                ),
                envelope=MdAttachRequestEnvelope.wrap(
                    MdAttachRequest(
                        session_id=session_id,
                        created_by=created_by,
                        subscriptions=feeds,
                        timeout=_ATTACH_TIMEOUT_S,
                    ),
                    type=MD_SESSION_ATTACH,
                    source="sts",
                    session_id=session_id,
                ),
                error_type=MD_ERROR,
            )
            session = self._sessions.get(session_id)
            if session is not None and reply is not None:
                try:
                    result = MdAttachResult.model_validate(reply.payload)
                except Exception:
                    result = None
                owner = (
                    (result.instance if result is not None else "")
                    or ("" if instance == ANY_INSTANCE else instance)
                )
                if owner:
                    session.note_md_owner(
                        list(result.subscriptions if result else feeds),
                        owner,
                    )

    async def _td_instance(self, api_id: int) -> str:
        """Which TD may take this attach.

        Falls back to the plane name only when nothing can answer — no lookup
        wired, or a credential that has been deleted. That is the instance a
        node which has never heard of instances runs under, so the fallback
        degrades to today's behaviour rather than to silence; a rebuild is
        already the wrong moment to discover a missing row.
        """
        if self._td_instance_lookup is None:
            return SessionDomain.TD.value
        try:
            name = await self._td_instance_lookup(api_id)
        except Exception:
            logger.exception(
                "STS could not resolve the TD instance for api_id=%s", api_id
            )
            return SessionDomain.TD.value
        if name is None:
            logger.warning(
                "STS found no credential api_id=%s — attaching on %s",
                api_id,
                SessionDomain.TD.value,
            )
            return SessionDomain.TD.value
        return name

    async def _attach_td(
        self, session_id: str, created_by: int, api_id: int
    ) -> None:
        instance = await self._td_instance(api_id)
        await self._attach_with_retry(
            what=f"td api_id={api_id} instance={instance}",
            subject=Topics.td(instance),
            envelope=TdAttachRequestEnvelope.wrap(
                TdAttachRequest(
                    api_id=api_id,
                    session_id=session_id,
                    created_by=created_by,
                    timeout=_ATTACH_TIMEOUT_S,
                ),
                type=TD_SESSION_ATTACH,
                source="sts",
                session_id=session_id,
            ),
            error_type=TD_ERROR,
        )

    async def _attach_with_retry(
        self,
        *,
        what: str,
        subject: str,
        envelope: Any,
        error_type: str,
    ) -> Any:
        """Send an attach until it lands, or give up and say so.

        Retried rather than gated on a readiness probe: the domain may simply
        not be up yet, and both attaches are idempotent — a request that was
        served after we stopped waiting for the reply makes the next attempt a
        no-op. What the retry is covering differs by transport: under Redis the
        request is already waiting on the subject and this loop is only waiting
        for the reply, while under NATS an unserved subject answers at once and
        the re-send *is* the mechanism. Both are bounded by the same budget.
        """
        last: Exception | None = None
        deadline = asyncio.get_running_loop().time() + _ATTACH_BUDGET_S
        attempt = 0
        while True:
            attempt += 1
            try:
                reply = await self._broker.request(
                    subject, envelope, timeout=_ATTACH_TIMEOUT_S
                )
            except RequestTimeoutError as exc:
                last = exc
            else:
                if reply.type != error_type:
                    return reply
                err = RpcError.model_validate(reply.payload)
                last = RuntimeError(f"{err.code}: {err.message}")
            left = deadline - asyncio.get_running_loop().time()
            logger.warning(
                "STS rebuild attach %s failed (attempt %d, %.0fs left): %s",
                what,
                attempt,
                max(0.0, left),
                last,
            )
            if left <= 0:
                raise RuntimeError(f"rebuild could not attach {what}: {last}")
            await asyncio.sleep(min(_ATTACH_BACKOFF_S * attempt, left))

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

        These land in ``interrupted``, not ``done``: nothing about the
        strategy ended, STS did. Telling the two apart is what lets a future
        rebuild know which sessions it would be putting back.
        """
        # First, and without awaiting: a settle timer that fires during
        # teardown would clear the attempt count of a session this method is
        # in the middle of interrupting.
        for task in list(self._settle_tasks):
            task.cancel()
        self._settle_tasks.clear()
        for session_id in list(self._sessions):
            if self._mark_done is not None:
                try:
                    await self._mark_done(
                        session_id,
                        status=SessionStatus.INTERRUPTED.value,
                        reason=_SHUTDOWN_REASON,
                    )
                except Exception:
                    logger.exception(
                        "STS shutdown pre-mark failed session=%s", session_id
                    )
        for session_id in list(self._sessions):
            await self.close(
                session_id,
                status=SessionStatus.INTERRUPTED.value,
                reason=_SHUTDOWN_REASON,
            )
        # Each was asked to stop as its session closed; they retire on their
        # own between polls. Waited for here so none is left pending when the
        # loop closes — bounded, because a shutdown is already racing SIGKILL
        # and a control loop nobody is talking to is not worth the deadline.
        if self._retiring:
            await asyncio.wait(self._retiring, timeout=_CONTROL_RETIRE_S)

    # --- per-session control ------------------------------------------------

    def _serve_control(self, session_id: str) -> None:
        """Start answering stop / fail for one session.

        Started where the session is registered rather than where it starts,
        so a session that ends inside ``on_start`` is still stoppable while it
        does — and so the deploy's own rollback, which fails the session it
        just created, has somebody to talk to.
        """
        if session_id in self._control:
            return
        stop = asyncio.Event()
        task = asyncio.create_task(
            self._control_loop(session_id, stop),
            name=f"sts-control-{session_id}",
        )
        self._control[session_id] = (stop, task)

    def _stop_serving_control(self, session_id: str) -> None:
        """Ask the loop to retire. Not awaited.

        ``serve`` parks in a blocking broker read and cancelling it there can
        leave an unread reply on a pooled connection, so the loop is asked to stop
        and left to notice between polls — the same rule TD's account loops
        follow. The session is already out of ``self._sessions`` by then, so
        anything that arrives in that window is answered ``not_found``, which
        is the truth.
        """
        entry = self._control.pop(session_id, None)
        if entry is None:
            return
        stop, task = entry
        stop.set()
        self._retiring.add(task)
        task.add_done_callback(self._retiring.discard)

    async def _control_loop(
        self, session_id: str, stop: asyncio.Event
    ) -> None:
        # Imported here, not at module scope: the rpc package imports this
        # module for typing only, and a runtime import the other way keeps the
        # dependency one-directional.
        from mftik_sts.rpc import dispatch

        subject = Topics.sts_control(session_id)
        while not stop.is_set():
            try:
                async for req in self._broker.serve(subject, stop=stop):
                    try:
                        await dispatch(req, sessions=self)
                    except Exception:
                        logger.exception(
                            "STS control handler failed session=%s type=%s",
                            session_id,
                            req.envelope.type,
                        )
            except asyncio.CancelledError:
                raise
            except Exception:
                # Same reasoning as the plane's own RPC loop: this returning
                # is how a session becomes one nobody can stop, which is the
                # failure this subject exists to prevent.
                logger.exception(
                    "STS control loop failed session=%s — restarting",
                    session_id,
                )
                try:
                    await asyncio.wait_for(stop.wait(), timeout=1.0)
                except TimeoutError:
                    continue
