"""STS session manager — independent strategy sessions."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import time
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from typing import Any

from mftik.broker import Broker
from mftik.broker.errors import RequestTimeoutError
from mftik.protocol import (
    ANY_INSTANCE,
    CREATE_REPLY_LOST_REASON,
    LEASE_HEARTBEAT_INTERVAL_S,
    LEASE_MISS_LIMIT,
    MD_ERROR,
    MD_SESSION_ATTACH,
    STS_ERROR,
    STS_REASON_OPERATOR_STOP,
    STS_REASON_STOP_TIMED_OUT,
    STS_SESSION_STATUS,
    STS_START_DEADLINE_S,
    TD_ERROR,
    TD_SESSION_ATTACH,
    ListSessionsRequest,
    MdAttachRequest,
    MdAttachRequestEnvelope,
    MdAttachResult,
    RpcError,
    RpcErrorEnvelope,
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
    start_deadline_reason,
    td_api_ids_of,
)
from mftik.strategy import Strategy
from mftik.strategy.client_order_id import is_v1_session_id
from mftik_db.models.session import SessionDomain, SessionStatus

from mftik_sts.impl import resolve as resolve_strategy
from mftik_sts.runtime_env import IncompatibleEnvironment, ensure_deployable
from mftik_sts.session.session import StsSession
from mftik_sts.spawn import (
    START_FAIL_REASON,
    WORKER_STOP_WAIT_S,
    SessionSpawner,
    WorkerSlot,
    kill_worker,
    parse_worker_result,
)

logger = logging.getLogger(__name__)

#: How long a worker may go without a beat before a conditional kill.
#: The same fuse a peer uses for a missed lease: one drop is nothing,
#: three intervals means the loop is not running.
BEAT_SILENCE_S = LEASE_HEARTBEAT_INTERVAL_S * LEASE_MISS_LIMIT

#: Tries for the kill's row write. A miss leaves the row ``live``, and
#: the reaper would then mark it ``interrupted`` and rebuild a session
#: the operator killed. Reporting ``failed`` without the write is the
#: same lie.
MARK_KILL_ATTEMPTS = 3

#: How long a missed start-failure write keeps being retried. The reaper's
#: first write is two scans apart, and a row it calls ``interrupted`` is a
#: rebuild candidate. This stays inside that window, so a database blip
#: during the deadline kill does not come back as a live session.
FAILED_WRITE_BUDGET_S = 90.0
FAILED_WRITE_BACKOFF_S = 0.5

#: How long an ``abort_start`` that arrived while the deadline kill owns
#: the slot waits for that kill's row write before answering ``not_found``.
ABORT_ROW_WAIT_S = 1.0

#: Session log line for ``/logs/sts/{id}``. ``on_stop`` did not run.
KILL_LOG_MESSAGE = "stop unanswered — worker killed; on_stop did not run"

#: What :meth:`SessionManager._row_view` returns. ``unread`` is a failed
#: read, not a terminal row.
_VIEW_LIVE = "live"
_VIEW_TERMINAL = "terminal"
_VIEW_UNREAD = "unread"
_VIEW_ABSENT = "absent"


class WorkerNotStuck(Exception):
    """A conditional kill found a worker that is still starting or beating."""

    def __init__(self, session_id: str) -> None:
        self.session_id = session_id
        super().__init__(session_id)


class StartDeadlineExceeded(Exception):
    """``on_start`` / ``on_ready`` did not finish inside the create budget."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


def _budget_s(request: StsCreateSessionRequest) -> float:
    """The create budget for this request.

    None is the module default, which a test shrinks by patching
    :data:`STS_START_DEADLINE_S`. A document's ``start_timeout`` replaces
    it for that deploy only.
    """
    if request.start_timeout is None:
        return STS_START_DEADLINE_S
    return float(request.start_timeout)


def _start_remaining(started: float, budget: float) -> float:
    """Seconds left in the create budget, never negative.

    ``started`` is ``loop.time()`` at the top of ``create_session``, so
    the row write and the fork already count. A remaining of zero means
    the budget was spent before the worker could report, and the wait
    fails at once.
    """
    now = asyncio.get_running_loop().time()
    return max(0.0, started + budget - now)


def _deadline_reason(budget: float, began: float | None) -> str:
    """What the row says. ``began`` is when ``on_start`` was seen."""
    ran = None
    if began is not None:
        ran = max(0.0, asyncio.get_running_loop().time() - began)
    return start_deadline_reason(budget, on_start_s=ran)


def _slot_on_start_at(slot: WorkerSlot) -> float | None:
    reader = slot.result_reader
    if reader is not None:
        at = getattr(reader, "on_start_at", None)
        if isinstance(at, (int, float)):
            return float(at)
    return slot.on_start_at


class ForceStopExpired(Exception):
    """The force-stop arrived after the caller's deadline."""

    def __init__(self, session_id: str) -> None:
        self.session_id = session_id
        super().__init__(session_id)


class AttachRefused(RuntimeError):
    """The domain answered and will not open this attach.

    Any error reply other than ``unavailable`` or ``timeout``. Those
    two mean the domain is not up yet, so they are retried; giving up
    on them leaves the session interrupted for the next boot.
    """


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
#: into the same crash, for as long as the age window holds.
#:
#: The count is cleared once a rebuild has stayed up for
#: ``_REBUILD_SETTLE_S``. A crash after that is a new run with a fresh
#: count, and the watcher starts it immediately — a strategy that lives
#: about five minutes and then dies is restored without a cap. The window
#: still means "died on the way back", not "may only ever crash three times".
_REBUILD_MAX_ATTEMPTS = 3

#: How long a rebuilt session must keep running before its attempt count is
#: forgiven. The count exists to break a restore-into-crash loop, and only
#: time can tell that loop from a session that simply lives through deploys:
#: the rebuild itself returns successfully in both cases. Long enough that a
#: strategy which dies on the way back has died before this fires, short
#: enough that a session which is plainly fine is not carrying attempts from
#: yesterday into tonight's restart. Compared by the holder — the session
#: object, or the worker slot when the session is a process.
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
#: ``(session_id)`` — one row, not a list. A live check that pages the
#: fleet would miss a session older than the page.
LoadSession = Callable[[str], Awaitable[Any]]
StrategyFactory = Callable[[str | None], Strategy]


def _row_key(row: Any) -> str | None:
    """Qualified type when the row has one; the old short name only otherwise."""
    return getattr(row, "type", None) or getattr(row, "strategy", None)


#: Why a row that names no strategy is refused rather than defaulted.
#: ``resolve(None)`` builds :data:`mftik_sts.impl.DEFAULT_STRATEGY`, and a
#: session restored onto a strategy nobody deployed would place orders of its
#: own against that session's accounts. A row reads this way when it was
#: written before ``0034_strategy_type_key`` — the short name it carries is in
#: a column this build does not select — so the fix is the migration, not a
#: guess here.
_NO_TYPE_REASON = (
    "the row names no strategy type. It predates "
    "0034_strategy_type_key, or that migration could not name it"
)


class SessionManager:
    """Owns STS sessions. Each binds exactly one Strategy (1-1)."""

    def __init__(
        self,
        broker: Broker,
        *,
        persist_live: PersistLive | None = None,
        mark_done: MarkDone | None = None,
        list_db_sessions: ListDbSessions | None = None,
        load_session: LoadSession | None = None,
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
        spawner: SessionSpawner | None = None,
        rebuild_on_worker_exit: bool = False,
        control_types: frozenset[str] | None = None,
    ) -> None:
        self._broker = broker
        self._persist_live = persist_live
        self._mark_done = mark_done
        self._list_db_sessions = list_db_sessions
        self._load_session = load_session
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
        #: None runs sessions in this process. The parent sets a spawner;
        #: tests and the worker leave it unset.
        self._spawner = spawner
        #: A worker that already reported success and then died. Off unless
        #: this deployment restores interrupted sessions at all.
        self._rebuild_on_worker_exit = rebuild_on_worker_exit
        #: The worker's control subject answers only stop and fail. None
        #: dispatches whatever arrives, which is the in-process manager.
        self._control_types = control_types
        self._shutting_down = False
        #: Set at the start of ``close_all``, before any await. A rebuild
        #: paused on a database read has no process yet, so the process
        #: table alone cannot stop it from spawning on the way out.
        self._closing = False
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
        #: ``close`` calls that have popped a session and not yet returned.
        #: A strategy that calls ``exit`` runs that ``close`` as its own
        #: task, after the session has already left ``_sessions`` and after
        #: the control loop has been asked to stop. The worker stays up
        #: until the last of these returns, which is when the row has been
        #: written. A ``close`` that finds nothing does not count: it must
        #: not release the worker while the real one is still in ``stop``.
        self._closes = 0
        self._sessions: dict[str, StsSession] = {}
        #: ``session_id`` → the worker running it. Not a :class:`StsSession`:
        #: stop and fail are served in the child, on the session's subject.
        self._workers: dict[str, WorkerSlot] = {}
        #: Ids a rebuild has claimed and not yet registered. Checked in the
        #: same breath as the insert, before any await, so two scans cannot
        #: both decide the id is free.
        self._claims: set[str] = set()
        # Held so shutdown can cancel them: each outlives the rebuild scan
        # that started it, and a pending task at loop close is a warning
        # nobody can act on.
        self._settle_tasks: set[asyncio.Task[None]] = set()
        #: Watcher-started ``rebuild_session`` tasks. ``close_all`` cancels
        #: these. It does not cancel the watcher itself, which is blocked
        #: in ``process.wait`` and has to observe the signal.
        self._rebuild_tasks: set[asyncio.Task[Any]] = set()
        #: Create RPCs waiting on a result line. They are not the serve
        #: loop: a worker stuck in ``on_start`` must not stop list.
        self._create_tasks: set[asyncio.Task[Any]] = set()
        #: Force-stop RPCs waiting on the kill and the row write. Same reason
        #: as creates: the instance subject serves one request at a time,
        #: and this wait must not hold list or another session's stop.
        self._escalation_tasks: set[asyncio.Task[Any]] = set()
        #: session id → the result of the escalation already running.
        #: Survives the slot leaving ``_workers``, until that result is set,
        #: so a second force-stop waits instead of signalling again or
        #: answering ``not_found``.
        self._stop_escalations: dict[str, asyncio.Future[StsSessionControlResult]] = {}
        #: Processes that failed before ``started`` and still need ``wait``.
        #: A worker that exits without a watcher is a zombie until something
        #: collects it.
        self._reaps: set[asyncio.Task[None]] = set()
        #: Start-deadline kills whose ``failed`` write has not landed.
        #: The reaper marks these ``failed`` rather than ``interrupted``,
        #: so a missed write cannot become a rebuild.
        self._unwritten_failures: dict[str, str] = {}
        self._failure_retries: set[asyncio.Task[None]] = set()

    @property
    def instance(self) -> str:
        """Which STS this is. Read by the event log, which has to say whose
        disk a part is on so a read can be addressed there."""
        return self._instance

    def get(self, session_id: str) -> StsSession | WorkerSlot | None:
        """The in-process session, or the worker slot when this is the parent.

        Callers that only need to know the session is held — the event log's
        ``live`` bit — accept either. A worker slot is not a strategy.
        """
        session = self._sessions.get(session_id)
        if session is not None:
            return session
        return self._workers.get(session_id)

    def _holds(self, session_id: str) -> bool:
        """Whether this process has the session, or has claimed it."""
        return (
            session_id in self._sessions
            or session_id in self._workers
            or session_id in self._claims
        )

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
            logger.exception("STS remember failed session=%s key=%s", session_id, key)

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
        return list(dict.fromkeys([*self._sessions, *self._workers]))

    async def create_session(
        self, request: StsCreateSessionRequest
    ) -> StsCreateSessionResult:
        # The budget starts here, not at ``read_result``. The row write and
        # the fork both count against the API's create timeout.
        started = asyncio.get_running_loop().time()
        if self._spawner is not None:
            return await self._create_via_worker(request, started)
        return await self._create_in_process(request, started)

    async def _create_in_process(
        self, request: StsCreateSessionRequest, started: float
    ) -> StsCreateSessionResult:
        if request.session_id in self._sessions:
            raise KeyError(f"sts session already exists: {request.session_id}")

        key = request.type or request.strategy
        ensure_deployable(key)
        strategy = self._strategy_factory(key)
        session = StsSession(
            session_id=request.session_id,
            broker=self._broker,
            created_by=request.created_by,
            strategy=strategy,
            td_instance=self._td_instance_lookup,
            remember=self._remember_fact,
            td=dict(request.td),
            md=dict(request.md),
            st_paras=dict(request.st_paras),
            heartbeat_interval=self._heartbeat_interval,
            on_exit=self._on_session_exit,
            strategy_type=key,
        )
        # Register before start so Strategy.exit() during on_start/on_ready works.
        # Also before persist: the reaper treats "names me and not in
        # ``_sessions``" as an orphan, and strikes only cover a short window.
        self._sessions[request.session_id] = session
        self._serve_control(request.session_id)
        budget = _budget_s(request)
        # Persist before start, not after: a strategy that ends inside
        # on_start / on_ready reaches close() before start() returns, and a
        # row written afterwards would resurrect it as live forever.
        if self._persist_live is not None:
            await self._persist_live(
                session_id=request.session_id,
                created_by=request.created_by,
                type=key,
                yaml_text=request.yaml_text,
                td=dump_td(dict(request.td)),
                md_ids=dict(request.md),
                st_paras=dict(request.st_paras),
                restart=request.restart,
                instance=request.instance,
            )
        # ``expired()`` tells the budget apart from a ``TimeoutError`` the
        # strategy's own ``on_start`` raised, which is an ordinary failure.
        clock = asyncio.timeout(_start_remaining(started, budget))
        try:
            async with clock:
                await session.start()
        except TimeoutError as exc:
            if not clock.expired():
                await self._abandon_in_process_start(
                    session, request, key, error=exc, budget=budget
                )
                raise
            await self._abandon_in_process_start(session, request, key, budget=budget)
            raise StartDeadlineExceeded(
                _deadline_reason(budget, session.on_start_began)
            ) from None
        except Exception as exc:
            await self._abandon_in_process_start(session, request, key, error=exc)
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
                strategy=key,
                created_by=request.created_by,
                type=key,
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
                "STS session ended during start id=%s strategy=%s status=%s reason=%s",
                request.session_id,
                key,
                status,
                session.exit_reason,
            )
            return StsCreateSessionResult(
                session_id=request.session_id,
                strategy=key,
                status=status,
                reason=session.exit_reason,
            )
        logger.info(
            "STS session created id=%s strategy=%s td=%s",
            request.session_id,
            key,
            list(request.td),
        )
        return StsCreateSessionResult(
            session_id=request.session_id,
            strategy=key,
        )

    async def _abandon_in_process_start(
        self,
        session: StsSession,
        request: StsCreateSessionRequest,
        key: str,
        *,
        error: BaseException | None = None,
        budget: float | None = None,
    ) -> None:
        """End a start that raised or ran past the deadline.

        ``on_stop`` does not run. The lease and feed tasks ``start`` already
        created would otherwise keep publishing. A strategy that already
        asked to exit owns its row; this does not overwrite it.
        """
        if session.exit_requested:
            # ``request_exit`` already scheduled ``close``, which pops the
            # session, stops it and writes the strategy's reason. Popping
            # here first would make that ``close`` a no-op: the row would
            # stay ``live`` and the lease task would keep running.
            return
        await session.abandon_start()
        self._sessions.pop(request.session_id, None)
        self._stop_serving_control(request.session_id)
        if await self._row_view(request.session_id) == _VIEW_TERMINAL:
            return
        if error is None:
            limit = STS_START_DEADLINE_S if budget is None else budget
            reason = _deadline_reason(limit, session.on_start_began)
        else:
            reason = f"start failed: {error}"
        slot = WorkerSlot(
            session_id=request.session_id,
            role="create",
            strategy_name=key,
            type=key,
            created_by=request.created_by,
        )
        await self._write_failed(slot, request, reason)

    async def _create_via_worker(
        self, request: StsCreateSessionRequest, started: float
    ) -> StsCreateSessionResult:
        """Persist the row here, then let a worker run the session.

        The slot is claimed before the row is written, same as an in-process
        session is registered before persist: the reaper treats a live row
        this instance does not hold as an orphan. The request goes to the
        worker on stdin. Rebuilding it from the row would not be the same
        object — ``load_md`` exists because those two have already drifted.
        """
        if self._holds(request.session_id):
            raise KeyError(f"sts session already exists: {request.session_id}")
        key = request.type or request.strategy
        ensure_deployable(key)
        # Constructed here so an unknown strategy fails before a process
        # exists. ``__init__`` therefore runs in the parent as well as the
        # worker; a fault in it can still take this process down. The
        # instance is the worker's to keep.
        self._strategy_factory(key)
        budget = _budget_s(request)
        slot = WorkerSlot(
            session_id=request.session_id,
            role="create",
            strategy_name=key,
            type=key,
            created_by=request.created_by,
            start_budget_s=budget,
        )
        self._workers[request.session_id] = slot
        reported = False
        try:
            if self._persist_live is not None:
                await self._persist_live(
                    session_id=request.session_id,
                    created_by=request.created_by,
                    type=key,
                    yaml_text=request.yaml_text,
                    td=dump_td(dict(request.td)),
                    md_ids=dict(request.md),
                    st_paras=dict(request.st_paras),
                    restart=request.restart,
                    instance=request.instance,
                )
            assert self._spawner is not None
            if self._closing:
                await self._fail_unstarted(slot, request, START_FAIL_REASON)
                raise RuntimeError(START_FAIL_REASON)
            spawned = await self._spawner.spawn(
                session_id=request.session_id,
                role="create",
                request_json=request.model_dump_json().encode(),
            )
            self._take_spawn(slot, spawned)
            if self._closing:
                await self._fail_unstarted(slot, request, START_FAIL_REASON)
                raise RuntimeError(START_FAIL_REASON)
            try:
                line = await asyncio.wait_for(
                    spawned.read_result(),
                    timeout=_start_remaining(started, budget),
                )
            except TimeoutError:
                # Set before the abort so the handler below does not replace
                # the deadline reason with "worker exited during start".
                reported = True
                reason = _deadline_reason(budget, _slot_on_start_at(slot))
                await self._abort_unstarted(slot, request, reason)
                raise StartDeadlineExceeded(reason) from None
            parsed = parse_worker_result(line)
            if parsed is None:
                # No line at all: the worker never said it had written the
                # row. This path is the one that marks ``failed``.
                reported = True
                await self._fail_unstarted(slot, request, START_FAIL_REASON)
                raise RuntimeError(START_FAIL_REASON)
            if not parsed.get("ok"):
                # The worker only writes the row when ``start()`` fails.
                # Validation, ``__init__``, and ``ensure_deployable`` raise
                # before that, and the row the parent persisted stays
                # ``live`` with no process behind it. A row that is already
                # terminal — ``start()`` wrote ``failed`` — is left alone.
                reported = True
                detail = START_FAIL_REASON
                error = parsed.get("error")
                if error:
                    detail = f"{START_FAIL_REASON}: {error}"
                self._drop_unstarted(slot)
                if await self._row_view(slot.session_id) in {_VIEW_LIVE, _VIEW_UNREAD}:
                    await self._write_failed(slot, request, detail)
                raise RuntimeError(detail)
            slot.started = True
            slot.started_at = asyncio.get_running_loop().time()
            slot.strategy_name = str(parsed.get("strategy") or "") or slot.strategy_name
            self._arm_watcher(slot)
            return StsCreateSessionResult(
                session_id=request.session_id,
                strategy=slot.strategy_name or key,
                status=str(parsed.get("status") or SessionStatus.LIVE.value),
                reason=(
                    str(parsed["reason"]) if parsed.get("reason") is not None else None
                ),
            )
        except asyncio.CancelledError:
            # ``except Exception`` does not see this. Shutdown cancels the
            # create task while it is still in ``read_result``; leaving the
            # slot would leak the process.
            if not slot.started and not reported:
                await self._fail_unstarted(slot, request, START_FAIL_REASON)
            raise
        except Exception:
            if not slot.started and not reported:
                await self._fail_unstarted(slot, request, START_FAIL_REASON)
            raise

    def _drop_unstarted(self, slot: WorkerSlot) -> None:
        """Take a worker that never reported success out of the table.

        Does not write the row. The caller decides that from the row's
        status: an error line leaves a ``live`` row to be marked, and a
        row the worker already finished is left as it is.
        """
        if slot.started or slot.abandoned:
            return
        slot.abandoned = True
        if self._workers.get(slot.session_id) is slot:
            self._workers.pop(slot.session_id, None)
        self._release_lifeline(slot)
        self._release_beat(slot)
        self._reap_failed(slot.process)

    async def _abort_unstarted(
        self,
        slot: WorkerSlot,
        request: StsCreateSessionRequest,
        reason: str | None = None,
    ) -> None:
        """SIGKILL a worker that did not report inside the create budget.

        TD is attached only after create returns live, so a strategy still
        in ``on_start`` has no resting orders for ``on_stop`` to cancel.
        SIGTERM would wait ``WORKER_STOP_WAIT_S``, which does not fit in
        the slack before the API's own timeout.

        The flag is claimed before any await. A force-stop that already
        owns the kill keeps its reason. The slot is dropped before the
        row write, so a force-stop that arrives during the write finds
        nothing and answers ``not_found`` instead of a half-torn-down slot.
        """
        if slot.started or slot.abandoned or slot.stop_escalated:
            return
        slot.stop_escalated = True
        slot.kill_reason = reason or _deadline_reason(
            slot.start_budget_s or STS_START_DEADLINE_S,
            _slot_on_start_at(slot),
        )
        process = slot.process
        if process is not None and process.returncode is None:
            kill_worker(process)
            await process.wait()
        self._drop_unstarted(slot)
        # A failed read is not "already terminal". Skipping the write here
        # leaves the row ``live``, and the reaper then marks it
        # ``interrupted`` for a rebuild.
        if await self._row_view(slot.session_id) == _VIEW_TERMINAL:
            return
        await self._write_failed(slot, request, slot.kill_reason)

    def _take_spawn(self, slot: WorkerSlot, spawned: Any) -> None:
        """Record the process and start reading its beat pipe."""
        slot.process = spawned.process
        slot.result_reader = spawned
        slot.lifeline_fd = getattr(spawned, "lifeline", None)
        slot.beat_fd = getattr(spawned, "beat", None)
        self._arm_beat_reader(slot)

    def _arm_beat_reader(self, slot: WorkerSlot) -> None:
        fd = slot.beat_fd
        if fd is None or slot.beat_task is not None:
            return
        slot.beat_task = asyncio.create_task(
            self._read_beats(slot, fd),
            name=f"sts-beat-{slot.session_id}",
        )

    async def _read_beats(self, slot: WorkerSlot, fd: int) -> None:
        """``last_beat`` moves each time the worker's loop writes a byte."""
        loop = asyncio.get_running_loop()
        reader = asyncio.StreamReader()
        fileobj = os.fdopen(fd, "rb", buffering=0)
        slot.beat_fd = None
        try:
            transport, _ = await loop.connect_read_pipe(
                lambda: asyncio.StreamReaderProtocol(reader),
                fileobj,
            )
        except Exception:
            logger.exception("STS beat pipe failed session=%s", slot.session_id)
            fileobj.close()
            return
        try:
            while True:
                chunk = await reader.read(4096)
                if not chunk:
                    return
                slot.last_beat = loop.time()
        except asyncio.CancelledError:
            raise
        finally:
            transport.close()

    def _beat_is_silent(self, slot: WorkerSlot) -> bool:
        """True when the worker's loop has not beaten for ``BEAT_SILENCE_S``.

        No beat yet, and ``started`` was only just set, is the first
        interval — not silence. No beat long after start means the pipe
        never moved, which is the same as a loop that stopped writing.
        """
        now = asyncio.get_running_loop().time()
        if slot.last_beat is not None:
            return (now - slot.last_beat) >= BEAT_SILENCE_S
        if slot.started_at is None:
            return False
        return (now - slot.started_at) >= BEAT_SILENCE_S

    def _release_lifeline(self, slot: WorkerSlot) -> None:
        """Close the write end. The worker reads that as its parent dying."""
        fd = slot.lifeline_fd
        slot.lifeline_fd = None
        if fd is None:
            return
        try:
            os.close(fd)
        except OSError:
            pass

    def _release_beat(self, slot: WorkerSlot) -> None:
        """Stop reading the beat pipe and close it if the reader never took it."""
        task = slot.beat_task
        slot.beat_task = None
        if task is not None and not task.done():
            task.cancel()
            self._reaps.add(task)
            task.add_done_callback(self._reaps.discard)
        fd = slot.beat_fd
        slot.beat_fd = None
        if fd is None:
            return
        try:
            os.close(fd)
        except OSError:
            pass

    async def _fail_unstarted(
        self,
        slot: WorkerSlot,
        request: StsCreateSessionRequest,
        reason: str,
    ) -> None:
        """A worker that never reported a result line. Not a rebuild candidate."""
        if slot.stop_escalated:
            # ``escalate_stop`` owns the row. A start-failure reason here
            # would replace ``stop timed out; worker killed``.
            return
        if slot.started or slot.abandoned:
            return
        self._drop_unstarted(slot)
        if await self._row_view(slot.session_id) == _VIEW_TERMINAL:
            # The worker ended the session itself before it could report —
            # an operator stop while it sat in on_start writes ``done`` and
            # exits without a result line. That row says what happened;
            # ``failed`` here would replace it with a deploy failure.
            logger.info(
                "STS worker exited before reporting, row already final session=%s",
                slot.session_id,
            )
            return
        await self._write_failed(slot, request, reason)

    async def _write_failed(
        self,
        slot: WorkerSlot,
        request: StsCreateSessionRequest,
        reason: str,
    ) -> None:
        """Mark the row ``failed``. A miss is retried, not announced.

        Publishing ``failed`` without the write is how the reaper later
        calls the same row ``interrupted`` and a restart rebuilds it.
        The quick attempts match a kill's row write. What they cannot
        cover is a database that stays down for seconds, so the rest of
        the budget runs in the background and the reaper, if it gets
        there first, writes this reason instead of ``interrupted``.
        """
        if self._mark_done is None:
            await self._publish_failed(slot, reason)
            return
        if await self._mark_failed_attempts(
            slot.session_id, reason, attempts=MARK_KILL_ATTEMPTS
        ):
            await self._publish_failed(slot, reason)
            return
        self._remember_unwritten(slot, reason)

    async def _mark_failed_attempts(
        self, session_id: str, reason: str, *, attempts: int
    ) -> bool:
        if self._mark_done is None or attempts < 1:
            return False
        for attempt in range(attempts):
            try:
                await self._mark_done(
                    session_id,
                    status=SessionStatus.FAILED.value,
                    reason=reason,
                )
                return True
            except Exception:
                logger.exception(
                    "STS failed to mark a start failure session=%s attempt=%s",
                    session_id,
                    attempt + 1,
                )
        return False

    def _remember_unwritten(self, slot: WorkerSlot, reason: str) -> None:
        self._unwritten_failures[slot.session_id] = reason
        if any(
            not task.done() and task.get_name() == f"sts-fail-write-{slot.session_id}"
            for task in self._failure_retries
        ):
            return
        task = asyncio.create_task(
            self._retry_failed_write(slot, reason),
            name=f"sts-fail-write-{slot.session_id}",
        )
        self._failure_retries.add(task)
        task.add_done_callback(self._failure_retries.discard)

    async def _publish_failed(self, slot: WorkerSlot, reason: str) -> None:
        await self._publish_status(
            slot.session_id,
            status=SessionStatus.FAILED.value,
            strategy=slot.strategy_name,
            created_by=slot.created_by,
            reason=reason,
            type=slot.type,
        )

    async def _retry_failed_write(self, slot: WorkerSlot, reason: str) -> None:
        delay = FAILED_WRITE_BACKOFF_S
        started = asyncio.get_running_loop().time()
        while True:
            if self._closing or self._shutting_down:
                return
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                raise
            if self._closing or self._shutting_down:
                return
            if await self._failure_already_written(slot.session_id):
                self._unwritten_failures.pop(slot.session_id, None)
                return
            if await self._mark_failed_attempts(slot.session_id, reason, attempts=1):
                self._unwritten_failures.pop(slot.session_id, None)
                await self._publish_failed(slot, reason)
                return
            if asyncio.get_running_loop().time() - started >= FAILED_WRITE_BUDGET_S:
                logger.error(
                    "STS gave up marking a start failure session=%s",
                    slot.session_id,
                )
                return
            delay = min(delay * 2, 5.0)

    async def _failure_already_written(self, session_id: str) -> bool:
        """True when the row is already terminal. A read error is not.

        ``_row_is_live`` treats a database error as "not live", which
        would stop this retry during the outage it exists to outlast.
        """
        if self._load_session is None:
            return False
        try:
            row = await self._load_session(session_id)
        except Exception:
            logger.exception(
                "STS could not read a start failure session=%s", session_id
            )
            return False
        if row is None:
            return True
        return getattr(row, "status", None) != SessionStatus.LIVE.value

    def _arm_watcher(self, slot: WorkerSlot) -> None:
        task = asyncio.create_task(
            self._on_worker_exit(slot),
            name=f"sts-worker-{slot.session_id}",
        )
        slot.watcher = task

    async def _on_worker_exit(self, slot: WorkerSlot) -> None:
        """A worker that dies after it reported success.

        Exit 0 means the worker wrote the terminal row itself. Anything else,
        while the row is still live, is the same interrupted state a dead
        STS process used to leave, and the same rebuild flag decides whether
        to bring it back. A worker that had not reported yet is the create
        path's problem: marking it interrupted would rebuild a deploy the
        API already failed.
        """
        process = slot.process
        if process is None:
            return
        try:
            code = await process.wait()
        except Exception:
            logger.exception("STS worker wait failed session=%s", slot.session_id)
            return
        if not slot.started or slot.abandoned or self._shutting_down:
            return
        if self._workers.get(slot.session_id) is not slot:
            return
        self._workers.pop(slot.session_id, None)
        self._release_lifeline(slot)
        self._release_beat(slot)
        if slot.stop_escalated:
            # A live row was SIGKILLed and is not a rebuild. A row that is
            # already terminal — the queued stop finished as the kill
            # arrived — stays as the worker wrote it.
            if (
                not self._closing
                and not self._shutting_down
                and (
                    await self._row_view(slot.session_id) in {_VIEW_LIVE, _VIEW_UNREAD}
                )
            ):
                await self._mark_stop_killed(slot)
            return
        if code == 0:
            # The worker returned from its last ``close`` before exiting, so
            # the row already has the strategy's reason. Rewriting it here,
            # or leaving a still-live row for the reaper, would replace that
            # reason. A 0 with the row still live is a bug, and the reaper
            # is only the fallback for that bug.
            return
        if not await self._row_is_live(slot.session_id):
            return
        reason = "process died"
        if self._mark_done is not None:
            try:
                await self._mark_done(
                    slot.session_id,
                    status=SessionStatus.INTERRUPTED.value,
                    reason=reason,
                )
            except Exception:
                logger.exception(
                    "STS failed to mark a dead worker session=%s",
                    slot.session_id,
                )
                return
        await self._publish_status(
            slot.session_id,
            status=SessionStatus.INTERRUPTED.value,
            strategy=slot.strategy_name,
            reason=reason,
            created_by=slot.created_by,
            type=slot.type,
        )
        if not self._rebuild_on_worker_exit or self._shutting_down or self._closing:
            return
        self._schedule_rebuild(slot.session_id)

    def _schedule_rebuild(self, session_id: str) -> None:
        """Run ``rebuild_session`` where ``close_all`` can cancel it."""
        if self._closing or self._shutting_down:
            return
        task = asyncio.create_task(
            self._rebuild_after_exit(session_id),
            name=f"sts-rebuild-{session_id}",
        )
        self._rebuild_tasks.add(task)
        task.add_done_callback(self._rebuild_tasks.discard)

    async def _rebuild_after_exit(self, session_id: str) -> None:
        try:
            await self.rebuild_session(session_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "STS rebuild after worker exit failed session=%s",
                session_id,
            )

    def track_create(self, task: asyncio.Task[Any]) -> None:
        """Hold a create RPC so shutdown can cancel it."""
        self._create_tasks.add(task)
        task.add_done_callback(self._create_tasks.discard)

    def track_escalation(self, task: asyncio.Task[Any]) -> None:
        """Hold a force-stop RPC so shutdown can cancel it."""
        self._escalation_tasks.add(task)
        task.add_done_callback(self._escalation_tasks.discard)

    async def _row_view(self, session_id: str) -> str:
        """``live``, ``terminal``, ``unread``, or ``absent``.

        ``unread`` is a failed read. It is not evidence the row already
        ended. A start-failure write that treats it as terminal is how a
        deadline kill stays ``live`` until the reaper calls it
        ``interrupted`` and a restart rebuilds it. ``absent`` means this
        process has no loader.
        """
        if self._load_session is None:
            return _VIEW_ABSENT
        try:
            row = await self._load_session(session_id)
        except Exception:
            logger.exception(
                "STS could not tell whether session=%s is still live",
                session_id,
            )
            return _VIEW_UNREAD
        if row is not None and getattr(row, "status", None) == SessionStatus.LIVE.value:
            return _VIEW_LIVE
        return _VIEW_TERMINAL

    async def _row_is_live(self, session_id: str) -> bool:
        """Whether a successful read says this one row is still ``live``.

        ``list_sessions`` pages the fleet, newest first, and would treat a
        live session past that page as already gone. A failed read is not
        live here. Callers that must not skip a ``failed`` write on that
        uncertainty use :meth:`_row_view` instead.
        """
        return await self._row_view(session_id) == _VIEW_LIVE

    async def _wait_until_row_settles(self, session_id: str) -> None:
        """Give an in-flight deadline kill a moment to write the row.

        The slot is already flagged. The write is the part that is still
        running, and ``not_found`` before it lands is a timeout to the API.
        """
        if self._load_session is None:
            return
        deadline = asyncio.get_running_loop().time() + ABORT_ROW_WAIT_S
        while await self._row_is_live(session_id):
            if asyncio.get_running_loop().time() >= deadline:
                return
            await asyncio.sleep(0.02)

    async def list_sessions(self, request: ListSessionsRequest) -> list[SessionInfo]:
        if request.domain not in (None, SessionDomain.STS.value, "sts"):
            return []

        if self._list_db_sessions is not None:
            db_rows = await self._list_db_sessions(
                status=request.status,
                created_by=request.created_by,
            )
            out: list[SessionInfo] = []
            for row in db_rows:
                live = self._sessions.get(row.session_id) or self._workers.get(
                    row.session_id
                )
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
                            (
                                getattr(live, "strategy_name", None)
                                if live is not None
                                else None
                            )
                            or _row_key(row)
                        ),
                        reason=getattr(row, "reason", None),
                        type=(
                            (live.type if live is not None else None) or _row_key(row)
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

    async def escalate_stop(
        self,
        session_id: str,
        *,
        deadline: float | None = None,
        only_if_silent: bool = False,
        abort_start: bool = False,
    ) -> StsSessionControlResult:
        """Kill a worker whose control subject did not answer stop.

        The slot may still be in ``on_start``: a sync call there is the
        same stuck loop, and it has a process whether or not it has
        reported success. No slot means this process has nothing to
        signal — in-process mode, or a session held somewhere else.

        ``deadline`` is the caller's wall clock. Past it, the kill is
        dropped: the API has already told the caller this failed.
        ``only_if_silent`` is the no-responders path. The stop was never
        delivered, so a worker that is still starting, or still beating,
        is left alone.
        """
        inflight = self._stop_escalations.get(session_id)
        if inflight is not None:
            return await inflight
        slot = self._workers.get(session_id)
        if slot is not None and slot.escalation is not None:
            return await slot.escalation
        if slot is None or slot.process is None:
            raise KeyError(f"no active sts session {session_id}")

        future: asyncio.Future[StsSessionControlResult] = (
            asyncio.get_running_loop().create_future()
        )
        slot.escalation = future
        self._stop_escalations[session_id] = future
        try:
            result = await self._run_escalation(
                slot,
                deadline=deadline,
                only_if_silent=only_if_silent,
                abort_start=abort_start,
            )
        except asyncio.CancelledError:
            # Shutdown cancelled this task. ``_stop_workers`` owns the row
            # from here; writing the kill reason would replace it.
            if not future.done():
                future.cancel()
            raise
        except (WorkerNotStuck, ForceStopExpired) as exc:
            # A refusal is not a kill. Concurrent waiters still see it;
            # a later force-stop must be able to try again.
            if slot.escalation is future:
                slot.escalation = None
            if not future.done():
                future.set_exception(exc)
                future.exception()
            raise
        except Exception as exc:
            if not future.done():
                future.set_exception(exc)
                # The caller is re-raising this. Mark it retrieved so a
                # second waiter is the only one who still has to see it,
                # and an un-awaited future does not warn at shutdown.
                future.exception()
            raise
        else:
            if not future.done():
                future.set_result(result)
            return result
        finally:
            if self._stop_escalations.get(session_id) is future:
                self._stop_escalations.pop(session_id, None)

    async def _run_escalation(
        self,
        slot: WorkerSlot,
        *,
        deadline: float | None,
        only_if_silent: bool,
        abort_start: bool = False,
    ) -> StsSessionControlResult:
        """SIGKILL, then the row. No SIGTERM first.

        A loop that could still run a signal handler would have answered
        the stop that was delivered. SIGTERM runs the worker's shutdown
        path, which writes ``interrupted`` and is then rebuilt. A stop
        that was never delivered only kills when the beat has gone silent.

        ``abort_start`` is the create-timeout kill. A worker still in
        ``on_start`` records the start deadline. One that already reported
        success records that the reply did not arrive: the budget was met,
        and blaming ``on_start`` for the wait points at ``start_timeout``.
        TD is still unattached — attach runs only after create returns
        live — so skipping ``on_stop`` leaves no resting order.
        """
        process = slot.process
        if process is None:
            raise KeyError(f"no active sts session {slot.session_id}")
        if slot.stop_escalated or slot.abandoned:
            # The create deadline, or an earlier kill, already owns the
            # reason. Writing here would replace it. ``abort_start`` waits
            # out that write: answering ``not_found`` while the row is
            # still ``live`` is how the API reports a timeout for a create
            # that is about to be ``start_deadline``.
            if abort_start:
                await self._wait_until_row_settles(slot.session_id)
            # ``unread`` is not terminal. Answering with whatever the last
            # read guessed would skip the failed write the deadline kill
            # is still retrying.
            if await self._row_view(slot.session_id) != _VIEW_TERMINAL:
                raise KeyError(f"no active sts session {slot.session_id}")
            return await self._escalation_result(slot)
        # ``abort_start`` has no wall-clock deadline. The API has already
        # told the caller this create failed, and the message may arrive
        # after any deadline it could have carried. Dropping it leaves the
        # worker live with no MD attached.
        if not abort_start and deadline is not None and time.time() >= deadline:
            raise ForceStopExpired(slot.session_id)
        if only_if_silent and process.returncode is None:
            if not slot.started or not self._beat_is_silent(slot):
                raise WorkerNotStuck(slot.session_id)
        slot.stop_escalated = True
        if abort_start:
            if slot.started:
                slot.kill_reason = CREATE_REPLY_LOST_REASON
            else:
                budget = slot.start_budget_s or STS_START_DEADLINE_S
                slot.kill_reason = _deadline_reason(budget, _slot_on_start_at(slot))
        killed = False
        if process.returncode is None:
            kill_worker(process)
            killed = True
            await process.wait()
        if killed:
            await self._note_kill(slot, process)
        watcher = slot.watcher
        if watcher is not None:
            # The watcher is the row writer. Waiting on ``process.wait``
            # alone replies before ``failed`` is stored.
            if not watcher.done():
                await watcher
        else:
            # Unstarted: nothing is watching the exit. ``on_start`` has
            # not armed one, and the create path must not record this as
            # a deploy failure once the flag is set.
            if (
                not self._closing
                and not self._shutting_down
                and (
                    await self._row_view(slot.session_id) in {_VIEW_LIVE, _VIEW_UNREAD}
                )
            ):
                await self._mark_stop_killed(slot)
            if not slot.abandoned:
                self._drop_unstarted(slot)
        return await self._escalation_result(slot)

    async def _note_kill(self, slot: WorkerSlot, process: Any) -> None:
        """The kill has to be visible. The worker will not write it."""
        pid = getattr(process, "pid", None)
        logger.warning(
            "STS stop unanswered; killing worker session=%s pid=%s",
            slot.session_id,
            pid,
        )
        try:
            await publish_sts_log(
                self._broker,
                slot.session_id,
                KILL_LOG_MESSAGE,
                source="sts",
                level="warning",
                type=slot.type,
            )
        except Exception:
            logger.exception("STS kill log failed session=%s", slot.session_id)

    async def _mark_stop_killed(self, slot: WorkerSlot) -> bool:
        """Write ``failed`` for a worker that never closed its own row.

        Skipped once shutdown has started. ``close_all`` writes its own
        reason first and then signals; this must not land on top of it.
        Retried: a miss leaves the row ``live``, and reporting ``failed``
        anyway is how a killed session comes back as a rebuild.
        """
        if self._closing or self._shutting_down:
            return False
        if self._mark_done is None:
            return False
        reason = slot.kill_reason or STS_REASON_STOP_TIMED_OUT
        for attempt in range(MARK_KILL_ATTEMPTS):
            try:
                await self._mark_done(
                    slot.session_id,
                    status=SessionStatus.FAILED.value,
                    reason=reason,
                )
                break
            except Exception:
                logger.exception(
                    "STS failed to mark a killed worker session=%s attempt=%s",
                    slot.session_id,
                    attempt + 1,
                )
                if attempt + 1 == MARK_KILL_ATTEMPTS:
                    # A start-deadline kill whose write missed is not an
                    # orphan the reaper should rebuild. Remember the reason
                    # and keep trying; the reaper uses it if it gets there
                    # first.
                    if slot.kill_reason:
                        self._remember_unwritten(slot, reason)
                    return False
        await self._publish_status(
            slot.session_id,
            status=SessionStatus.FAILED.value,
            strategy=slot.strategy_name,
            reason=reason,
            created_by=slot.created_by,
            type=slot.type,
        )
        return True

    async def _escalation_result(self, slot: WorkerSlot) -> StsSessionControlResult:
        """The row as it stands after the process is gone.

        A row the worker already closed — often ``done`` /
        ``operator_stop``, when the queued stop finished as the kill
        arrived — is left alone. A kill the row write did not record is
        not reported as ``failed``: the row is still ``live``, and saying
        otherwise is how the reaper rebuilds it.
        """
        row = None
        if self._load_session is not None:
            try:
                row = await self._load_session(slot.session_id)
            except Exception:
                logger.exception(
                    "STS could not read the row after killing session=%s",
                    slot.session_id,
                )
        status = getattr(row, "status", None) if row is not None else None
        if not status or status == SessionStatus.LIVE.value:
            raise RuntimeError(
                f"stop kill did not land on the row for {slot.session_id}"
            )
        return StsSessionControlResult(
            session_id=slot.session_id,
            status=str(status),
            strategy=slot.strategy_name,
            reason=getattr(row, "reason", None),
        )

    async def fail_session(
        self, session_id: str, *, reason: str
    ) -> StsSessionControlResult:
        """Tear down a live session as a failure — attach-rollback, not stop."""
        session = self._sessions.get(session_id)
        if session is None:
            raise KeyError(f"no active sts session {session_id}")
        strategy = session.strategy_name
        await self.close(session_id, status=SessionStatus.FAILED.value, reason=reason)
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
        # Counted before the first await. ``wait_until_quiet`` treats this
        # as still running until the row write below has returned.
        self._closes += 1
        broker = session.broker
        strategy_name = session.strategy_name
        created_by = session.created_by
        session_type = session.type
        try:
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
        finally:
            self._closes -= 1

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
            if session_id is None or self._holds(session_id):
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
            #
            # A start-deadline kill whose row write missed is the exception.
            # That deploy already failed. Calling it interrupted is how a
            # restart runs ``on_start`` again, this time with MD attached.
            pending = self._unwritten_failures.get(session_id)
            if pending is not None:
                status = SessionStatus.FAILED.value
                reason = pending
            else:
                status = SessionStatus.INTERRUPTED.value
                reason = "process died: no session heartbeat"
            try:
                await self._mark_done(
                    session_id,
                    status=status,
                    reason=reason,
                )
            except Exception:
                logger.exception("STS orphan reap failed session=%s", session_id)
                continue
            self._unwritten_failures.pop(session_id, None)
            await self._publish_status(
                session_id,
                status=status,
                strategy=_row_key(row),
                reason=reason,
                created_by=getattr(row, "created_by", None),
                type=_row_key(row),
            )
            self._orphan_strikes.pop(session_id, None)
            reaped.append(session_id)
            logger.warning(
                "STS reaped orphaned session id=%s strategy=%s",
                session_id,
                _row_key(row),
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
        for row in rows:
            session_id = getattr(row, "session_id", None)
            if session_id is None:
                continue
            if await self.rebuild_session(session_id, row=row):
                rebuilt.append(session_id)
        return rebuilt

    async def rebuild_session(self, session_id: str, *, row: Any | None = None) -> bool:
        """Restore one interrupted session, and only that one.

        The claim happens before the first await. A boot scan and a
        worker-exit watcher can both reach here; the second finds the id
        already held and does not spawn another process. Releasing the
        claim on the way out is what makes a session that was not eligible
        available to a later scan — a spawn that reported success keeps
        its worker slot, which is a different hold.
        """
        if self._closing or self._holds(session_id):
            return False
        if session_id in self._unwritten_failures:
            # The deadline kill did not land on the row. Rebuilding it
            # would run the strategy the API already failed.
            return False
        slot: WorkerSlot | None = None
        if self._spawner is not None:
            slot = WorkerSlot(session_id=session_id, role="rebuild")
            self._workers[session_id] = slot
        else:
            self._claims.add(session_id)
        try:
            if row is None:
                row = await self._find_interrupted(session_id)
                if row is None:
                    return False
            self._note_rebuild_row(slot, row)
            return await self._rebuild_claimed(row, slot)
        finally:
            self._claims.discard(session_id)
            if (
                slot is not None
                and not slot.started
                and self._workers.get(session_id) is slot
            ):
                self._workers.pop(session_id, None)
                self._release_lifeline(slot)
                self._reap_failed(slot.process)

    def _note_rebuild_row(self, slot: WorkerSlot | None, row: Any) -> None:
        """The slot is claimed before the row is read, so its name starts empty.

        Filled as soon as the row is in hand, before the next await. List
        also falls back to the row while this is still None.
        """
        if slot is None:
            return
        slot.strategy_name = _row_key(row)
        slot.type = _row_key(row)
        created_by = getattr(row, "created_by", None)
        if created_by is not None:
            slot.created_by = int(created_by)

    async def _find_interrupted(self, session_id: str) -> Any | None:
        if self._load_session is not None:
            try:
                row = await self._load_session(session_id)
            except Exception:
                logger.exception(
                    "STS could not load interrupted session=%s", session_id
                )
                return None
            if (
                row is None
                or getattr(row, "status", None) != SessionStatus.INTERRUPTED.value
            ):
                return None
            return row
        if self._list_db_sessions is None:
            return None
        try:
            rows = await self._list_db_sessions(
                status=SessionStatus.INTERRUPTED.value,
                created_by=None,
                limit=_REBUILD_SCAN_LIMIT,
            )
        except Exception:
            logger.exception("STS could not load interrupted session=%s", session_id)
            return None
        for row in rows:
            if getattr(row, "session_id", None) == session_id:
                return row
        return None

    async def _rebuild_claimed(self, row: Any, slot: WorkerSlot | None) -> bool:
        """Eligibility and the restore itself. The id is already claimed."""
        session_id = row.session_id
        now = datetime.now(UTC)
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
            return False
        if not await self._placement_is_mine(row):
            # Somebody else's run, or a null row whose derivation is
            # not this process — including "not unique", which waits
            # on the Attention list rather than a coin flip.
            logger.debug(
                "STS not rebuilding session=%s: placement is not %s",
                session_id,
                self._instance,
            )
            return False
        if str(getattr(row, "restart", "always")) != "always":
            # This run said it would rather stay ended. Nothing to warn
            # about — it is doing what it was deployed to do.
            logger.info(
                "STS not rebuilding session=%s: deployed with restart=%s",
                session_id,
                getattr(row, "restart", None),
            )
            return False
        attempts = int(getattr(row, "rebuild_count", 0) or 0)
        if attempts >= self._rebuild_max_attempts:
            logger.warning(
                "STS not rebuilding session=%s: already tried %d times",
                session_id,
                attempts,
            )
            return False
        if not is_v1_session_id(session_id):
            # Predates the 6-hex session id. Rebuilding would fail inside
            # the client_order_id factory — worse than leaving the row.
            logger.warning(
                "STS cannot rebuild session=%s: not a v1 session_id",
                session_id,
            )
            return False
        key = _row_key(row)
        if key is None:
            # Before the factory, not after: it answers ``None`` with the
            # default strategy, and "not rebuildable" is then reported
            # against a class this session never ran.
            logger.warning(
                "STS not rebuilding session=%s: %s", session_id, _NO_TYPE_REASON
            )
            return False
        try:
            ensure_deployable(key)
            strategy = self._strategy_factory(key)
        except IncompatibleEnvironment as exc:
            logger.warning(
                "STS not rebuilding session=%s: incompatible environment (%s)",
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
            return False
        except KeyError:
            # A row naming a strategy this build does not have. Expected —
            # a strategy can be renamed or withdrawn while a session that
            # ran it is still on file — so it reads like its neighbours
            # here rather than like a fault. A stack trace every boot for
            # a row that will never resolve teaches the operator to skip
            # the tracebacks.
            logger.warning(
                "STS not rebuilding session=%s: no strategy named %r in this build",
                session_id,
                key,
            )
            return False
        except Exception:
            # Anything else is the class failing to construct, which is a
            # fault and keeps its traceback.
            logger.exception(
                "STS cannot rebuild session=%s: strategy %r would not build",
                session_id,
                key,
            )
            return False
        if not strategy.rebuildable:
            # Readiness is the class flag, not the env var. Without it a
            # rebuilt instance would treat recon as a clean account and
            # place beside the orders this session left resting.
            logger.warning(
                "STS not rebuilding session=%s: %s does not support it",
                session_id,
                strategy.registry_key,
            )
            return False
        if self._bump_rebuild_count is not None:
            try:
                await self._bump_rebuild_count(session_id)
            except Exception:
                logger.exception("STS rebuild count bump failed session=%s", session_id)
        if slot is not None:
            return await self._spawn_rebuild(row, strategy, slot)
        try:
            await self._rebuild_one(row, strategy)
        except Exception:
            logger.exception("STS rebuild failed session=%s", session_id)
            await self._abandon_rebuild(session_id)
            return False
        session = self._sessions.get(session_id)
        if session is not None:
            self._watch_rebuild_settle(session_id, session)
        logger.info(
            "STS rebuilt session=%s strategy=%s",
            session_id,
            key,
        )
        return True

    async def _spawn_rebuild(
        self, row: Any, strategy: Strategy, slot: WorkerSlot
    ) -> bool:
        """Hand a claimed slot to a worker. No success line is not a retry.

        A missing result line leaves the row interrupted. This method does
        not mark it again, and ``close_all`` does not either: the slot is
        dropped on the way out, and an unstarted rebuild slot is not a live
        session this shutdown interrupted.
        """
        session_id = row.session_id
        assert self._spawner is not None
        if self._closing or self._workers.get(session_id) is not slot:
            return False
        try:
            spawned = await self._spawner.spawn(
                session_id=session_id,
                role="rebuild",
                request_json=None,
            )
        except Exception:
            logger.exception(
                "STS could not spawn a rebuild worker session=%s", session_id
            )
            return False
        self._take_spawn(slot, spawned)
        if self._closing or self._workers.get(session_id) is not slot:
            if self._workers.get(session_id) is not slot:
                self._release_lifeline(slot)
                self._release_beat(slot)
                self._reap_failed(slot.process)
            return False
        parsed = parse_worker_result(await spawned.read_result())
        if parsed is None or not parsed.get("ok"):
            # Collect the process. This path does not arm a watcher, and a
            # worker that exits without ``wait`` stays a zombie.
            self._reap_failed(slot.process)
            # No second write. The row stays interrupted when the worker
            # managed to say so, and live when it died after ``mark_live``
            # without a result line — the reaper collects that one. This
            # call does not spawn again. An interrupted row waits for the
            # next scan. ``close_all`` does not see the slot: it is dropped
            # below, and an unstarted rebuild is not pre-marked.
            logger.warning(
                "STS rebuild worker exited during start session=%s",
                session_id,
            )
            return False
        slot.started = True
        slot.started_at = asyncio.get_running_loop().time()
        slot.strategy_name = str(parsed.get("strategy") or _row_key(row) or "")
        slot.type = _row_key(row)
        slot.created_by = int(getattr(row, "created_by", 0) or 0)
        self._arm_watcher(slot)
        self._watch_rebuild_settle(session_id, slot)
        logger.info(
            "STS rebuilt session=%s strategy=%s",
            session_id,
            slot.strategy_name,
        )
        return True

    async def adopt_interrupted(self, session_id: str) -> StsCreateSessionResult:
        """Run one interrupted row in this process. The parent already counted it.

        The worker calls this. Eligibility and the attempt bump happened
        before the spawn, so doing them again would charge the session twice
        and could refuse a row the parent has already decided to restore.
        """
        row = await self._find_interrupted(session_id)
        if row is None:
            raise RuntimeError(f"no interrupted session {session_id}")
        key = _row_key(row)
        if key is None:
            # The parent refuses this row too. Said again here because the
            # worker is also reached by a rebuild the parent never planned —
            # and because defaulting is the one thing that must not happen.
            raise RuntimeError(
                f"cannot rebuild session {session_id}: {_NO_TYPE_REASON}"
            )
        strategy = self._strategy_factory(key)
        try:
            await self._rebuild_one(row, strategy)
        except Exception:
            await self._unwind_failed_rebuild(session_id)
            raise
        return StsCreateSessionResult(
            session_id=session_id,
            strategy=strategy.registry_key,
        )

    async def _unwind_failed_rebuild(self, session_id: str) -> None:
        """Leave an interrupted row's reason and ``finished_at`` alone.

        ``_rebuild_one`` writes the row itself when attach fails, and pops
        the session as it does. A failure before ``mark_live`` has not
        touched the row; dropping the session must not stamp the shutdown
        reason over it. Only a row that was already put back to ``live``
        is marked interrupted again.
        """
        session = self._sessions.get(session_id)
        if session is None:
            return
        if await self._row_is_live(session_id):
            await self.close(
                session_id,
                status=SessionStatus.INTERRUPTED.value,
                reason="rebuild failed",
            )
            return
        await self._abandon_rebuild(session_id)
        try:
            await session.stop()
        except Exception:
            logger.exception("STS rebuild unwind failed to stop session=%s", session_id)

    async def drain_exit_requests(self) -> None:
        """Let a ``fail`` or ``exit`` that already ran finish its ``close``.

        ``close`` pops the session and writes the strategy's reason. A
        shutdown that runs first marks the row ``interrupted``, and that
        ``close`` then finds nothing left to stop.
        """
        if any(session.exit_requested for session in self._sessions.values()):
            await self.wait_until_quiet()

    async def wait_until_quiet(self) -> None:
        """Block until the last ``close`` has returned and control loops are done.

        ``close`` pops the session and stops the control loop before it
        awaits ``stop``, the row write, and the publish. A strategy that
        calls ``exit`` runs that ``close`` as its own task. Waiting only
        for an empty ``_sessions`` returns while that write is still in
        flight, and the process exit cancels it. ``_closes`` stays above
        zero until the write has returned. The control task stays in
        ``_retiring`` until its reply has been handed to the broker.
        """
        while self._sessions or self._retiring or self._closes:
            retiring = set(self._retiring)
            if retiring:
                await asyncio.wait(retiring, timeout=0.05)
            else:
                await asyncio.sleep(0.05)

    def _watch_rebuild_settle(self, session_id: str, token: object) -> None:
        """Start the timer that forgives this session's attempt count.

        The session is read here rather than inside the task: a task does not
        run until the loop next yields, by which time the session it was
        started for may already have been replaced by another one under the
        same id.
        """
        if self._reset_rebuild_count is None or self._rebuild_settle_s <= 0:
            return
        task = asyncio.create_task(
            self._settle_rebuild(session_id, token),
            name=f"sts-rebuild-settle-{session_id}",
        )
        self._settle_tasks.add(task)
        task.add_done_callback(self._settle_tasks.discard)

    async def _settle_rebuild(self, session_id: str, token: object) -> None:
        """Clear the attempt count once a rebuilt session has kept running.

        The holder is compared by identity, not by id: a session that stopped
        and was deployed again under the same id is a different run, and
        clearing the count on its behalf would credit it for surviving
        something it was never part of. A worker slot is that holder when
        the session is a process.
        """
        await asyncio.sleep(self._rebuild_settle_s)
        current = self._sessions.get(session_id)
        if current is None:
            current = self._workers.get(session_id)
        if current is not token:
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
            logger.exception("STS rebuild count reset failed session=%s", session_id)
            return
        logger.info(
            "STS rebuild settled session=%s — attempt count cleared after %.0fs",
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
            td=td,
            md=md,
            st_paras=dict(getattr(row, "st_paras", None) or {}),
            heartbeat_interval=self._heartbeat_interval,
            on_exit=self._on_session_exit,
            remember=self._remember_fact,
            strategy_type=_row_key(row),
        )
        self._sessions[session_id] = session
        self._serve_control(session_id)

        # Before on_start, so every hook that follows already sees whatever
        # the strategy restored — including on_recon_done, which is where a
        # strategy has to know these orders are its own.
        remembered = {
            str(k): str(v) for k, v in (getattr(row, "st_facts", None) or {}).items()
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
        except AttachRefused as exc:
            # The domain is up and refused the feeds. Retrying the
            # rebuild would fail the same way.
            await self.close(
                session_id,
                status=SessionStatus.FAILED.value,
                reason=f"rebuild failed to attach: {exc}",
            )
            raise
        except Exception:
            # A session with half its attaches is worse than one still marked
            # interrupted: it heartbeats and looks alive while blind to a feed
            # or an account. Put it back for the next boot to try. This is
            # the timeout path: MD or TD never answered inside the budget.
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
                    Topics.MD if instance == ANY_INSTANCE else Topics.md(instance)
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
                owner = (result.instance if result is not None else "") or (
                    "" if instance == ANY_INSTANCE else instance
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

    async def _attach_td(self, session_id: str, created_by: int, api_id: int) -> None:
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
                # The domain is up and refused. Retrying the rebuild
                # would fail the same way, including a TD ``attach_failed``.
                # ``unavailable`` and ``timeout`` stay in the loop: that
                # is the domain not answering yet.
                if err.code not in {"unavailable", "timeout"}:
                    raise AttachRefused(str(last))
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

        Workers are signalled together. ``ON_STOP_TIMEOUT_S`` is longer than
        the wait below, so a strategy that spends the whole of ``on_stop``
        is still killed; the wait is not a claim that every child exited
        cleanly. The row is written first, before the signal, for the same
        reason an in-process shutdown writes it before ``close``.
        """
        self._shutting_down = True
        self._closing = True
        for task in list(self._failure_retries):
            task.cancel()
        # Cancel before the first await. Create and rebuild are not awaited
        # yet: their ``read_result`` sits in a thread, and that thread
        # unblocks when the worker is signalled below. Awaiting them first
        # would stall shutdown on a worker stuck in ``on_start``.
        settle = self._drain_tracked(self._settle_tasks)
        rebuilds = self._drain_tracked(self._rebuild_tasks)
        creates = self._drain_tracked(self._create_tasks)
        escalations = self._drain_tracked(self._escalation_tasks)
        for task in (*settle, *rebuilds, *creates, *escalations):
            task.cancel()
        if settle:
            await asyncio.gather(*settle, return_exceptions=True)
        await self._stop_workers()
        if rebuilds or creates or escalations:
            await asyncio.gather(
                *rebuilds, *creates, *escalations, return_exceptions=True
            )
        if self._reaps:
            await asyncio.wait(list(self._reaps), timeout=WORKER_STOP_WAIT_S)
        await self._close_in_process()

    def _drain_tracked(self, tasks: set[asyncio.Task[Any]]) -> list[asyncio.Task[Any]]:
        pending = [task for task in list(tasks) if not task.done()]
        tasks.clear()
        return pending

    async def _stop_workers(self) -> None:
        if not self._workers:
            return
        for slot in list(self._workers.values()):
            # The cancelled create task also tries to fail this slot. One
            # write is enough, and it is the one below.
            if not slot.started:
                slot.abandoned = True
        for slot in list(self._workers.values()):
            # An unstarted rebuild slot is a claim, not a live session.
            # Marking it would stamp the shutdown reason over the
            # interrupted row and reset ``finished_at``.
            if slot.role == "rebuild" and not slot.started:
                continue
            if self._mark_done is None:
                break
            try:
                await self._mark_done(
                    slot.session_id,
                    status=SessionStatus.INTERRUPTED.value,
                    reason=_SHUTDOWN_REASON,
                )
            except Exception:
                logger.exception(
                    "STS shutdown pre-mark failed session=%s", slot.session_id
                )
        waiters: list[asyncio.Task[Any]] = []
        for slot in list(self._workers.values()):
            process = slot.process
            if process is None or process.returncode is not None:
                continue
            try:
                process.send_signal(signal.SIGTERM)
            except ProcessLookupError:
                continue
            if slot.watcher is not None and not slot.watcher.done():
                waiters.append(slot.watcher)
            else:
                waiters.append(asyncio.create_task(process.wait()))
        if waiters:
            await asyncio.wait(waiters, timeout=WORKER_STOP_WAIT_S)
        for slot in list(self._workers.values()):
            process = slot.process
            if process is not None and process.returncode is None:
                kill_worker(process)
        still = [
            task
            for task in (
                *(
                    slot.watcher
                    for slot in self._workers.values()
                    if slot.watcher is not None
                ),
                *waiters,
            )
            if not task.done()
        ]
        for slot in list(self._workers.values()):
            self._release_lifeline(slot)
            self._release_beat(slot)
        self._workers.clear()
        if still:
            # ``kill`` unblocks ``wait``. Give those tasks a moment to
            # finish, so none is pending at loop close. This is not a
            # claim that ``on_stop`` ran.
            await asyncio.wait(still, timeout=1.0)

    def _reap_failed(self, process: Any) -> None:
        """Collect a worker that will not get a watcher.

        Signalled only when it is still running. One that already exited
        just needs ``wait``, or it stays a zombie for the life of this
        process.
        """
        if process is None:
            return

        async def _reap() -> None:
            if process.returncode is None:
                try:
                    process.send_signal(signal.SIGTERM)
                except ProcessLookupError:
                    return
            try:
                await asyncio.wait_for(process.wait(), timeout=WORKER_STOP_WAIT_S)
            except TimeoutError:
                kill_worker(process)
                await process.wait()

        task = asyncio.create_task(_reap(), name="sts-worker-reap")
        self._reaps.add(task)
        task.add_done_callback(self._reaps.discard)

    async def _close_in_process(self) -> None:
        """Shut every in-process session down, recording the terminal status first.

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

    async def _control_loop(self, session_id: str, stop: asyncio.Event) -> None:
        # Imported here, not at module scope: the rpc package imports this
        # module for typing only, and a runtime import the other way keeps the
        # dependency one-directional.
        from mftik_sts.rpc import dispatch

        subject = Topics.sts_control(session_id)
        while not stop.is_set():
            try:
                async for req in self._broker.serve(subject, stop=stop):
                    try:
                        if (
                            self._control_types is not None
                            and req.envelope.type not in self._control_types
                        ):
                            await req.reply(
                                RpcErrorEnvelope.wrap(
                                    RpcError(
                                        code="unknown_type",
                                        message=(f"unknown type: {req.envelope.type}"),
                                    ),
                                    type=STS_ERROR,
                                    source="sts",
                                    session_id=session_id,
                                )
                            )
                            continue
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
