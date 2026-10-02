"""``StsOrchestrator`` — one reconcile per session (§5.1).

B4-02 wires this into the STS process: create, stop, mark terminal,
reattach, and the status snapshot. Crash class, cleanup and rehang stay
``NotImplementedError("IF-04")`` for B5-06. Registry and env handler
signatures are IF-16 and raise until B5-10. This module does not import
strategy code (F39).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path

from mftik.clock import Clock, SystemClock
from mftik.procman import (
    ALIVE_PHASES,
    CapacityExceeded,
    CloseMode,
    DesiredSlot,
    ObservedWorker,
    ProcmanError,
    ReattachAction,
    ReattachObservation,
    Supervisor,
    WorkerPhase,
    current_release,
    reattach_action,
)
from mftik.protocol import (
    STS_SESSION_STATUS,
    Envelope,
    ListSessionsRequest,
    ListSessionsResult,
    ProcmanWorker,
    SessionInfo,
    StsCreateSessionRequest,
    StsCreateSessionResult,
    StsSessionEndRequest,
    StsSessionEndResult,
    StsSessionStatus,
    Topics,
)

from mftik_sts.controller._ticket import unimplemented
from mftik_sts.controller.defaults import (
    FIRST_INCARNATION,
    SESSION_HB_TIMEOUT_S,
    SESSION_START_TIMEOUT_S,
    SESSION_STOP_GRACE_S,
)
from mftik_sts.controller.spawn import session_worker_argv, write_session_request
from mftik_sts.controller.status import (
    StatusStore,
    StatusWrite,
    StoredSession,
    column_status_for,
)
from mftik_sts.controller.types import (
    ActionKind,
    DesiredPhase,
    OrchestratorAction,
    SessionPhase,
    SessionSpec,
    SessionStatus,
    session_worker_id,
)
from mftik_sts.controller.worker import session_worker_spec

logger = logging.getLogger(__name__)

#: How often :meth:`StsOrchestrator.watch` reads the supervisor. The same
#: cadence as the supervisor's own status poll. Not a plan number.
_OBSERVE_POLL_S = 0.05

#: Spawn, then the observation that follows it, or stop, then the terminal
#: mark. Four steps covers both without spinning.
_DRIVE_BOUND = 4

_DEAD_PHASES = frozenset(
    {
        WorkerPhase.FAILED,
        WorkerPhase.CRASHED,
        WorkerPhase.FATAL,
        WorkerPhase.LOST,
        WorkerPhase.STOPPED,
        WorkerPhase.BACKOFF,
    }
)

Publish = Callable[[str, Envelope[StsSessionStatus]], Awaitable[None]]
ArgvFor = Callable[[Path], Sequence[str]]


@dataclass(frozen=True)
class CallError:
    """A handler reply, not an exception the subject should see."""

    code: str
    message: str


@dataclass
class _Held:
    spec: SessionSpec
    request: StsCreateSessionRequest | None
    phase: SessionPhase
    column: str
    worker_incarnation: int
    ready: bool
    exit_recorded: bool
    pid: int | None
    pid_gone: bool
    exit_code: int | None
    signal: int | None
    reason: str | None
    finished_at: float | None
    created_at: float
    created_by: int
    type_name: str | None
    restart_count: int
    observed_generation: int | None
    conditions: dict[str, str]
    published: tuple[object, ...] | None = None
    #: True once :meth:`Supervisor.spawn` has been entered. The pending
    #: report entry stops there: the slot, if admitted, is ``STARTING``.
    spawn_started: bool = False
    #: Set when that spawn refused with ``capacity_exceeded``.
    refusal: CallError | None = None


def _terminal(phase: SessionPhase) -> bool:
    return phase in (SessionPhase.DONE, SessionPhase.FAILED)


def _needs_stop(status: SessionStatus) -> bool:
    """A worker this controller still has to signal."""
    if status.phase in (
        SessionPhase.STARTING,
        SessionPhase.RUNNING,
        SessionPhase.STOPPING,
    ):
        return not (status.exit_recorded and status.pid_gone)
    return status.pid is not None and not status.pid_gone


def _present(status: SessionStatus) -> bool:
    if status.phase not in (
        SessionPhase.STARTING,
        SessionPhase.RUNNING,
        SessionPhase.STOPPING,
    ):
        return False
    return not (status.exit_recorded and status.pid_gone)


def _crash_pending(spec: SessionSpec, status: SessionStatus) -> bool:
    """A death B5-06 has to classify. Create and stop never look like this."""
    if spec.desired is not DesiredPhase.RUNNING:
        return False
    if status.crash_class is not None or status.phase is SessionPhase.RESTARTING:
        return True
    return (
        status.exit_recorded
        and status.pid_gone
        and status.worker_incarnation > 0
    )


def _restart_mode(value: str) -> str:
    """``always`` was rebuild. A historical row is adopted as ``never``."""
    if value == "on_failure":
        return "on_failure"
    return "never"


def _api_ids(td: Mapping[str, object]) -> tuple[int, ...]:
    found: list[int] = []
    for value in td.values():
        api_id = getattr(value, "api_id", None)
        if api_id is None and isinstance(value, dict):
            api_id = value.get("api_id")
        if type(api_id) is not int:
            raise ValueError("td api_id must be an int")
        found.append(api_id)
    return tuple(found)


class StsOrchestrator:
    """Session manager for this STS instance (§5.1, §5.2).

    **State authority (§3.3).**

    * Writes session status: the column word, ``conditions["phase"]``,
      worker incarnation, ``restart_count``, and the failure reason. The
      row is Postgres ``sts_sessions``. The live snapshot is
      ``sts.status.{session_id}``. The strategy does not write it.
    * Reads :class:`SessionSpec`. The API is the authority for the spec.
      This process's sessions are the ones accepted on its subject plus
      the ``sts/session/<id>`` workers :meth:`Supervisor.start` finds.
      Rows are read by ``session_id``. Nothing here selects by instance.
    * Reads the shim's exit record. The shim is the authority for whether
      the process exists and for the exit code and signal.
    * Does not own MD or TD intents and does not set ``released_at``.
      A session that ends by itself is recorded terminal; who releases
      that intent is B5 (#314). The API releases after a successful end
      reply. MD and TD drop an owner after it is missing from two
      ``procman.report.sts`` publications (B4-07). A session accepted
      but not yet spawned is on that report via :meth:`extra_workers`.
      Once the slot is ``STARTING``, the supervisor lists it itself.
    * Does not import strategy code (F39). Crash and rehang are B5-06.
      ``CapacityExceeded`` on the first spawn is the start refusal
      (``capacity_exceeded``), not a later snapshot.

    ``reconcile`` is pure. Idle is an empty tuple. A crash-shaped status
    still raises ``NotImplementedError("IF-04")``.
    """

    def __init__(
        self,
        supervisor: Supervisor,
        *,
        clock: Clock | None = None,
        store: StatusStore | None = None,
        publish: Publish | None = None,
        argv_for: ArgvFor | None = None,
        code_ref: str | None = None,
    ) -> None:
        if supervisor.plane != "sts":
            raise ValueError(
                f"STS orchestrator requires an sts supervisor, got {supervisor.plane!r}"
            )
        self.supervisor = supervisor
        self._clock: Clock = clock if clock is not None else SystemClock()
        self._store = store
        self._publish = publish
        self._argv_for = argv_for if argv_for is not None else session_worker_argv
        self._code_ref = code_ref
        self._sessions: dict[str, _Held] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def reconcile(
        self, spec: SessionSpec, status: SessionStatus
    ) -> tuple[OrchestratorAction, ...]:
        """Compare desired phase with the worker and name the actions.

        Create, stop, or mark terminal (§5.1). Empty means the worker
        already matches the spec. A crash — exit recorded, pid gone, an
        incarnation already spawned, or ``crash_class`` / ``restarting``
        set while the session is still desired running — raises
        ``NotImplementedError("IF-04")``. B5-06 names cleanup, the
        ``restarting`` write, and the rehang. Those actions are not
        returned from here.

        ``spec.instance`` has to be this supervisor's instance. A session
        addressed to another STS is not reconciled here.
        """
        if not isinstance(spec, SessionSpec):
            raise TypeError("spec must be a SessionSpec")
        if not isinstance(status, SessionStatus):
            raise TypeError("status must be a SessionStatus")
        if spec.instance != self.supervisor.instance:
            raise ValueError(
                f"spec instance {spec.instance!r} does not match "
                f"orchestrator instance {self.supervisor.instance!r}"
            )
        if _terminal(status.phase):
            return ()
        if _crash_pending(spec, status):
            unimplemented()
        if spec.desired is DesiredPhase.STOPPED:
            if (
                status.phase is SessionPhase.STOPPING
                and status.exit_recorded
                and status.pid_gone
            ):
                return (
                    OrchestratorAction(
                        kind=ActionKind.MARK_TERMINAL,
                        session_id=spec.session_id,
                        phase=SessionPhase.DONE,
                    ),
                )
            if _needs_stop(status):
                return (
                    OrchestratorAction(
                        kind=ActionKind.STOP,
                        session_id=spec.session_id,
                        incarnation=status.worker_incarnation,
                    ),
                )
            if status.phase is SessionPhase.STOPPING:
                return ()
            return (
                OrchestratorAction(
                    kind=ActionKind.MARK_TERMINAL,
                    session_id=spec.session_id,
                    phase=SessionPhase.DONE,
                ),
            )
        if _present(status):
            return ()
        if status.worker_incarnation == 0 and not status.exit_recorded:
            return (
                OrchestratorAction(
                    kind=ActionKind.SPAWN,
                    session_id=spec.session_id,
                    incarnation=FIRST_INCARNATION,
                ),
            )
        unimplemented()

    async def boot(self) -> None:
        """Apply :meth:`Supervisor.start` observations. Does not spawn.

        ``start`` itself returns the observations and opens reports.
        Membership is the worker id, not ``sts_sessions.instance``.
        """
        observations = await self.supervisor.start()
        for observed in observations:
            await self._reattach(observed)

    async def accept(
        self, request: StsCreateSessionRequest
    ) -> StsCreateSessionResult | CallError:
        """Record the session and reply ``starting``. Does not spawn.

        The router awaits :meth:`finish_start` before sending the reply.
        A second start of a session that is not terminal replies
        ``starting`` again and does not reset it. A start of a terminal
        session is ``session_ended``.
        """
        try:
            spec = self._spec_from_request(request)
        except ValueError as exc:
            return CallError("invalid_request", str(exc))
        async with self._lock(spec.session_id):
            held = self._sessions.get(spec.session_id)
            if held is not None and _terminal(held.phase):
                return CallError(
                    "session_ended",
                    f"session {spec.session_id} is already {held.phase.value}",
                )
            if held is None:
                self._sessions[spec.session_id] = self._fresh(spec, request)
                await self._commit(self._sessions[spec.session_id])
        return StsCreateSessionResult(
            session_id=spec.session_id, status="starting"
        )

    def extra_workers(self) -> tuple[ProcmanWorker, ...]:
        """Sessions accepted but not yet handed to :meth:`Supervisor.spawn`.

        ``STARTING``, ``RUNNING`` and ``STOPPING`` are already on
        :meth:`Supervisor.report`. The gap before spawn is not. MD and TD
        release an owner missing from two consecutive
        ``procman.report.sts`` publications (B4-07), so that gap is
        filled here. The id is ``sts/session/<session_id>``. Restarting
        with no process is B5-06 and is not included. Entries do not
        repeat an id the supervisor already lists: this stops when spawn
        is entered.
        """
        chosen: list[str] = []
        for session_id in sorted(self._sessions):
            held = self._sessions[session_id]
            if held.spec.desired is not DesiredPhase.RUNNING:
                continue
            if held.spawn_started or held.phase is not SessionPhase.PENDING:
                continue
            if held.worker_incarnation != 0:
                continue
            chosen.append(session_id)
        if not chosen:
            return ()
        code_ref = self._release_name()
        return tuple(
            ProcmanWorker(
                id=session_worker_id(session_id),
                code_ref=code_ref,
                rss_bytes=None,
                phase=WorkerPhase.STARTING.value,
                ready=False,
                incarnation=FIRST_INCARNATION,
            )
            for session_id in chosen
        )

    async def finish_start(self, session_id: str) -> CallError | None:
        """Spawn an accepted session before the start reply is sent.

        :class:`~mftik.procman.CapacityExceeded` becomes the refusal the
        handler returns. Any other failure stays on the snapshot; the
        reply is still ``starting``. ``on_start`` has not run.
        """
        await self.converge(session_id)
        held = self._sessions.get(session_id)
        if held is None:
            return None
        return held.refusal

    async def end_session(
        self, request: StsSessionEndRequest
    ) -> StsSessionEndResult | CallError:
        """Stop the worker, if there is one, and reply the terminal status.

        An unknown session is :class:`CallError`, not an exception. An
        already terminal session replies that status. The lock is held
        across :meth:`Supervisor.stop`.
        """
        session_id = request.session_id
        async with self._lock(session_id):
            held = self._sessions.get(session_id)
            if held is None:
                return CallError(
                    "unknown_session",
                    f"session {session_id} is not on this STS",
                )
            if _terminal(held.phase):
                return StsSessionEndResult(
                    session_id=session_id, status=held.phase.value
                )
            reason = request.reason[:256] if request.reason else None
            held.reason = reason
            held.spec = replace(held.spec, desired=DesiredPhase.STOPPED)
            await self._drive(held)
            status = held.phase.value if _terminal(held.phase) else "failed"
            if not _terminal(held.phase):
                await self._fail(held, reason or "stop_unsettled")
                status = held.phase.value
            return StsSessionEndResult(session_id=session_id, status=status)

    def list_sessions(self, request: ListSessionsRequest) -> ListSessionsResult:
        """Sessions this controller holds. ``domain`` other than STS is empty."""
        if request.domain not in (None, "sts"):
            return ListSessionsResult(sessions=[])
        rows: list[SessionInfo] = []
        for session_id in sorted(self._sessions):
            held = self._sessions[session_id]
            if request.status is not None and held.column != request.status:
                continue
            if (
                request.created_by is not None
                and held.created_by != request.created_by
            ):
                continue
            rows.append(
                SessionInfo(
                    session_id=session_id,
                    domain="sts",
                    created_by=held.created_by,
                    created_at=held.created_at,
                    finished_at=held.finished_at,
                    status=held.column,
                    strategy=held.spec.strategy,
                    reason=held.reason,
                    type=held.type_name,
                )
            )
        return ListSessionsResult(sessions=rows)

    async def converge(self, session_id: str) -> None:
        """Drive one accepted session until it is idle or terminal.

        Scheduled after the start reply. A second call does not spawn a
        second worker. Failures are logged: the reply has already gone.
        """
        try:
            async with self._lock(session_id):
                held = self._sessions.get(session_id)
                if held is None or _terminal(held.phase):
                    return
                await self._drive(held)
        except Exception:
            logger.exception("STS converge failed session=%s", session_id)

    async def observe_all(self) -> None:
        """Read every held session once. Watch and tests call this."""
        for session_id in list(self._sessions):
            try:
                async with self._lock(session_id):
                    held = self._sessions.get(session_id)
                    if held is None or _terminal(held.phase):
                        continue
                    await self._drive(held)
            except Exception:
                logger.exception("STS observe failed session=%s", session_id)

    async def watch(self, stop: asyncio.Event) -> None:
        """Observe until cancelled.

        ``stop`` ends the poll. The task then waits to be cancelled, so a
        normal shutdown is not "the watch task finished" — that is how
        :func:`mftik.runtime.run_until_stopped` tells a crash from a stop.
        Sleep goes through the injected clock.
        """
        while not stop.is_set():
            await self.observe_all()
            await self._clock.sleep(_OBSERVE_POLL_S)
        await asyncio.Future()

    async def close(self, mode: CloseMode = CloseMode.DETACH) -> None:
        """Detach by default, so a controller roll leaves workers running."""
        await self.supervisor.close(mode)

    def _lock(self, session_id: str) -> asyncio.Lock:
        lock = self._locks.get(session_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[session_id] = lock
        return lock

    def _release_name(self) -> str:
        """This process's release, resolved once. Tests inject ``code_ref``.

        :func:`mftik.procman.current_release` is the only reader of
        ``STRATEGON_RELEASE_VERSION`` (B3-07).
        """
        if self._code_ref is None:
            self._code_ref = current_release()
        return self._code_ref

    def _spec_from_request(self, request: StsCreateSessionRequest) -> SessionSpec:
        return SessionSpec(
            session_id=request.session_id,
            instance=self.supervisor.instance,
            strategy=request.strategy,
            desired=DesiredPhase.RUNNING,
            restart=_restart_mode(request.restart),
            generation=1,
            api_ids=_api_ids(request.td),
        )

    def _fresh(
        self, spec: SessionSpec, request: StsCreateSessionRequest | None
    ) -> _Held:
        return _Held(
            spec=spec,
            request=request,
            phase=SessionPhase.PENDING,
            column=column_status_for(SessionPhase.PENDING),
            worker_incarnation=0,
            ready=False,
            exit_recorded=False,
            pid=None,
            pid_gone=True,
            exit_code=None,
            signal=None,
            reason=None,
            finished_at=None,
            created_at=self._clock.now(),
            created_by=0 if request is None else request.created_by,
            type_name=None if request is None else request.type,
            restart_count=0,
            observed_generation=spec.generation,
            conditions={"phase": SessionPhase.PENDING.value},
        )

    def _observed(self, held: _Held) -> SessionStatus:
        return SessionStatus(
            phase=held.phase,
            worker_incarnation=held.worker_incarnation,
            restart_count=held.restart_count,
            ready=held.ready,
            exit_recorded=held.exit_recorded,
            pid=held.pid,
            pid_gone=held.pid_gone,
            generation=held.spec.generation,
            observed_generation=held.observed_generation,
            conditions=held.conditions,
        )

    async def _drive(self, held: _Held) -> None:
        for _ in range(_DRIVE_BOUND):
            if _terminal(held.phase):
                await self._commit(held)
                return
            await self._refresh(held)
            if self._died(held):
                await self._fail(held, self._death_reason(held))
                return
            actions = self.reconcile(held.spec, self._observed(held))
            if not actions:
                await self._commit(held)
                return
            for action in actions:
                if action.kind is ActionKind.SPAWN:
                    await self._spawn(held, action)
                elif action.kind is ActionKind.STOP:
                    await self._stop(held)
                elif action.kind is ActionKind.MARK_TERMINAL:
                    self._mark_terminal(held, action)
                else:
                    unimplemented()
                if _terminal(held.phase):
                    await self._commit(held)
                    return
            await self._commit(held)
        logger.error(
            "STS drive did not settle session=%s phase=%s",
            held.spec.session_id,
            held.phase.value,
        )

    def _died(self, held: _Held) -> bool:
        if held.spec.desired is not DesiredPhase.RUNNING:
            return False
        if _terminal(held.phase) or held.worker_incarnation <= 0:
            return False
        return held.exit_recorded and held.pid_gone

    def _death_reason(self, held: _Held) -> str:
        if held.exit_code is not None:
            return f"worker_exited:{held.exit_code}"
        if held.signal is not None:
            return f"worker_signal:{held.signal}"
        return "worker_exited"

    async def _refresh(self, held: _Held) -> None:
        worker_id = session_worker_id(held.spec.session_id)
        try:
            view = await self.supervisor.status(worker_id)
        except ProcmanError:
            logger.exception("STS status failed session=%s", held.spec.session_id)
            return
        if view is None:
            self._note_absent(held)
            return
        if view.phase in ALIVE_PHASES:
            held.exit_recorded = False
            held.pid_gone = False
            held.pid = view.pid
            held.ready = view.ready
            held.exit_code = None
            held.signal = None
            if view.phase is WorkerPhase.STOPPING:
                held.phase = SessionPhase.STOPPING
            elif view.phase is WorkerPhase.RUNNING:
                held.phase = SessionPhase.RUNNING
            else:
                held.phase = SessionPhase.STARTING
            held.column = column_status_for(held.phase)
            held.conditions = {"phase": held.phase.value}
            return
        if view.phase in _DEAD_PHASES:
            held.exit_recorded = True
            held.pid_gone = True
            held.ready = False
            held.pid = None
            held.exit_code = view.exit_code
            held.signal = view.signal

    def _note_absent(self, held: _Held) -> None:
        if held.worker_incarnation == 0 and held.phase is SessionPhase.PENDING:
            held.pid = None
            held.pid_gone = True
            held.exit_recorded = False
            return
        held.pid = None
        held.pid_gone = True
        held.exit_recorded = True

    async def _spawn(self, held: _Held, action: OrchestratorAction) -> None:
        if held.request is None:
            await self._fail(held, "missing_request")
            return
        path = write_session_request(self.supervisor.work_dir, held.request)
        incarnation = action.incarnation or FIRST_INCARNATION
        worker = session_worker_spec(
            held.spec,
            incarnation=incarnation,
            argv=tuple(self._argv_for(path)),
            code_ref=self._release_name(),
            start_timeout_s=SESSION_START_TIMEOUT_S,
            hb_timeout_s=SESSION_HB_TIMEOUT_S,
            stop_grace_s=SESSION_STOP_GRACE_S,
        )
        held.spawn_started = True
        try:
            await self.supervisor.spawn(worker)
        except CapacityExceeded as exc:
            held.refusal = CallError(exc.code, str(exc))
            await self._fail(held, exc.code)
            return
        except Exception:
            held.spawn_started = False
            raise
        held.worker_incarnation = incarnation
        held.phase = SessionPhase.STARTING
        held.column = column_status_for(SessionPhase.STARTING)
        held.pid_gone = False
        held.exit_recorded = False
        held.conditions = {"phase": SessionPhase.STARTING.value}

    async def _stop(self, held: _Held) -> None:
        held.phase = SessionPhase.STOPPING
        held.column = column_status_for(SessionPhase.STOPPING)
        held.conditions = {"phase": SessionPhase.STOPPING.value}
        await self._commit(held)
        worker_id = session_worker_id(held.spec.session_id)
        try:
            await self.supervisor.stop(worker_id)
        except ProcmanError:
            logger.info(
                "STS stop found no live worker session=%s",
                held.spec.session_id,
            )
        await self._refresh(held)

    def _mark_terminal(self, held: _Held, action: OrchestratorAction) -> None:
        phase = action.phase or SessionPhase.DONE
        if phase not in (SessionPhase.DONE, SessionPhase.FAILED):
            phase = SessionPhase.DONE
        held.phase = phase
        held.column = column_status_for(phase)
        held.conditions = {"phase": phase.value}
        if held.finished_at is None:
            held.finished_at = self._clock.now()

    async def _fail(self, held: _Held, reason: str) -> None:
        if _terminal(held.phase):
            return
        held.phase = SessionPhase.FAILED
        held.column = column_status_for(SessionPhase.FAILED)
        held.reason = reason[:256] if reason else "worker_exited"
        held.conditions = {"phase": SessionPhase.FAILED.value}
        held.exit_recorded = True
        held.pid_gone = True
        held.pid = None
        if held.finished_at is None:
            held.finished_at = self._clock.now()
        await self._commit(held)
        await self._release_quiet(session_worker_id(held.spec.session_id))

    async def _release_quiet(self, worker_id: str) -> None:
        try:
            view = await self.supervisor.status(worker_id)
        except ProcmanError:
            logger.warning("STS status failed before release id=%s", worker_id)
            return
        if view is None or view.phase in ALIVE_PHASES:
            return
        try:
            await self.supervisor.release_slot(worker_id)
        except ProcmanError:
            logger.warning(
                "leaving slot %s in place: release was refused", worker_id
            )

    async def _commit(self, held: _Held) -> None:
        signature = (
            held.phase,
            held.column,
            held.worker_incarnation,
            held.ready,
            held.reason,
            held.finished_at,
            held.observed_generation,
            tuple(sorted(held.conditions.items())),
        )
        if signature == held.published:
            return
        held.published = signature
        if self._store is not None:
            await self._store.save(self._status_write(held))
        if self._publish is not None:
            await self._publish(
                Topics.sts_status(held.spec.session_id),
                Envelope[StsSessionStatus].wrap(
                    self._snapshot(held),
                    type=STS_SESSION_STATUS,
                    source="sts",
                    session_id=held.spec.session_id,
                ),
            )

    def _status_write(self, held: _Held) -> StatusWrite:
        incarnation = (
            None if held.worker_incarnation == 0 else held.worker_incarnation
        )
        return StatusWrite(
            session_id=held.spec.session_id,
            status=held.column,
            reason=held.reason,
            finished_at=held.finished_at,
            observed_generation=held.observed_generation,
            worker_incarnation=incarnation,
            conditions=dict(held.conditions),
            restart_count=held.restart_count,
        )

    def _snapshot(self, held: _Held) -> StsSessionStatus:
        incarnation = (
            None if held.worker_incarnation == 0 else held.worker_incarnation
        )
        return StsSessionStatus(
            session_id=held.spec.session_id,
            status=held.phase.value,
            strategy=held.spec.strategy,
            reason=held.reason,
            created_by=held.created_by,
            finished_at=held.finished_at,
            type=held.type_name,
            conditions=dict(held.conditions),
            generation=held.spec.generation,
            observed_generation=held.observed_generation,
            worker_incarnation=incarnation,
            restart_count=held.restart_count,
        )

    async def _reattach(self, observed: ReattachObservation) -> None:
        session_id = _session_id_of(observed.id)
        if session_id is None:
            return
        stored = None if self._store is None else await self._store.load(session_id)
        desired = (
            DesiredSlot.PRESENT
            if stored is not None and stored.status == "live"
            else DesiredSlot.ABSENT
        )
        action = reattach_action(
            plane="sts", desired=desired, observed=observed.observed
        )
        worker_id = observed.id
        if action is ReattachAction.ADOPT and stored is not None:
            await self._adopt(stored, observed)
            return
        if action is ReattachAction.MARK_FAILED and stored is not None:
            await self._mark_failed(stored, observed)
            return
        if action is ReattachAction.STOP_AND_RELEASE:
            await self._stop_unwanted(worker_id)
            return
        if action is ReattachAction.NONE:
            await self._release_quiet(worker_id)
            return
        if action is ReattachAction.APPLY_RESTART:
            logger.error(
                "STS reattach will not restart %s; that cell is not this plane",
                worker_id,
            )

    async def _adopt(
        self, stored: StoredSession, observed: ReattachObservation
    ) -> None:
        ready = bool(observed.status is not None and observed.status.ready)
        phase = SessionPhase.RUNNING if ready else SessionPhase.STARTING
        incarnation = observed.incarnation or FIRST_INCARNATION
        if incarnation < 1:
            incarnation = FIRST_INCARNATION
        pid = None if observed.status is None else observed.status.pid
        spec = self._spec_from_row(stored)
        held = self._fresh(spec, None)
        held.phase = phase
        held.column = column_status_for(phase)
        held.worker_incarnation = incarnation
        held.ready = ready
        held.pid = pid
        held.pid_gone = False
        held.exit_recorded = False
        held.created_by = stored.created_by
        held.created_at = stored.created_at
        held.type_name = stored.type_name
        held.restart_count = stored.restart_count
        held.observed_generation = (
            stored.observed_generation
            if stored.observed_generation is not None
            else spec.generation
        )
        held.reason = stored.reason
        held.finished_at = stored.finished_at
        held.conditions = {"phase": phase.value}
        async with self._lock(spec.session_id):
            self._sessions[spec.session_id] = held
            await self._commit(held)

    async def _mark_failed(
        self, stored: StoredSession, observed: ReattachObservation
    ) -> None:
        spec = self._spec_from_row(stored)
        held = self._fresh(spec, None)
        held.created_by = stored.created_by
        held.created_at = stored.created_at
        held.type_name = stored.type_name
        held.restart_count = stored.restart_count
        held.worker_incarnation = observed.incarnation or FIRST_INCARNATION
        held.observed_generation = (
            stored.observed_generation
            if stored.observed_generation is not None
            else spec.generation
        )
        async with self._lock(spec.session_id):
            self._sessions[spec.session_id] = held
            await self._fail(held, _reattach_reason(observed))

    def _spec_from_row(self, stored: StoredSession) -> SessionSpec:
        return SessionSpec(
            session_id=stored.session_id,
            instance=self.supervisor.instance,
            strategy=stored.strategy,
            desired=DesiredPhase.RUNNING,
            restart=_restart_mode(stored.restart),
            generation=stored.generation if stored.generation >= 1 else 1,
            api_ids=stored.api_ids,
            strategy_digest=stored.strategy_digest,
            env_generation=stored.env_generation,
        )

    async def _stop_unwanted(self, worker_id: str) -> None:
        try:
            view = await self.supervisor.status(worker_id)
        except ProcmanError:
            logger.warning("STS status failed before stop id=%s", worker_id)
            return
        if view is not None and view.phase in ALIVE_PHASES:
            try:
                await self.supervisor.stop(worker_id)
            except ProcmanError:
                logger.warning("STS stop failed id=%s", worker_id)
            return
        await self._release_quiet(worker_id)


def _session_id_of(worker_id: str) -> str | None:
    prefix = "sts/session/"
    if not worker_id.startswith(prefix):
        return None
    session_id = worker_id[len(prefix) :]
    if not session_id or "/" in session_id:
        return None
    return session_id


def _reattach_reason(observed: ReattachObservation) -> str:
    record = observed.exit_record
    if record is not None:
        if record.exit_code is not None:
            return f"worker_exited:{record.exit_code}"
        if record.signal is not None:
            return f"worker_signal:{record.signal}"
    if observed.status is not None:
        if observed.status.exit_code is not None:
            return f"worker_exited:{observed.status.exit_code}"
        if observed.status.signal is not None:
            return f"worker_signal:{observed.status.signal}"
    if observed.observed is ObservedWorker.LOST:
        return "lost"
    if observed.observed is ObservedWorker.ABSENT:
        return "absent"
    return "worker_exited"
