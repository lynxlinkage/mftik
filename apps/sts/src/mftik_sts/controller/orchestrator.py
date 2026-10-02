"""``StsOrchestrator`` — one reconcile per session (§5.1, §5.2).

B4-02 wires create, stop, mark terminal, reattach, and the status
snapshot. B5-06 fills the crash path: cleanup, the ``restarting`` write,
the error line, and the rehang. Registry and env handlers write the
digest replica and do not import strategy code (F39). This module does
not import the session worker.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Protocol

from mftik.broker import NoRespondersError, RequestTimeoutError
from mftik.clock import Clock, SystemClock
from mftik.environment import NodeEnv
from mftik.procman import (
    ALIVE_PHASES,
    CapacityExceeded,
    CloseMode,
    DesiredSlot,
    MessageError,
    ProcmanError,
    ReattachAction,
    ReattachObservation,
    Supervisor,
    WorkerPhase,
    count_restarts_in_window,
    current_release,
    decode_exit,
    exit_record_path,
    load_supervisor_state,
    reattach_action,
)
from mftik.protocol import (
    STS_SESSION_STATUS,
    TD_ORDER_CANCEL_SESSION,
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
    TdCancelSessionRequest,
    TdCancelSessionResult,
    Topics,
)
from mftik.protocol.session_log import publish_sts_log
from pydantic import ValidationError

from mftik_sts.controller.decisions import (
    CONTROLLER_LOG_SOURCE,
    classify_crash,
    crash_log_message,
    decide_restart,
    retains_intents,
    spawn_allowed,
)
from mftik_sts.controller.defaults import (
    FIRST_INCARNATION,
    SESSION_HB_TIMEOUT_S,
    SESSION_STOP_GRACE_S,
    STS_CLEANUP_TIMEOUT_S,
)
from mftik_sts.controller.env import forwarded_env
from mftik_sts.controller.spawn import session_worker_argv, write_session_request
from mftik_sts.controller.status import (
    StatusStore,
    StatusWrite,
    StoredSession,
    column_status_for,
)
from mftik_sts.controller.types import (
    ActionKind,
    Cleanup,
    CrashCause,
    CrashClass,
    DesiredPhase,
    OrchestratorAction,
    RestartVerdict,
    SessionPhase,
    SessionSpec,
    SessionStatus,
    session_worker_id,
)
from mftik_sts.controller.worker import (
    LABEL_ENV_GENERATION,
    LABEL_STRATEGY_DIGEST,
    procman_start_timeout_s,
    session_worker_spec,
)
from mftik_sts.exit_codes import cause_for_exit
from mftik_sts.hostdisk.checks import (
    REASON_DIGEST_ABSENT,
    REASON_STRATEGY_UNAVAILABLE,
    deployable,
    installed_release,
    rehang_code,
)
from mftik_sts.hostdisk.identity import ENV_GENERATION_ENV, STRATEGY_DIGEST_ENV
from mftik_sts.hostdisk.replica import TreeReplica, require_digest

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

Publish = Callable[[str, object], Awaitable[None]]
ArgvFor = Callable[[Path], Sequence[str]]


class _CancelBroker(Protocol):
    """The ``request`` half of a broker. Tests pass a double."""

    async def request(
        self, subject: str, envelope: object, *, timeout: float
    ) -> object: ...


class _LogBus:
    """Adapts the orchestrator's publish callback to :func:`publish_sts_log`."""

    def __init__(self, publish: Publish) -> None:
        self._publish = publish

    async def publish(self, topic: str, envelope: object) -> None:
        await self._publish(topic, envelope)


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
    cleanup: Cleanup = Cleanup.NOT_RUN
    crash_class: CrashClass | None = None
    #: Monotonic instants of rehanges this process has spawned. A controller
    #: restart drops the window. The lifetime count is ``restart_count``.
    restarted_at: list[float] = field(default_factory=list)
    restarts_in_window: int = 0
    #: True after this controller's stop, until the next spawn. A kill that
    #: follows it is ``stop_stuck``, not a death we did not ask for.
    stop_sent: bool = False
    #: Account → outstanding client order ids, or a failure token.
    unconfirmed: dict[int, tuple[str, ...]] = field(default_factory=dict)


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


def _plain_signal(value: object) -> int | None:
    """A signal number. ``signal.Signals`` is an ``int`` enum, not ``int``."""
    if value is None or type(value) is bool or not isinstance(value, int):
        return None
    return int(value)


def _clean_exit(status: SessionStatus) -> bool:
    """Exit 0 and no signal. Cleanup does not run. The strategy finished."""
    return (
        status.worker_incarnation > 0
        and status.exit_recorded
        and status.pid_gone
        and status.crash_class is None
        and status.signal is None
        and status.exit_code == 0
    )


def _non_clean(status: SessionStatus) -> bool:
    if status.crash_class is not None:
        return True
    if status.signal is not None:
        return True
    return status.exit_code is not None and status.exit_code != 0


def _crash_pending(spec: SessionSpec, status: SessionStatus) -> bool:
    """A death the crash path has to finish. Create and stop never look like this."""
    if spec.desired is not DesiredPhase.RUNNING:
        return False
    if status.crash_class is not None or status.phase is SessionPhase.RESTARTING:
        return True
    return (
        status.exit_recorded
        and status.pid_gone
        and status.worker_incarnation > 0
    )


def _stop_actions(
    spec: SessionSpec, status: SessionStatus
) -> tuple[OrchestratorAction, ...]:
    """End the session. A kill or a non-zero exit is cleaned up first.

    A clean exit, and a session that never spawned, are ``done`` with no
    cancel. Desired ``stopped`` does not rehang, even when F11 would.
    """
    if _needs_stop(status):
        return (
            OrchestratorAction(
                kind=ActionKind.STOP,
                session_id=spec.session_id,
                incarnation=status.worker_incarnation,
            ),
        )
    exited = status.exit_recorded and status.pid_gone
    if status.phase is SessionPhase.STOPPING and not exited:
        return ()
    if not exited or status.worker_incarnation == 0 or not _non_clean(status):
        return (
            OrchestratorAction(
                kind=ActionKind.MARK_TERMINAL,
                session_id=spec.session_id,
                phase=SessionPhase.DONE,
            ),
        )
    return _crash_actions(spec, status, allow_rehang=False)


def _crash_actions(
    spec: SessionSpec,
    status: SessionStatus,
    *,
    allow_rehang: bool = True,
) -> tuple[OrchestratorAction, ...]:
    """Cleanup, then fail or rehang. Waiting on the exit returns nothing."""
    attempt = status.restarts_in_window + 1
    if attempt < 1:
        attempt = 1
    decision = decide_restart(
        restart=spec.restart,
        crash_class=status.crash_class,
        ready=status.ready,
        cleanup=status.cleanup,
        exit_recorded=status.exit_recorded,
        pid_gone=status.pid_gone,
        restarts_in_window=status.restarts_in_window,
        incarnation=status.worker_incarnation,
        attempt=attempt,
        max_restarts=spec.max_restarts,
    )
    if decision.verdict is RestartVerdict.WAIT:
        return ()
    if decision.verdict is RestartVerdict.CLEANUP:
        return (
            OrchestratorAction(
                kind=ActionKind.CLEANUP,
                session_id=spec.session_id,
                incarnation=status.worker_incarnation,
                api_ids=spec.api_ids,
            ),
        )
    if decision.verdict is RestartVerdict.REHANG and allow_rehang:
        actions = [
            OrchestratorAction(
                kind=ActionKind.MARK_RESTARTING,
                session_id=spec.session_id,
                phase=SessionPhase.RESTARTING,
                reason=decision.reason,
                incarnation=status.worker_incarnation,
            )
        ]
        if decision.error_log or decision.alert:
            actions.append(_alert_action(spec, status, decision))
        actions.append(
            OrchestratorAction(
                kind=ActionKind.SPAWN,
                session_id=spec.session_id,
                incarnation=decision.next_incarnation,
                delay_s=decision.delay_s,
            )
        )
        return tuple(actions)
    actions = [
        OrchestratorAction(
            kind=ActionKind.MARK_TERMINAL,
            session_id=spec.session_id,
            phase=SessionPhase.FAILED,
            reason=decision.reason,
            incarnation=status.worker_incarnation,
        )
    ]
    if decision.error_log or decision.alert:
        actions.append(_alert_action(spec, status, decision))
    return tuple(actions)


def _alert_action(
    spec: SessionSpec, status: SessionStatus, decision: object
) -> OrchestratorAction:
    reason = getattr(decision, "reason", None)
    alert = bool(getattr(decision, "alert", False))
    return OrchestratorAction(
        kind=ActionKind.ALERT,
        session_id=spec.session_id,
        reason=reason,
        alert=alert,
        incarnation=status.worker_incarnation,
    )


def _exit_facts(
    observed: ReattachObservation,
) -> tuple[int | None, int | None, bool]:
    """Exit code, signal, and the ready bit from a reattach observation."""
    code: int | None = None
    signal_no: int | None = None
    ready = False
    record = observed.exit_record
    if record is not None:
        code = record.exit_code
        signal_no = _plain_signal(record.signal)
        ready = bool(record.ready)
    view = observed.status
    if view is not None:
        ready = bool(view.ready) or ready
        if code is None and signal_no is None:
            code = view.exit_code
            signal_no = _plain_signal(view.signal)
    return code, signal_no, ready


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
    * Does not import strategy code (F39). A rehang carries no strategy
      state. ``CapacityExceeded`` on the first spawn is the start refusal
      (``capacity_exceeded``), not a later snapshot.
    * Asks ``td.order.cancel_session`` only after the old incarnation is
      dead (R1). ``Cleanup.CONFIRMED`` requires ``ok`` from every
      ``api_id``. Anything else is ``UNCONFIRMED`` and the session fails.

    ``reconcile`` is pure. Idle is an empty tuple.
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
        broker: _CancelBroker | None = None,
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
        self._broker = broker
        self._sessions: dict[str, _Held] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def reconcile(
        self, spec: SessionSpec, status: SessionStatus
    ) -> tuple[OrchestratorAction, ...]:
        """Compare desired phase with the worker and name the actions.

        Create, stop, mark terminal, or the crash path (§5.1, §5.2).
        Empty means the worker already matches the spec, or the exit is
        not confirmed yet (R1). A crash names cleanup, then ``restarting``
        plus the error line plus a delayed spawn, or ``failed`` plus the
        error line. A rehang carries no strategy state.

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
        if spec.desired is DesiredPhase.STOPPED:
            return _stop_actions(spec, status)
        if _clean_exit(status):
            return (
                OrchestratorAction(
                    kind=ActionKind.MARK_TERMINAL,
                    session_id=spec.session_id,
                    phase=SessionPhase.DONE,
                    reason="worker_exited:0",
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
        if _crash_pending(spec, status) or status.worker_incarnation > 0:
            return _crash_actions(spec, status)
        return ()

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
            if self._store is not None:
                stored = await self._store.load(request.session_id)
                if stored is not None:
                    spec = replace(
                        spec,
                        strategy_digest=stored.strategy_digest,
                        env_generation=stored.env_generation,
                    )
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
        """Sessions the supervisor's own report does not already list.

        ``STARTING``, ``RUNNING`` and ``STOPPING`` with a live pid are on
        :meth:`Supervisor.report`. The gap before the first spawn is not,
        and neither is a dead or ``restarting`` session (R4, §8.2). MD and
        TD release an owner missing from two consecutive
        ``procman.report.sts`` publications (B4-07). The id is
        ``sts/session/<session_id>``. ``restarting`` is not a procman
        phase, so a session between incarnations is reported as
        ``crashed``: the supervisor is not listing that slot, and the
        intents stay. Entries do not repeat an id the supervisor lists.
        """
        chosen: list[ProcmanWorker] = []
        code_ref: str | None = None
        for session_id in sorted(self._sessions):
            held = self._sessions[session_id]
            if not retains_intents(held.phase):
                continue
            if (
                held.spec.desired is DesiredPhase.STOPPED
                and held.phase is SessionPhase.PENDING
            ):
                continue
            supervisor_lists = (
                held.spawn_started
                and not held.pid_gone
                and held.phase
                in (
                    SessionPhase.STARTING,
                    SessionPhase.RUNNING,
                    SessionPhase.STOPPING,
                )
            )
            if supervisor_lists:
                continue
            if code_ref is None:
                code_ref = self._release_name()
            if held.phase is SessionPhase.PENDING and held.worker_incarnation == 0:
                phase = WorkerPhase.STARTING.value
                incarnation = FIRST_INCARNATION
                ready = False
            else:
                phase = WorkerPhase.CRASHED.value
                incarnation = held.worker_incarnation or FIRST_INCARNATION
                ready = held.ready
            chosen.append(
                ProcmanWorker(
                    id=session_worker_id(session_id),
                    code_ref=code_ref,
                    rss_bytes=None,
                    phase=phase,
                    ready=ready,
                    incarnation=incarnation,
                )
            )
        return tuple(chosen)

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

    async def code_pins(self) -> tuple[frozenset[str], frozenset[int]]:
        """Digests and env generations this instance must keep (F39).

        The union of non-terminal specs in memory, non-terminal
        ``sts_sessions`` rows whose ``instance`` is null or this
        supervisor's, and every slot in this instance's
        ``supervisor.json``. A pin only one of those names is still
        kept. Over-keeping is safe.
        """
        digests: set[str] = set()
        generations: set[int] = set()

        def add_pin(digest: str | None, generation: int | None) -> None:
            if isinstance(digest, str) and digest:
                try:
                    require_digest(digest)
                except ValueError:
                    pass
                else:
                    digests.add(digest)
            if type(generation) is int and generation >= 0:
                generations.add(generation)

        for held in self._sessions.values():
            if _terminal(held.phase):
                continue
            add_pin(held.spec.strategy_digest, held.spec.env_generation)
        list_pins = None
        if self._store is not None:
            list_pins = getattr(self._store, "list_code_pins", None)
        if list_pins is not None:
            instance = self.supervisor.instance
            for pin in await list_pins():
                if pin.instance is not None and pin.instance != instance:
                    continue
                add_pin(pin.strategy_digest, pin.env_generation)
        try:
            records = load_supervisor_state(self.supervisor.work_dir)
        except ProcmanError:
            records = ()
        for record in records:
            labels = record.spec.labels
            raw_digest = labels.get(LABEL_STRATEGY_DIGEST)
            raw_generation = labels.get(LABEL_ENV_GENERATION)
            generation: int | None = None
            if isinstance(raw_generation, str) and raw_generation.isdigit():
                generation = int(raw_generation)
            add_pin(raw_digest if isinstance(raw_digest, str) else None, generation)
        return frozenset(digests), frozenset(generations)

    async def _code_guard(self, held: _Held) -> CallError | None:
        """Refuse a spawn whose pinned tree or generation is not runnable.

        A built-in strategy (no digest) skips ``requires_mftik``. A
        digest that exists only as a legacy name directory is copied
        into ``trees/`` on a worker thread, under the same lock as
        registry sync. That directory is not deleted and the index is
        not rebound. A missing pinned tree stays ``alert=False`` and
        still publishes the same error line a crash does, so the alert
        pipeline can match it. This does not import the tree.
        """
        spec = held.spec
        if spec.strategy_digest is None and spec.env_generation is None:
            return None
        replica = TreeReplica(NodeEnv.from_env().data_dir)
        if spec.strategy_digest is not None:
            # The pin may still be only a name directory (shared volume,
            # or no sync yet). Copy it off this loop: the copy takes
            # TREES_LOCK and reads the disk. Do not bind or delete.
            from mftik_sts.hostdisk.sync import materialize_legacy_digest

            await asyncio.to_thread(
                materialize_legacy_digest, replica, spec.strategy_digest
            )
            code = rehang_code(
                spec, replica=replica, release=installed_release()
            )
            if code.failed:
                reason = code.reason or REASON_STRATEGY_UNAVAILABLE
                await self._log_code_failure(held, reason)
                held.refusal = CallError(reason, reason)
                await self._fail(held, reason)
                return held.refusal
        verdict = deployable(spec, replica=replica, env=NodeEnv.from_env())
        if verdict.ok:
            return None
        reason = verdict.reason or REASON_DIGEST_ABSENT
        await self._log_code_failure(held, reason)
        held.refusal = CallError(reason, reason)
        await self._fail(held, reason)
        return held.refusal

    async def _log_code_failure(self, held: _Held, reason: str) -> None:
        message = crash_log_message(
            crash_class=None,
            reason=reason,
            incarnation=held.worker_incarnation,
        )
        if self._publish is None:
            logger.error(
                "STS code session=%s %s", held.spec.session_id, message
            )
            return
        await publish_sts_log(
            _LogBus(self._publish),
            held.spec.session_id,
            message,
            source=CONTROLLER_LOG_SOURCE,
            level="error",
        )

    def _worker_env(self, spec: SessionSpec) -> dict[str, str]:
        """Forwarded environment plus the two pins, when the spec has them."""
        env = forwarded_env()
        if spec.strategy_digest is not None:
            env[STRATEGY_DIGEST_ENV] = spec.strategy_digest
        if spec.env_generation is not None:
            env[ENV_GENERATION_ENV] = str(spec.env_generation)
        return env

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
            exit_code=held.exit_code,
            signal=held.signal,
            cleanup=held.cleanup,
            crash_class=held.crash_class,
            restarts_in_window=held.restarts_in_window,
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
            held.restarts_in_window = self._restarts_in_window(held)
            actions = self.reconcile(held.spec, self._observed(held))
            if not actions:
                await self._commit(held)
                return
            for action in actions:
                await self._apply(held, action)
            await self._commit(held)
            if _terminal(held.phase):
                await self._release_quiet(session_worker_id(held.spec.session_id))
                return
        logger.error(
            "STS drive did not settle session=%s phase=%s",
            held.spec.session_id,
            held.phase.value,
        )

    def _restarts_in_window(self, held: _Held) -> int:
        return count_restarts_in_window(
            held.restarted_at,
            now_s=self._clock.monotonic(),
            window_s=float(held.spec.restart_window_s),
        )

    async def _apply(self, held: _Held, action: OrchestratorAction) -> None:
        if action.kind is ActionKind.SPAWN:
            await self._spawn(held, action)
        elif action.kind is ActionKind.STOP:
            await self._stop(held)
        elif action.kind is ActionKind.CLEANUP:
            await self._cleanup(held)
        elif action.kind is ActionKind.MARK_RESTARTING:
            self._mark_restarting(held, action)
            await self._commit(held)
        elif action.kind is ActionKind.MARK_TERMINAL:
            self._mark_terminal(held, action)
        elif action.kind is ActionKind.ALERT:
            await self._alert(held, action)

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
            held.ready = bool(view.ready)
            held.exit_code = None
            held.signal = None
            held.crash_class = None
            held.cleanup = Cleanup.NOT_RUN
            held.unconfirmed = {}
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
            held.ready = bool(view.ready)
            held.pid = None
            if view.exit_code is not None or view.signal is not None:
                held.exit_code = view.exit_code
                held.signal = _plain_signal(view.signal)
            else:
                self._apply_exit_file(held)
            self._classify(held)

    def _note_absent(self, held: _Held) -> None:
        if held.worker_incarnation == 0 and held.phase is SessionPhase.PENDING:
            held.pid = None
            held.pid_gone = True
            held.exit_recorded = False
            return
        held.pid = None
        held.pid_gone = True
        held.exit_recorded = True
        self._apply_exit_file(held)
        self._classify(held)

    def _apply_exit_file(self, held: _Held) -> None:
        """Read the shim's exit record when the slot no longer carries it.

        ``Supervisor.stop`` releases the slot, so ``status`` is ``None``
        and the file is the only remaining fact. A missing file leaves the
        code and signal unset.
        """
        if held.exit_code is not None or held.signal is not None:
            return
        path = exit_record_path(
            self.supervisor.work_dir, session_worker_id(held.spec.session_id)
        )
        try:
            raw = path.read_bytes()
        except OSError:
            return
        try:
            record = decode_exit(raw)
        except MessageError:
            logger.warning(
                "STS exit record is unreadable session=%s", held.spec.session_id
            )
            return
        held.exit_code = record.exit_code
        held.signal = _plain_signal(record.signal)

    def _classify(self, held: _Held) -> None:
        """Map the exit onto a crash class. A clean exit stays unclassified."""
        if held.crash_class is not None:
            return
        if not (
            held.exit_recorded and held.pid_gone and held.worker_incarnation > 0
        ):
            return
        if held.exit_code == 0 and held.signal is None:
            return
        if (
            held.exit_code is None
            and held.signal is None
            and (held.stop_sent or held.spec.desired is DesiredPhase.STOPPED)
        ):
            # No exit file after a stop this controller asked for. A real
            # kill writes the file before the slot is released. Treat the
            # gap as clean so a finished ``on_stop`` stays ``done``.
            return
        if held.exit_code is None and held.signal is None:
            held.crash_class = CrashClass.C
            return
        name = cause_for_exit(
            exit_code=held.exit_code,
            signal_no=held.signal,
            stopped_by_controller=held.stop_sent,
        )
        if name is None:
            return
        held.crash_class = classify_crash(CrashCause(name))

    async def _spawn(self, held: _Held, action: OrchestratorAction) -> None:
        if action.delay_s:
            await self._clock.sleep(action.delay_s)
        if held.request is None:
            await self._fail(held, "missing_request")
            return
        incarnation = action.incarnation or FIRST_INCARNATION
        previous = held.worker_incarnation
        if previous > 0 and not spawn_allowed(
            exit_recorded=held.exit_recorded,
            pid_gone=held.pid_gone,
            cleanup=held.cleanup,
        ):
            await self._fail(held, "spawn_refused")
            return
        refusal = await self._code_guard(held)
        if refusal is not None:
            return
        path = write_session_request(self.supervisor.work_dir, held.request)
        worker = session_worker_spec(
            held.spec,
            incarnation=incarnation,
            argv=tuple(self._argv_for(path)),
            code_ref=self._release_name(),
            start_timeout_s=procman_start_timeout_s(held.spec),
            hb_timeout_s=SESSION_HB_TIMEOUT_S,
            stop_grace_s=SESSION_STOP_GRACE_S,
            env=self._worker_env(held.spec),
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
        held.exit_code = None
        held.signal = None
        held.crash_class = None
        held.cleanup = Cleanup.NOT_RUN
        held.unconfirmed = {}
        held.stop_sent = False
        held.ready = False
        held.conditions = {"phase": SessionPhase.STARTING.value}
        if previous > 0 and incarnation > previous:
            held.restart_count += 1
            held.restarted_at.append(self._clock.monotonic())

    async def _stop(self, held: _Held) -> None:
        held.stop_sent = True
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
        if action.reason:
            held.reason = action.reason[:256]
        if held.finished_at is None:
            held.finished_at = self._clock.now()

    def _mark_restarting(self, held: _Held, action: OrchestratorAction) -> None:
        """Step 1. The row says ``restarting`` before the next spawn.

        The restart is counted when that spawn succeeds, not here. A
        reconcile that runs again before the spawn must not see the
        attempt twice.
        """
        held.phase = SessionPhase.RESTARTING
        held.column = column_status_for(SessionPhase.RESTARTING)
        held.conditions = {"phase": SessionPhase.RESTARTING.value}
        if action.reason:
            held.reason = action.reason[:256]

    async def _cleanup(self, held: _Held) -> None:
        """One ``cancel_session`` per account. No retry.

        Asked only after the old incarnation is dead. Empty ``api_ids``
        is confirmed without a call. A timeout, no responders, a call
        error, a decode error, or ``ok`` false on any account is
        ``UNCONFIRMED``.
        """
        if held.cleanup is not Cleanup.NOT_RUN:
            return
        if not (held.exit_recorded and held.pid_gone):
            return
        api_ids = held.spec.api_ids
        if not api_ids:
            held.cleanup = Cleanup.CONFIRMED
            held.unconfirmed = {}
            return
        if self._broker is None:
            held.cleanup = Cleanup.UNCONFIRMED
            held.unconfirmed = {
                api_id: ("no_responders",) for api_id in api_ids
            }
            return
        outcomes = await asyncio.gather(
            *(
                self._cancel_one(held.spec.session_id, api_id)
                for api_id in api_ids
            )
        )
        unconfirmed: dict[int, tuple[str, ...]] = {}
        for api_id, outcome in zip(api_ids, outcomes, strict=True):
            if outcome is not None:
                unconfirmed[api_id] = outcome
        held.unconfirmed = unconfirmed
        held.cleanup = (
            Cleanup.UNCONFIRMED if unconfirmed else Cleanup.CONFIRMED
        )

    async def _cancel_one(
        self, session_id: str, api_id: int
    ) -> tuple[str, ...] | None:
        """``None`` when this account confirmed. Otherwise the tokens."""
        assert self._broker is not None
        envelope = Envelope[TdCancelSessionRequest].wrap(
            TdCancelSessionRequest(session_id=session_id),
            type=TD_ORDER_CANCEL_SESSION,
            source="sts",
            session_id=session_id,
        )
        try:
            reply = await self._broker.request(
                Topics.td_order(api_id),
                envelope,
                timeout=STS_CLEANUP_TIMEOUT_S,
            )
        except NoRespondersError:
            return ("no_responders",)
        except (RequestTimeoutError, TimeoutError):
            return ("timeout",)
        except Exception:
            logger.warning(
                "STS cancel_session failed session=%s api_id=%s",
                session_id,
                api_id,
                exc_info=True,
            )
            return ("call_error",)
        reply_type = getattr(reply, "type", None)
        if reply_type != TD_ORDER_CANCEL_SESSION:
            return ("call_error",)
        payload = getattr(reply, "payload", None)
        try:
            if isinstance(payload, TdCancelSessionResult):
                result = payload
            elif hasattr(payload, "model_dump"):
                result = TdCancelSessionResult.model_validate(payload.model_dump())
            else:
                result = TdCancelSessionResult.model_validate(payload)
        except ValidationError:
            return ("decode_error",)
        if result.ok:
            return None
        if not result.unconfirmed:
            return ("unconfirmed",)
        return tuple(result.unconfirmed)

    async def _alert(self, held: _Held, action: OrchestratorAction) -> None:
        message = crash_log_message(
            crash_class=held.crash_class,
            reason=action.reason or "",
            incarnation=held.worker_incarnation,
            unconfirmed=(
                held.unconfirmed if held.cleanup is Cleanup.UNCONFIRMED else None
            ),
        )
        if self._publish is None:
            logger.error(
                "STS crash session=%s %s", held.spec.session_id, message
            )
            return
        await publish_sts_log(
            _LogBus(self._publish),
            held.spec.session_id,
            message,
            source=CONTROLLER_LOG_SOURCE,
            level="error",
        )

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
            held.restart_count,
            held.cleanup,
            held.crash_class,
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
        held.spawn_started = True
        held.phase = SessionPhase.RUNNING
        held.column = column_status_for(SessionPhase.RUNNING)
        held.conditions = {"phase": SessionPhase.RUNNING.value}
        code, signal_no, ready = _exit_facts(observed)
        held.exit_code = code
        held.signal = signal_no
        held.ready = ready
        held.exit_recorded = True
        held.pid_gone = True
        held.pid = None
        held.request = self._load_request(spec.session_id)
        held.observed_generation = (
            stored.observed_generation
            if stored.observed_generation is not None
            else spec.generation
        )
        async with self._lock(spec.session_id):
            self._sessions[spec.session_id] = held
            await self._drive(held)

    def _load_request(self, session_id: str) -> StsCreateSessionRequest | None:
        """The pinned request, so a rehang after reattach can spawn again."""
        path = self.supervisor.work_dir / "sessions" / f"{session_id}.json"
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError:
            return None
        try:
            return StsCreateSessionRequest.model_validate_json(raw)
        except ValueError:
            logger.warning(
                "STS request file is unreadable session=%s", session_id
            )
            return None

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

