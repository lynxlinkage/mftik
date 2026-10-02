"""Facts the orchestrator reads, and the actions it will name.

Constructing these is real. Deciding anything from them is
:mod:`mftik_sts.controller.decisions`, and that raises.

``strategy_digest`` and ``env_generation`` are the two pins F39 adds to
the spec (IF-16). They are not procman fields. The orchestrator copies
them onto ``WorkerSpec.labels``.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType

from mftik.instance import validate_instance_name
from mftik.procman import RESTART_MODES, RestartMode, validate_worker_id
from mftik.protocol.strategy_yml import (
    DEFAULT_READY_TIMEOUT_S,
    DEFAULT_START_TIMEOUT_S,
    MAX_START_TIMEOUT_S,
)
from mftik.registry.digest import DIGEST_PREFIX

from mftik_sts.controller.defaults import STS_MAX_RESTARTS, STS_RESTART_WINDOW_S

#: ``WorkerSpec.kind`` for an STS session worker (§3.1, §4.3).
SESSION_KIND = "session"

#: ``sha256:`` plus 64 hex characters, the width of ``sts_sessions.strategy_digest``.
_DIGEST = re.compile(rf"^{re.escape(DIGEST_PREFIX)}[0-9a-f]{{64}}$")


class DesiredPhase(StrEnum):
    """What the spec currently asks the session to be.

    ``RUNNING`` is a session the API has started and not ended, including
    one that is ``restarting``. ``STOPPED`` is an end the API has asked
    for. The observed phase lives on :class:`SessionStatus`.
    """

    RUNNING = "running"
    STOPPED = "stopped"


class SessionPhase(StrEnum):
    """Session phase vocabulary (§5.2).

    ``pending → starting → running → stopping → done | failed``, plus
    ``restarting`` (F10). ``interrupted`` is not a value here.
    """

    PENDING = "pending"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    RESTARTING = "restarting"
    DONE = "done"
    FAILED = "failed"


class CrashClass(StrEnum):
    """A, B or C (§5.2). Procman does not have this vocabulary (P6)."""

    A = "A"
    B = "B"
    C = "C"


class CrashCause(StrEnum):
    """What the session worker reported about its own death.

    The orchestrator classifies this. A shim exit code alone is not a
    class: B ends in a kill, and C is a kill, and those are not the same
    crash.

    * ``STRATEGY_EXCEPTION`` — strategy code raised, the process was still
      alive, ingress queued ``on_stop``. Class A.
    * ``HOOK_BLOCKED`` — a general hook held the strategy loop for the
      hard limit (30 seconds, F15). Class B.
    * ``STOP_STUCK`` — stop waited out its grace, including ``on_stop``
      past its wall-clock limit. Class B.
    * ``PROCESS_DEATH`` — the process is gone (OOM, segfault, SIGKILL)
      and ``on_stop`` did not run. Class C.
    """

    STRATEGY_EXCEPTION = "strategy_exception"
    HOOK_BLOCKED = "hook_blocked"
    STOP_STUCK = "stop_stuck"
    PROCESS_DEATH = "process_death"


class Cleanup(StrEnum):
    """Where ``td.order.cancel_session`` is for this session (F10, §7.1).

    ``NOT_RUN`` means the orchestrator has not asked yet. ``CONFIRMED``
    means every account replied ``ok``. ``UNCONFIRMED`` means the wait
    ended with orders still outstanding. Positions are never part of this:
    a cancel does not flatten (R3).
    """

    NOT_RUN = "not_run"
    CONFIRMED = "confirmed"
    UNCONFIRMED = "unconfirmed"


class RestartVerdict(StrEnum):
    """What :func:`mftik_sts.controller.decide_restart` will return."""

    #: Old incarnation is not confirmed dead. Do not spawn (R1).
    WAIT = "wait"
    #: Exit is confirmed and cleanup has not been asked. Ask it. Do not spawn.
    CLEANUP = "cleanup"
    #: The session ends. No new incarnation.
    FAILED = "failed"
    #: Hang the strategy up again from ``on_start``, with no prior state (F10).
    REHANG = "rehang"


class ActionKind(StrEnum):
    """One step :meth:`StsOrchestrator.reconcile` names.

    Applying the step — spawn, stop, the cancel RPC, the row write — is
    not this ticket.
    """

    SPAWN = "spawn"
    STOP = "stop"
    CLEANUP = "cleanup"
    MARK_RESTARTING = "mark_restarting"
    MARK_TERMINAL = "mark_terminal"
    ALERT = "alert"


#: Reasons on a :class:`RestartDecision`. The first matching rule supplies
#: the reason. Alert is separate: a later rule can still require one.
REASON_WAITING_FOR_EXIT = "waiting_for_exit"
REASON_CLEANUP_PENDING = "cleanup_pending"
REASON_RESTART_NEVER = "restart_never"
REASON_CRASH_CLASS_B = "crash_class_b"
REASON_CRASH_CLASS_C = "crash_class_c"
REASON_INIT_FAILURE = "init_failure"
REASON_RESTART_INTENSITY = "restart_intensity"
REASON_CLEANUP_UNCONFIRMED = "cleanup_unconfirmed"
REASON_ON_FAILURE = "on_failure"


def _as_int(value: object, name: str) -> int:
    # ``bool`` is an ``int`` subclass; a flag here would be a spec bug.
    if type(value) is not int:
        raise ValueError(f"{name} must be an int")
    return value


def _as_optional_int(value: object, name: str) -> int | None:
    if value is None:
        return None
    return _as_int(value, name)


def _as_digest(value: object) -> str | None:
    """``None`` for a built-in strategy. Otherwise ``sha256:<64 hex>``."""
    if value is None:
        return None
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise ValueError(
            "strategy_digest must be sha256:<64 hex> or None. "
            "A built-in strategy has no digest; its code is the release (F39)."
        )
    return value


def _as_float(value: object, name: str) -> float:
    if type(value) is bool or not isinstance(value, int | float):
        raise ValueError(f"{name} must be a number")
    return float(value)


def session_worker_id(session_id: str) -> str:
    """``sts/session/<session_id>`` (§4.3). One worker, one session."""
    return f"sts/session/{_session_segment(session_id)}"


def _session_segment(session_id: object) -> str:
    if not isinstance(session_id, str) or session_id == "" or "/" in session_id:
        raise ValueError(
            "session_id must be one path segment, so the worker id "
            "sts/session/<session_id> stays under run/"
        )
    validate_worker_id(f"sts/session/{session_id}")
    return session_id


@dataclass(frozen=True)
class SessionSpec:
    """Desired session, as the API wrote it (§3.3).

    The API is the authority. This object is the copy the orchestrator
    reads. It does not write the row.

    ``restart`` is the deploy's mode (F11), default ``never``. It is not
    copied onto the worker. Procman is handed ``restart="never"`` so it
    cannot start the next incarnation on its own; see
    :func:`mftik_sts.controller.session_worker_spec`.

    ``start_timeout_s`` and ``ready_timeout_s`` are the F12 budgets. The
    first counts ``on_start`` only. The second counts from the moment
    ``on_start`` returns. They are not :attr:`mftik.procman.WorkerSpec.start_timeout_s`.
    That field is procman's own ready timer, and this ticket does not
    decide which number an STS worker's spec carries there.

    ``generation`` is the session's reconcile generation (§8.4), starting
    at 1. It is not an extras generation and not the worker incarnation.
    The extras pin is :attr:`env_generation`.

    ``strategy_digest`` and ``env_generation`` are the code identity the
    API pinned at start (F39, §5.7). They do not change for the life of
    the session, and a rehang uses this pair rather than whatever the
    registry index currently names. ``strategy_digest`` is ``None`` for
    a built-in strategy (``mftik_sts.impl``): that code is the platform
    release. Both stay ``None`` on a row that predates the columns.
    """

    session_id: str
    instance: str
    strategy: str
    desired: DesiredPhase = DesiredPhase.RUNNING
    restart: RestartMode = "never"
    max_restarts: int = STS_MAX_RESTARTS
    restart_window_s: int = STS_RESTART_WINDOW_S
    start_timeout_s: float = DEFAULT_START_TIMEOUT_S
    ready_timeout_s: float = DEFAULT_READY_TIMEOUT_S
    generation: int = 1
    api_ids: tuple[int, ...] = ()
    strategy_digest: str | None = None
    env_generation: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "session_id", _session_segment(self.session_id))
        object.__setattr__(self, "instance", validate_instance_name(self.instance))
        if not isinstance(self.strategy, str) or self.strategy == "":
            raise ValueError("strategy must be a non-empty string")
        try:
            desired = DesiredPhase(self.desired)
        except ValueError as exc:
            raise ValueError(f"desired {self.desired!r} is not a phase") from exc
        if self.restart not in RESTART_MODES:
            raise ValueError(
                f"restart {self.restart!r} is not one of {', '.join(RESTART_MODES)}. "
                "always was rebuild, and nothing restarts that way (F11)."
            )
        max_restarts = _as_int(self.max_restarts, "max_restarts")
        if max_restarts < 0:
            raise ValueError("max_restarts must be >= 0")
        window = _as_int(self.restart_window_s, "restart_window_s")
        if window < 1:
            raise ValueError("restart_window_s must be >= 1")
        start = _as_float(self.start_timeout_s, "start_timeout_s")
        if not 0 < start <= MAX_START_TIMEOUT_S:
            raise ValueError(
                f"start_timeout_s must be in (0, {MAX_START_TIMEOUT_S}]"
            )
        ready = _as_float(self.ready_timeout_s, "ready_timeout_s")
        if ready <= 0:
            raise ValueError("ready_timeout_s must be > 0")
        generation = _as_int(self.generation, "generation")
        if generation < 1:
            raise ValueError("generation must be >= 1")
        if isinstance(self.api_ids, str) or not isinstance(self.api_ids, Sequence):
            raise ValueError("api_ids must be a sequence of ints")
        api_ids: list[int] = []
        for api_id in self.api_ids:
            if type(api_id) is not int:
                raise ValueError("api_ids must be a sequence of ints")
            api_ids.append(api_id)
        object.__setattr__(self, "desired", desired)
        object.__setattr__(self, "max_restarts", max_restarts)
        object.__setattr__(self, "restart_window_s", window)
        object.__setattr__(self, "start_timeout_s", start)
        object.__setattr__(self, "ready_timeout_s", ready)
        env_generation = _as_optional_int(self.env_generation, "env_generation")
        if env_generation is not None and env_generation < 0:
            raise ValueError("env_generation must be >= 0")
        object.__setattr__(self, "generation", generation)
        object.__setattr__(self, "api_ids", tuple(api_ids))
        object.__setattr__(self, "strategy_digest", _as_digest(self.strategy_digest))
        object.__setattr__(self, "env_generation", env_generation)


@dataclass(frozen=True)
class SessionStatus:
    """Observed session. The orchestrator's Supervisor is the authority
    for the row this was read from (§3.3): phase, conditions, incarnation,
    ``restart_count``, and the failure reason.

    The live copy is published on ``sts.status.{session_id}``. This object
    is what :meth:`StsOrchestrator.reconcile` compares to the spec. Filling
    it from a shim and a row is not this ticket.

    ``ready`` is true only after ``on_ready`` has returned. Procman's ready
    bit is a different fact (the process came up). A crash before this flag
    is an init failure and is not restarted (F11, F12).

    ``worker_incarnation`` is 0 when no worker has been spawned.
    ``restarts_in_window`` counts restarts already started inside the
    deploy's window. The orchestrator produces that count with
    :func:`mftik.procman.count_restarts_in_window` and
    :data:`~mftik_sts.controller.STS_RESTART_WINDOW_S`. It is not the
    lifetime ``restart_count``.

    ``exit_recorded`` is the shim's ``<id>.exit.json`` (S3). ``pid_gone``
    is the ``/proc`` check. Both have to be true before a new incarnation
    (R1). The shim is the authority for the exit; this layer reads it.
    """

    phase: SessionPhase = SessionPhase.PENDING
    worker_incarnation: int = 0
    restart_count: int = 0
    ready: bool = False
    exit_recorded: bool = False
    pid: int | None = None
    pid_gone: bool = True
    cleanup: Cleanup = Cleanup.NOT_RUN
    crash_class: CrashClass | None = None
    restarts_in_window: int = 0
    generation: int | None = None
    observed_generation: int | None = None
    conditions: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        try:
            phase = SessionPhase(self.phase)
        except ValueError as exc:
            raise ValueError(f"phase {self.phase!r} is not a session phase") from exc
        incarnation = _as_int(self.worker_incarnation, "worker_incarnation")
        if incarnation < 0:
            raise ValueError("worker_incarnation must be >= 0")
        restart_count = _as_int(self.restart_count, "restart_count")
        if restart_count < 0:
            raise ValueError("restart_count must be >= 0")
        for name, value in (
            ("ready", self.ready),
            ("exit_recorded", self.exit_recorded),
            ("pid_gone", self.pid_gone),
        ):
            if not isinstance(value, bool):
                raise ValueError(f"{name} must be a bool")
        if self.pid is not None:
            pid = _as_int(self.pid, "pid")
            if pid <= 0:
                raise ValueError("pid must be > 0 or None")
        else:
            pid = None
        try:
            cleanup = Cleanup(self.cleanup)
        except ValueError as exc:
            raise ValueError(
                f"cleanup {self.cleanup!r} is not a cleanup state"
            ) from exc
        if self.crash_class is not None:
            try:
                crash_class = CrashClass(self.crash_class)
            except ValueError as exc:
                raise ValueError(
                    f"crash_class {self.crash_class!r} is not A, B or C"
                ) from exc
        else:
            crash_class = None
        in_window = _as_int(self.restarts_in_window, "restarts_in_window")
        if in_window < 0:
            raise ValueError("restarts_in_window must be >= 0")
        for name, value in (
            ("generation", self.generation),
            ("observed_generation", self.observed_generation),
        ):
            if value is not None and (_as_int(value, name) < 1):
                raise ValueError(f"{name} must be >= 1 or None")
        if isinstance(self.conditions, str) or not isinstance(self.conditions, Mapping):
            raise ValueError("conditions must map strings to strings")
        copied: dict[str, str] = {}
        for key, item in self.conditions.items():
            if not isinstance(key, str) or key == "" or not isinstance(item, str):
                raise ValueError("conditions must map non-empty strings to strings")
            copied[key] = item
        object.__setattr__(self, "phase", phase)
        object.__setattr__(self, "worker_incarnation", incarnation)
        object.__setattr__(self, "restart_count", restart_count)
        object.__setattr__(self, "pid", pid)
        object.__setattr__(self, "cleanup", cleanup)
        object.__setattr__(self, "crash_class", crash_class)
        object.__setattr__(self, "restarts_in_window", in_window)
        object.__setattr__(self, "conditions", MappingProxyType(copied))


@dataclass(frozen=True)
class ReportSlot:
    """One session considered for the liveness report (R4, §8.2).

    ``pid`` may be ``None``. A ``restarting`` session has no process and
    is still reported.
    """

    session_id: str
    phase: SessionPhase
    pid: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "session_id", _session_segment(self.session_id))
        try:
            phase = SessionPhase(self.phase)
        except ValueError as exc:
            raise ValueError(f"phase {self.phase!r} is not a session phase") from exc
        if self.pid is not None:
            pid = _as_int(self.pid, "pid")
            if pid <= 0:
                raise ValueError("pid must be > 0 or None")
        else:
            pid = None
        object.__setattr__(self, "phase", phase)
        object.__setattr__(self, "pid", pid)


@dataclass(frozen=True)
class RestartDecision:
    """Fail, rehang, or not yet.

    ``reason`` is the first matching rule. ``alert`` is true when any
    matching rule asks for one, which is not always the rule that supplied
    ``reason``. ``error_log`` is the step-1 error log on
    ``log.sts.{session_id}`` (§5.2): true once the decision list is
    reached, false while the orchestrator is still waiting on the exit or
    on cleanup.

    ``delay_s`` and ``next_incarnation`` are set only for :attr:`RestartVerdict.REHANG`.
    ``cancels_positions`` stays false. Cleanup cancels resting orders, not
    exposure (R3).
    """

    verdict: RestartVerdict
    reason: str
    alert: bool
    error_log: bool
    delay_s: float | None
    next_incarnation: int | None
    cancels_positions: bool = False


@dataclass(frozen=True)
class OrchestratorAction:
    """One action from :meth:`StsOrchestrator.reconcile`.

    There is no strategy-state field. A rehang does not carry ``st_facts``
    or anything else the previous incarnation held (F10).
    """

    kind: ActionKind
    session_id: str
    incarnation: int | None = None
    phase: SessionPhase | None = None
    reason: str | None = None
    delay_s: float | None = None
    alert: bool = False
    api_ids: tuple[int, ...] = ()
