"""Decisions the supervisor will make.

``observe_heartbeat`` is the S6 rule and is real (B3-01).
:func:`classify_failure`, :func:`plan_restart` and
:func:`count_restarts_in_window` are real (B3-02). :func:`reattach_action`
is the §4.4 table and is real (B3-03). :func:`previous_worker_gone` is the
F36 pid-reuse rule and is real (B3-03).

The state machine's edges are :data:`~mftik.procman.state.TRANSITIONS`.
These functions choose an edge from an observation. They are pure: no
socket, no clock of their own, no plane vocabulary beyond the ``plane``
argument :func:`reattach_action` takes from §4.4's table. Crash class
(A/B/C) is not an argument (P6).

Restart intensity numbers are the caller's. STS deploy defaults (5 restarts
inside 600 seconds, F11) live on the STS orchestrator (IF-04). MD and TD
are described as exponential backoff plus a restart intensity (§4.3) and
the plan does not give them numbers, so this layer does not invent any.
The supervisor does not apply :func:`plan_restart` itself. The orchestrator
does, then waits ``delay_s`` and calls :meth:`~mftik.procman.Supervisor.spawn`
for the next incarnation.

Admission (B3-05, §4.7) is :func:`decide_admission`. It is pure: no
``/proc`` walk, no shim. The orchestrator supplies an
:class:`AdmissionBudget` and this module does not choose the numbers or
read the environment. A spawn whose id is already held is a restart, not
a start, and is admitted.
"""

from __future__ import annotations

import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType

from mftik.procman.errors import InvalidWorkerSpec
from mftik.procman.messages import WorkerHeartbeat
from mftik.procman.spec import PLANES, RESTART_MODES, Plane, RestartMode, WorkerSpec
from mftik.procman.state import ALIVE_PHASES, WorkerPhase

#: How fast a ``BACKOFF`` delay grows with ``attempt``.
#:
#: Attempt 1 waits ``min_backoff_s``. Attempt ``n`` (``n >= 1``) waits
#: ``min_backoff_s * BACKOFF_RATIO ** (n - 1)``. There is no cap, and this
#: module does not reset ``attempt``: both are the caller's, and issue #286
#: leaves the multiplier open. A floor of 0 stays 0, because the curve
#: multiplies the floor; callers that need a growing delay pass a positive
#: floor (F11's STS floor is 1 second).
BACKOFF_RATIO = 2.0


class FailureCause(StrEnum):
    """Why a worker that had not been asked to stop is no longer running."""

    DEATH = "death"
    START_TIMEOUT = "start_timeout"
    HEARTBEAT_TIMEOUT = "heartbeat_timeout"


class DesiredSlot(StrEnum):
    """Whether reattach still wants this worker (§4.4).

    ``ABSENT`` covers both "no desired row" and "desired, already terminal".
    """

    PRESENT = "present"
    ABSENT = "absent"


class ObservedWorker(StrEnum):
    """What reattach found under ``run/`` for that id (§4.4)."""

    RUNNING = "running"
    ABSENT = "absent"
    EXITED = "exited"
    LOST = "lost"


class ReattachAction(StrEnum):
    """What :func:`reattach_action` returns for one cell of the §4.4 table.

    ``MARK_FAILED`` is the STS cell of the table: record the exit and do not
    spawn. It is not the state machine's ``FAILED`` phase.
    A worker that had reached ready is still not rebuilt. ``APPLY_RESTART``
    is the MD/TD cell: the caller then runs :func:`plan_restart`. ``NONE``
    is the cell the table leaves blank — nothing is desired and nothing is
    running.
    """

    ADOPT = "adopt"
    MARK_FAILED = "mark_failed"
    APPLY_RESTART = "apply_restart"
    STOP_AND_RELEASE = "stop_and_release"
    NONE = "none"


@dataclass(frozen=True)
class RestartIntensity:
    """How many restarts a window may hold before the machine goes ``FATAL``.

    ``max_restarts`` restarts are allowed inside ``window_s``. The crash that
    would start one more goes to ``FATAL`` (§4.3).
    Compared as ``restarts_in_window >= max_restarts`` where
    ``restarts_in_window`` counts restarts already started in the window.
    With ``max_restarts=5``, four prior restarts still back off (the fifth
    is allowed) and five prior restarts are ``FATAL`` (a sixth would exceed).

    ``min_backoff_s`` is the floor of the delay. :data:`BACKOFF_RATIO` is
    the multiplier :func:`plan_restart` applies. The plan does not fix that
    ratio (issue #286); the constant is the value this layer uses until it
    does.
    """

    max_restarts: int
    window_s: float
    min_backoff_s: float

    def __post_init__(self) -> None:
        if type(self.max_restarts) is not int or self.max_restarts < 0:
            raise InvalidWorkerSpec("max_restarts must be an int >= 0")
        if (
            type(self.window_s) is bool
            or not isinstance(self.window_s, int | float)
            or self.window_s < 0
        ):
            raise InvalidWorkerSpec("window_s must be >= 0")
        if (
            type(self.min_backoff_s) is bool
            or not isinstance(self.min_backoff_s, int | float)
            or self.min_backoff_s < 0
        ):
            raise InvalidWorkerSpec("min_backoff_s must be >= 0")
        object.__setattr__(self, "window_s", float(self.window_s))
        object.__setattr__(self, "min_backoff_s", float(self.min_backoff_s))


@dataclass(frozen=True)
class RestartDecision:
    """Where a classified failure goes, and how long ``BACKOFF`` waits."""

    phase: WorkerPhase
    delay_s: float | None


def classify_failure(*, ready: bool, cause: FailureCause) -> WorkerPhase:
    """Death or timeout before ready is ``FAILED``; after ready, ``CRASHED``.

    ``FAILED`` is not restarted. ``CRASHED`` is restarted only when the spec
    says ``on_failure`` and :func:`plan_restart` still has room in the
    window (§4.3). ``cause`` names which timer or wait status fired;
    ``ready`` is the distinction. The cause does not change the phase.
    """
    if type(ready) is not bool:
        raise TypeError("ready must be a bool")
    if not isinstance(cause, FailureCause):
        raise TypeError("cause must be a FailureCause")
    if ready:
        return WorkerPhase.CRASHED
    return WorkerPhase.FAILED


def _backoff_delay_s(*, attempt: int, min_backoff_s: float) -> float:
    """``min_backoff_s * BACKOFF_RATIO ** (attempt - 1)``, and at least the floor.

    A zero floor stays 0. An exponent that overflows a float returns the
    largest finite float instead of raising. That ceiling is not a policy
    cap: the curve still has none (issue #286).
    """
    if min_backoff_s == 0.0:
        return 0.0
    try:
        delay = min_backoff_s * BACKOFF_RATIO ** (attempt - 1)
    except OverflowError:
        return sys.float_info.max
    if delay < min_backoff_s:
        return min_backoff_s
    return delay


def plan_restart(
    *,
    phase: WorkerPhase,
    restart: RestartMode,
    restarts_in_window: int,
    intensity: RestartIntensity,
    attempt: int,
) -> RestartDecision:
    """Choose the edge out of a classified failure.

    ``FAILED`` stays ``FAILED`` with no delay, whatever ``restart`` says.
    ``CRASHED`` with ``restart="never"`` stays ``CRASHED`` with no delay.
    ``CRASHED`` with ``restart="on_failure"`` goes to ``BACKOFF`` while
    ``restarts_in_window < intensity.max_restarts``, and to ``FATAL`` once
    the count has reached the maximum. ``attempt`` starts at 1 and is the
    input to the backoff curve only; it does not decide ``FATAL``.

    The delay is at least ``intensity.min_backoff_s`` and is strictly
    increasing in ``attempt`` when that floor is positive
    (:data:`BACKOFF_RATIO`). Crash class (A/B/C) is not an argument:
    procman does not know why a process died (P6).

    The supervisor does not call this and does not wait the delay. The
    orchestrator does both, then :meth:`~mftik.procman.Supervisor.spawn`.
    """
    if not isinstance(intensity, RestartIntensity):
        raise TypeError(
            "intensity must be a RestartIntensity; "
            "this layer does not choose the numbers"
        )
    try:
        named = WorkerPhase(phase)
    except ValueError as exc:
        raise ValueError(f"unknown phase {phase!r}") from exc
    if restart not in RESTART_MODES:
        raise ValueError(
            f"restart {restart!r} is not one of {', '.join(RESTART_MODES)}"
        )
    if type(restarts_in_window) is not int or restarts_in_window < 0:
        raise ValueError("restarts_in_window must be an int >= 0")
    if type(attempt) is not int or attempt < 1:
        raise ValueError("attempt starts at 1")
    if named is WorkerPhase.FAILED:
        return RestartDecision(phase=WorkerPhase.FAILED, delay_s=None)
    if named is not WorkerPhase.CRASHED:
        raise ValueError(
            f"plan_restart applies to FAILED or CRASHED, not {named}"
        )
    if restart == "never":
        return RestartDecision(phase=WorkerPhase.CRASHED, delay_s=None)
    # ``>=`` so max_restarts already-started restarts fill the window.
    # The crash in hand would be one more. ``attempt`` is not consulted.
    if restarts_in_window >= intensity.max_restarts:
        return RestartDecision(phase=WorkerPhase.FATAL, delay_s=None)
    return RestartDecision(
        phase=WorkerPhase.BACKOFF,
        delay_s=_backoff_delay_s(
            attempt=attempt, min_backoff_s=intensity.min_backoff_s
        ),
    )


def count_restarts_in_window(
    restarted_at_s: Sequence[float],
    *,
    now_s: float,
    window_s: float,
) -> int:
    """How many of ``restarted_at_s`` fall inside the window ending at ``now_s``.

    A sample counts when ``now_s - at <= window_s`` (the edge is inside).
    Older samples do not. Times are the caller's monotonic seconds.
    """
    if isinstance(restarted_at_s, str) or not isinstance(restarted_at_s, Sequence):
        raise TypeError("restarted_at_s must be a sequence of times")
    if type(now_s) is bool or not isinstance(now_s, int | float):
        raise TypeError("now_s must be a number")
    if type(window_s) is bool or not isinstance(window_s, int | float):
        raise TypeError("window_s must be a number")
    count = 0
    for at in restarted_at_s:
        if type(at) is bool or not isinstance(at, int | float):
            raise TypeError("a restart time must be a number")
        if now_s - float(at) <= window_s:
            count += 1
    return count


def previous_worker_gone(
    *,
    recorded_start_ticks: int | None,
    live_start_ticks: int | None,
) -> bool:
    """Whether the recorded worker process is gone (F36, §7.1).

    ``live_start_ticks`` is field 22 of ``/proc/<pid>/stat`` (clock ticks
    since boot), or ``None`` when that pid is not a process. ``None`` means
    the pid is gone. The same start time means the same process is still
    alive, so a new incarnation must not be spawned. A different start time
    is pid reuse: the recorded process is gone. A live pid whose start time
    was never recorded is treated as still alive, because reuse cannot be
    ruled out.

    This does not read ``/proc`` and does not signal. :meth:`Supervisor.spawn`
    reads the start time and refuses while this returns false.
    """
    recorded = _start_ticks(recorded_start_ticks, "recorded_start_ticks")
    live = _start_ticks(live_start_ticks, "live_start_ticks")
    if live is None:
        return True
    if recorded is None:
        return False
    return live != recorded


def _start_ticks(value: int | None, name: str) -> int | None:
    if value is None:
        return None
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be an int >= 0 or None")
    return value


def reattach_action(
    *,
    plane: Plane,
    desired: DesiredSlot,
    observed: ObservedWorker,
) -> ReattachAction:
    """§4.4's reconciliation table.

    ==============  ======================  ================================
    desired         worker                  action
    ==============  ======================  ================================
    present         running                 ``ADOPT``, every plane
    present         absent, exited or lost  STS: ``MARK_FAILED`` (no spawn);
                                            MD and TD: ``APPLY_RESTART``
    absent          running                 ``STOP_AND_RELEASE``, every plane
    absent          anything else           ``NONE``
    ==============  ======================  ================================

    The supervisor does not read desired state and does not apply the
    action. The orchestrator does, with :meth:`~mftik.procman.Supervisor.stop`,
    :meth:`~mftik.procman.Supervisor.release_slot`,
    :meth:`~mftik.procman.Supervisor.record_restart` and
    :meth:`~mftik.procman.Supervisor.spawn`. ``close("detach")`` does not
    signal workers. ``close("stop")`` does. Those are
    :meth:`~mftik.procman.Supervisor.close`, not rows of this table.
    """
    if plane not in PLANES:
        raise ValueError(f"plane {plane!r} is not one of {', '.join(PLANES)}")
    if not isinstance(desired, DesiredSlot):
        raise TypeError("desired must be a DesiredSlot")
    if not isinstance(observed, ObservedWorker):
        raise TypeError("observed must be an ObservedWorker")
    if desired is DesiredSlot.PRESENT and observed is ObservedWorker.RUNNING:
        return ReattachAction.ADOPT
    if desired is DesiredSlot.PRESENT:
        if plane == "sts":
            return ReattachAction.MARK_FAILED
        return ReattachAction.APPLY_RESTART
    if observed is ObservedWorker.RUNNING:
        return ReattachAction.STOP_AND_RELEASE
    return ReattachAction.NONE


def observe_heartbeat(
    *, previous_ready: bool, beat: WorkerHeartbeat | None
) -> bool:
    """Apply one status-pipe outcome (S6).

    ``beat is None`` means the shim dropped the write because the pipe was
    full: the previous observation stands. A beat replaces it wholesale,
    because every message carries the full snapshot rather than a delta.
    """
    if beat is None:
        return previous_ready
    return beat.ready


#: One shim's RSS, added once per counted worker (§4.7).
#:
#: B3-01 measured ``VmRSS`` at 14576 kB (Python 3.12.3, the worker in
#: ``time.sleep``, the shim blocked in ``poll``, read from
#: ``/proc/<pid>/status``). Linux reports that field in KiB, so the byte
#: figure is ``14576 * 1024`` (about 14.2 MiB). Admission uses this
#: constant. It does not walk ``/proc``. This is not
#: :data:`~mftik.procman.spec.SHIM_OOM_SCORE_ADJ`, which stays 0.
SHIM_VMRSS_BYTES = 14576 * 1024

#: ``memory_budget_mb`` and ``estimate_mb`` are mebibytes. Converted once.
_BYTES_PER_MIB = 1024 * 1024


class AdmissionReason(StrEnum):
    """Why :func:`decide_admission` refused a new worker.

    ``WORKERS`` and ``MEMORY`` become :class:`~mftik.procman.CapacityExceeded`
    (``capacity_exceeded``). ``UNKNOWN_KIND`` is a :class:`~mftik.procman.ProcmanError`:
    a memory budget is set and a counted worker's kind has no estimate.
    That is not taken as zero, and it is not a capacity code.
    """

    WORKERS = "workers"
    MEMORY = "memory"
    UNKNOWN_KIND = "unknown_kind"


@dataclass(frozen=True)
class AdmissionBudget:
    """The orchestrator's admission budget for one plane instance (§4.7).

    ``max_workers`` and ``memory_budget_mb`` are ``None`` when that limit
    is off. Procman does not choose either number, has no default for
    them, and does not read the environment. The orchestrator does. No
    budget at all, on :class:`~mftik.procman.Supervisor`, is the same as
    both limits ``None``: nothing is refused.

    ``memory_budget_mb`` and ``estimate_mb`` are mebibytes (1024×1024
    bytes), the MiB §4.7 uses for the shim. :func:`decide_admission`
    converts them to bytes once. ``estimate_mb`` is keyed by
    :attr:`~mftik.procman.WorkerSpec.kind` and is the worker only. One
    shim (:data:`SHIM_VMRSS_BYTES`) is added per counted worker on top.
    A kind with no estimate, while ``memory_budget_mb`` is set, is
    refused. It is not taken as zero.

    Only a spawn for an id the supervisor does not already hold is
    subject to this budget. Replacing a held slot of the same id — a
    restart after ``record_restart``, or a new incarnation over
    ``FAILED``, ``CRASHED``, ``BACKOFF``, ``FATAL``, ``STOPPED`` or
    ``LOST`` — is not refused, and that slot is not counted twice.
    Refusing a restart would turn an MD or TD crash into an outage.
    ``release_slot`` and ``close(stop)`` drop the slot, so it leaves
    this count with ``_slots``.
    """

    max_workers: int | None
    memory_budget_mb: int | None
    estimate_mb: Mapping[str, int]

    def __post_init__(self) -> None:
        max_workers = _optional_positive_int(self.max_workers, "max_workers")
        memory = _optional_positive_int(self.memory_budget_mb, "memory_budget_mb")
        raw_estimate = self.estimate_mb
        if isinstance(raw_estimate, str) or not isinstance(raw_estimate, Mapping):
            raise InvalidWorkerSpec("estimate_mb must map kinds to positive ints")
        estimate: dict[str, int] = {}
        for key, value in raw_estimate.items():
            if not isinstance(key, str) or key == "":
                raise InvalidWorkerSpec(
                    "estimate_mb must map non-empty kind strings to positive ints"
                )
            estimate[key] = _positive_int(value, f"estimate_mb[{key!r}]")
        object.__setattr__(self, "max_workers", max_workers)
        object.__setattr__(self, "memory_budget_mb", memory)
        object.__setattr__(self, "estimate_mb", MappingProxyType(estimate))


@dataclass(frozen=True)
class AdmissionWorker:
    """One worker :func:`decide_admission` can see.

    A held slot has a :class:`~mftik.procman.WorkerPhase`. An in-flight
    spawn has no slot yet: pass it in ``spawning`` with ``phase=None``.
    ``rss_bytes`` is the last measured tree Pss, or ``None`` when no
    report has stored one. ``0`` is a measurement. Admission does not
    read ``/proc``.
    """

    id: str
    kind: str | None
    phase: WorkerPhase | None
    rss_bytes: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.id, str) or self.id == "":
            raise ValueError("id must be a non-empty string")
        if self.kind is not None and (type(self.kind) is not str or self.kind == ""):
            raise ValueError("kind must be a non-empty string or None")
        if self.phase is not None and not isinstance(self.phase, WorkerPhase):
            raise TypeError("phase must be a WorkerPhase or None")
        rss = self.rss_bytes
        if rss is not None and (type(rss) is not int or rss < 0):
            raise ValueError("rss_bytes must be an int >= 0 or None")


@dataclass(frozen=True)
class AdmissionDecision:
    """Admit, or refuse with a reason the supervisor turns into an error.

    ``admitted`` with ``reason is None`` is an admission. Otherwise
    ``reason`` says which check failed and ``message`` names the limit
    and the numbers, or the kind that has no estimate.
    """

    admitted: bool
    reason: AdmissionReason | None
    message: str


def decide_admission(
    *,
    budget: AdmissionBudget | None,
    held: Sequence[AdmissionWorker],
    spawning: Sequence[AdmissionWorker],
    candidate: WorkerSpec,
) -> AdmissionDecision:
    """Admit ``candidate`` or refuse it, without launching anything.

    ``budget is None``, and a budget whose ``max_workers`` and
    ``memory_budget_mb`` are both ``None``, admit. That is no limit.

    Workers that count: held slots in :data:`~mftik.procman.ALIVE_PHASES`,
    plus every id in ``spawning`` that is not already one of those, plus
    ``candidate`` when it is not already in that set. A held slot in any
    other phase does not count. The same id is counted once; a live slot's
    measured ``rss_bytes`` wins over the in-flight estimate.

    An id that appears in ``held`` — any phase, including ``LOST`` — is
    a restart of a slot the supervisor already holds. It is admitted and
    not counted twice. ``spawn`` still refuses a live phase before it
    asks, which is the state machine's rule, not this one. A ``LOST``
    slot is admitted here. The F36 fence runs after this check and still
    refuses one whose worker pid is alive.

    Memory, when ``memory_budget_mb`` is set: for each counted worker,
    the last measured ``rss_bytes`` when it has one (including ``0``),
    otherwise ``estimate_mb`` for its kind, plus one
    :data:`SHIM_VMRSS_BYTES`. A missing estimate is
    :attr:`AdmissionReason.UNKNOWN_KIND`, not zero. The worker count is
    checked first. Over ``max_workers`` is reported even when a kind
    also has no estimate. Equal to a limit is inside it; only a count
    or a byte total past the limit is refused.

    This does not read ``/proc`` and does not reserve the id.
    """
    if budget is not None and not isinstance(budget, AdmissionBudget):
        raise TypeError(
            "budget must be an AdmissionBudget or None; "
            "this layer does not choose the numbers"
        )
    if not isinstance(candidate, WorkerSpec):
        raise TypeError("candidate must be a WorkerSpec")
    held_workers = _workers(held, "held")
    spawning_workers = _workers(spawning, "spawning")
    for worker in held_workers:
        if not isinstance(worker.phase, WorkerPhase):
            raise TypeError("a held worker needs a WorkerPhase")
    if budget is None or (
        budget.max_workers is None and budget.memory_budget_mb is None
    ):
        return _allow()
    if any(worker.id == candidate.id for worker in held_workers):
        return _allow()

    counted = _counted(held_workers, spawning_workers, candidate)
    if budget.max_workers is not None and len(counted) > budget.max_workers:
        return _refuse(
            AdmissionReason.WORKERS,
            (
                f"max_workers exceeded: spawning {candidate.id} would make "
                f"{len(counted)} workers, max_workers is {budget.max_workers}"
            ),
        )
    if budget.memory_budget_mb is None:
        return _allow()

    estimates = {
        kind: mib * _BYTES_PER_MIB for kind, mib in budget.estimate_mb.items()
    }
    budget_bytes = budget.memory_budget_mb * _BYTES_PER_MIB
    total = 0
    for worker in sorted(counted, key=lambda item: item.id):
        resident = _resident_bytes(worker, estimates)
        if resident is None:
            return _refuse(
                AdmissionReason.UNKNOWN_KIND,
                (
                    f"no memory estimate for kind {worker.kind!r} "
                    f"of worker {worker.id!r}; a memory budget is set "
                    "and procman does not treat a missing estimate as zero"
                ),
            )
        total += resident
    if total > budget_bytes:
        return _refuse(
            AdmissionReason.MEMORY,
            (
                f"memory_budget_mb exceeded: spawning {candidate.id} would use "
                f"{total} bytes, memory_budget_mb is {budget.memory_budget_mb} "
                f"({budget_bytes} bytes)"
            ),
        )
    return _allow()


def _optional_positive_int(value: object, name: str) -> int | None:
    if value is None:
        return None
    return _positive_int(value, name)


def _positive_int(value: object, name: str) -> int:
    # ``bool`` is an ``int`` subclass. A flag here would be a budget bug.
    if type(value) is not int or value <= 0:
        raise InvalidWorkerSpec(f"{name} must be a positive int")
    return value


def _workers(value: object, name: str) -> tuple[AdmissionWorker, ...]:
    if isinstance(value, str) or not isinstance(value, Sequence):
        raise TypeError(f"{name} must be a sequence of AdmissionWorker")
    workers: list[AdmissionWorker] = []
    for item in value:
        if not isinstance(item, AdmissionWorker):
            raise TypeError(f"{name} must be a sequence of AdmissionWorker")
        workers.append(item)
    return tuple(workers)


def _counted(
    held: Sequence[AdmissionWorker],
    spawning: Sequence[AdmissionWorker],
    candidate: WorkerSpec,
) -> tuple[AdmissionWorker, ...]:
    """Live slots, in-flight spawns not already live, then the candidate."""
    alive: dict[str, AdmissionWorker] = {}
    for worker in held:
        if worker.phase in ALIVE_PHASES and worker.id not in alive:
            alive[worker.id] = worker
    counted: list[AdmissionWorker] = list(alive.values())
    seen = set(alive)
    for worker in spawning:
        if worker.id in seen:
            continue
        seen.add(worker.id)
        counted.append(worker)
    if candidate.id not in seen:
        counted.append(
            AdmissionWorker(
                id=candidate.id,
                kind=candidate.kind,
                phase=None,
                rss_bytes=None,
            )
        )
    return tuple(counted)


def _resident_bytes(
    worker: AdmissionWorker, estimates: Mapping[str, int]
) -> int | None:
    """Worker bytes plus one shim, or ``None`` when the kind has no estimate.

    A stored ``rss_bytes`` (including ``0``) is the measurement. Only a
    missing one falls through to ``estimates``.
    """
    if worker.rss_bytes is not None:
        return worker.rss_bytes + SHIM_VMRSS_BYTES
    if worker.kind is None or worker.kind not in estimates:
        return None
    return estimates[worker.kind] + SHIM_VMRSS_BYTES


def _allow() -> AdmissionDecision:
    return AdmissionDecision(admitted=True, reason=None, message="")


def _refuse(reason: AdmissionReason, message: str) -> AdmissionDecision:
    return AdmissionDecision(admitted=False, reason=reason, message=message)
