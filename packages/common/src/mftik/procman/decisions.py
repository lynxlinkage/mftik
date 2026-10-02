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
"""

from __future__ import annotations

import sys
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum

from mftik.procman.errors import InvalidWorkerSpec
from mftik.procman.messages import WorkerHeartbeat
from mftik.procman.spec import PLANES, RESTART_MODES, Plane, RestartMode
from mftik.procman.state import WorkerPhase

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
