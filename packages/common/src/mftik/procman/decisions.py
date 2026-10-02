"""Decisions the supervisor will make.

``observe_heartbeat`` is the S6 rule and is real (B3-01). Restart,
failure classification and reattach still raise until B3-02 and B3-03.

The state machine's edges are :data:`~mftik.procman.state.TRANSITIONS`.
These functions choose an edge from an observation. They are pure: no
socket, no clock of their own, no plane vocabulary beyond the ``plane``
argument :func:`reattach_action` takes from §4.4's table.

Restart intensity numbers are the caller's. STS deploy defaults (5 restarts
inside 600 seconds, F11) live on the STS orchestrator (IF-04). MD and TD
are described as exponential backoff plus a restart intensity (§4.3) and
the plan does not give them numbers, so this layer does not invent any.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum

from mftik.procman._ticket import TICKET
from mftik.procman.errors import InvalidWorkerSpec
from mftik.procman.messages import WorkerHeartbeat
from mftik.procman.spec import Plane, RestartMode
from mftik.procman.state import WorkerPhase


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
    """What :func:`reattach_action` will return once B3-03 implements it.

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

    ``min_backoff_s`` is the floor of the exponential delay. The ratio is
    not fixed here; :func:`plan_restart` only has to grow with ``attempt``
    and stay at or above this floor.
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
    ``ready`` is the distinction.
    """
    del ready, cause
    raise NotImplementedError(TICKET)


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
    increasing in ``attempt``. Crash class (A/B/C) is not an argument:
    procman does not know why a process died (P6).
    """
    del phase, restart, restarts_in_window, intensity, attempt
    raise NotImplementedError(TICKET)


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
    del restarted_at_s, now_s, window_s
    raise NotImplementedError(TICKET)


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

    ``close("detach")`` does not signal workers. ``close("stop")`` does.
    Those are :meth:`Supervisor.close`, not rows of this table.
    """
    del plane, desired, observed
    raise NotImplementedError(TICKET)


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
