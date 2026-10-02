"""The worker state machine (§4.3).

The diagram, as a table. Reading it is real. Choosing an edge from a live
worker — ready or not, restart or not, window full or not — is
:mod:`mftik.procman.decisions`, and that raises until B3-02.

::

    STOPPED ─▶ STARTING ─ready─▶ RUNNING ─SIGTERM─▶ STOPPING ─▶ STOPPED
                  │ death / timeout    │ death / heartbeat timeout
                  ▼                    ▼
                FAILED              CRASHED ─▶ BACKOFF ─▶ STARTING
                                       └─ window exceeded ─▶ FATAL
    alive ─shim disappears─▶ LOST

``SIGTERM`` is drawn from ``RUNNING``. :meth:`Supervisor.stop` can also
arrive while the worker is still ``STARTING``, so the table has that edge
too: same signal, earlier phase.
"""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from types import MappingProxyType

from mftik.procman.errors import InvalidTransition


class WorkerPhase(StrEnum):
    """Where one worker sits in the §4.3 machine."""

    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    FAILED = "failed"
    CRASHED = "crashed"
    BACKOFF = "backoff"
    FATAL = "fatal"
    LOST = "lost"


class Trigger(StrEnum):
    """What moves a worker between phases.

    ``DEATH`` is the process ending while the supervisor had not asked it
    to. ``EXITED`` is the process ending after ``SIGTERM``: a clean exit
    and a kill during the stop grace are both this trigger, and both land
    on ``STOPPED``. ``READY`` is only armed from ``STARTING``.
    ``HEARTBEAT_TIMEOUT`` is only armed from ``RUNNING``, and only when
    the spec's ``hb_timeout_s`` is a number. ``SHIM_LOST`` is the shim
    disappearing while the worker was supposed to be alive (``STARTING``,
    ``RUNNING``, ``STOPPING``). Once the shim has written the exit record
    the phase stays on that record; reattach reads ``LOST`` only when the
    socket and the exit file are both gone (§4.4).
    """

    SPAWN = "spawn"
    READY = "ready"
    DEATH = "death"
    START_TIMEOUT = "start_timeout"
    SIGTERM = "sigterm"
    EXITED = "exited"
    HEARTBEAT_TIMEOUT = "heartbeat_timeout"
    RESTART = "restart"
    INTENSITY_EXCEEDED = "intensity_exceeded"
    BACKOFF_ELAPSED = "backoff_elapsed"
    SHIM_LOST = "shim_lost"


#: Phases a shim is responsible for a living worker. ``SHIM_LOST`` leaves
#: these. ``CRASHED`` is past the death: the exit file is the record.
ALIVE_PHASES: frozenset[WorkerPhase] = frozenset(
    {WorkerPhase.STARTING, WorkerPhase.RUNNING, WorkerPhase.STOPPING}
)

TRANSITIONS: Mapping[tuple[WorkerPhase, Trigger], WorkerPhase] = MappingProxyType(
    {
        (WorkerPhase.STOPPED, Trigger.SPAWN): WorkerPhase.STARTING,
        (WorkerPhase.STARTING, Trigger.READY): WorkerPhase.RUNNING,
        (WorkerPhase.STARTING, Trigger.DEATH): WorkerPhase.FAILED,
        (WorkerPhase.STARTING, Trigger.START_TIMEOUT): WorkerPhase.FAILED,
        (WorkerPhase.STARTING, Trigger.SIGTERM): WorkerPhase.STOPPING,
        (WorkerPhase.STARTING, Trigger.SHIM_LOST): WorkerPhase.LOST,
        (WorkerPhase.RUNNING, Trigger.SIGTERM): WorkerPhase.STOPPING,
        (WorkerPhase.RUNNING, Trigger.DEATH): WorkerPhase.CRASHED,
        (WorkerPhase.RUNNING, Trigger.HEARTBEAT_TIMEOUT): WorkerPhase.CRASHED,
        (WorkerPhase.RUNNING, Trigger.SHIM_LOST): WorkerPhase.LOST,
        (WorkerPhase.STOPPING, Trigger.EXITED): WorkerPhase.STOPPED,
        (WorkerPhase.STOPPING, Trigger.SHIM_LOST): WorkerPhase.LOST,
        (WorkerPhase.CRASHED, Trigger.RESTART): WorkerPhase.BACKOFF,
        (WorkerPhase.CRASHED, Trigger.INTENSITY_EXCEEDED): WorkerPhase.FATAL,
        (WorkerPhase.BACKOFF, Trigger.BACKOFF_ELAPSED): WorkerPhase.STARTING,
    }
)


def transition(phase: WorkerPhase, trigger: Trigger) -> WorkerPhase:
    """The phase ``trigger`` leads to from ``phase``.

    A missing edge raises :class:`InvalidTransition`. The table is the
    diagram; it does not look at a spec, a clock, or a process.
    """
    try:
        return TRANSITIONS[(WorkerPhase(phase), Trigger(trigger))]
    except (KeyError, ValueError):
        raise InvalidTransition(f"no transition from {phase} on {trigger}") from None
