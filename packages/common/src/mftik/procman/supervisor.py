"""``Supervisor``: the decisions, with no processes behind them yet.

Embedded in each plane's controller (§4.1). :meth:`start` loads
``supervisor.json`` and reattaches. :meth:`close` is ``detach`` (signal
nothing, flush, exit 0) or ``stop`` (the whole host is going down).
:meth:`spawn`, :meth:`stop`, :meth:`status` and :meth:`report` are the
rest of the surface the orchestrators call.

A new incarnation is spawned only after the previous pid is gone (F36).
The check is a ``/proc`` read, which ``oci_host_pid`` makes possible; B3-03
implements it. This class does not spawn today.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from mftik.instance import validate_instance_name
from mftik.procman._ticket import TICKET
from mftik.procman.report import ProcmanReport
from mftik.procman.spec import PLANES, Plane, WorkerSpec, validate_worker_id
from mftik.procman.state import WorkerPhase


class CloseMode(StrEnum):
    """Argument of :meth:`Supervisor.close` (§4.4).

    ``DETACH`` stops accepting control RPCs, signals no worker, flushes
    status and exits 0. Workers keep running. ``STOP`` is the whole host
    going offline: every worker is signalled, reaped and released.
    """

    DETACH = "detach"
    STOP = "stop"


@dataclass(frozen=True)
class WorkerStatus:
    """The supervisor's view of one worker: the spec plus the phase it chose.

    ``pid``, ``ready``, ``exit_code``, ``signal`` and ``rss_bytes`` are the
    shim's facts. ``phase`` is the state machine. ``exit_code`` and
    ``signal`` follow the exit record: both empty while the worker is
    alive, exactly one set after it has been reaped.
    """

    spec: WorkerSpec
    phase: WorkerPhase
    pid: int | None
    ready: bool
    exit_code: int | None
    signal: int | None
    rss_bytes: int | None


class Supervisor:
    """One plane instance's supervisor.

    ``plane`` is ``sts``, ``md`` or ``td``. ``instance`` is one NATS subject
    segment, the same rule as :func:`mftik.instance.validate_instance_name`,
    because the report subject is ``procman.report.{plane}.{instance}``.
    """

    def __init__(self, work_dir: Path, *, plane: Plane, instance: str) -> None:
        if plane not in PLANES:
            raise ValueError(f"plane {plane!r} is not one of {', '.join(PLANES)}")
        self.work_dir = Path(work_dir)
        self.plane: Plane = plane
        self.instance = validate_instance_name(instance)

    async def start(self) -> None:
        """Load ``supervisor.json``, then reattach each socket (§4.4).

        Control subjects stay dark until reconciliation finishes. Workers
        that are already running keep running through it (P1).
        """
        raise NotImplementedError(TICKET)

    async def close(self, mode: CloseMode) -> None:
        """``detach`` leaves workers running; ``stop`` ends them (§4.4)."""
        CloseMode(mode)
        raise NotImplementedError(TICKET)

    async def spawn(self, spec: WorkerSpec) -> None:
        """Spawn ``spec`` on this supervisor's plane.

        The spec's plane has to be this supervisor's plane. A new
        incarnation waits until the previous pid is gone (F36).
        """
        if spec.plane != self.plane:
            raise ValueError(
                f"spec plane {spec.plane!r} does not match "
                f"supervisor plane {self.plane!r}"
            )
        raise NotImplementedError(TICKET)

    async def stop(self, worker_id: str) -> None:
        """SIGTERM the worker through its shim, then release it after exit."""
        validate_worker_id(worker_id)
        raise NotImplementedError(TICKET)

    async def status(self, worker_id: str) -> WorkerStatus | None:
        """The supervisor's view, or ``None`` when it does not hold ``worker_id``."""
        validate_worker_id(worker_id)
        raise NotImplementedError(TICKET)

    async def report(self) -> ProcmanReport:
        """The payload :meth:`start` will publish. Not persisted (§3.3, F32)."""
        raise NotImplementedError(TICKET)
