"""``Supervisor``: spawn, stop, and the live phase of one plane instance.

Embedded in each plane's controller (§4.1). :meth:`start` loads
``supervisor.json`` and reattaches each socket under ``run/`` (B3-03).
It reports what it found and does not stop, spawn, release or signal.
:meth:`close` is ``detach`` (signal nothing, flush, exit 0) or ``stop``
(the whole host is going down); publication pauses before that work.
:meth:`report` is the liveness snapshot (B3-04). The orchestrator
publishes it; this class has no NATS client.

:meth:`spawn`, :meth:`stop` and :meth:`status` are real (B3-02). So is
the live machine: start timeout, death, ready, heartbeat timeout, a shim
that disappears, and ``SIGTERM`` then ``SIGKILL`` after ``stop_grace_s``.

**Who restarts.** The orchestrator does. This class classifies a failure
with :func:`~mftik.procman.classify_failure` and holds the slot, shim
included, until the orchestrator calls :meth:`record_restart`. It has no
restart loop, no backoff timer, and no :class:`~mftik.procman.RestartIntensity`.
The orchestrator calls :func:`~mftik.procman.plan_restart` with its own
intensity, waits the delay, and calls :meth:`spawn` with the next
incarnation. :meth:`record_restart` is how that decision is written onto
the held slot (``CRASHED`` to ``BACKOFF`` or ``FATAL``) so :meth:`status`
shows it.

**Heartbeat.** The shim counts valid beats and does not kill on a miss.
This class polls :meth:`~mftik.procman.ShimClient.status` every
:data:`STATUS_POLL_S`. ``watch`` is not used for freshness: it pushes
when ``ready`` changes or the worker exits, not on every beat. A counter
needs no shared clock. While the phase is ``RUNNING`` and
``hb_timeout_s`` is a number, a counter that has not moved for that long
is :class:`~mftik.procman.Trigger.HEARTBEAT_TIMEOUT`: ``SIGKILL`` through
the shim, then ``CRASHED``. The trigger is not armed in any other phase.

**Clock.** Deadlines are read from the injected :class:`~mftik.clock.Clock`.
The default is the process clock. Tests pass a
:class:`~mftik.clock.FakeClock`.

A new incarnation's ``/proc`` fence (F36) is inside :meth:`spawn`, after
the per-id reservation. The previous worker pid and its ``/proc/<pid>/stat``
start time are recorded in ``supervisor.json``. ``spawn`` refuses while
that process is still alive, including over ``LOST``. A socket that still
answers ``status`` for this id, with no exit, is the same refusal when
neither the slot nor the file has a live pid. The spec is written, phase
``STARTING`` and pids null, before the shim is spawned.

``rss_bytes`` is the Pss of the worker's process tree, measured when a
report is built, not on the status poll. It is not the shim and not
plain ``VmRSS`` (§4.7). The shim's frame leaves the field ``None``.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import socket
import struct
import tempfile
import time
from dataclasses import dataclass, replace
from enum import StrEnum
from pathlib import Path
from typing import Any

from mftik.clock import Clock, SystemClock
from mftik.instance import validate_instance_name
from mftik.procman.decisions import (
    AdmissionBudget,
    AdmissionReason,
    AdmissionWorker,
    FailureCause,
    ObservedWorker,
    RestartDecision,
    classify_failure,
    decide_admission,
    previous_worker_gone,
)
from mftik.procman.errors import (
    CapacityExceeded,
    InvalidWorkerId,
    MessageError,
    ProcmanError,
)
from mftik.procman.messages import (
    ExitRecord,
    ShimStatus,
    decode_exit,
    dump_frame,
    exit_record_path,
    load_frame,
    run_dir,
    socket_path,
    supervisor_state_path,
)
from mftik.procman.shim import ShimClient, SpawnedShim, spawn_shim
from mftik.procman.spec import PLANES, Plane, WorkerSpec, validate_worker_id
from mftik.procman.state import ALIVE_PHASES, Trigger, WorkerPhase, transition
from mftik.protocol.v2 import ProcmanReport, ProcmanWorker

#: How often a live slot's socket is polled.
#:
#: ``watch`` does not push each beat, so the heartbeat counter, the start
#: deadline and the stop grace are all noticed here. 50 ms is one short
#: unix-socket round trip and a small fraction of the deadlines this layer
#: sees (a start timeout is seconds; F11's backoff floor is 1 s). The
#: counter deadline can therefore land up to one poll late: the supervisor
#: timestamps a new beat when it first observes the new count.
STATUS_POLL_S = 0.05

#: How long to wait for a shim to leave its socket after ``release``.
_RETIRE_WAIT_S = 2.0

#: Phases a later :meth:`Supervisor.spawn` may replace without the F36
#: check deciding it. ``LOST`` is not here: nothing in the table leaves
#: it. :meth:`Supervisor.spawn` still replaces a ``LOST`` slot, through
#: the same ``STOPPED`` + ``SPAWN`` entry, once :func:`previous_worker_gone`
#: says the old pid is gone. Alive phases are not here either.
_REPLACEABLE: frozenset[WorkerPhase] = frozenset(
    {
        WorkerPhase.FAILED,
        WorkerPhase.CRASHED,
        WorkerPhase.BACKOFF,
        WorkerPhase.FATAL,
        WorkerPhase.STOPPED,
    }
)


class _InFlight(set[str]):
    """Ids :meth:`Supervisor.spawn` has reserved, and the kind of each.

    A ``set``, so the reservation check stays ``id in self._spawning``.
    ``kinds`` is how admission prices an in-flight spawn before a report
    has stored a Pss. :meth:`discard` and :meth:`clear` drop the kind too,
    including when a launch fails after the id was reserved.
    """

    def __init__(self) -> None:
        super().__init__()
        self.kinds: dict[str, str] = {}

    def note(self, worker_id: str, kind: str) -> None:
        self.kinds[worker_id] = kind

    def discard(self, element: str) -> None:
        super().discard(element)
        self.kinds.pop(element, None)

    def clear(self) -> None:
        super().clear()
        self.kinds.clear()


class CloseMode(StrEnum):
    """Argument of :meth:`Supervisor.close` (§4.4).

    ``DETACH`` stops accepting control RPCs, signals no worker, flushes
    status and exits 0. Workers keep running. ``STOP`` is the whole host
    going offline: every worker is signalled, reaped and released.
    """

    DETACH = "detach"
    STOP = "stop"


class _ReportGate(StrEnum):
    """Whether :meth:`Supervisor.report` may return a set.

    ``PENDING`` until :meth:`Supervisor.allow_reports`, which B3-03's
    ``start`` calls after reconciliation. ``CLOSED`` after
    :meth:`Supervisor.pause_reports`, which ``close`` calls. Both refuse.
    An absent report is not an observation (P5, F32).
    """

    PENDING = "pending"
    OPEN = "open"
    CLOSED = "closed"


@dataclass(frozen=True)
class WorkerStatus:
    """The supervisor's view of one worker: the spec plus the phase it chose.

    ``pid``, ``ready``, ``exit_code`` and ``signal`` are the shim's facts,
    copied onto the slot. ``phase`` is the state machine. ``exit_code`` and
    ``signal`` follow the exit record: both empty while the worker is
    alive, exactly one set after it has been reaped. ``rss_bytes`` is the
    Pss of the worker's process tree from the last :meth:`Supervisor.report`,
    or ``None`` when that has not run, the phase is not live, or the
    worker's ``smaps_rollup`` could not be read. Status polls do not
    measure it.
    """

    spec: WorkerSpec
    phase: WorkerPhase
    pid: int | None
    ready: bool
    exit_code: int | None
    signal: int | None
    rss_bytes: int | None


@dataclass(frozen=True)
class _Snapshot:
    """One slot, as :func:`_advance_worker` reads it. No socket and no clock."""

    phase: WorkerPhase
    ready: bool
    pid: int | None
    exit_code: int | None
    signal: int | None
    since_s: float
    beats: int
    beats_at_s: float | None
    start_timeout_s: float
    hb_timeout_s: float | None
    stop_grace_s: float
    term_sent: bool
    kill_sent: bool


@dataclass(frozen=True)
class _Step:
    """What one observation does to a :class:`_Snapshot`."""

    snapshot: _Snapshot
    send_signal: int | None = None
    release: bool = False
    shim_gone: bool = False


@dataclass
class _Slot:
    """A worker this supervisor still holds, including one in ``BACKOFF``."""

    spec: WorkerSpec
    phase: WorkerPhase
    ready: bool
    pid: int | None
    exit_code: int | None
    signal: int | None
    since_s: float
    beats: int
    beats_at_s: float | None
    term_sent: bool
    kill_sent: bool
    released: bool
    shim_gone: bool
    shim_pid: int
    socket: Path
    #: Pss from the last report, for a live phase. Not read on the poll.
    rss_bytes: int | None = None
    worker_start_ticks: int | None = None
    shim_start_ticks: int | None = None
    stopped: asyncio.Future[None] | None = None

    def snapshot(self) -> _Snapshot:
        return _Snapshot(
            phase=self.phase,
            ready=self.ready,
            pid=self.pid,
            exit_code=self.exit_code,
            signal=self.signal,
            since_s=self.since_s,
            beats=self.beats,
            beats_at_s=self.beats_at_s,
            start_timeout_s=self.spec.start_timeout_s,
            hb_timeout_s=self.spec.hb_timeout_s,
            stop_grace_s=self.spec.stop_grace_s,
            term_sent=self.term_sent,
            kill_sent=self.kill_sent,
        )

    def apply(self, step: _Step) -> None:
        snap = step.snapshot
        self.phase = snap.phase
        self.ready = snap.ready
        self.pid = snap.pid
        self.exit_code = snap.exit_code
        self.signal = snap.signal
        self.since_s = snap.since_s
        self.beats = snap.beats
        self.beats_at_s = snap.beats_at_s
        self.term_sent = snap.term_sent
        self.kill_sent = snap.kill_sent
        if step.shim_gone:
            self.shim_gone = True


def _quiet(snap: _Snapshot) -> _Step:
    return _Step(snap)


def _moved(
    snap: _Snapshot, phase: WorkerPhase, *, now_s: float, **rest: object
) -> _Snapshot:
    since = now_s if phase is not snap.phase else snap.since_s
    return replace(snap, phase=phase, since_s=since, **rest)


def _failure(
    snap: _Snapshot,
    *,
    phase: WorkerPhase,
    ready: bool,
    cause: FailureCause,
    now_s: float,
    kill: bool,
) -> _Step:
    """Take the table's edge, which is the same phase :func:`classify_failure` names."""
    trigger = {
        FailureCause.DEATH: Trigger.DEATH,
        FailureCause.START_TIMEOUT: Trigger.START_TIMEOUT,
        FailureCause.HEARTBEAT_TIMEOUT: Trigger.HEARTBEAT_TIMEOUT,
    }[cause]
    nxt = transition(phase, trigger)
    classified = classify_failure(ready=ready, cause=cause)
    if nxt is not classified:
        raise ProcmanError(
            f"transition {phase} on {trigger} is {nxt}, "
            f"classify_failure is {classified}"
        )
    updated = _moved(snap, nxt, now_s=now_s, kill_sent=snap.kill_sent or kill)
    return _Step(updated, send_signal=signal.SIGKILL if kill else None)


def _on_exit(snap: _Snapshot, phase: WorkerPhase, *, now_s: float) -> _Step:
    """The worker has been reaped. ``phase`` is where it was when it died.

    A stop we asked for lands on ``STOPPED`` and releases the shim. Any
    other death is classified and the shim stays, so the exit record is
    still there when the orchestrator decides.
    """
    if phase is WorkerPhase.STOPPING:
        nxt = transition(phase, Trigger.EXITED)
        return _Step(_moved(snap, nxt, now_s=now_s), release=True)
    if phase is WorkerPhase.STARTING:
        return _failure(
            snap,
            phase=phase,
            ready=False,
            cause=FailureCause.DEATH,
            now_s=now_s,
            kill=False,
        )
    if phase is WorkerPhase.RUNNING:
        return _failure(
            snap,
            phase=phase,
            ready=True,
            cause=FailureCause.DEATH,
            now_s=now_s,
            kill=False,
        )
    return _quiet(snap)


def _stopping(snap: _Snapshot, *, now_s: float) -> _Step:
    """``SIGTERM`` once, then ``SIGKILL`` once the grace has elapsed.

    One step sends one signal. The first observation sends ``SIGTERM``.
    A later observation at or after ``stop_grace_s`` sends ``SIGKILL``,
    and keeps sending it while the worker is still alive.
    """
    if not snap.term_sent:
        return _Step(replace(snap, term_sent=True), send_signal=signal.SIGTERM)
    if now_s - snap.since_s >= snap.stop_grace_s:
        return _Step(replace(snap, kill_sent=True), send_signal=signal.SIGKILL)
    return _quiet(snap)


def _advance_status(snap: _Snapshot, status: ShimStatus, *, now_s: float) -> _Step:
    alive = status.exit_code is None and status.signal is None
    base = replace(
        snap,
        ready=status.ready,
        pid=status.pid,
        beats=status.beats,
        exit_code=status.exit_code,
        signal=status.signal,
    )
    # A beat that reports ready and an exit in the same snapshot still
    # counts as ready: the death is ``CRASHED``, not ``FAILED``.
    phase = snap.phase
    if phase is WorkerPhase.STARTING and status.ready:
        phase = transition(phase, Trigger.READY)
        base = _moved(base, phase, now_s=now_s, beats_at_s=now_s)
    if not alive:
        return _on_exit(base, phase, now_s=now_s)
    if phase is WorkerPhase.STARTING and now_s - snap.since_s >= snap.start_timeout_s:
        return _failure(
            base,
            phase=phase,
            ready=False,
            cause=FailureCause.START_TIMEOUT,
            now_s=now_s,
            kill=True,
        )
    if phase is WorkerPhase.RUNNING and snap.hb_timeout_s is not None:
        beats_at = base.beats_at_s
        if status.beats != snap.beats or beats_at is None:
            return _quiet(replace(base, beats_at_s=now_s))
        if now_s - beats_at >= snap.hb_timeout_s:
            return _failure(
                base,
                phase=phase,
                ready=True,
                cause=FailureCause.HEARTBEAT_TIMEOUT,
                now_s=now_s,
                kill=True,
            )
    if phase is WorkerPhase.STOPPING:
        return _stopping(base, now_s=now_s)
    if phase in (WorkerPhase.FAILED, WorkerPhase.CRASHED):
        # The timeout kill has not landed yet. Ask again.
        return _Step(base, send_signal=signal.SIGKILL)
    return _quiet(base)


def _advance_unreachable(
    snap: _Snapshot,
    exit_record: ExitRecord | None,
    *,
    now_s: float,
) -> _Step:
    """The socket refused. No exit file is ``LOST``. A file is the death it recorded.

    Nothing synthesises an exit code. A phase the table cannot leave
    (``FAILED``, ``CRASHED``, and the rest) stays where it is.
    """
    if snap.phase not in ALIVE_PHASES:
        return _Step(snap, shim_gone=True)
    if exit_record is None:
        nxt = transition(snap.phase, Trigger.SHIM_LOST)
        return _Step(_moved(snap, nxt, now_s=now_s), shim_gone=True)
    base = replace(
        snap,
        ready=exit_record.ready,
        pid=exit_record.pid,
        exit_code=exit_record.exit_code,
        signal=exit_record.signal,
    )
    phase = snap.phase
    if phase is WorkerPhase.STARTING and exit_record.ready:
        phase = transition(phase, Trigger.READY)
        base = _moved(base, phase, now_s=now_s)
    step = _on_exit(base, phase, now_s=now_s)
    return _Step(
        step.snapshot,
        send_signal=step.send_signal,
        release=step.release,
        shim_gone=True,
    )


def _advance_worker(
    snap: _Snapshot,
    status: ShimStatus | None,
    exit_record: ExitRecord | None,
    *,
    now_s: float,
) -> _Step:
    """One observation, given ``now_s`` from the supervisor's clock.

    ``status is None`` means the socket refused. ``exit_record`` is only
    read in that case. The function does not sleep and does not touch a
    process.
    """
    if status is None:
        return _advance_unreachable(snap, exit_record, now_s=now_s)
    return _advance_status(snap, status, now_s=now_s)


def _apply_recorded_restart(
    phase: WorkerPhase, decision: RestartDecision
) -> WorkerPhase:
    """The edge :meth:`Supervisor.record_restart` writes for an orchestrator.

    ``BACKOFF`` and ``FATAL`` are the edges out of ``CRASHED``. ``FAILED``
    and a ``CRASHED`` the policy does not restart stay put. This does not
    wait ``delay_s`` and does not spawn.
    """
    if not isinstance(decision, RestartDecision):
        raise TypeError("decision must be a RestartDecision")
    try:
        target = WorkerPhase(decision.phase)
    except ValueError as exc:
        raise ProcmanError(f"unknown phase {decision.phase!r}") from exc
    if target is WorkerPhase.BACKOFF:
        _require_delay(decision.delay_s)
        return transition(phase, Trigger.RESTART)
    if target is WorkerPhase.FATAL:
        _require_no_delay(decision.delay_s, target)
        return transition(phase, Trigger.INTENSITY_EXCEEDED)
    if target in (WorkerPhase.FAILED, WorkerPhase.CRASHED) and target is phase:
        _require_no_delay(decision.delay_s, target)
        return phase
    raise ProcmanError(f"cannot record {target} from {phase}")


def _require_delay(delay_s: float | None) -> None:
    if (
        delay_s is None
        or type(delay_s) is bool
        or not isinstance(delay_s, int | float)
        or delay_s < 0
    ):
        raise ProcmanError("BACKOFF requires a delay >= 0")


def _require_no_delay(delay_s: float | None, phase: WorkerPhase) -> None:
    if delay_s is not None:
        raise ProcmanError(f"{phase} has no delay")


def _needs_poll(slot: _Slot) -> bool:
    if slot.released or slot.shim_gone:
        return False
    if slot.phase in ALIVE_PHASES:
        return True
    # A timeout kill is in flight until the shim reports the exit.
    return (
        slot.phase in (WorkerPhase.FAILED, WorkerPhase.CRASHED)
        and slot.exit_code is None
        and slot.signal is None
    )


def _pid_alive(pid: int) -> bool:
    """True while ``pid`` is a process that can still hold a socket.

    A zombie is not alive for this purpose: the shim unlinks its socket
    before it exits, and init may not have reaped the zombie yet.
    """
    if pid <= 1:
        return False
    stat = Path(f"/proc/{pid}/stat")
    try:
        text = stat.read_text()
    except OSError:
        return False
    try:
        state = text.rsplit(")", 1)[1].split()[0]
    except (IndexError, ValueError):
        return True
    return state != "Z"


def _read_pid_list(path: Path) -> list[int]:
    try:
        text = path.read_text()
    except OSError:
        return []
    found: list[int] = []
    for part in text.split():
        try:
            found.append(int(part))
        except ValueError:
            continue
    return found


def _proc_children(pid: int) -> list[int]:
    """Children of the main thread, on the host ``/proc``.

    The kill path uses this. A report walks every thread; see
    :func:`_thread_children`.
    """
    return _read_pid_list(Path(f"/proc/{pid}/task/{pid}/children"))


def _thread_children(pid: int, proc_root: Path) -> list[int]:
    """Children of every thread: ``proc_root/<pid>/task/*/children``.

    A pid listed by two threads is returned once. A missing ``task``
    directory means the process is gone; the caller skips it.
    """
    task = proc_root / str(pid) / "task"
    try:
        names = sorted(entry.name for entry in task.iterdir() if entry.is_dir())
    except OSError:
        return []
    seen: set[int] = set()
    found: list[int] = []
    for name in names:
        for child in _read_pid_list(task / name / "children"):
            if child in seen:
                continue
            seen.add(child)
            found.append(child)
    return found


def _pss_bytes(pid: int, proc_root: Path) -> int | None:
    """``Pss:`` from ``smaps_rollup``, in bytes. ``None`` if unreadable.

    Only the ``Pss:`` key counts. ``Pss_Anon`` and the other breakdowns
    are the same memory again. The kernel reports kibibytes.
    """
    path = proc_root / str(pid) / "smaps_rollup"
    try:
        text = path.read_text()
    except OSError:
        return None
    total = 0
    found = False
    for line in text.splitlines():
        if not line.startswith("Pss:"):
            continue
        parts = line.split()
        if len(parts) < 3 or parts[2] != "kB":
            continue
        try:
            kb = int(parts[1])
        except ValueError:
            continue
        if kb < 0:
            continue
        total += kb * 1024
        found = True
    if not found:
        return None
    return total


def _tree_pss_bytes(pid: int | None, proc_root: Path) -> int | None:
    """Pss of ``pid`` and its descendants, in bytes.

    ``pid`` is the worker, not the shim. ``None`` when ``pid`` is missing
    or the worker's own ``smaps_rollup`` cannot be read (missing file or
    permission): a partial sum of the children is not a measurement. A
    descendant that exits during the walk is skipped.
    """
    if pid is None or pid <= 0:
        return None
    own = _pss_bytes(pid, proc_root)
    if own is None:
        return None
    total = own
    seen = {pid}
    stack = [pid]
    while stack:
        current = stack.pop()
        for child in _thread_children(current, proc_root):
            if child in seen or child <= 0:
                continue
            seen.add(child)
            child_pss = _pss_bytes(child, proc_root)
            if child_pss is None:
                continue
            total += child_pss
            stack.append(child)
    return total


def _measure_pss(
    live: list[tuple[_Slot, int | None]], proc_root: Path
) -> list[tuple[_Slot, int | None]]:
    """Pss for each live slot. Runs off the event loop."""
    return [(slot, _tree_pss_bytes(pid, proc_root)) for slot, pid in live]


def _kill(pid: int) -> None:
    if pid <= 1:
        return
    try:
        os.kill(pid, signal.SIGKILL)
    except OSError:
        pass


def _kill_tree(pid: int) -> None:
    """``SIGKILL`` ``pid`` and the children ``/proc`` lists under it.

    Does not walk up to the parent. The shim's parent is init or the
    host subreaper.
    """
    if pid <= 1:
        return
    for child in _proc_children(pid):
        _kill_tree(child)
    _kill(pid)


def _proc_start_ticks(pid: int) -> int | None:
    """Field 22 of ``/proc/<pid>/stat``: start time in clock ticks since boot.

    ``None`` when the pid is gone, including a zombie: a zombie is not a
    running worker, matching :func:`_pid_alive`. ``comm`` is in parentheses
    and may contain spaces, so the fields start after the last ``)``.
    """
    if not _pid_alive(pid):
        return None
    try:
        text = Path(f"/proc/{pid}/stat").read_text()
        return int(text.rsplit(")", 1)[1].split()[19])
    except (OSError, IndexError, ValueError):
        return None


def _same_process(pid: int | None, start_ticks: int | None) -> bool:
    """True when ``pid`` is still the process ``start_ticks`` recorded (F36)."""
    if pid is None or pid <= 1:
        return False
    return not previous_worker_gone(
        recorded_start_ticks=start_ticks,
        live_start_ticks=_proc_start_ticks(pid),
    )


def _blocking_pid(pid: int | None, start_ticks: int | None) -> int | None:
    """The pid that still blocks a new incarnation, or ``None`` when it is gone."""
    if _same_process(pid, start_ticks):
        return pid
    return None


def _signal_host_pid(pid: int, sig: int, start_ticks: int | None) -> None:
    """Signal a worker by host pid when its shim is already gone (§4.6, F36).

    The worker is its own process group. A pid that is no longer that
    process is not signalled.
    """
    if _blocking_pid(pid, start_ticks) is None:
        return
    try:
        os.killpg(pid, sig)
    except OSError:
        try:
            os.kill(pid, sig)
        except OSError:
            pass


def _send_signal(slot: _Slot, sig: int) -> None:
    try:
        ShimClient(slot.socket).signal(sig)
    except OSError:
        pass


def _release_socket(slot: _Slot) -> None:
    try:
        ShimClient(slot.socket).release()
    except OSError:
        # The socket is already gone. Kill the shim itself if it is still
        # that process, after its children, and not its parent. A reused
        # pid is left alone (F36).
        if _same_process(slot.shim_pid, slot.shim_start_ticks):
            _kill_tree(slot.shim_pid)


def _read_exit(work_dir: Path, worker_id: str) -> ExitRecord | None:
    path = exit_record_path(work_dir, worker_id)
    try:
        payload = path.read_bytes()
    except OSError:
        return None
    try:
        return decode_exit(payload)
    except MessageError:
        return None


def _socket_listening(path: Path) -> bool:
    """True when a connect to ``path`` is accepted.

    A missing path is not listening. The probe does not speak the
    protocol: it only asks whether the shim is still there.
    """
    if not path.exists() and not path.is_symlink():
        return False
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        probe.settimeout(0.2)
        probe.connect(os.fspath(path))
    except OSError:
        return False
    else:
        return True
    finally:
        probe.close()


def _unlink_quiet(path: Path) -> None:
    """Remove a shim socket, including the short symlink target (S5)."""
    try:
        if path.is_symlink():
            target = Path(os.readlink(path))
            path.unlink(missing_ok=True)
            target.unlink(missing_ok=True)
            try:
                target.parent.rmdir()
            except OSError:
                pass
            return
        path.unlink(missing_ok=True)
    except OSError:
        pass


def _wait_retired(slot: _Slot) -> None:
    """``release`` the shim and wait until its socket stops accepting.

    The next incarnation binds the same path. On the happy path the shim
    unlinks that path as it exits. If it is still accepting when the wait
    ends, kill that shim — its children first, never its parent — and
    unlink the path. The kill is skipped when the pid has been reused
    (F36). The worker pid's fence is :meth:`Supervisor.spawn`, not here.
    """
    _release_socket(slot)
    deadline = time.monotonic() + _RETIRE_WAIT_S
    while time.monotonic() < deadline:
        if not _socket_listening(slot.socket):
            break
        time.sleep(0.01)
    else:
        if _same_process(slot.shim_pid, slot.shim_start_ticks):
            _kill_tree(slot.shim_pid)
    _unlink_quiet(slot.socket)


_STATE_VERSION = 1

_STATE_KEYS = frozenset({"version", "workers"})

_RECORD_KEYS = frozenset(
    {
        "spec",
        "phase",
        "worker_pid",
        "worker_start_ticks",
        "shim_pid",
        "shim_start_ticks",
        "since_s",
    }
)

_SPEC_KEYS = frozenset(
    {
        "id",
        "plane",
        "kind",
        "incarnation",
        "argv",
        "env",
        "code_ref",
        "restart",
        "start_timeout_s",
        "hb_timeout_s",
        "oom_score_adj",
        "rlimit_data_bytes",
        "stop_grace_s",
        "labels",
    }
)


@dataclass(frozen=True)
class SupervisorRecord:
    """One worker row in ``supervisor.json`` (B3-03).

    Local state, not the database. ``spec`` is what was spawned, including
    ``id``, ``incarnation``, ``code_ref`` and ``labels`` — the fields
    ``mftik workers --stale`` will read (B3-07). ``worker_pid`` and
    ``shim_pid`` are paired with the ``/proc/<pid>/stat`` start times so a
    later spawn can tell the process from a reused pid (F36). ``phase`` is
    the state machine's phase at the last write. ``since_s`` is the
    supervisor clock's monotonic instant when that phase began.
    """

    spec: WorkerSpec
    phase: WorkerPhase
    worker_pid: int | None
    worker_start_ticks: int | None
    shim_pid: int | None
    shim_start_ticks: int | None
    since_s: float


@dataclass(frozen=True)
class ReattachObservation:
    """One worker id, as :meth:`Supervisor.start` found it (§4.4).

    Ready for :func:`~mftik.procman.reattach_action`. ``observed`` is the
    disk fact. ``spec`` and ``incarnation`` come from ``supervisor.json``;
    both are ``None`` when the id was only a socket. ``status`` is the
    shim's reply when the socket answered. ``exit_record`` is the file
    when the socket did not answer and the file was there. ``start`` does
    not choose a :class:`~mftik.procman.DesiredSlot`.
    """

    id: str
    observed: ObservedWorker
    spec: WorkerSpec | None
    incarnation: int | None
    status: ShimStatus | None
    exit_record: ExitRecord | None


def _spec_payload(spec: WorkerSpec) -> dict[str, Any]:
    return {
        "id": spec.id,
        "plane": spec.plane,
        "kind": spec.kind,
        "incarnation": spec.incarnation,
        "argv": list(spec.argv),
        "env": dict(spec.env),
        "code_ref": spec.code_ref,
        "restart": spec.restart,
        "start_timeout_s": spec.start_timeout_s,
        "hb_timeout_s": spec.hb_timeout_s,
        "oom_score_adj": spec.oom_score_adj,
        "rlimit_data_bytes": spec.rlimit_data_bytes,
        "stop_grace_s": spec.stop_grace_s,
        "labels": dict(spec.labels),
    }


def _opt_int(value: object, name: str, *, minimum: int) -> int | None:
    if value is None:
        return None
    if type(value) is not int or value < minimum:
        raise MessageError(f"{name} must be an int >= {minimum} or null")
    return value


def _record_payload(record: SupervisorRecord) -> dict[str, Any]:
    if not isinstance(record, SupervisorRecord):
        raise MessageError("supervisor state row must be a SupervisorRecord")
    return {
        "spec": _spec_payload(record.spec),
        "phase": record.phase.value,
        "worker_pid": record.worker_pid,
        "worker_start_ticks": record.worker_start_ticks,
        "shim_pid": record.shim_pid,
        "shim_start_ticks": record.shim_start_ticks,
        "since_s": record.since_s,
    }


def encode_supervisor_state(
    records: tuple[SupervisorRecord, ...] | list[SupervisorRecord],
) -> bytes:
    """``supervisor.json``: one JSON object and a newline, keys sorted."""
    if isinstance(records, str) or not isinstance(records, tuple | list):
        raise MessageError("supervisor state must be a sequence of rows")
    seen: set[str] = set()
    rows: list[dict[str, Any]] = []
    for record in records:
        payload = _record_payload(record)
        worker_id = record.spec.id
        if worker_id in seen:
            raise MessageError(f"supervisor state lists {worker_id} twice")
        seen.add(worker_id)
        rows.append(payload)
    rows.sort(key=lambda row: row["spec"]["id"])
    return dump_frame({"version": _STATE_VERSION, "workers": rows})


def _decode_spec(payload: object) -> WorkerSpec:
    if not isinstance(payload, dict):
        raise MessageError("supervisor state spec must be an object")
    if set(payload) != _SPEC_KEYS:
        raise MessageError(
            f"supervisor state spec keys {sorted(payload)} != {sorted(_SPEC_KEYS)}"
        )
    try:
        return WorkerSpec(
            id=payload["id"],
            plane=payload["plane"],
            kind=payload["kind"],
            incarnation=payload["incarnation"],
            argv=payload["argv"],
            env=payload["env"],
            code_ref=payload["code_ref"],
            restart=payload["restart"],
            start_timeout_s=payload["start_timeout_s"],
            hb_timeout_s=payload["hb_timeout_s"],
            oom_score_adj=payload["oom_score_adj"],
            rlimit_data_bytes=payload["rlimit_data_bytes"],
            stop_grace_s=payload["stop_grace_s"],
            labels=payload["labels"],
        )
    except (ProcmanError, TypeError, ValueError) as exc:
        raise MessageError(f"supervisor state spec is not a WorkerSpec: {exc}") from exc


def _decode_record(payload: object) -> SupervisorRecord:
    if not isinstance(payload, dict):
        raise MessageError("supervisor state row must be an object")
    if set(payload) != _RECORD_KEYS:
        raise MessageError(
            f"supervisor state row keys {sorted(payload)} != {sorted(_RECORD_KEYS)}"
        )
    spec = _decode_spec(payload["spec"])
    try:
        phase = WorkerPhase(payload["phase"])
    except ValueError as exc:
        raise MessageError(f"unknown phase {payload['phase']!r}") from exc
    since = payload["since_s"]
    if type(since) is bool or not isinstance(since, int | float):
        raise MessageError("since_s must be a number")
    return SupervisorRecord(
        spec=spec,
        phase=phase,
        worker_pid=_opt_int(payload["worker_pid"], "worker_pid", minimum=2),
        worker_start_ticks=_opt_int(
            payload["worker_start_ticks"], "worker_start_ticks", minimum=0
        ),
        shim_pid=_opt_int(payload["shim_pid"], "shim_pid", minimum=2),
        shim_start_ticks=_opt_int(
            payload["shim_start_ticks"], "shim_start_ticks", minimum=0
        ),
        since_s=float(since),
    )


def decode_supervisor_state(payload: bytes) -> tuple[SupervisorRecord, ...]:
    """The inverse of :func:`encode_supervisor_state`."""
    try:
        body = load_frame(payload)
    except MessageError:
        raise
    if set(body) != _STATE_KEYS:
        raise MessageError(
            f"supervisor state keys {sorted(body)} != {sorted(_STATE_KEYS)}"
        )
    if body["version"] != _STATE_VERSION:
        raise MessageError(f"supervisor state version {body['version']!r} is not 1")
    workers = body["workers"]
    if not isinstance(workers, list):
        raise MessageError("supervisor state workers must be a list")
    records = tuple(_decode_record(row) for row in workers)
    ids = [record.spec.id for record in records]
    if len(ids) != len(set(ids)):
        raise MessageError("supervisor state lists a worker id twice")
    return records


async def _await_blocking(func: Any, *args: Any) -> None:
    """Run ``func(*args)`` in a thread, and finish it even if we are cancelled.

    ``asyncio.to_thread`` does not stop the thread when the awaiting task
    is cancelled, and cancelling drops an ``async with`` lock at the
    ``await``. This holds the caller in the function until the thread
    returns, then re-raises :class:`asyncio.CancelledError`.
    """
    task = asyncio.ensure_future(asyncio.to_thread(func, *args))
    current = asyncio.current_task()
    if current is None:
        await task
        return
    try:
        await asyncio.shield(task)
    except asyncio.CancelledError:
        pending = 0
        while current.cancelling():
            current.uncancel()
            pending += 1
        try:
            await task
        finally:
            for _ in range(pending):
                current.cancel()
        raise


def _write_state(
    work_dir: Path, records: tuple[SupervisorRecord, ...] | list[SupervisorRecord]
) -> None:
    """Write ``supervisor.json`` via a temp file and ``rename``, like ``exit.json``.

    The temp name is unique. ``close`` can cancel the driver while a write
    is inside ``asyncio.to_thread``, and that thread keeps running after
    the task has been cancelled. Two writers must not share one temp file.
    """
    path = supervisor_state_path(work_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, raw = tempfile.mkstemp(
        prefix="supervisor.json.", suffix=".tmp", dir=path.parent
    )
    tmp = Path(raw)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(encode_supervisor_state(records))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def load_supervisor_state(work_dir: Path) -> tuple[SupervisorRecord, ...]:
    """Read ``supervisor.json``. A missing file is an empty tuple."""
    path = supervisor_state_path(work_dir)
    try:
        payload = path.read_bytes()
    except FileNotFoundError:
        return ()
    except OSError as exc:
        raise ProcmanError(f"cannot read {path}: {exc}") from exc
    try:
        return decode_supervisor_state(payload)
    except MessageError as exc:
        raise ProcmanError(f"cannot read {path}: {exc}") from exc


def _scan_sockets(work_dir: Path) -> dict[str, Path]:
    """``run/**/<id>.sock``, including a symlink left for a long path (S5)."""
    root = run_dir(work_dir)
    found: dict[str, Path] = {}
    if not root.is_dir():
        return found
    for path in root.rglob("*"):
        name = path.name
        if not name.endswith(".sock"):
            continue
        if not path.is_symlink() and not path.is_socket():
            continue
        worker_id = path.relative_to(root).as_posix()[: -len(".sock")]
        try:
            validate_worker_id(worker_id)
        except InvalidWorkerId:
            continue
        found[worker_id] = path
    return found


def _socket_peer_pid(path: Path) -> int | None:
    """Pid of the process that accepted on ``path``, or ``None``."""
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.2)
            sock.connect(os.fspath(path))
            size = struct.calcsize("iii")
            pid, _uid, _gid = struct.unpack(
                "iii",
                sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, size),
            )
    except OSError:
        return None
    return pid if pid > 1 else None


def _spawn_noted(spec: WorkerSpec, work_dir: Path) -> tuple[SpawnedShim, int | None]:
    spawned = spawn_shim(spec, work_dir=work_dir)
    return spawned, _proc_start_ticks(spawned.pid)


def _live_socket_pid(work_dir: Path, worker_id: str) -> int | None:
    """Pid from ``status`` when that socket still has a live worker.

    A crash before ``supervisor.json`` is written leaves the socket and
    no row. The id has to match, and both exit fields have to be empty:
    a shim that has already reaped the worker does not block the next
    incarnation.
    """
    try:
        status = ShimClient(socket_path(work_dir, worker_id)).status()
    except (OSError, MessageError):
        return None
    if status.id != worker_id:
        return None
    if status.exit_code is not None or status.signal is not None:
        return None
    if status.pid is None or status.pid <= 1:
        return None
    return status.pid


def _fence_pid(work_dir: Path, worker_id: str, retiring: _Slot | None) -> int | None:
    """The live worker pid that blocks ``worker_id``, or ``None``.

    The held slot wins. With no slot, ``supervisor.json`` is the record a
    supervisor that has not called :meth:`Supervisor.start` still has to
    honour. When neither has a live pid, the socket's ``status`` is the
    last fence: a worker can be up before its pid is recorded.
    """
    if retiring is not None:
        blocked = _blocking_pid(retiring.pid, retiring.worker_start_ticks)
        if blocked is not None:
            return blocked
    else:
        for record in load_supervisor_state(work_dir):
            if record.spec.id != worker_id:
                continue
            blocked = _blocking_pid(record.worker_pid, record.worker_start_ticks)
            if blocked is not None:
                return blocked
            break
    return _live_socket_pid(work_dir, worker_id)


def _merge_spawn_intent(work_dir: Path, spec: WorkerSpec, since_s: float) -> None:
    """Record ``spec`` before the shim exists.

    A controller that dies after this write and before ``spawn_shim``
    still leaves the id, the full spec and phase ``STARTING``. Pids are
    null until the shim is observed. Other rows in the file stay.
    """
    previous = load_supervisor_state(work_dir)
    others = tuple(row for row in previous if row.spec.id != spec.id)
    intent = SupervisorRecord(
        spec=spec,
        phase=WorkerPhase.STARTING,
        worker_pid=None,
        worker_start_ticks=None,
        shim_pid=None,
        shim_start_ticks=None,
        since_s=since_s,
    )
    _write_state(work_dir, (*others, intent))


def _write_slots(
    work_dir: Path,
    records: tuple[SupervisorRecord, ...],
    keep: tuple[str, ...],
) -> None:
    """Write ``records``, retaining rows whose ids are still being spawned.

    ``keep`` is the ids :meth:`Supervisor.spawn` has reserved that are not
    in ``records`` yet. Their on-disk row is the spec written before
    ``spawn_shim``. A snapshot that omitted them must not delete it.
    """
    if keep:
        wanted = set(keep)
        retained = tuple(
            row
            for row in load_supervisor_state(work_dir)
            if row.spec.id in wanted
        )
        records = tuple(
            sorted((*records, *retained), key=lambda row: row.spec.id)
        )
    _write_state(work_dir, records)


def _record_from_slot(slot: _Slot) -> SupervisorRecord:
    return SupervisorRecord(
        spec=slot.spec,
        phase=slot.phase,
        worker_pid=slot.pid if slot.pid is not None and slot.pid > 1 else None,
        worker_start_ticks=slot.worker_start_ticks,
        shim_pid=slot.shim_pid if slot.shim_pid > 1 else None,
        shim_start_ticks=slot.shim_start_ticks,
        since_s=slot.since_s,
    )


def _slot_signature(slot: _Slot) -> tuple[object, ...]:
    return (
        slot.phase,
        slot.pid,
        slot.worker_start_ticks,
        slot.shim_pid,
        slot.shim_start_ticks,
        slot.exit_code,
        slot.signal,
        slot.ready,
        slot.since_s,
        slot.released,
        slot.spec.incarnation,
    )


def _shim_identity(
    record: SupervisorRecord | None, sock: Path | None
) -> tuple[int, int | None]:
    if record is not None and record.shim_pid is not None:
        if _same_process(record.shim_pid, record.shim_start_ticks):
            live = _proc_start_ticks(record.shim_pid)
            ticks = live if live is not None else record.shim_start_ticks
            return record.shim_pid, ticks
    if sock is not None:
        peer = _socket_peer_pid(sock)
        if peer is not None:
            return peer, _proc_start_ticks(peer)
    if record is not None and record.shim_pid is not None:
        return record.shim_pid, record.shim_start_ticks
    return 0, None


def _held_slot(
    *,
    spec: WorkerSpec,
    phase: WorkerPhase,
    ready: bool,
    pid: int | None,
    exit_code: int | None,
    sig: int | None,
    beats: int,
    since_s: float,
    shim_pid: int,
    shim_start_ticks: int | None,
    worker_start_ticks: int | None,
    sock: Path,
    shim_gone: bool,
) -> _Slot:
    return _Slot(
        spec=spec,
        phase=phase,
        ready=ready,
        pid=pid,
        exit_code=exit_code,
        signal=sig,
        since_s=since_s,
        beats=beats,
        beats_at_s=None,
        term_sent=False,
        kill_sent=False,
        released=False,
        shim_gone=shim_gone,
        shim_pid=shim_pid,
        socket=sock,
        worker_start_ticks=worker_start_ticks,
        shim_start_ticks=shim_start_ticks,
    )


def _probe_worker(
    work_dir: Path,
    worker_id: str,
    record: SupervisorRecord | None,
    sock: Path | None,
    *,
    now_s: float,
) -> tuple[ReattachObservation, _Slot | None]:
    """One id: socket ``status``, else ``exit.json``, else the recorded pid.

    Does not signal, release, spawn or write ``supervisor.json``.
    """
    status: ShimStatus | None = None
    exit_record: ExitRecord | None = None
    if sock is not None:
        try:
            status = ShimClient(sock).status()
        except OSError:
            status = None
    if status is None:
        exit_record = _read_exit(work_dir, worker_id)
    if status is not None and status.id != worker_id:
        raise ProcmanError(f"socket for {worker_id} reported {status.id}")
    if exit_record is not None and exit_record.id != worker_id:
        raise ProcmanError(
            f"exit record for {worker_id} names {exit_record.id}"
        )
    spec = record.spec if record is not None else None
    incarnation = spec.incarnation if spec is not None else None
    socket = sock if sock is not None else socket_path(work_dir, worker_id)
    if status is not None:
        alive = status.exit_code is None and status.signal is None
        observed = ObservedWorker.RUNNING if alive else ObservedWorker.EXITED
        slot = None
        if spec is not None:
            # The file's incarnation is what the observation reports. The
            # held slot follows the shim, so a mismatch can still be stopped
            # and still fences the live pid.
            held_spec = spec
            if spec.incarnation != status.incarnation:
                held_spec = replace(spec, incarnation=status.incarnation)
            shim_pid, shim_ticks = _shim_identity(record, sock)
            worker_ticks = (
                _proc_start_ticks(status.pid) if status.pid is not None else None
            )
            if alive:
                phase = WorkerPhase.RUNNING if status.ready else WorkerPhase.STARTING
                exit_code = None
                sig = None
                shim_gone = False
            else:
                phase = classify_failure(
                    ready=status.ready, cause=FailureCause.DEATH
                )
                exit_code = status.exit_code
                sig = status.signal
                shim_gone = False
            slot = _held_slot(
                spec=held_spec,
                phase=phase,
                ready=status.ready,
                pid=status.pid,
                exit_code=exit_code,
                sig=sig,
                beats=status.beats,
                since_s=now_s,
                shim_pid=shim_pid,
                shim_start_ticks=shim_ticks,
                worker_start_ticks=worker_ticks,
                sock=socket,
                shim_gone=shim_gone,
            )
        return (
            ReattachObservation(
                id=worker_id,
                observed=observed,
                spec=spec,
                incarnation=incarnation,
                status=status,
                exit_record=None,
            ),
            slot,
        )
    if exit_record is not None:
        slot = None
        if spec is not None:
            held_spec = spec
            if spec.incarnation != exit_record.incarnation:
                held_spec = replace(spec, incarnation=exit_record.incarnation)
            phase = classify_failure(ready=exit_record.ready, cause=FailureCause.DEATH)
            shim_pid, shim_ticks = _shim_identity(record, None)
            slot = _held_slot(
                spec=held_spec,
                phase=phase,
                ready=exit_record.ready,
                pid=exit_record.pid,
                exit_code=exit_record.exit_code,
                sig=exit_record.signal,
                beats=0,
                since_s=now_s,
                shim_pid=shim_pid,
                shim_start_ticks=shim_ticks,
                worker_start_ticks=record.worker_start_ticks if record else None,
                sock=socket,
                shim_gone=True,
            )
        return (
            ReattachObservation(
                id=worker_id,
                observed=ObservedWorker.EXITED,
                spec=spec,
                incarnation=incarnation,
                status=None,
                exit_record=exit_record,
            ),
            slot,
        )
    # Nothing answered and no exit file.
    pid_alive = False
    if record is not None:
        pid_alive = _same_process(record.worker_pid, record.worker_start_ticks)
    if sock is None and not pid_alive:
        return (
            ReattachObservation(
                id=worker_id,
                observed=ObservedWorker.ABSENT,
                spec=spec,
                incarnation=incarnation,
                status=None,
                exit_record=None,
            ),
            None,
        )
    slot = None
    if spec is not None:
        worker_ticks = record.worker_start_ticks if record is not None else None
        if record is not None and record.worker_pid is not None and pid_alive:
            worker_ticks = _proc_start_ticks(record.worker_pid)
        shim_pid, shim_ticks = _shim_identity(record, None)
        slot = _held_slot(
            spec=spec,
            phase=WorkerPhase.LOST,
            ready=False,
            pid=record.worker_pid if record is not None else None,
            exit_code=None,
            sig=None,
            beats=0,
            since_s=now_s,
            shim_pid=shim_pid,
            shim_start_ticks=shim_ticks,
            worker_start_ticks=worker_ticks,
            sock=socket,
            shim_gone=True,
        )
    return (
        ReattachObservation(
            id=worker_id,
            observed=ObservedWorker.LOST,
            spec=spec,
            incarnation=incarnation,
            status=None,
            exit_record=None,
        ),
        slot,
    )


class Supervisor:
    """One plane instance's supervisor.

    ``plane`` is ``sts``, ``md`` or ``td``. ``instance`` is one NATS subject
    segment, the same rule as :func:`mftik.instance.validate_instance_name`,
    because the report subject is ``procman.report.{plane}.{instance}``.

    ``clock`` supplies ``monotonic`` and ``sleep`` for the live deadlines.
    It defaults to the process clock. Restart intensity is not an argument:
    the orchestrator owns those numbers.

    ``proc_root`` is the ``/proc`` a report reads. Tests pass a fake tree.
    Production leaves it as the host ``/proc``, which ``oci_host_pid``
    bind-mounts (§4.5).

    ``budget`` is the orchestrator's :class:`~mftik.procman.AdmissionBudget`,
    or ``None`` for no limit. This class does not read the environment and
    does not invent ``max_workers`` or ``memory_budget_mb`` (§4.7).
    """

    def __init__(
        self,
        work_dir: Path,
        *,
        plane: Plane,
        instance: str,
        clock: Clock | None = None,
        proc_root: Path | None = None,
        budget: AdmissionBudget | None = None,
    ) -> None:
        if plane not in PLANES:
            raise ValueError(f"plane {plane!r} is not one of {', '.join(PLANES)}")
        if budget is not None and not isinstance(budget, AdmissionBudget):
            raise TypeError(
                "budget must be an AdmissionBudget or None; "
                "this layer does not choose the numbers"
            )
        self.work_dir = Path(work_dir)
        self.plane: Plane = plane
        self.instance = validate_instance_name(instance)
        self._clock: Clock = clock if clock is not None else SystemClock()
        self._proc_root = Path("/proc") if proc_root is None else Path(proc_root)
        self._budget = budget
        self._slots: dict[str, _Slot] = {}
        # Ids whose :meth:`spawn` has passed the slot check and not yet
        # published the new slot. Held across the unlocked launch.
        # ``kinds`` prices an in-flight spawn that has no measured Pss yet.
        self._spawning = _InFlight()
        self._lock = asyncio.Lock()
        self._persist_lock = asyncio.Lock()
        self._driver: asyncio.Task[None] | None = None
        self._failure: BaseException | None = None
        self._report_gate = _ReportGate.PENDING
        # Publications this process has produced. Not durable. close does
        # not reset it; a new process starts at zero.
        self._generation = 0
        self._closed = False
        self._booted = False

    async def start(self) -> tuple[ReattachObservation, ...]:
        """Load ``supervisor.json``, then reattach each socket under ``run/``.

        Returns one observation per worker id, for the orchestrator to pass
        to :func:`~mftik.procman.reattach_action` with the desired row it
        read from its own database. This method does not read the database,
        does not choose a desired row, and does not stop, spawn, release
        or signal (DECIDED 1).

        A worker that is still running is held, so the driver polls it
        again. Its heartbeat deadline starts at the first observation
        after this call: ``beats`` has no timestamp of its own, so the
        window cannot be reconstructed (the cost noted with B3-02). The
        start-timeout window is re-armed the same way, so reconciliation
        itself does not ``SIGKILL`` a worker that was already ``STARTING``.
        Control subjects stay dark until this returns; serving them is the
        controller's. Workers keep running throughout (P1).

        The last call is :meth:`allow_reports`, unless :meth:`close` has
        already paused publication. Until reports are open, :meth:`report`
        refuses, so a rolling controller does not publish a partial set.
        ``allow_reports`` does nothing once the gate is closed, so a close
        during this scan cannot reopen them. ``_generation`` is not reset.
        """
        self._check_failure()
        self._check_open()
        async with self._lock:
            if self._booted or self._slots or self._spawning:
                raise ProcmanError("supervisor has already started")
            self._booted = True
        records = {
            record.spec.id: record
            for record in await asyncio.to_thread(load_supervisor_state, self.work_dir)
        }
        sockets = await asyncio.to_thread(_scan_sockets, self.work_dir)
        now = self._clock.monotonic()
        observations: list[ReattachObservation] = []
        for worker_id in sorted(set(records) | set(sockets)):
            observed, slot = await asyncio.to_thread(
                _probe_worker,
                self.work_dir,
                worker_id,
                records.get(worker_id),
                sockets.get(worker_id),
                now_s=now,
            )
            observations.append(observed)
            if slot is None:
                continue
            async with self._lock:
                self._slots[worker_id] = slot
                if _needs_poll(slot):
                    self._ensure_driver_locked()
        found = tuple(observations)
        # ``close`` sets the gate to closed before ``_closed``. Skip when
        # this scan already lost the race, and let ``allow_reports`` ignore
        # a gate that closed between the check and the call.
        if not self._closed:
            self.allow_reports()
        return found

    async def close(self, mode: CloseMode) -> None:
        """``detach`` leaves workers running; ``stop`` ends them (§4.4).

        Publication stops first (:meth:`pause_reports`), before detach or
        stop. An unknown mode is refused before that, so it does not pause.
        Both modes cancel the observation task and wait for it, so nothing
        the supervisor started outlives this call. ``detach`` signals no
        worker and leaves each held slot in ``supervisor.json``. ``stop``
        ends every held worker — ``SIGTERM``, then ``SIGKILL`` after
        ``stop_grace_s``, the same path as :meth:`stop` — and then releases
        the shim. A second call pauses again and does nothing else.
        ``_generation`` is left as it is.
        """
        named = CloseMode(mode)
        self.pause_reports()
        if self._closed:
            return
        try:
            if named is CloseMode.STOP:
                await self._stop_held_workers()
        finally:
            await self._cancel_driver()
            await self._persist()

    async def release_slot(self, worker_id: str) -> None:
        """Release the shim and drop a held terminal slot.

        The orchestrator calls this for :attr:`ReattachAction.MARK_FAILED`:
        the exit is already on the observation and nothing is spawned.
        :attr:`ReattachAction.NONE` uses it too when a shim is still
        waiting after the worker has exited (S3); the table's ``NONE`` does
        not spawn or stop, and the shim still has to be released or it
        waits. A live phase is refused; that is :meth:`stop`. A ``LOST``
        slot whose worker pid is still that process is refused as well:
        dropping it would forget the fence, and killing the pid is the
        orchestrator's call.
        """
        self._check_failure()
        self._check_open()
        validate_worker_id(worker_id)
        async with self._lock:
            slot = self._slots.get(worker_id)
            if slot is None:
                raise ProcmanError(f"supervisor does not hold {worker_id}")
            if slot.phase in ALIVE_PHASES:
                raise ProcmanError(
                    f"cannot release {worker_id} from {slot.phase}"
                )
            if slot.phase is WorkerPhase.LOST and _blocking_pid(
                slot.pid, slot.worker_start_ticks
            ):
                raise ProcmanError(
                    f"{worker_id} worker pid {slot.pid} is still alive"
                )
            already = slot.released
            slot.released = True
            self._slots.pop(worker_id, None)
        if not already:
            await asyncio.to_thread(_wait_retired, slot)
        await self._persist()

    async def spawn(self, spec: WorkerSpec) -> None:
        """Spawn ``spec`` on this supervisor's plane.

        Admission (B3-05, §4.7) runs under this lock before the id is
        reserved, before :func:`_fence_pid`, and before
        :meth:`_write_spawn_intent`. It sits outside the try that
        releases the reservation, so a refusal does not reserve the id,
        does not retire a held slot, and does not leave a
        ``supervisor.json`` row. Only an id this supervisor does not
        hold is subject to it. Replacing a held slot — a restart after
        :meth:`record_restart`, or a new incarnation over ``FAILED``,
        ``CRASHED``, ``BACKOFF``, ``FATAL``, ``STOPPED`` or ``LOST`` —
        is not refused, and that slot is not counted twice. Refusing a
        restart would turn an MD or TD crash into an outage. The count
        is the current ``_slots`` plus ids in ``_spawning``.
        :meth:`release_slot` and ``close(stop)`` remove slots, so a
        dropped worker leaves the count with them. With no budget, or
        with both limits ``None``, nothing is refused. Exceeding
        ``max_workers`` or ``memory_budget_mb`` raises
        :class:`~mftik.procman.CapacityExceeded` (``capacity_exceeded``).
        A counted kind with no estimate raises
        :class:`~mftik.procman.ProcmanError` and is not
        ``capacity_exceeded``. Either refusal launches nothing: no shim,
        no socket, and the id is not left in ``_spawning``. The check
        uses the last reported Pss and the orchestrator's per-kind
        estimate. It does not walk ``/proc``.

        The spec's plane has to be this supervisor's plane. Replacing a
        held slot releases that incarnation's shim first, because the
        socket path is per worker id. A slot in ``BACKOFF`` enters
        ``STARTING`` through :attr:`~mftik.procman.Trigger.BACKOFF_ELAPSED`.
        Any other held terminal phase, including ``LOST`` once the old
        worker pid is gone, is a new incarnation: the old phase has no
        edge to ``STARTING``, so the new one enters through
        :attr:`~mftik.procman.Trigger.SPAWN`. A live phase is refused.
        ``LOST`` is refused too while that worker pid is still the recorded
        process, and the error names the pid (F36). So is a second call
        for the same id while this one has not returned: the id is reserved
        before the ``/proc`` check, and the check runs under that
        reservation.

        The check is the recorded ``(pid, start time)`` from the held slot,
        or from ``supervisor.json`` when this supervisor has not held the
        id. When neither has a live pid, the socket's ``status`` is the
        last fence: a reply whose ``id`` matches, and whose ``exit_code``
        and ``signal`` are both empty, refuses with that ``pid``. Before
        ``spawn_shim``, the full spec is written with phase ``STARTING``
        and null pids, so a crash in between still leaves a row
        :meth:`start` can hold. ``oci_host_pid`` is what makes host
        ``/proc`` that pid (§4.5).
        """
        self._check_failure()
        self._check_open()
        if spec.plane != self.plane:
            raise ValueError(
                f"spec plane {spec.plane!r} does not match "
                f"supervisor plane {self.plane!r}"
            )
        retiring: _Slot | None
        must_retire = False
        phase = WorkerPhase.STARTING
        async with self._lock:
            if spec.id in self._spawning:
                raise ProcmanError(f"{spec.id} is already being spawned")
            retiring = self._slots.get(spec.id)
            if retiring is not None:
                if (
                    retiring.phase not in _REPLACEABLE
                    and retiring.phase is not WorkerPhase.LOST
                ):
                    raise ProcmanError(
                        f"{spec.id} is {retiring.phase}; "
                        "the supervisor will not spawn over it"
                    )
                if spec.incarnation <= retiring.spec.incarnation:
                    raise ProcmanError(
                        f"incarnation {spec.incarnation} must be greater than "
                        f"the held incarnation {retiring.spec.incarnation}"
                    )
            # Before the reservation, the fence, and the pre-shim record.
            # A refusal stays outside the try that releases ``_spawning``.
            self._enforce_admission(spec)
            self._spawning.add(spec.id)
            try:
                blocked = _fence_pid(self.work_dir, spec.id, retiring)
                if blocked is not None:
                    raise ProcmanError(
                        f"{spec.id} worker pid {blocked} is still alive"
                    )
                if retiring is not None:
                    must_retire = not retiring.released
                    retiring.released = True
                    self._slots.pop(spec.id, None)
                    phase = (
                        transition(WorkerPhase.BACKOFF, Trigger.BACKOFF_ELAPSED)
                        if retiring.phase is WorkerPhase.BACKOFF
                        else transition(WorkerPhase.STOPPED, Trigger.SPAWN)
                    )
                else:
                    must_retire = False
                    phase = transition(WorkerPhase.STOPPED, Trigger.SPAWN)
            except Exception:
                self._spawning.discard(spec.id)
                raise
        try:
            if must_retire and retiring is not None:
                await asyncio.to_thread(_wait_retired, retiring)
            await self._write_spawn_intent(spec)
            spawned, shim_ticks = await asyncio.to_thread(
                _spawn_noted, spec, self.work_dir
            )
            now = self._clock.monotonic()
            slot = _Slot(
                spec=spec,
                phase=phase,
                ready=False,
                pid=None,
                exit_code=None,
                signal=None,
                since_s=now,
                beats=0,
                beats_at_s=None,
                term_sent=False,
                kill_sent=False,
                released=False,
                shim_gone=False,
                shim_pid=spawned.pid,
                socket=spawned.socket,
                shim_start_ticks=shim_ticks,
            )
            async with self._lock:
                self._slots[spec.id] = slot
                self._ensure_driver_locked()
            await self._ingest(slot)
            await self._persist()
        finally:
            async with self._lock:
                self._spawning.discard(spec.id)

    def _enforce_admission(self, spec: WorkerSpec) -> None:
        """Refuse a new id the budget cannot hold. Called under the spawn lock.

        A held id is a restart and is not refused, including ``LOST``.
        The workers that count are the slots still in ``_slots`` and the
        ids already in ``_spawning``. On success the kind is noted so the
        next spawn can count this reservation once it is in
        ``_spawning``. Nothing here reads ``/proc``, writes
        ``supervisor.json``, or launches a shim.
        """
        budget = self._budget
        if budget is None:
            return
        held = tuple(
            AdmissionWorker(
                id=slot.spec.id,
                kind=slot.spec.kind,
                phase=slot.phase,
                rss_bytes=slot.rss_bytes,
            )
            for slot in self._slots.values()
        )
        spawning = tuple(
            AdmissionWorker(
                id=worker_id,
                kind=self._spawning.kinds.get(worker_id),
                phase=None,
                rss_bytes=None,
            )
            for worker_id in self._spawning
        )
        decision = decide_admission(
            budget=budget,
            held=held,
            spawning=spawning,
            candidate=spec,
        )
        if decision.admitted:
            self._spawning.note(spec.id, spec.kind)
            return
        if decision.reason is AdmissionReason.UNKNOWN_KIND:
            raise ProcmanError(decision.message)
        raise CapacityExceeded(decision.message)

    async def stop(self, worker_id: str) -> None:
        """``SIGTERM`` the worker, then ``SIGKILL`` if it outlives ``stop_grace_s``.

        Returns when the phase is ``STOPPED`` and the shim has been
        released. The slot is no longer held. A worker that was not alive
        is refused: a failed slot stays until :meth:`record_restart` or a
        later :meth:`spawn`.
        """
        self._check_failure()
        self._check_open()
        validate_worker_id(worker_id)
        loop = asyncio.get_running_loop()
        async with self._lock:
            slot = self._slots.get(worker_id)
            if slot is None:
                raise ProcmanError(f"supervisor does not hold {worker_id}")
            if slot.phase in (WorkerPhase.STARTING, WorkerPhase.RUNNING):
                slot.phase = transition(slot.phase, Trigger.SIGTERM)
                slot.since_s = self._clock.monotonic()
                slot.term_sent = False
                slot.kill_sent = False
                slot.stopped = loop.create_future()
                self._ensure_driver_locked()
            elif slot.phase is not WorkerPhase.STOPPING:
                raise ProcmanError(f"cannot stop {worker_id} from {slot.phase}")
            elif slot.stopped is None:
                slot.stopped = loop.create_future()
                self._ensure_driver_locked()
            waiter = slot.stopped
        if waiter is None:
            raise ProcmanError(f"cannot stop {worker_id}")
        await waiter

    async def status(self, worker_id: str) -> WorkerStatus | None:
        """The supervisor's view, or ``None`` when it does not hold ``worker_id``."""
        self._check_failure()
        validate_worker_id(worker_id)
        async with self._lock:
            slot = self._slots.get(worker_id)
            if slot is None:
                return None
            rss = slot.rss_bytes if slot.phase in ALIVE_PHASES else None
            return WorkerStatus(
                spec=slot.spec,
                phase=slot.phase,
                pid=slot.pid,
                ready=slot.ready,
                exit_code=slot.exit_code,
                signal=slot.signal,
                rss_bytes=rss,
            )

    async def record_restart(self, worker_id: str, decision: RestartDecision) -> None:
        """Write the orchestrator's restart decision onto a held slot.

        ``CRASHED`` becomes ``BACKOFF`` or ``FATAL`` through the state
        machine. ``FAILED``, and ``CRASHED`` when the policy does not
        restart, stay. The shim is released: the orchestrator has decided,
        and the exit facts are already on the slot. This method does not
        wait ``delay_s`` and does not spawn. ``status`` still returns the
        slot afterwards, including one in ``BACKOFF``.
        """
        self._check_failure()
        self._check_open()
        validate_worker_id(worker_id)
        async with self._lock:
            slot = self._slots.get(worker_id)
            if slot is None:
                raise ProcmanError(f"supervisor does not hold {worker_id}")
            slot.phase = _apply_recorded_restart(slot.phase, decision)
            slot.since_s = self._clock.monotonic()
            already = slot.released
            slot.released = True
        if not already:
            await asyncio.to_thread(_wait_retired, slot)
            record = await asyncio.to_thread(_read_exit, self.work_dir, worker_id)
            async with self._lock:
                slot.shim_gone = True
                if (
                    record is not None
                    and slot.exit_code is None
                    and slot.signal is None
                ):
                    slot.exit_code = record.exit_code
                    slot.signal = record.signal
                    slot.ready = record.ready
                    slot.pid = record.pid
        await self._persist()

    def allow_reports(self) -> None:
        """Open publication. B3-03's ``start`` calls this after reconciliation.

        Until this runs, :meth:`report` raises :class:`ProcmanError`
        instead of returning a partial set. A gate :meth:`pause_reports`
        has already closed stays closed: :meth:`close` can run while
        :meth:`start` is still scanning, and this must not reopen it.
        """
        if self._report_gate is _ReportGate.CLOSED:
            return
        self._report_gate = _ReportGate.OPEN

    def pause_reports(self) -> None:
        """Stop publication. :meth:`close` calls this.

        The next report is not published. Absence is not an observation
        (P5, F32). Callers must not publish an empty report to mean this.
        """
        self._report_gate = _ReportGate.CLOSED

    def reports_open(self) -> bool:
        """True only after :meth:`allow_reports` and before :meth:`pause_reports`."""
        return self._report_gate is _ReportGate.OPEN

    def reports_closed(self) -> bool:
        """True after :meth:`pause_reports`. A publish loop returns on this."""
        return self._report_gate is _ReportGate.CLOSED

    async def report(self) -> ProcmanReport:
        """One publication of the live slots. Not persisted (§3.3, F32).

        Lists held slots in ``STARTING``, ``RUNNING`` and ``STOPPING``
        only. ``FAILED``, ``FATAL``, ``LOST``, ``CRASHED`` and
        ``BACKOFF`` are absent, so two reports that omit an owner let
        §8.2 rule 3 reclaim that owner's intents. ``STOPPING`` stays
        listed while the process is still in the stop grace (issue
        #298 is the STS session phase, not this one). A session in
        ``restarting`` with no live worker is not here; the plane's
        orchestrator appends it when it publishes (R4).

        ``generation`` increases by one per successful call and is not
        durable. ``rss_bytes`` is the Pss of that worker's process tree,
        read off the event loop, or ``None`` when the worker's
        ``smaps_rollup`` cannot be read.

        Raises :class:`ProcmanError` before :meth:`allow_reports` and
        after :meth:`pause_reports`. That refusal is the pause.
        """
        self._check_failure()
        async with self._lock:
            self._require_reports_open()
            live = [
                (slot, slot.pid)
                for slot in self._slots.values()
                if slot.phase in ALIVE_PHASES
            ]
        # No /proc read, no thread. A pid of None is "not measured", not
        # a walk of the host. The 50 ms poll never gets here.
        if any(pid is not None and pid > 0 for _slot, pid in live):
            measured = await asyncio.to_thread(
                _measure_pss, live, self._proc_root
            )
        else:
            measured = [(slot, None) for slot, _pid in live]
        async with self._lock:
            self._require_reports_open()
            workers: list[ProcmanWorker] = []
            for slot, rss in measured:
                current = self._slots.get(slot.spec.id)
                if current is not slot or slot.phase not in ALIVE_PHASES:
                    continue
                slot.rss_bytes = rss
                workers.append(
                    ProcmanWorker(
                        id=slot.spec.id,
                        code_ref=slot.spec.code_ref,
                        rss_bytes=rss,
                        phase=slot.phase.value,
                        ready=slot.ready,
                        incarnation=slot.spec.incarnation,
                    )
                )
            self._generation += 1
            generation = self._generation
        return ProcmanReport(generation=generation, workers=workers)

    def _require_reports_open(self) -> None:
        if self._report_gate is _ReportGate.OPEN:
            return
        if self._report_gate is _ReportGate.CLOSED:
            raise ProcmanError("procman report is paused: supervisor is closed")
        raise ProcmanError(
            "procman report is paused until start finishes reconciling"
        )

    def _check_failure(self) -> None:
        failure = self._failure
        if failure is not None:
            raise ProcmanError(
                f"supervisor observation stopped: {failure}"
            ) from failure

    def _check_open(self) -> None:
        if self._closed:
            raise ProcmanError("supervisor is closed")

    def _ensure_driver_locked(self) -> None:
        if self._closed:
            return
        if self._driver is not None and not self._driver.done():
            return
        task = asyncio.get_running_loop().create_task(self._drive())
        task.add_done_callback(self._driver_done)
        self._driver = task

    def _driver_done(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is None:
            return
        self._failure = exc
        for slot in self._slots.values():
            waiter = slot.stopped
            if waiter is not None and not waiter.done():
                waiter.set_exception(exc)

    async def _drive(self) -> None:
        while True:
            async with self._lock:
                slots = [slot for slot in self._slots.values() if _needs_poll(slot)]
            if not slots:
                return
            for slot in slots:
                await self._ingest(slot)
            await self._clock.sleep(STATUS_POLL_S)

    async def _cancel_driver(self) -> None:
        async with self._lock:
            self._closed = True
            task = self._driver
            self._driver = None
        if task is None:
            return
        if not task.done():
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task

    async def _write_spawn_intent(self, spec: WorkerSpec) -> None:
        """Record ``spec`` before ``spawn_shim``, merged with the other rows."""
        since_s = self._clock.monotonic()
        async with self._persist_lock:
            await _await_blocking(
                _merge_spawn_intent, self.work_dir, spec, since_s
            )

    async def _persist(self) -> None:
        async with self._persist_lock:
            async with self._lock:
                records = tuple(
                    _record_from_slot(slot)
                    for slot in sorted(
                        self._slots.values(), key=lambda item: item.spec.id
                    )
                )
                held = {record.spec.id for record in records}
                keep = tuple(sorted(self._spawning - held))
            # Finish the write before releasing the lock. Cancelling the
            # driver (``close``) must not let a second snapshot rename over
            # this one, and must not drop the lock while the thread still
            # holds the temp file. An id reserved by :meth:`spawn` and not
            # yet in a slot keeps the row already on disk: that is the
            # spec written before ``spawn_shim``.
            await _await_blocking(_write_slots, self.work_dir, records, keep)

    async def _stop_held_workers(self) -> None:
        async with self._lock:
            worker_ids = list(self._slots)
        for worker_id in worker_ids:
            async with self._lock:
                slot = self._slots.get(worker_id)
                if slot is None:
                    continue
                phase = slot.phase
            if phase in (
                WorkerPhase.STARTING,
                WorkerPhase.RUNNING,
                WorkerPhase.STOPPING,
            ):
                try:
                    await self.stop(worker_id)
                except ProcmanError:
                    await self._finish_held(worker_id)
            else:
                await self._finish_held(worker_id)

    async def _finish_held(self, worker_id: str) -> None:
        """Release a terminal slot. A live ``LOST`` worker is signalled by pid."""
        async with self._lock:
            slot = self._slots.get(worker_id)
            if slot is None or slot.phase in ALIVE_PHASES:
                return
            pid = slot.pid
            ticks = slot.worker_start_ticks
            grace = slot.spec.stop_grace_s
            lost = slot.phase is WorkerPhase.LOST
            already = slot.released
            slot.released = True
            self._slots.pop(worker_id, None)
        if lost and _blocking_pid(pid, ticks) is not None:
            await asyncio.to_thread(
                _signal_host_pid, pid if pid is not None else 0, signal.SIGTERM, ticks
            )
            await self._clock.sleep(grace)
            if _blocking_pid(pid, ticks) is not None:
                await asyncio.to_thread(
                    _signal_host_pid,
                    pid if pid is not None else 0,
                    signal.SIGKILL,
                    ticks,
                )
                if pid is not None:
                    await asyncio.to_thread(_kill_tree, pid)
        if not already:
            await asyncio.to_thread(_wait_retired, slot)

    async def _ingest(self, slot: _Slot) -> None:
        if slot.released:
            return
        status, record = await asyncio.to_thread(self._observe, slot)
        worker_ticks: int | None = None
        shim_ticks: int | None = None
        if status is not None and status.pid is not None and (
            slot.worker_start_ticks is None or slot.pid != status.pid
        ):
            worker_ticks = await asyncio.to_thread(_proc_start_ticks, status.pid)
        if slot.shim_start_ticks is None and slot.shim_pid > 1:
            shim_ticks = await asyncio.to_thread(_proc_start_ticks, slot.shim_pid)
        changed = False
        async with self._lock:
            if slot.released or self._slots.get(slot.spec.id) is not slot:
                return
            before = _slot_signature(slot)
            if status is not None and (
                status.id != slot.spec.id or status.incarnation != slot.spec.incarnation
            ):
                raise ProcmanError(
                    f"shim for {slot.spec.id} reported "
                    f"{status.id} incarnation {status.incarnation}"
                )
            if status is not None and status.pid != slot.pid:
                slot.worker_start_ticks = None
            step = _advance_worker(
                slot.snapshot(),
                status,
                record,
                now_s=self._clock.monotonic(),
            )
            slot.apply(step)
            if (
                worker_ticks is not None
                and status is not None
                and slot.pid == status.pid
                and slot.worker_start_ticks is None
            ):
                slot.worker_start_ticks = worker_ticks
            if shim_ticks is not None and slot.shim_start_ticks is None:
                slot.shim_start_ticks = shim_ticks
            changed = _slot_signature(slot) != before
            send = step.send_signal
            do_release = step.release
            waiter = slot.stopped
            if do_release:
                slot.released = True
                self._slots.pop(slot.spec.id, None)
                changed = True
            elif (
                waiter is not None
                and not waiter.done()
                and slot.phase is not WorkerPhase.STOPPING
            ):
                # ``stop`` was waiting and the shim disappeared. Leaving
                # the future pending would hang the caller. The slot stays
                # ``LOST``; there is no edge out of it.
                waiter.set_exception(
                    ProcmanError(
                        f"{slot.spec.id} is {slot.phase}; stop did not finish"
                    )
                )
                waiter = None
        if send is not None:
            await asyncio.to_thread(_send_signal, slot, send)
        if do_release:
            await asyncio.to_thread(_wait_retired, slot)
            if waiter is not None and not waiter.done():
                waiter.set_result(None)
        if changed:
            await self._persist()

    def _observe(self, slot: _Slot) -> tuple[ShimStatus | None, ExitRecord | None]:
        try:
            return ShimClient(slot.socket).status(), None
        except OSError:
            return None, _read_exit(self.work_dir, slot.spec.id)
