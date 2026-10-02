"""``Supervisor``: spawn, stop, and the live phase of one plane instance.

Embedded in each plane's controller (§4.1). :meth:`start` loads
``supervisor.json`` and reattaches (B3-03). :meth:`close` is ``detach``
(signal nothing, flush, exit 0) or ``stop`` (the whole host is going
down) and is B3-03 as well. :meth:`report` is B3-04.

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

A new incarnation's ``/proc`` fence (F36) is B3-03. This class does not
keep a pid table beyond the slots it holds in memory. ``rss_bytes`` stays
``None`` (B3-04).
"""

from __future__ import annotations

import asyncio
import os
import signal
import socket
import time
from dataclasses import dataclass, replace
from enum import StrEnum
from pathlib import Path

from mftik.clock import Clock, SystemClock
from mftik.instance import validate_instance_name
from mftik.procman._ticket import TICKET
from mftik.procman.decisions import FailureCause, RestartDecision, classify_failure
from mftik.procman.errors import MessageError, ProcmanError
from mftik.procman.messages import (
    ExitRecord,
    ShimStatus,
    decode_exit,
    exit_record_path,
)
from mftik.procman.report import ProcmanReport
from mftik.procman.shim import ShimClient, spawn_shim
from mftik.procman.spec import PLANES, Plane, WorkerSpec, validate_worker_id
from mftik.procman.state import ALIVE_PHASES, Trigger, WorkerPhase, transition

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

#: Phases a later :meth:`Supervisor.spawn` may replace. ``LOST`` is not
#: here: nothing in the table leaves it, and the old worker may still be
#: alive. Alive phases are not here either.
_REPLACEABLE: frozenset[WorkerPhase] = frozenset(
    {
        WorkerPhase.FAILED,
        WorkerPhase.CRASHED,
        WorkerPhase.BACKOFF,
        WorkerPhase.FATAL,
        WorkerPhase.STOPPED,
    }
)


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

    ``pid``, ``ready``, ``exit_code`` and ``signal`` are the shim's facts,
    copied onto the slot. ``phase`` is the state machine. ``exit_code`` and
    ``signal`` follow the exit record: both empty while the worker is
    alive, exactly one set after it has been reaped. ``rss_bytes`` stays
    ``None`` until B3-04 reads the process tree.
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


def _proc_children(pid: int) -> list[int]:
    path = Path(f"/proc/{pid}/task/{pid}/children")
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
        # there, after its children, and not its parent.
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
    unlink the path. This does not read the worker's ``/proc`` entry
    (F36, B3-03).
    """
    _release_socket(slot)
    deadline = time.monotonic() + _RETIRE_WAIT_S
    while time.monotonic() < deadline:
        if not _socket_listening(slot.socket):
            break
        time.sleep(0.01)
    else:
        if _pid_alive(slot.shim_pid):
            _kill_tree(slot.shim_pid)
    _unlink_quiet(slot.socket)


class Supervisor:
    """One plane instance's supervisor.

    ``plane`` is ``sts``, ``md`` or ``td``. ``instance`` is one NATS subject
    segment, the same rule as :func:`mftik.instance.validate_instance_name`,
    because the report subject is ``procman.report.{plane}.{instance}``.

    ``clock`` supplies ``monotonic`` and ``sleep`` for the live deadlines.
    It defaults to the process clock. Restart intensity is not an argument:
    the orchestrator owns those numbers.
    """

    def __init__(
        self,
        work_dir: Path,
        *,
        plane: Plane,
        instance: str,
        clock: Clock | None = None,
    ) -> None:
        if plane not in PLANES:
            raise ValueError(f"plane {plane!r} is not one of {', '.join(PLANES)}")
        self.work_dir = Path(work_dir)
        self.plane: Plane = plane
        self.instance = validate_instance_name(instance)
        self._clock: Clock = clock if clock is not None else SystemClock()
        self._slots: dict[str, _Slot] = {}
        self._lock = asyncio.Lock()
        self._driver: asyncio.Task[None] | None = None
        self._failure: BaseException | None = None

    async def start(self) -> None:
        """Load ``supervisor.json``, then reattach each socket (§4.4).

        Control subjects stay dark until reconciliation finishes. Workers
        that are already running keep running through it (P1). B3-03.
        """
        raise NotImplementedError(TICKET)

    async def close(self, mode: CloseMode) -> None:
        """``detach`` leaves workers running; ``stop`` ends them (§4.4). B3-03."""
        CloseMode(mode)
        raise NotImplementedError(TICKET)

    async def spawn(self, spec: WorkerSpec) -> None:
        """Spawn ``spec`` on this supervisor's plane.

        The spec's plane has to be this supervisor's plane. Replacing a
        held slot releases that incarnation's shim first, because the
        socket path is per worker id. A slot in ``BACKOFF`` enters
        ``STARTING`` through :attr:`~mftik.procman.Trigger.BACKOFF_ELAPSED`.
        Any other held terminal phase is a new incarnation: the old phase
        has no edge to ``STARTING``, so the new one enters through
        :attr:`~mftik.procman.Trigger.SPAWN`. ``LOST`` and a live phase are
        refused.

        The previous worker pid's ``/proc`` check is B3-03 (F36). This
        method does not do it.
        """
        self._check_failure()
        if spec.plane != self.plane:
            raise ValueError(
                f"spec plane {spec.plane!r} does not match "
                f"supervisor plane {self.plane!r}"
            )
        async with self._lock:
            retiring = self._slots.get(spec.id)
            if retiring is not None:
                if retiring.phase not in _REPLACEABLE:
                    raise ProcmanError(
                        f"{spec.id} is {retiring.phase}; "
                        "the supervisor will not spawn over it"
                    )
                if spec.incarnation <= retiring.spec.incarnation:
                    raise ProcmanError(
                        f"incarnation {spec.incarnation} must be greater than "
                        f"the held incarnation {retiring.spec.incarnation}"
                    )
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
        if must_retire:
            await asyncio.to_thread(_wait_retired, retiring)
        spawned = await asyncio.to_thread(spawn_shim, spec, work_dir=self.work_dir)
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
        )
        async with self._lock:
            self._slots[spec.id] = slot
            self._ensure_driver_locked()
        await self._ingest(slot)

    async def stop(self, worker_id: str) -> None:
        """``SIGTERM`` the worker, then ``SIGKILL`` if it outlives ``stop_grace_s``.

        Returns when the phase is ``STOPPED`` and the shim has been
        released. The slot is no longer held. A worker that was not alive
        is refused: a failed slot stays until :meth:`record_restart` or a
        later :meth:`spawn`.
        """
        self._check_failure()
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
            return WorkerStatus(
                spec=slot.spec,
                phase=slot.phase,
                pid=slot.pid,
                ready=slot.ready,
                exit_code=slot.exit_code,
                signal=slot.signal,
                rss_bytes=None,
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

    async def report(self) -> ProcmanReport:
        """The payload :meth:`start` will publish. Not persisted (§3.3, F32). B3-04."""
        raise NotImplementedError(TICKET)

    def _check_failure(self) -> None:
        failure = self._failure
        if failure is not None:
            raise ProcmanError(
                f"supervisor observation stopped: {failure}"
            ) from failure

    def _ensure_driver_locked(self) -> None:
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

    async def _ingest(self, slot: _Slot) -> None:
        if slot.released:
            return
        status, record = await asyncio.to_thread(self._observe, slot)
        async with self._lock:
            if slot.released or self._slots.get(slot.spec.id) is not slot:
                return
            if status is not None and (
                status.id != slot.spec.id or status.incarnation != slot.spec.incarnation
            ):
                raise ProcmanError(
                    f"shim for {slot.spec.id} reported "
                    f"{status.id} incarnation {status.incarnation}"
                )
            step = _advance_worker(
                slot.snapshot(),
                status,
                record,
                now_s=self._clock.monotonic(),
            )
            slot.apply(step)
            send = step.send_signal
            do_release = step.release
            waiter = slot.stopped
            if do_release:
                slot.released = True
                self._slots.pop(slot.spec.id, None)
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

    def _observe(self, slot: _Slot) -> tuple[ShimStatus | None, ExitRecord | None]:
        try:
            return ShimClient(slot.socket).status(), None
        except OSError:
            return None, _read_exit(self.work_dir, slot.spec.id)
