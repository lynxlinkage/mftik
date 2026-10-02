"""B3-02: the live state machine, and the restart decision the orchestrator records.

Phase changes are pure and unmarked (unit). They take ``now_s`` from a
:class:`~mftik.clock.FakeClock` and do not sleep. Tests that spawn a shim
are ``integration`` and clean up every process they start.
"""

from __future__ import annotations

import asyncio
import os
import signal
import socket
import struct
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest
from mftik.clock import FakeClock
from mftik.procman import (
    BACKOFF_RATIO,
    CloseMode,
    FailureCause,
    ObservedWorker,
    ProcmanError,
    ReattachObservation,
    RestartDecision,
    RestartIntensity,
    ShimClient,
    ShimStatus,
    SpawnedShim,
    Supervisor,
    SupervisorRecord,
    WorkerPhase,
    WorkerSpec,
    classify_failure,
    decode_exit,
    encode_exit,
    encode_supervisor_state,
    exit_record_path,
    exit_record_tmp_path,
    load_supervisor_state,
    plan_restart,
    previous_worker_gone,
    socket_path,
    supervisor_state_path,
)
from mftik.procman.messages import ExitRecord
from mftik.procman.supervisor import (
    STATUS_POLL_S,
    _advance_worker,
    _apply_recorded_restart,
    _Slot,
    _Snapshot,
)

_INTENSITY = RestartIntensity(max_restarts=5, window_s=600, min_backoff_s=1)


def _argv(source: str, *args: str) -> tuple[str, ...]:
    return (sys.executable, "-c", textwrap.dedent(source).strip(), *args)


def _spec(argv: tuple[str, ...] = ("/bin/true",), **overrides: object) -> WorkerSpec:
    raw: dict[str, object] = {
        "id": "td/account/42",
        "plane": "td",
        "kind": "account",
        "incarnation": 1,
        "argv": argv,
        "env": {},
        "code_ref": "v1",
        "restart": "on_failure",
        "start_timeout_s": 30,
        "hb_timeout_s": 3,
        "oom_score_adj": 100,
        "rlimit_data_bytes": None,
        "stop_grace_s": 8,
        "labels": {},
    }
    raw.update(overrides)
    return WorkerSpec(**raw)  # type: ignore[arg-type]


def _snap(**overrides: object) -> _Snapshot:
    raw: dict[str, object] = {
        "phase": WorkerPhase.STARTING,
        "ready": False,
        "pid": 10,
        "exit_code": None,
        "signal": None,
        "since_s": 0.0,
        "beats": 0,
        "beats_at_s": None,
        "start_timeout_s": 60.0,
        "hb_timeout_s": 3.0,
        "stop_grace_s": 8.0,
        "term_sent": False,
        "kill_sent": False,
    }
    raw.update(overrides)
    return _Snapshot(**raw)  # type: ignore[arg-type]


def _status(**overrides: object) -> ShimStatus:
    raw: dict[str, object] = {
        "id": "td/account/42",
        "incarnation": 1,
        "pid": 10,
        "ready": False,
        "beats": 0,
    }
    raw.update(overrides)
    return ShimStatus(**raw)  # type: ignore[arg-type]


def _exit(**overrides: object) -> ExitRecord:
    raw: dict[str, object] = {
        "id": "td/account/42",
        "incarnation": 1,
        "pid": 10,
        "exit_code": 3,
        "signal": None,
        "ready": False,
    }
    raw.update(overrides)
    return ExitRecord(**raw)  # type: ignore[arg-type]


def _hold(
    supervisor: Supervisor,
    spec: WorkerSpec,
    phase: WorkerPhase,
    **overrides: object,
) -> _Slot:
    raw: dict[str, object] = {
        "spec": spec,
        "phase": phase,
        "ready": phase is WorkerPhase.RUNNING,
        "pid": 10,
        "exit_code": 1,
        "signal": None,
        "since_s": 0.0,
        "beats": 0,
        "beats_at_s": None,
        "term_sent": False,
        "kill_sent": False,
        "released": False,
        "shim_gone": False,
        "shim_pid": 0,
        "socket": supervisor.work_dir / "no-such.sock",
    }
    raw.update(overrides)
    slot = _Slot(**raw)  # type: ignore[arg-type]
    supervisor._slots[spec.id] = slot
    return slot


# --- pure decisions --------------------------------------------------------


def test_the_backoff_curve_uses_the_named_ratio() -> None:
    """Attempt 1 is the floor. Each later attempt multiplies by BACKOFF_RATIO.
    There is no cap. Issue #286 has not fixed the ratio; 2 is what B3-02 uses."""
    assert BACKOFF_RATIO == 2.0
    delays = [
        plan_restart(
            phase=WorkerPhase.CRASHED,
            restart="on_failure",
            restarts_in_window=0,
            intensity=_INTENSITY,
            attempt=attempt,
        ).delay_s
        for attempt in range(1, 5)
    ]
    assert delays == [1.0, 2.0, 4.0, 8.0]


def test_a_huge_attempt_stays_a_finite_delay() -> None:
    """``2.0 ** (attempt - 1)`` overflows a float. The delay stays finite.

    A zero floor still does not grow. The ceiling is the float range,
    not a policy cap.
    """
    huge = plan_restart(
        phase=WorkerPhase.CRASHED,
        restart="on_failure",
        restarts_in_window=0,
        intensity=_INTENSITY,
        attempt=10_000,
    )
    assert huge.phase is WorkerPhase.BACKOFF
    assert huge.delay_s == sys.float_info.max
    zero = RestartIntensity(max_restarts=5, window_s=600, min_backoff_s=0)
    stayed = plan_restart(
        phase=WorkerPhase.CRASHED,
        restart="on_failure",
        restarts_in_window=0,
        intensity=zero,
        attempt=10_000,
    )
    assert stayed.delay_s == 0.0


def test_a_zero_backoff_floor_does_not_grow() -> None:
    """The curve multiplies the floor. Zero stays zero at every attempt."""
    intensity = RestartIntensity(max_restarts=5, window_s=600, min_backoff_s=0)
    first = plan_restart(
        phase=WorkerPhase.CRASHED,
        restart="on_failure",
        restarts_in_window=0,
        intensity=intensity,
        attempt=1,
    )
    second = plan_restart(
        phase=WorkerPhase.CRASHED,
        restart="on_failure",
        restarts_in_window=0,
        intensity=intensity,
        attempt=4,
    )
    assert first.delay_s == 0.0
    assert second.delay_s == 0.0


def test_plan_restart_rejects_a_phase_that_is_not_a_failure() -> None:
    with pytest.raises(ValueError):
        plan_restart(
            phase=WorkerPhase.RUNNING,
            restart="on_failure",
            restarts_in_window=0,
            intensity=_INTENSITY,
            attempt=1,
        )


def test_start_timeout_fires_at_the_deadline_and_kills() -> None:
    clock = FakeClock()
    snap = _snap(since_s=clock.monotonic(), start_timeout_s=60)
    early = _advance_worker(snap, _status(), None, now_s=59)
    assert early.snapshot.phase is WorkerPhase.STARTING
    assert early.send_signal is None
    assert early.release is False
    clock.advance(60)
    step = _advance_worker(snap, _status(), None, now_s=clock.monotonic())
    assert step.snapshot.phase is WorkerPhase.FAILED
    assert step.snapshot.phase is classify_failure(
        ready=False, cause=FailureCause.START_TIMEOUT
    )
    assert step.send_signal == signal.SIGKILL
    assert step.release is False


def test_death_before_ready_is_failed_and_the_shim_is_kept() -> None:
    step = _advance_worker(
        _snap(), _status(exit_code=3), None, now_s=1
    )
    assert step.snapshot.phase is WorkerPhase.FAILED
    assert step.snapshot.exit_code == 3
    assert step.send_signal is None
    assert step.release is False


def test_ready_moves_to_running_and_starts_the_beat_clock() -> None:
    step = _advance_worker(
        _snap(beats=0), _status(ready=True, beats=1), None, now_s=4
    )
    assert step.snapshot.phase is WorkerPhase.RUNNING
    assert step.snapshot.ready is True
    assert step.snapshot.beats == 1
    assert step.snapshot.beats_at_s == 4
    assert step.send_signal is None


def test_ready_and_exit_in_one_snapshot_is_crashed() -> None:
    """The beat that says ready counts even when the exit is in the same read."""
    step = _advance_worker(
        _snap(),
        _status(ready=True, beats=1, exit_code=0),
        None,
        now_s=1,
    )
    assert step.snapshot.phase is WorkerPhase.CRASHED
    assert step.release is False


def test_death_after_ready_is_crashed_and_the_shim_is_kept() -> None:
    step = _advance_worker(
        _snap(phase=WorkerPhase.RUNNING, ready=True, beats=1, beats_at_s=0.0),
        _status(ready=True, beats=1, signal=signal.SIGKILL),
        None,
        now_s=1,
    )
    assert step.snapshot.phase is WorkerPhase.CRASHED
    assert step.snapshot.signal == signal.SIGKILL
    assert step.release is False
    assert step.send_signal is None


def test_heartbeat_timeout_uses_the_injected_clock_and_only_running() -> None:
    clock = FakeClock()
    running = _snap(
        phase=WorkerPhase.RUNNING,
        ready=True,
        beats=1,
        beats_at_s=clock.monotonic(),
        hb_timeout_s=3,
    )
    clock.advance(2)
    held = _advance_worker(
        running, _status(ready=True, beats=1), None, now_s=clock.monotonic()
    )
    assert held.snapshot.phase is WorkerPhase.RUNNING
    assert held.send_signal is None
    clock.advance(1)
    step = _advance_worker(
        running, _status(ready=True, beats=1), None, now_s=clock.monotonic()
    )
    assert step.snapshot.phase is WorkerPhase.CRASHED
    assert step.snapshot.phase is classify_failure(
        ready=True, cause=FailureCause.HEARTBEAT_TIMEOUT
    )
    assert step.send_signal == signal.SIGKILL
    assert step.release is False
    # Not armed before ready, even though the beat counter is stuck.
    starting = _advance_worker(
        _snap(beats=1, hb_timeout_s=3, start_timeout_s=60),
        _status(beats=1),
        None,
        now_s=10,
    )
    assert starting.snapshot.phase is WorkerPhase.STARTING
    assert starting.send_signal is None


def test_a_new_beat_resets_the_heartbeat_deadline() -> None:
    """A beat that does not change ``ready`` still counts."""
    snap = _snap(
        phase=WorkerPhase.RUNNING,
        ready=True,
        beats=1,
        beats_at_s=0.0,
        hb_timeout_s=3,
    )
    step = _advance_worker(snap, _status(ready=True, beats=2), None, now_s=2)
    assert step.snapshot.phase is WorkerPhase.RUNNING
    assert step.snapshot.beats_at_s == 2
    later = _advance_worker(
        step.snapshot, _status(ready=True, beats=2), None, now_s=5
    )
    assert later.snapshot.phase is WorkerPhase.CRASHED


def test_heartbeat_timeout_is_not_armed_when_the_spec_has_none() -> None:
    snap = _snap(
        phase=WorkerPhase.RUNNING,
        ready=True,
        beats=1,
        beats_at_s=0.0,
        hb_timeout_s=None,
    )
    step = _advance_worker(snap, _status(ready=True, beats=1), None, now_s=1000)
    assert step.snapshot.phase is WorkerPhase.RUNNING
    assert step.send_signal is None


def test_stop_sends_sigterm_then_sigkill_after_the_grace() -> None:
    first = _advance_worker(
        _snap(phase=WorkerPhase.STOPPING, since_s=0.0, stop_grace_s=8),
        _status(),
        None,
        now_s=0,
    )
    assert first.snapshot.phase is WorkerPhase.STOPPING
    assert first.send_signal == signal.SIGTERM
    assert first.snapshot.term_sent is True
    waiting = _advance_worker(first.snapshot, _status(), None, now_s=7.9)
    assert waiting.send_signal is None
    killed = _advance_worker(first.snapshot, _status(), None, now_s=8)
    assert killed.send_signal == signal.SIGKILL
    assert killed.snapshot.phase is WorkerPhase.STOPPING
    assert killed.release is False


def test_an_exit_during_stop_releases_the_shim() -> None:
    step = _advance_worker(
        _snap(phase=WorkerPhase.STOPPING, term_sent=True, since_s=0.0),
        _status(exit_code=0),
        None,
        now_s=1,
    )
    assert step.snapshot.phase is WorkerPhase.STOPPED
    assert step.release is True
    assert step.send_signal is None


def test_a_refused_socket_with_no_exit_file_is_lost() -> None:
    for phase in (
        WorkerPhase.STARTING,
        WorkerPhase.RUNNING,
        WorkerPhase.STOPPING,
    ):
        step = _advance_worker(_snap(phase=phase, pid=10), None, None, now_s=1)
        assert step.snapshot.phase is WorkerPhase.LOST
        assert step.snapshot.exit_code is None
        assert step.snapshot.signal is None
        assert step.snapshot.pid == 10
        assert step.release is False
        assert step.shim_gone is True


def test_a_refused_socket_with_an_exit_file_is_the_recorded_death() -> None:
    failed = _advance_worker(_snap(), None, _exit(ready=False, exit_code=3), now_s=1)
    assert failed.snapshot.phase is WorkerPhase.FAILED
    assert failed.snapshot.exit_code == 3
    assert failed.shim_gone is True
    crashed = _advance_worker(
        _snap(), None, _exit(ready=True, exit_code=None, signal=signal.SIGTERM), now_s=1
    )
    assert crashed.snapshot.phase is WorkerPhase.CRASHED
    assert crashed.snapshot.signal == signal.SIGTERM


def test_a_refused_socket_does_not_leave_a_terminal_phase() -> None:
    step = _advance_worker(
        _snap(phase=WorkerPhase.FAILED, exit_code=1), None, None, now_s=5
    )
    assert step.snapshot.phase is WorkerPhase.FAILED
    assert step.shim_gone is True


def test_status_is_polled_often_enough_to_see_a_short_deadline() -> None:
    # watch does not push beats, so this is the freshness interval.
    assert STATUS_POLL_S == 0.05


def test_recording_a_restart_uses_the_table() -> None:
    backoff = _apply_recorded_restart(
        WorkerPhase.CRASHED,
        RestartDecision(phase=WorkerPhase.BACKOFF, delay_s=1.0),
    )
    assert backoff is WorkerPhase.BACKOFF
    fatal = _apply_recorded_restart(
        WorkerPhase.CRASHED,
        RestartDecision(phase=WorkerPhase.FATAL, delay_s=None),
    )
    assert fatal is WorkerPhase.FATAL
    stayed = _apply_recorded_restart(
        WorkerPhase.FAILED,
        RestartDecision(phase=WorkerPhase.FAILED, delay_s=None),
    )
    assert stayed is WorkerPhase.FAILED
    with pytest.raises(ProcmanError):
        _apply_recorded_restart(
            WorkerPhase.FAILED,
            RestartDecision(phase=WorkerPhase.BACKOFF, delay_s=1.0),
        )


# --- supervisor, no processes ----------------------------------------------


async def test_status_of_an_unknown_worker_is_none(tmp_path: Path) -> None:
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    assert await supervisor.status("td/account/42") is None


async def test_stop_of_an_unknown_worker_is_refused(tmp_path: Path) -> None:
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    with pytest.raises(ProcmanError):
        await supervisor.stop("td/account/42")


async def test_spawn_refuses_a_live_slot_and_a_live_lost_pid(tmp_path: Path) -> None:
    """A live phase is refused. ``LOST`` is refused only while that pid lives.

    The pid is this process, so the refusal does not spawn. Replacing a
    ``LOST`` slot whose pid is gone is the integration test: it needs a
    real worker.
    """
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    spec = _spec()
    _hold(supervisor, spec, WorkerPhase.RUNNING)
    with pytest.raises(ProcmanError):
        await supervisor.spawn(spec)
    supervisor._slots.clear()
    pid = os.getpid()
    ticks = int(Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19])
    _hold(
        supervisor,
        spec,
        WorkerPhase.LOST,
        exit_code=None,
        pid=pid,
        worker_start_ticks=ticks,
    )
    with pytest.raises(ProcmanError, match=f"worker pid {pid} is still alive"):
        await supervisor.spawn(_spec(incarnation=2))
    assert supervisor._slots[spec.id].phase is WorkerPhase.LOST


async def test_spawn_refuses_an_incarnation_that_does_not_move_forward(
    tmp_path: Path,
) -> None:
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    spec = _spec()
    _hold(supervisor, spec, WorkerPhase.CRASHED)
    with pytest.raises(ProcmanError):
        await supervisor.spawn(spec)


async def test_an_in_flight_spawn_reserves_the_worker_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The id is reserved before the lock is dropped, and dropped if launch fails."""
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    spec = _spec()
    slot = _hold(supervisor, spec, WorkerPhase.FAILED)
    supervisor._spawning.add(spec.id)

    def fail_launch(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("spawn_shim ran")

    monkeypatch.setattr("mftik.procman.supervisor.spawn_shim", fail_launch)
    with pytest.raises(ProcmanError, match="already being spawned"):
        await supervisor.spawn(_spec(incarnation=2))
    assert supervisor._slots[spec.id] is slot
    assert spec.id in supervisor._spawning

    supervisor._spawning.clear()
    supervisor._slots.clear()

    def boom(*_args: object, **_kwargs: object) -> None:
        raise OSError("launch failed")

    monkeypatch.setattr("mftik.procman.supervisor.spawn_shim", boom)
    with pytest.raises(OSError, match="launch failed"):
        await supervisor.spawn(spec)
    assert spec.id not in supervisor._spawning
    assert supervisor._slots == {}


async def test_record_restart_holds_the_slot_and_clears_rss(tmp_path: Path) -> None:
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    spec = _spec()
    _hold(supervisor, spec, WorkerPhase.CRASHED, ready=True)
    decision = plan_restart(
        phase=WorkerPhase.CRASHED,
        restart="on_failure",
        restarts_in_window=4,
        intensity=_INTENSITY,
        attempt=5,
    )
    await supervisor.record_restart(spec.id, decision)
    status = await supervisor.status(spec.id)
    assert status is not None
    assert status.phase is WorkerPhase.BACKOFF
    assert status.rss_bytes is None
    assert status.spec is spec


async def test_record_restart_fatal_stays_held(tmp_path: Path) -> None:
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    spec = _spec(restart="on_failure")
    _hold(supervisor, spec, WorkerPhase.CRASHED)
    decision = plan_restart(
        phase=WorkerPhase.CRASHED,
        restart="on_failure",
        restarts_in_window=5,
        intensity=_INTENSITY,
        attempt=1,
    )
    assert decision.phase is WorkerPhase.FATAL
    assert decision.delay_s is None
    await supervisor.record_restart(spec.id, decision)
    status = await supervisor.status(spec.id)
    assert status is not None
    assert status.phase is WorkerPhase.FATAL
    assert status.rss_bytes is None


# --- real shims ------------------------------------------------------------

_SLEEP = """
import time
time.sleep(30)
"""

_EXIT_3 = """
raise SystemExit(3)
"""

_READY_EXIT = """
import os
fd = int(os.environ["MFTIK_STATUS_FD"])
os.write(fd, b'{"ready":true}\\n')
raise SystemExit(0)
"""

_READY_SLEEP = """
import os, time
fd = int(os.environ["MFTIK_STATUS_FD"])
os.write(fd, b'{"ready":true}\\n')
time.sleep(30)
"""

_BEAT_LOOP = """
import os, time
fd = int(os.environ["MFTIK_STATUS_FD"])
deadline = time.monotonic() + 2.0
while time.monotonic() < deadline:
    os.write(fd, b'{"ready":true}\\n')
    time.sleep(0.1)
time.sleep(30)
"""

_CATCH_TERM = """
import signal, sys, time
marker, ready = sys.argv[1], sys.argv[2]
def _on_term(signum, frame):
    with open(marker, "w") as handle:
        handle.write("sigterm")
    raise SystemExit(0)
signal.signal(signal.SIGTERM, _on_term)
with open(ready, "w") as handle:
    handle.write("ready")
time.sleep(30)
"""

_IGNORE_TERM = """
import signal, sys, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
with open(sys.argv[1], "w") as handle:
    handle.write("ignoring")
time.sleep(30)
"""


def _pid_running(pid: int | None) -> bool:
    if pid is None or pid <= 1:
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
    if pid <= 1 or pid == os.getpid():
        return
    try:
        os.kill(pid, signal.SIGKILL)
    except OSError:
        pass


def _kill_tree(pid: int) -> None:
    if pid <= 1 or pid == os.getpid():
        return
    for child in _proc_children(pid):
        _kill_tree(child)
    _kill(pid)


def _ps_family(work_dir: Path) -> tuple[set[int], set[int]]:
    """Live shim pids whose command line names ``work_dir``, and their children.

    ``ps`` is the source of the rows. A zombie is not a live process.
    """
    listing = subprocess.check_output(
        ["ps", "-ww", "-eo", "pid=,ppid=,args="],
        text=True,
    )
    rows: list[tuple[int, int, str]] = []
    for line in listing.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) < 2:
            continue
        args = parts[2] if len(parts) == 3 else ""
        rows.append((int(parts[0]), int(parts[1]), args))
    marker = str(work_dir)
    shims = {
        pid
        for pid, _ppid, args in rows
        if marker in args and _pid_running(pid)
    }
    workers = {
        pid
        for pid, ppid, _args in rows
        if ppid in shims and _pid_running(pid)
    }
    return shims, workers


def _reap_workdir(work_dir: Path) -> None:
    shims, _workers = _ps_family(work_dir)
    for pid in shims:
        _kill_tree(pid)


def _peer(path: Path) -> int | None:
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


def _stop_held(slot: _Slot) -> None:
    """Kill the worker, its siblings under the shim, then the shim.

    Never the shim's parent: that is init or the host subreaper.
    """
    try:
        client = ShimClient(slot.socket)
        try:
            client.signal(signal.SIGKILL)
        except OSError:
            pass
    except OSError:
        client = None
    shim_pid = slot.shim_pid if slot.shim_pid > 1 else (_peer(slot.socket) or 0)
    if shim_pid > 1:
        for child in _proc_children(shim_pid):
            _kill_tree(child)
        if client is not None:
            try:
                client.release()
            except OSError:
                pass
        _kill(shim_pid)
    if slot.pid is not None:
        _kill_tree(slot.pid)


async def _cleanup(supervisor: Supervisor) -> None:
    """``close(stop)`` when this supervisor has not already detached.

    After ``detach`` the public close will not signal, so the slots are
    killed directly. ``ps`` is the backstop either way.
    """
    try:
        if supervisor._closed:
            for slot in list(supervisor._slots.values()):
                _stop_held(slot)
            supervisor._slots.clear()
        else:
            await supervisor.close(CloseMode.STOP)
    finally:
        _reap_workdir(supervisor.work_dir)


async def _until(supervisor: Supervisor, worker_id: str, predicate, timeout: float = 4):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = await supervisor.status(worker_id)
        if predicate(last):
            return last
        await asyncio.sleep(0.02)
    raise AssertionError(last)


@pytest.mark.integration
async def test_death_before_ready_stays_failed_and_held(tmp_path: Path) -> None:
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    spec = _spec(_argv(_EXIT_3), start_timeout_s=30, hb_timeout_s=None)
    try:
        await supervisor.spawn(spec)
        status = await _until(
            supervisor,
            spec.id,
            lambda item: item is not None
            and item.phase is WorkerPhase.FAILED
            and item.exit_code == 3,
        )
        assert status.ready is False
        assert status.rss_bytes is None
        assert status.signal is None
        # The orchestrator has not decided. The shim is still there.
        assert ShimClient(socket_path(tmp_path, spec.id)).status().exit_code == 3
    finally:
        await _cleanup(supervisor)


@pytest.mark.integration
async def test_death_after_ready_is_crashed_and_held(tmp_path: Path) -> None:
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    spec = _spec(_argv(_READY_EXIT), start_timeout_s=30, hb_timeout_s=30)
    try:
        await supervisor.spawn(spec)
        status = await _until(
            supervisor,
            spec.id,
            lambda item: item is not None
            and item.phase is WorkerPhase.CRASHED
            and item.ready is True
            and item.exit_code == 0,
        )
        assert status.rss_bytes is None
        assert ShimClient(socket_path(tmp_path, spec.id)).status().beats >= 1
    finally:
        await _cleanup(supervisor)


@pytest.mark.integration
async def test_start_timeout_kills_and_classifies_failed(tmp_path: Path) -> None:
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    # spawn_shim also uses this as its launch deadline, so it has to be
    # long enough for the interpreter to start. The ready timer starts
    # when that call returns.
    spec = _spec(_argv(_SLEEP), start_timeout_s=3, hb_timeout_s=None)
    try:
        await supervisor.spawn(spec)
        status = await _until(
            supervisor,
            spec.id,
            lambda item: item is not None
            and item.phase is WorkerPhase.FAILED
            and item.signal == signal.SIGKILL,
            timeout=8,
        )
        assert status.ready is False
        assert status.pid is not None
        assert not _pid_running(status.pid)
        assert status.rss_bytes is None
    finally:
        await _cleanup(supervisor)


@pytest.mark.integration
async def test_heartbeat_timeout_kills_and_classifies_crashed(tmp_path: Path) -> None:
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    spec = _spec(_argv(_READY_SLEEP), start_timeout_s=30, hb_timeout_s=0.5)
    try:
        await supervisor.spawn(spec)
        status = await _until(
            supervisor,
            spec.id,
            lambda item: item is not None
            and item.phase is WorkerPhase.CRASHED
            and item.signal == signal.SIGKILL,
            timeout=4,
        )
        assert status.ready is True
        assert not _pid_running(status.pid)
    finally:
        await _cleanup(supervisor)


@pytest.mark.integration
async def test_heartbeats_that_keep_arriving_do_not_kill(tmp_path: Path) -> None:
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    spec = _spec(_argv(_BEAT_LOOP), start_timeout_s=30, hb_timeout_s=0.5)
    try:
        await supervisor.spawn(spec)
        await _until(
            supervisor,
            spec.id,
            lambda item: item is not None and item.phase is WorkerPhase.RUNNING,
        )
        await asyncio.sleep(0.8)
        status = await supervisor.status(spec.id)
        assert status is not None
        assert status.phase is WorkerPhase.RUNNING
        assert status.ready is True
        assert _pid_running(status.pid)
    finally:
        await _cleanup(supervisor)


@pytest.mark.integration
async def test_stop_reaps_within_the_grace_and_releases(tmp_path: Path) -> None:
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    marker = tmp_path / "caught"
    ready = tmp_path / "ready"
    spec = _spec(
        _argv(_CATCH_TERM, str(marker), str(ready)),
        start_timeout_s=30,
        stop_grace_s=2,
    )
    try:
        await supervisor.spawn(spec)
        await _until(
            supervisor,
            spec.id,
            lambda item: item is not None and item.pid is not None and ready.exists(),
        )
        worker = (await supervisor.status(spec.id)).pid
        await supervisor.stop(spec.id)
        assert await supervisor.status(spec.id) is None
        assert marker.read_text() == "sigterm"
        assert not _pid_running(worker)
        record = decode_exit(exit_record_path(tmp_path, spec.id).read_bytes())
        assert record.exit_code == 0
        assert record.signal is None
    finally:
        await _cleanup(supervisor)


@pytest.mark.integration
async def test_stop_sigkills_when_the_grace_expires(tmp_path: Path) -> None:
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    ignoring = tmp_path / "ignoring"
    spec = _spec(
        _argv(_IGNORE_TERM, str(ignoring)),
        start_timeout_s=30,
        stop_grace_s=0.4,
    )
    try:
        await supervisor.spawn(spec)
        await _until(
            supervisor,
            spec.id,
            lambda item: item is not None
            and item.pid is not None
            and ignoring.exists(),
        )
        worker = (await supervisor.status(spec.id)).pid
        await supervisor.stop(spec.id)
        assert await supervisor.status(spec.id) is None
        assert not _pid_running(worker)
        record = decode_exit(exit_record_path(tmp_path, spec.id).read_bytes())
        assert record.signal == signal.SIGKILL
        assert record.exit_code is None
    finally:
        await _cleanup(supervisor)


@pytest.mark.integration
async def test_a_killed_shim_is_lost_with_no_exit_record(tmp_path: Path) -> None:
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    spec = _spec(_argv(_SLEEP), start_timeout_s=30, hb_timeout_s=None)
    try:
        await supervisor.spawn(spec)
        await _until(
            supervisor,
            spec.id,
            lambda item: item is not None and item.pid is not None,
        )
        shim = _peer(socket_path(tmp_path, spec.id))
        assert shim is not None
        os.kill(shim, signal.SIGKILL)
        status = await _until(
            supervisor,
            spec.id,
            lambda item: item is not None and item.phase is WorkerPhase.LOST,
        )
        assert status.exit_code is None
        assert status.signal is None
        assert status.rss_bytes is None
        assert not exit_record_path(tmp_path, spec.id).exists()
        assert not exit_record_tmp_path(tmp_path, spec.id).exists()
    finally:
        await _cleanup(supervisor)


@pytest.mark.integration
async def test_the_orchestrator_spawns_the_next_incarnation(tmp_path: Path) -> None:
    """The supervisor does not restart by itself. record_restart, then spawn."""
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    spec = _spec(_argv(_READY_EXIT), start_timeout_s=30, hb_timeout_s=30)
    try:
        await supervisor.spawn(spec)
        await _until(
            supervisor,
            spec.id,
            lambda item: item is not None and item.phase is WorkerPhase.CRASHED,
        )
        decision = plan_restart(
            phase=WorkerPhase.CRASHED,
            restart="on_failure",
            restarts_in_window=0,
            intensity=_INTENSITY,
            attempt=1,
        )
        await supervisor.record_restart(spec.id, decision)
        held = await supervisor.status(spec.id)
        assert held is not None
        assert held.phase is WorkerPhase.BACKOFF
        nxt = _spec(_argv(_SLEEP), incarnation=2, start_timeout_s=30)
        await supervisor.spawn(nxt)
        status = await supervisor.status(spec.id)
        assert status is not None
        assert status.spec.incarnation == 2
        assert status.phase is WorkerPhase.STARTING
        assert status.pid is not None
        assert _pid_running(status.pid)
    finally:
        await _cleanup(supervisor)


@pytest.mark.integration
async def test_two_concurrent_spawns_keep_one_shim_and_one_worker(
    tmp_path: Path,
) -> None:
    """One id has one launch in flight. The other call is refused."""
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    spec = _spec(_argv(_SLEEP), start_timeout_s=30, hb_timeout_s=None)
    try:
        outcomes = await asyncio.gather(
            supervisor.spawn(spec),
            supervisor.spawn(spec),
            return_exceptions=True,
        )
        assert outcomes.count(None) == 1
        errors = [item for item in outcomes if isinstance(item, ProcmanError)]
        assert len(errors) == 1
        assert len(outcomes) == 2
        status = await supervisor.status(spec.id)
        assert status is not None
        assert status.pid is not None
        shims, workers = _ps_family(tmp_path)
        assert shims == {_peer(socket_path(tmp_path, spec.id))}
        assert workers == {status.pid}
    finally:
        await _cleanup(supervisor)
        _reap_workdir(tmp_path)


# --- reattach and the F36 fence: B3-03 ------------------------------------


def test_previous_worker_gone_is_the_pid_reuse_rule() -> None:
    """A missing process is gone. The same start time is not. A different
    one is reuse. An unrecorded start time cannot prove reuse."""
    assert previous_worker_gone(recorded_start_ticks=10, live_start_ticks=None)
    assert previous_worker_gone(recorded_start_ticks=None, live_start_ticks=None)
    assert not previous_worker_gone(recorded_start_ticks=10, live_start_ticks=10)
    assert previous_worker_gone(recorded_start_ticks=10, live_start_ticks=11)
    assert not previous_worker_gone(recorded_start_ticks=None, live_start_ticks=11)
    with pytest.raises(ValueError):
        previous_worker_gone(recorded_start_ticks=True, live_start_ticks=None)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        previous_worker_gone(recorded_start_ticks=-1, live_start_ticks=None)


def test_reattach_refuses_a_plane_that_is_not_a_supervisor_plane() -> None:
    from mftik.procman import DesiredSlot, reattach_action

    with pytest.raises(ValueError):
        reattach_action(
            plane="sym",  # type: ignore[arg-type]
            desired=DesiredSlot.PRESENT,
            observed=ObservedWorker.RUNNING,
        )


def _dead_pid() -> int:
    pid = 1 << 22
    while pid < (1 << 22) + 2000 and Path(f"/proc/{pid}").exists():
        pid += 1
    return pid


def _save_record(work_dir: Path, record: SupervisorRecord) -> None:
    path = supervisor_state_path(work_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(encode_supervisor_state((record,)))


def _record(
    spec: WorkerSpec, phase: WorkerPhase, **overrides: object
) -> SupervisorRecord:
    raw: dict[str, object] = {
        "spec": spec,
        "phase": phase,
        "worker_pid": None,
        "worker_start_ticks": None,
        "shim_pid": None,
        "shim_start_ticks": None,
        "since_s": 0.0,
    }
    raw.update(overrides)
    return SupervisorRecord(**raw)  # type: ignore[arg-type]


async def test_start_reports_absent_when_nothing_is_left_on_disk(
    tmp_path: Path,
) -> None:
    spec = _spec(labels={"desk": "a"})
    gone = _record(
        spec, WorkerPhase.RUNNING, worker_pid=_dead_pid(), worker_start_ticks=3
    )
    _save_record(tmp_path, gone)
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    found = await supervisor.start()
    assert found == (
        ReattachObservation(
            id=spec.id,
            observed=ObservedWorker.ABSENT,
            spec=spec,
            incarnation=1,
            status=None,
            exit_record=None,
        ),
    )
    assert await supervisor.status(spec.id) is None


async def test_start_holds_an_exited_worker_from_the_exit_file(tmp_path: Path) -> None:
    spec = _spec()
    dead = _dead_pid()
    _save_record(
        tmp_path,
        _record(spec, WorkerPhase.RUNNING, worker_pid=dead, worker_start_ticks=4),
    )
    path = exit_record_path(tmp_path, spec.id)
    path.write_bytes(
        encode_exit(
            ExitRecord(
                id=spec.id,
                incarnation=1,
                pid=dead,
                exit_code=3,
                signal=None,
                ready=False,
            )
        )
    )
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    found = await supervisor.start()
    assert len(found) == 1
    assert found[0].observed is ObservedWorker.EXITED
    assert found[0].exit_record is not None
    assert found[0].exit_record.exit_code == 3
    assert found[0].status is None
    held = await supervisor.status(spec.id)
    assert held is not None
    assert held.phase is WorkerPhase.FAILED
    assert held.exit_code == 3
    await supervisor.release_slot(spec.id)
    assert await supervisor.status(spec.id) is None


async def test_start_holds_lost_when_the_socket_refuses_and_no_exit_file_exists(
    tmp_path: Path,
) -> None:
    spec = _spec()
    dead = _dead_pid()
    _save_record(
        tmp_path,
        _record(spec, WorkerPhase.RUNNING, worker_pid=dead, worker_start_ticks=5),
    )
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    path = socket_path(tmp_path, spec.id)
    path.parent.mkdir(parents=True, exist_ok=True)
    sock.bind(os.fspath(path))
    sock.close()
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    found = await supervisor.start()
    assert found[0].observed is ObservedWorker.LOST
    assert found[0].status is None
    assert found[0].exit_record is None
    held = await supervisor.status(spec.id)
    assert held is not None
    assert held.phase is WorkerPhase.LOST
    await supervisor.release_slot(spec.id)
    assert await supervisor.status(spec.id) is None


async def test_release_slot_refuses_a_lost_worker_whose_pid_is_still_alive(
    tmp_path: Path,
) -> None:
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    spec = _spec()
    pid = os.getpid()
    ticks = int(Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19])
    _hold(
        supervisor,
        spec,
        WorkerPhase.LOST,
        exit_code=None,
        pid=pid,
        worker_start_ticks=ticks,
        released=True,
    )
    with pytest.raises(ProcmanError, match=f"worker pid {pid} is still alive"):
        await supervisor.release_slot(spec.id)
    assert await supervisor.status(spec.id) is not None


async def test_detach_leaves_a_held_slot_and_cancels_the_driver(tmp_path: Path) -> None:
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    spec = _spec()
    slot = _hold(supervisor, spec, WorkerPhase.RUNNING, shim_pid=0)
    supervisor._ensure_driver_locked()
    task = supervisor._driver
    assert task is not None
    await supervisor.close(CloseMode.DETACH)
    assert task.done()
    assert supervisor._driver is None
    assert slot.phase in (WorkerPhase.RUNNING, WorkerPhase.LOST)
    assert await supervisor.status(spec.id) is not None
    with pytest.raises(ProcmanError, match="closed"):
        await supervisor.spawn(spec)


async def test_a_second_start_is_refused(tmp_path: Path) -> None:
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    assert await supervisor.start() == ()
    with pytest.raises(ProcmanError, match="already started"):
        await supervisor.start()


async def test_spawn_replaces_a_lost_slot_whose_pid_is_gone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The new incarnation enters through ``STOPPED`` + ``SPAWN``. No edge
    leaves ``LOST``. The launch itself is faked: the real process is the
    integration test."""
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    spec = _spec()
    _hold(
        supervisor,
        spec,
        WorkerPhase.LOST,
        exit_code=None,
        pid=_dead_pid(),
        worker_start_ticks=9,
        released=True,
        shim_pid=0,
    )

    def fake(launched: WorkerSpec, *, work_dir: Path) -> SpawnedShim:
        del work_dir
        return SpawnedShim(
            worker_id=launched.id,
            socket=tmp_path / "missing.sock",
            pid=0,
        )

    monkeypatch.setattr("mftik.procman.supervisor.spawn_shim", fake)
    await supervisor.spawn(_spec(incarnation=2))
    status = await supervisor.status(spec.id)
    assert status is not None
    assert status.spec.incarnation == 2
    await supervisor.close(CloseMode.DETACH)


def _wait_until(predicate, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("condition was still false")


@pytest.mark.integration
async def test_detach_reattach_does_not_spawn_a_duplicate_and_killing_the_shim_stops(
    tmp_path: Path,
) -> None:
    """§4.4 and the ticket: detach leaves the worker, a new supervisor
    reattaches it, and killing the shim makes that worker stop on SIGTERM
    with no exit file."""
    marker = tmp_path / "caught"
    ready = tmp_path / "ready"
    spec = _spec(
        _argv(_CATCH_TERM, str(marker), str(ready)),
        labels={"desk": "a"},
        hb_timeout_s=None,
        start_timeout_s=30,
    )
    first = Supervisor(tmp_path, plane="td", instance="td")
    second: Supervisor | None = None
    try:
        await first.start()
        await first.spawn(spec)
        await _until(
            first,
            spec.id,
            lambda item: item is not None and item.pid is not None and ready.exists(),
        )
        status = await first.status(spec.id)
        assert status is not None and status.pid is not None
        pid = status.pid
        recorded: tuple[SupervisorRecord, ...] = ()

        def _written() -> bool:
            nonlocal recorded
            try:
                recorded = load_supervisor_state(tmp_path)
            except ProcmanError:
                return False
            return bool(
                recorded
                and recorded[0].worker_pid == pid
                and recorded[0].worker_start_ticks is not None
                and recorded[0].shim_pid is not None
                and recorded[0].shim_start_ticks is not None
            )

        _wait_until(_written)
        row = recorded[0]
        assert row.spec.id == spec.id
        assert row.spec.incarnation == 1
        assert row.spec.code_ref == "v1"
        assert dict(row.spec.labels) == {"desk": "a"}
        assert row.phase in (WorkerPhase.STARTING, WorkerPhase.RUNNING)
        task = first._driver
        await first.close(CloseMode.DETACH)
        assert task is not None and task.done()
        assert _pid_running(pid)
        assert not (marker.is_file() and marker.read_text() == "sigterm")
        second = Supervisor(tmp_path, plane="td", instance="td")
        found = await second.start()
        assert [item.id for item in found] == [spec.id]
        assert found[0].observed is ObservedWorker.RUNNING
        assert found[0].incarnation == 1
        assert found[0].spec is not None
        assert found[0].spec.code_ref == "v1"
        assert dict(found[0].spec.labels) == {"desk": "a"}
        assert found[0].status is not None and found[0].status.pid == pid
        held = await second.status(spec.id)
        assert held is not None
        assert held.pid == pid
        assert held.phase in (WorkerPhase.STARTING, WorkerPhase.RUNNING)
        with pytest.raises(ProcmanError):
            await second.spawn(
                _spec(
                    _argv(_SLEEP),
                    incarnation=2,
                    hb_timeout_s=None,
                    start_timeout_s=30,
                )
            )
        shims, workers = _ps_family(tmp_path)
        assert workers == {pid}
        assert len(shims) == 1
        os.kill(next(iter(shims)), signal.SIGKILL)
        _wait_until(
            lambda: marker.is_file()
            and marker.read_text() == "sigterm"
            and not _pid_running(pid)
        )
        assert not exit_record_path(tmp_path, spec.id).exists()
        assert not exit_record_tmp_path(tmp_path, spec.id).exists()
        shims, workers = _ps_family(tmp_path)
        assert shims == set()
        assert pid not in workers
    finally:
        if second is not None:
            await _cleanup(second)
        await _cleanup(first)


@pytest.mark.integration
async def test_reattach_rearms_the_heartbeat_window(tmp_path: Path) -> None:
    """The counter has no timestamp. The deadline starts at the first
    observation after reattach, so a worker is not killed during start."""
    spec = _spec(_argv(_READY_SLEEP), start_timeout_s=30, hb_timeout_s=0.6)
    first = Supervisor(tmp_path, plane="td", instance="td")
    second: Supervisor | None = None
    try:
        await first.spawn(spec)
        await _until(
            first,
            spec.id,
            lambda item: item is not None and item.phase is WorkerPhase.RUNNING,
        )
        pid = (await first.status(spec.id)).pid
        await first.close(CloseMode.DETACH)
        second = Supervisor(tmp_path, plane="td", instance="td")
        found = await second.start()
        assert found[0].observed is ObservedWorker.RUNNING
        assert found[0].status is not None and found[0].status.pid == pid
        await asyncio.sleep(0.25)
        status = await second.status(spec.id)
        assert status is not None
        assert status.phase is WorkerPhase.RUNNING
        assert status.pid == pid
        assert _pid_running(pid)
    finally:
        if second is not None:
            await _cleanup(second)
        await _cleanup(first)


@pytest.mark.integration
async def test_spawn_over_lost_refuses_while_the_old_pid_is_alive(
    tmp_path: Path,
) -> None:
    ignoring = tmp_path / "ignoring"
    spec = _spec(
        _argv(_IGNORE_TERM, str(ignoring)),
        start_timeout_s=30,
        hb_timeout_s=None,
        stop_grace_s=0.3,
    )
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    try:
        await supervisor.spawn(spec)
        await _until(
            supervisor,
            spec.id,
            lambda item: (
                item is not None and item.pid is not None and ignoring.exists()
            ),
        )
        pid = (await supervisor.status(spec.id)).pid
        assert pid is not None
        shim = _peer(socket_path(tmp_path, spec.id))
        assert shim is not None
        os.kill(shim, signal.SIGKILL)
        await _until(
            supervisor,
            spec.id,
            lambda item: item is not None and item.phase is WorkerPhase.LOST,
        )
        assert _pid_running(pid)
        with pytest.raises(ProcmanError, match=f"worker pid {pid} is still alive"):
            await supervisor.spawn(
                _spec(
                    _argv(_SLEEP),
                    incarnation=2,
                    start_timeout_s=30,
                    hb_timeout_s=None,
                )
            )
        assert _pid_running(pid)
        assert (await supervisor.status(spec.id)).phase is WorkerPhase.LOST
        shims, _workers = _ps_family(tmp_path)
        assert shim not in shims
    finally:
        await _cleanup(supervisor)
        _reap_workdir(tmp_path)


@pytest.mark.integration
async def test_spawn_over_lost_replaces_the_worker_once_its_pid_is_gone(
    tmp_path: Path,
) -> None:
    spec = _spec(_argv(_SLEEP), start_timeout_s=30, hb_timeout_s=None)
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    try:
        await supervisor.spawn(spec)
        await _until(
            supervisor,
            spec.id,
            lambda item: item is not None and item.pid is not None,
        )
        pid = (await supervisor.status(spec.id)).pid
        assert pid is not None
        shim = _peer(socket_path(tmp_path, spec.id))
        assert shim is not None
        os.kill(shim, signal.SIGKILL)
        await _until(
            supervisor,
            spec.id,
            lambda item: item is not None and item.phase is WorkerPhase.LOST,
        )
        _wait_until(lambda: not _pid_running(pid))
        await supervisor.spawn(
            _spec(_argv(_SLEEP), incarnation=2, start_timeout_s=30, hb_timeout_s=None)
        )
        status = await supervisor.status(spec.id)
        assert status is not None
        assert status.spec.incarnation == 2
        assert status.pid is not None and status.pid != pid
        assert _pid_running(status.pid)
        shims, workers = _ps_family(tmp_path)
        assert workers == {status.pid}
        assert len(shims) == 1
    finally:
        await _cleanup(supervisor)
        _reap_workdir(tmp_path)


@pytest.mark.integration
async def test_spawn_without_start_refuses_a_worker_still_recorded_alive(
    tmp_path: Path,
) -> None:
    """The fence reads ``supervisor.json`` when this process holds no slot."""
    ready = tmp_path / "ready"
    spec = _spec(
        _argv(_CATCH_TERM, str(tmp_path / "caught"), str(ready)),
        hb_timeout_s=None,
        start_timeout_s=30,
    )
    first = Supervisor(tmp_path, plane="td", instance="td")
    second: Supervisor | None = None
    try:
        await first.spawn(spec)
        await _until(
            first,
            spec.id,
            lambda item: item is not None and item.pid is not None and ready.exists(),
        )
        pid = (await first.status(spec.id)).pid
        assert pid is not None
        _wait_until(
            lambda: any(
                row.worker_pid == pid for row in load_supervisor_state(tmp_path)
            )
        )
        await first.close(CloseMode.DETACH)
        second = Supervisor(tmp_path, plane="td", instance="td")
        with pytest.raises(ProcmanError, match=f"worker pid {pid} is still alive"):
            await second.spawn(
                _spec(
                    _argv(_SLEEP),
                    incarnation=2,
                    hb_timeout_s=None,
                    start_timeout_s=30,
                )
            )
        assert _pid_running(pid)
        _shims, workers = _ps_family(tmp_path)
        assert workers == {pid}
    finally:
        if second is not None:
            await _cleanup(second)
        await _cleanup(first)

