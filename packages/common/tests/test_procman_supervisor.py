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
import sys
import textwrap
import time
from pathlib import Path

import pytest
from mftik.clock import FakeClock
from mftik.procman import (
    BACKOFF_RATIO,
    FailureCause,
    ProcmanError,
    RestartDecision,
    RestartIntensity,
    ShimClient,
    ShimStatus,
    Supervisor,
    WorkerPhase,
    WorkerSpec,
    classify_failure,
    decode_exit,
    exit_record_path,
    exit_record_tmp_path,
    plan_restart,
    socket_path,
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


async def test_spawn_refuses_a_live_slot_and_a_lost_one(tmp_path: Path) -> None:
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    spec = _spec()
    _hold(supervisor, spec, WorkerPhase.RUNNING)
    with pytest.raises(ProcmanError):
        await supervisor.spawn(spec)
    supervisor._slots.clear()
    _hold(supervisor, spec, WorkerPhase.LOST, exit_code=None)
    with pytest.raises(ProcmanError):
        await supervisor.spawn(_spec(incarnation=2))


async def test_spawn_refuses_an_incarnation_that_does_not_move_forward(
    tmp_path: Path,
) -> None:
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    spec = _spec()
    _hold(supervisor, spec, WorkerPhase.CRASHED)
    with pytest.raises(ProcmanError):
        await supervisor.spawn(spec)


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
import signal, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
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
    task = supervisor._driver
    if task is not None and not task.done():
        task.cancel()
        await asyncio.wait({task})
    for slot in list(supervisor._slots.values()):
        _stop_held(slot)
    supervisor._slots.clear()


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
    spec = _spec(_argv(_IGNORE_TERM), start_timeout_s=30, stop_grace_s=0.4)
    try:
        await supervisor.spawn(spec)
        await _until(
            supervisor,
            spec.id,
            lambda item: item is not None and item.pid is not None,
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
