"""What B3 has to make true of procman, written before it exists.

S1–S7 spawn real processes and are ``integration``. B3-01 makes them
pass. Failure classification, backoff and intensity pass as of B3-02.
Reattach and detach/stop pass as of B3-03.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import textwrap
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest
from mftik.procman import (
    CloseMode,
    DesiredSlot,
    FailureCause,
    ObservedWorker,
    ReattachAction,
    RestartIntensity,
    ShimClient,
    SpawnedShim,
    Supervisor,
    Trigger,
    WorkerHeartbeat,
    WorkerPhase,
    WorkerSpec,
    classify_failure,
    count_restarts_in_window,
    decode_exit,
    exit_record_path,
    exit_record_tmp_path,
    observe_heartbeat,
    plan_restart,
    reattach_action,
    socket_path,
    spawn_shim,
    transition,
)

_INTENSITY = RestartIntensity(max_restarts=5, window_s=600, min_backoff_s=1)


def _argv(source: str, *args: str) -> tuple[str, ...]:
    return (sys.executable, "-c", textwrap.dedent(source).strip(), *args)


def _spec(argv: tuple[str, ...], **overrides: object) -> WorkerSpec:
    raw: dict[str, object] = {
        "id": "td/account/42",
        "plane": "td",
        "kind": "account",
        "incarnation": 1,
        "argv": argv,
        "env": {},
        "code_ref": "v1",
        "restart": "on_failure",
        "start_timeout_s": 60,
        "hb_timeout_s": 3,
        "oom_score_adj": 100,
        "rlimit_data_bytes": None,
        "stop_grace_s": 8,
        "labels": {},
    }
    raw.update(overrides)
    return WorkerSpec(**raw)  # type: ignore[arg-type]


def _ppid(pid: int) -> int:
    """Field 4 of ``/proc/<pid>/stat``. ``comm`` is in parentheses and may
    contain spaces, so the split starts after the last ``)``."""
    stat = Path(f"/proc/{pid}/stat").read_text()
    after = stat.rsplit(")", 1)[1].split()
    return int(after[1])


def _alive(pid: int) -> bool:
    return Path(f"/proc/{pid}").exists()


def _wait_for(predicate, timeout_s: float = 3.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError(f"condition was still false after {timeout_s}s")


def _children(pid: int) -> list[int]:
    found: list[int] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            text = (entry / "status").read_text()
        except OSError:
            continue
        for line in text.splitlines():
            if line.startswith("PPid:"):
                if int(line.split()[1]) == pid:
                    found.append(int(entry.name))
                break
    return found


def _kill_tree(pid: int) -> None:
    """SIGKILL ``pid`` and everything reparented onto it (S1's grandchild)."""
    if pid <= 1 or pid == os.getpid():
        return
    for child in _children(pid):
        _kill_tree(child)
    try:
        os.kill(pid, signal.SIGKILL)
    except OSError:
        pass


def _ancestors(pid: int) -> set[int]:
    found: set[int] = set()
    while pid > 0 and pid not in found:
        found.add(pid)
        if pid == 1:
            break
        try:
            pid = _ppid(pid)
        except OSError:
            break
    found.add(1)
    return found


def _adopter() -> int:
    """Who adopts an orphan of this process: pid 1 or the nearest subreaper.

    ``prctl(PR_GET_CHILD_SUBREAPER)`` reports only the calling process, so
    an ancestor's flag is not readable from here. Orphan a grandchild and
    record its new parent. That is the same rule the kernel applies to
    the shim after the double-fork (§4.2 S1), including inside a
    container whose init is not the host's pid 1.
    """
    # A fresh interpreter, not this process: pytest's timeout hook is
    # already multi-threaded, and forking here deadlocks.
    script = (
        "import os, time\n"
        "grand = os.fork()\n"
        "if grand > 0:\n"
        "    os._exit(0)\n"
        "parent = os.getppid()\n"
        "deadline = time.monotonic() + 2.0\n"
        "while os.getppid() == parent and time.monotonic() < deadline:\n"
        "    time.sleep(0)\n"
        "print(os.getppid(), flush=True)\n"
        "os._exit(0)\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
    )
    return int(proc.stdout.strip())


def _socket_refuses(path: Path) -> bool:
    """True when nothing accepts on ``path`` (S2: the shim is gone)."""
    if not path.exists() and not path.is_symlink():
        return True
    try:
        ShimClient(path).status()
    except OSError:
        return True
    return False


def _cleanup(spawned: SpawnedShim) -> None:
    client = ShimClient(spawned.socket)
    try:
        client.signal(signal.SIGKILL)
    except Exception:
        pass
    # A setsid grandchild is not in the worker's group, so killpg misses
    # it. It is the shim's child once the intermediate has exited. Do not
    # walk up to the shim's parent: that is init or the host subreaper.
    for child in _children(spawned.pid):
        _kill_tree(child)
    try:
        client.release()
    except Exception:
        pass
    _kill_tree(spawned.pid)


@contextmanager
def _running(spec: WorkerSpec, work_dir: Path) -> Iterator[SpawnedShim]:
    spawned: SpawnedShim | None = None
    try:
        spawned = spawn_shim(spec, work_dir=work_dir)
        yield spawned
    finally:
        if spawned is not None:
            _cleanup(spawned)


# --- S1 to S7: real subprocesses, B3-01 ------------------------------------

_S1 = """
import os, sys, time
marker = sys.argv[1]
intermediate = os.fork()
if intermediate == 0:
    os.setsid()
    grandchild = os.fork()
    if grandchild == 0:
        # The intermediate exits immediately. If this process is scheduled
        # after that exit, the first getppid() is already the shim and a
        # loop that waits for a change never ends. Bound the wait, then
        # record whoever the parent is: the assertion still requires the shim.
        parent = os.getppid()
        deadline = time.monotonic() + 0.5
        while os.getppid() == parent and time.monotonic() < deadline:
            time.sleep(0.01)
        with open(marker, "w") as handle:
            handle.write(f"{os.getpid()} {os.getppid()}")
        time.sleep(30)
    else:
        os._exit(0)
else:
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

_EXIT_3 = """
raise SystemExit(3)
"""

_STDOUT = """
import sys, time
marker = sys.argv[1]
sys.stdout.write("x" * (2 * 1024 * 1024))
sys.stdout.flush()
with open(marker, "w") as handle:
    handle.write("wrote")
time.sleep(30)
"""

_GROUP = """
import os, signal, sys, time
parent_marker, child_marker, ready = sys.argv[1:]
def _arm(path):
    def _on_term(signum, frame):
        with open(path, "w") as handle:
            handle.write("sigterm")
        raise SystemExit(0)
    signal.signal(signal.SIGTERM, _on_term)
child = os.fork()
if child == 0:
    _arm(child_marker)
    time.sleep(30)
else:
    _arm(parent_marker)
    with open(ready, "w") as handle:
        handle.write("ready")
    time.sleep(30)
"""

_HEARTBEAT = """
import os, sys, time
fd = int(os.environ["MFTIK_STATUS_FD"])
marker = sys.argv[1]
os.write(fd, b'{"ready":true}\\n')
with open(marker, "w") as handle:
    handle.write("sent")
time.sleep(30)
"""


@pytest.mark.integration
def test_s1_shim_is_the_only_parent_and_reaps_descendants(tmp_path: Path) -> None:
    """S1: the shim parents the worker, init parents the shim, and an
    orphaned descendant is reparented to the shim rather than to init."""
    marker = tmp_path / "grandchild"
    spec = _spec(_argv(_S1, str(marker)))
    with _running(spec, tmp_path) as spawned:
        status = ShimClient(spawned.socket).status()
        assert status.id == spec.id
        assert status.incarnation == spec.incarnation
        assert status.pid is not None
        assert _ppid(status.pid) == spawned.pid
        adopter = _adopter()
        assert adopter in _ancestors(os.getpid())
        assert _ppid(spawned.pid) == adopter

        def _grandchild() -> list[str]:
            try:
                parts = marker.read_text().split()
            except OSError:
                return []
            return parts if len(parts) == 2 else []

        # ``open`` creates the file before the write is visible.
        _wait_for(lambda: len(_grandchild()) == 2)
        grandchild_pid, grandchild_ppid = (int(part) for part in _grandchild())
        assert grandchild_ppid == spawned.pid
        os.kill(grandchild_pid, signal.SIGKILL)


@pytest.mark.integration
def test_s2_killing_the_shim_stops_the_worker_gracefully(tmp_path: Path) -> None:
    """S2: PDEATHSIG is SIGTERM (or the status pipe's EPIPE, whichever is
    first). The worker exits on its own. The dead shim leaves no exit
    record, so reattach reads the slot as LOST (§4.4)."""
    marker = tmp_path / "caught"
    ready = tmp_path / "ready"
    spec = _spec(_argv(_CATCH_TERM, str(marker), str(ready)))
    spawned = spawn_shim(spec, work_dir=tmp_path)
    worker_pid: int | None = None
    try:
        _wait_for(ready.exists)
        status = ShimClient(spawned.socket).status()
        worker_pid = status.pid
        assert worker_pid is not None
        os.kill(spawned.pid, signal.SIGKILL)
        # ``open`` creates the file before the write is visible.
        _wait_for(lambda: marker.is_file() and marker.read_text() == "sigterm")
        _wait_for(lambda: not _alive(worker_pid), timeout_s=spec.stop_grace_s)
        assert not exit_record_path(tmp_path, spec.id).exists()
        assert not exit_record_tmp_path(tmp_path, spec.id).exists()
        assert _socket_refuses(spawned.socket)
        # state.py: reattach reads LOST only when the socket and the exit
        # file are both gone. That observation is Trigger.SHIM_LOST.
        for phase in (
            WorkerPhase.STARTING,
            WorkerPhase.RUNNING,
            WorkerPhase.STOPPING,
        ):
            assert transition(phase, Trigger.SHIM_LOST) is WorkerPhase.LOST
    finally:
        if worker_pid is not None:
            _kill_tree(worker_pid)
        _cleanup(spawned)


@pytest.mark.integration
def test_s3_exit_record_is_durable_and_the_shim_waits_for_release(
    tmp_path: Path,
) -> None:
    """S3: the code is on disk before ``release``, and the shim is still
    alive to be asked. ``release`` is what lets it exit."""
    spec = _spec(_argv(_EXIT_3))
    spawned = spawn_shim(spec, work_dir=tmp_path)
    try:
        record_path = exit_record_path(tmp_path, spec.id)
        _wait_for(record_path.exists)
        body = record_path.read_bytes()
        record = decode_exit(body)
        assert record.exit_code == 3
        assert record.signal is None
        assert record.ready is False
        assert record.id == spec.id
        assert record.incarnation == spec.incarnation
        assert _alive(spawned.pid)
        status = ShimClient(spawned.socket).status()
        assert status.exit_code == 3
        ShimClient(spawned.socket).release()
        _wait_for(lambda: not _alive(spawned.pid))
        assert record_path.read_bytes() == body
    finally:
        _cleanup(spawned)


@pytest.mark.integration
def test_s4_a_full_stdout_does_not_stall_or_kill_the_worker(tmp_path: Path) -> None:
    """S4: nobody is reading the shim's log. A multi-megabyte write still
    finishes, and the worker is still running afterwards."""
    marker = tmp_path / "wrote"
    spec = _spec(_argv(_STDOUT, str(marker)))
    with _running(spec, tmp_path) as spawned:
        _wait_for(marker.exists)
        status = ShimClient(spawned.socket).status()
        assert status.pid is not None
        assert status.exit_code is None
        assert status.signal is None
        assert _alive(status.pid)


@pytest.mark.integration
def test_s5_status_reports_id_incarnation_and_the_socket_path(tmp_path: Path) -> None:
    spec = _spec(_argv(_CATCH_TERM, str(tmp_path / "caught"), str(tmp_path / "ready")))
    with _running(spec, tmp_path) as spawned:
        assert spawned.socket == socket_path(tmp_path, spec.id)
        assert spawned.socket.is_socket()
        status = ShimClient(spawned.socket).status()
        assert status.id == spec.id
        assert status.incarnation == spec.incarnation
        assert status.pid is not None


@pytest.mark.integration
def test_s5_signal_reaches_the_workers_process_group(tmp_path: Path) -> None:
    """S5: ``signal`` is ``killpg`` on the worker's group, so a child that
    stayed in the group receives it too."""
    parent_marker = tmp_path / "parent"
    child_marker = tmp_path / "child"
    ready = tmp_path / "ready"
    spec = _spec(_argv(_GROUP, str(parent_marker), str(child_marker), str(ready)))
    with _running(spec, tmp_path) as spawned:
        _wait_for(ready.exists)
        ShimClient(spawned.socket).signal(signal.SIGTERM)
        # ``open`` creates the file before the write is visible.
        _wait_for(
            lambda: parent_marker.is_file()
            and child_marker.is_file()
            and parent_marker.read_text() == "sigterm"
            and child_marker.read_text() == "sigterm"
        )


@pytest.mark.integration
def test_s5_watch_yields_the_current_status_without_waiting_for_a_beat(
    tmp_path: Path,
) -> None:
    spec = _spec(_argv(_CATCH_TERM, str(tmp_path / "caught"), str(tmp_path / "ready")))
    with _running(spec, tmp_path) as spawned:
        seen: list[object] = []

        def _take_first() -> None:
            for item in ShimClient(spawned.socket).watch():
                seen.append(item)
                return

        thread = threading.Thread(target=_take_first)
        thread.start()
        thread.join(3)
        assert not thread.is_alive()
        assert seen
        assert seen[0].id == spec.id  # type: ignore[attr-defined]
        assert seen[0].incarnation == spec.incarnation  # type: ignore[attr-defined]


@pytest.mark.integration
def test_s6_a_heartbeat_makes_status_ready(tmp_path: Path) -> None:
    """S6, the channel: the shim puts ``MFTIK_STATUS_FD`` in the worker's
    environment, and a full snapshot on that pipe shows up on ``status``."""
    marker = tmp_path / "sent"
    spec = _spec(_argv(_HEARTBEAT, str(marker)))
    with _running(spec, tmp_path) as spawned:
        _wait_for(marker.exists)
        status = ShimClient(spawned.socket).status()
        assert status.ready is True
        assert status.beats == 1


@pytest.mark.parametrize(
    ("previous", "ready", "expected"),
    [
        (False, None, False),
        (True, None, True),
        (False, True, True),
        (True, False, False),
        (False, False, False),
    ],
)
def test_s6_a_heartbeat_replaces_ready_wholesale(
    previous: bool, ready: bool | None, expected: bool
) -> None:
    """S6: ``None`` is a beat the shim dropped because the pipe was full.
    The previous snapshot stands. A beat that arrives replaces it, because
    the message is the whole state."""
    beat = None if ready is None else WorkerHeartbeat(ready=ready)
    assert observe_heartbeat(previous_ready=previous, beat=beat) is expected


@pytest.mark.integration
def test_s7_sigterm_to_the_shim_is_forwarded_and_the_shim_stays(
    tmp_path: Path,
) -> None:
    """S7: the shim forwards SIGTERM to the worker and does not exit.
    ``release`` is still what ends it."""
    marker = tmp_path / "caught"
    ready = tmp_path / "ready"
    spec = _spec(_argv(_CATCH_TERM, str(marker), str(ready)))
    spawned = spawn_shim(spec, work_dir=tmp_path)
    try:
        _wait_for(ready.exists)
        os.kill(spawned.pid, signal.SIGTERM)
        # ``open`` creates the file before the write is visible.
        _wait_for(lambda: marker.is_file() and marker.read_text() == "sigterm")
        assert _alive(spawned.pid)
        status = ShimClient(spawned.socket).status()
        assert status.exit_code == 0
        ShimClient(spawned.socket).release()
        _wait_for(lambda: not _alive(spawned.pid))
    finally:
        _cleanup(spawned)


# --- §4.4 reattach ---------------------------------------------------------


def _reattach_rows() -> list[tuple[str, DesiredSlot, ObservedWorker, ReattachAction]]:
    rows: list[tuple[str, DesiredSlot, ObservedWorker, ReattachAction]] = []
    gone = (ObservedWorker.ABSENT, ObservedWorker.EXITED, ObservedWorker.LOST)
    for plane in ("sts", "md", "td"):
        rows.append(
            (plane, DesiredSlot.PRESENT, ObservedWorker.RUNNING, ReattachAction.ADOPT)
        )
        rows.append(
            (
                plane,
                DesiredSlot.ABSENT,
                ObservedWorker.RUNNING,
                ReattachAction.STOP_AND_RELEASE,
            )
        )
    for observed in gone:
        rows.append(("sts", DesiredSlot.PRESENT, observed, ReattachAction.MARK_FAILED))
        rows.append(("md", DesiredSlot.PRESENT, observed, ReattachAction.APPLY_RESTART))
        rows.append(("td", DesiredSlot.PRESENT, observed, ReattachAction.APPLY_RESTART))
        for plane in ("sts", "md", "td"):
            rows.append((plane, DesiredSlot.ABSENT, observed, ReattachAction.NONE))
    return rows


@pytest.mark.parametrize(("plane", "desired", "observed", "action"), _reattach_rows())
def test_reattach_follows_the_section_4_4_table(
    plane: str, desired: DesiredSlot, observed: ObservedWorker, action: ReattachAction
) -> None:
    assert (
        reattach_action(plane=plane, desired=desired, observed=observed) is action  # type: ignore[arg-type]
    )


@pytest.mark.integration
async def test_detach_leaves_the_worker_running(tmp_path: Path) -> None:
    """§4.4: ``close(detach)`` does not signal. The same pid answers after."""
    ready = tmp_path / "ready"
    spec = _spec(_argv(_CATCH_TERM, str(tmp_path / "caught"), str(ready)))
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    client: ShimClient | None = None
    worker: int | None = None
    try:
        await supervisor.start()
        await supervisor.spawn(spec)
        _wait_for(ready.exists)
        client = ShimClient(socket_path(tmp_path, spec.id))
        before = client.status()
        assert before.pid is not None and before.exit_code is None
        worker = before.pid
        await supervisor.close(CloseMode.DETACH)
        after = client.status()
        assert after.pid == before.pid
        assert after.exit_code is None
        assert _alive(before.pid)
    finally:
        if client is not None:
            try:
                client.signal(signal.SIGKILL)
            except OSError:
                pass
            try:
                client.release()
            except OSError:
                pass
        if worker is not None:
            _kill_tree(worker)


@pytest.mark.integration
async def test_stop_ends_the_worker(tmp_path: Path) -> None:
    """§4.4: ``close(stop)`` is the close that signals workers."""
    ready = tmp_path / "ready"
    spec = _spec(_argv(_CATCH_TERM, str(tmp_path / "caught"), str(ready)))
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    pid: int | None = None
    try:
        await supervisor.start()
        await supervisor.spawn(spec)
        _wait_for(ready.exists)
        reported = ShimClient(socket_path(tmp_path, spec.id)).status().pid
        assert reported is not None
        pid = reported
        await supervisor.close(CloseMode.STOP)
        _wait_for(lambda: not _alive(pid))
    finally:
        if pid is not None:
            _kill_tree(pid)


# --- FAILED / CRASHED, backoff, intensity: B3-02 ---------------------------


@pytest.mark.parametrize(
    ("ready", "cause", "phase"),
    [
        (False, FailureCause.DEATH, WorkerPhase.FAILED),
        (False, FailureCause.START_TIMEOUT, WorkerPhase.FAILED),
        (True, FailureCause.DEATH, WorkerPhase.CRASHED),
        (True, FailureCause.HEARTBEAT_TIMEOUT, WorkerPhase.CRASHED),
    ],
)
def test_failure_before_ready_is_failed_and_after_ready_is_crashed(
    ready: bool, cause: FailureCause, phase: WorkerPhase
) -> None:
    assert classify_failure(ready=ready, cause=cause) is phase


def test_a_failure_before_ready_is_not_restarted() -> None:
    phase = classify_failure(ready=False, cause=FailureCause.DEATH)
    assert phase is WorkerPhase.FAILED
    decision = plan_restart(
        phase=phase,
        restart="on_failure",
        restarts_in_window=0,
        intensity=_INTENSITY,
        attempt=1,
    )
    assert decision.phase is WorkerPhase.FAILED
    assert decision.delay_s is None


def test_never_leaves_a_crash_where_it_is() -> None:
    decision = plan_restart(
        phase=WorkerPhase.CRASHED,
        restart="never",
        restarts_in_window=0,
        intensity=_INTENSITY,
        attempt=1,
    )
    assert decision.phase is WorkerPhase.CRASHED
    assert decision.delay_s is None


def test_four_restarts_in_the_window_still_back_off() -> None:
    """``max_restarts=5`` allows a fifth restart. Four already done, so this
    one still backs off."""
    decision = plan_restart(
        phase=WorkerPhase.CRASHED,
        restart="on_failure",
        restarts_in_window=4,
        intensity=_INTENSITY,
        attempt=5,
    )
    assert decision.phase is WorkerPhase.BACKOFF
    assert decision.delay_s is not None
    assert decision.delay_s >= _INTENSITY.min_backoff_s


def test_the_restart_past_max_restarts_is_fatal() -> None:
    """Five already done fills the window. ``attempt`` does not reopen it."""
    decision = plan_restart(
        phase=WorkerPhase.CRASHED,
        restart="on_failure",
        restarts_in_window=5,
        intensity=_INTENSITY,
        attempt=1,
    )
    assert decision.phase is WorkerPhase.FATAL
    assert decision.delay_s is None


def test_backoff_grows_and_respects_the_minimum() -> None:
    delays: list[float] = []
    for attempt in range(1, 5):
        decision = plan_restart(
            phase=WorkerPhase.CRASHED,
            restart="on_failure",
            restarts_in_window=attempt - 1,
            intensity=_INTENSITY,
            attempt=attempt,
        )
        assert decision.phase is WorkerPhase.BACKOFF
        assert decision.delay_s is not None
        delays.append(decision.delay_s)
    assert delays[0] >= _INTENSITY.min_backoff_s
    assert delays == sorted(delays)
    assert delays[-1] > delays[0]


def test_restarts_outside_the_window_do_not_count() -> None:
    """Age ``== window_s`` is inside. Anything older is not."""
    assert (
        count_restarts_in_window(
            (0.0, 10.0, 100.0, 700.0), now_s=700.0, window_s=600.0
        )
        == 2
    )
    assert count_restarts_in_window((99.9,), now_s=700.0, window_s=600.0) == 0
    assert count_restarts_in_window((100.0,), now_s=700.0, window_s=600.0) == 1
