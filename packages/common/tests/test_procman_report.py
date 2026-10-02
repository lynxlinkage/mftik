"""B3-04: the liveness report, its generation, and the worker-tree Pss.

Payload, membership, generation, period and pause are unit tests: a
:class:`~mftik.clock.FakeClock`, a fake publisher, and a fake ``/proc``.
The one real worker is ``integration``.
"""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest
from mftik.clock import FakeClock
from mftik.procman import (
    REPORT_PERIOD_S,
    CloseMode,
    ProcmanError,
    Supervisor,
    WorkerPhase,
    WorkerSpec,
    publish_reports,
)
from mftik.procman.supervisor import _Slot
from mftik.protocol import (
    PROCMAN_REPORT,
    PROTOCOL_VERSION,
    ProcmanReportEnvelope,
    ProcmanWorker,
    Topics,
)

_LIVE = (WorkerPhase.STARTING, WorkerPhase.RUNNING, WorkerPhase.STOPPING)
_HELD_OUT = (
    WorkerPhase.FAILED,
    WorkerPhase.CRASHED,
    WorkerPhase.BACKOFF,
    WorkerPhase.FATAL,
    WorkerPhase.LOST,
    WorkerPhase.STOPPED,
)


def _spec(worker_id: str = "td/account/42", **overrides: object) -> WorkerSpec:
    raw: dict[str, object] = {
        "id": worker_id,
        "plane": "td",
        "kind": "account",
        "incarnation": 1,
        "argv": ("/bin/true",),
        "env": {},
        "code_ref": "v1",
        "restart": "on_failure",
        "start_timeout_s": 30,
        "hb_timeout_s": None,
        "oom_score_adj": 100,
        "rlimit_data_bytes": None,
        "stop_grace_s": 8,
        "labels": {},
    }
    raw.update(overrides)
    return WorkerSpec(**raw)  # type: ignore[arg-type]


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
        "pid": None,
        "exit_code": None,
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


def _write_proc(
    root: Path,
    pid: int,
    *,
    pss_kb: int | None,
    threads: dict[str, str] | None = None,
) -> None:
    base = root / str(pid)
    (base / "task").mkdir(parents=True)
    if pss_kb is not None:
        # ``Pss_Anon`` is the same memory broken out. It must not be added.
        (base / "smaps_rollup").write_text(
            f"Rss: {pss_kb * 2} kB\nPss: {pss_kb} kB\nPss_Anon: {pss_kb} kB\n"
        )
    for tid, children in (threads or {}).items():
        tid_dir = base / "task" / tid
        tid_dir.mkdir(parents=True, exist_ok=True)
        (tid_dir / "children").write_text(children)


def _worker(worker_id: str, **overrides: object) -> ProcmanWorker:
    raw: dict[str, object] = {
        "id": worker_id,
        "code_ref": "v1",
        "rss_bytes": None,
        "phase": "running",
        "ready": False,
        "incarnation": 1,
    }
    raw.update(overrides)
    return ProcmanWorker(**raw)  # type: ignore[arg-type]


async def _finish(task: asyncio.Task[None]) -> None:
    if not task.done():
        task.cancel()
    await asyncio.wait({task})


# --- pause and membership --------------------------------------------------


async def test_report_refuses_until_start_has_reconciled(tmp_path: Path) -> None:
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    _hold(supervisor, _spec(), WorkerPhase.RUNNING, pid=None)
    with pytest.raises(ProcmanError, match="until start finishes"):
        await supervisor.report()
    assert supervisor._generation == 0


async def test_report_opens_after_start_and_closes_with_the_supervisor(
    tmp_path: Path,
) -> None:
    """B3-03 calls ``allow_reports`` only after reconcile, and ``close`` pauses."""
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    with pytest.raises(ProcmanError, match="until start finishes"):
        await supervisor.report()
    assert supervisor._generation == 0
    await supervisor.start()
    report = await supervisor.report()
    assert report.generation == 1
    assert report.workers == []
    await supervisor.close(CloseMode.DETACH)
    with pytest.raises(ProcmanError, match="closed"):
        await supervisor.report()
    assert supervisor._generation == 1


async def test_close_pauses_and_an_unknown_mode_does_not(tmp_path: Path) -> None:
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    supervisor.allow_reports()
    with pytest.raises(ValueError):
        await supervisor.close("reboot")  # type: ignore[arg-type]
    report = await supervisor.report()
    assert report.generation == 1

    await supervisor.close(CloseMode.DETACH)
    with pytest.raises(ProcmanError, match="closed"):
        await supervisor.report()
    assert supervisor._generation == 1


async def test_report_lists_only_live_slots(tmp_path: Path) -> None:
    """STARTING, RUNNING, STOPPING. The rest stay held and stay off the wire."""
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    supervisor.allow_reports()
    live_ids: list[str] = []
    for index, phase in enumerate(_LIVE, start=1):
        worker_id = f"td/account/{index}"
        live_ids.append(worker_id)
        _hold(
            supervisor,
            _spec(worker_id, incarnation=index, code_ref=f"rel-{index}"),
            phase,
            ready=phase is not WorkerPhase.STARTING,
        )
    for index, phase in enumerate(_HELD_OUT, start=10):
        _hold(
            supervisor,
            _spec(f"td/account/{index}", incarnation=1),
            phase,
            pid=99999,
        )
    report = await supervisor.report()
    assert [worker.id for worker in report.workers] == live_ids
    running = report.workers[1]
    assert running.phase == "running"
    assert running.ready is True
    assert running.incarnation == 2
    assert running.code_ref == "rel-2"
    assert running.rss_bytes is None
    assert report.generation == 1


async def test_generation_increments_per_report_and_restarts_at_one(
    tmp_path: Path,
) -> None:
    first = Supervisor(tmp_path, plane="td", instance="td")
    first.allow_reports()
    assert [(await first.report()).generation for _ in range(3)] == [1, 2, 3]
    again = Supervisor(tmp_path, plane="md", instance="md-1")
    again.allow_reports()
    assert (await again.report()).generation == 1


async def test_status_returns_the_last_report_and_does_not_measure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[int | None] = []

    def fake(pid: int | None, _root: Path) -> int | None:
        calls.append(pid)
        return 4096 if pid else None

    monkeypatch.setattr("mftik.procman.supervisor._tree_pss_bytes", fake)
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    slot = _hold(supervisor, _spec(), WorkerPhase.RUNNING, pid=40, ready=True)
    before = await supervisor.status(slot.spec.id)
    assert before is not None
    assert before.rss_bytes is None
    assert calls == []

    supervisor.allow_reports()
    report = await supervisor.report()
    assert calls == [40]
    assert report.workers[0].rss_bytes == 4096
    after = await supervisor.status(slot.spec.id)
    assert after is not None
    assert after.rss_bytes == 4096
    assert calls == [40]


# --- Pss of a fake /proc tree ----------------------------------------------


def _tree(root: Path) -> None:
    # 10's main thread lists 20. Thread 11 lists 21, which lists 30.
    # 99 is named and has already exited. ``Pss_Anon`` must not be added.
    _write_proc(root, 10, pss_kb=100, threads={"10": "20\n", "11": "21\n"})
    _write_proc(root, 20, pss_kb=10, threads={"20": ""})
    _write_proc(root, 21, pss_kb=5, threads={"21": "30 99\n"})
    _write_proc(root, 30, pss_kb=7, threads={"30": ""})


async def test_rss_bytes_is_pss_of_the_worker_tree(tmp_path: Path) -> None:
    proc = tmp_path / "proc"
    _tree(proc)
    supervisor = Supervisor(tmp_path, plane="td", instance="td", proc_root=proc)
    supervisor.allow_reports()
    _hold(supervisor, _spec(), WorkerPhase.RUNNING, pid=10, ready=True)
    report = await supervisor.report()
    assert report.workers[0].rss_bytes == (100 + 10 + 5 + 7) * 1024


async def test_a_missing_worker_smaps_is_none_and_a_gone_child_is_skipped(
    tmp_path: Path,
) -> None:
    proc = tmp_path / "proc"
    _write_proc(proc, 51, pss_kb=1000, threads={"51": ""})
    _write_proc(proc, 60, pss_kb=40, threads={"60": "61 62\n"})
    _write_proc(proc, 61, pss_kb=8, threads={"61": ""})
    supervisor = Supervisor(tmp_path, plane="td", instance="td", proc_root=proc)
    supervisor.allow_reports()
    missing = _spec("td/account/1")
    partial = _spec("td/account/2")
    _hold(supervisor, missing, WorkerPhase.STARTING, pid=50)
    _hold(supervisor, partial, WorkerPhase.STOPPING, pid=60, ready=True)
    report = await supervisor.report()
    by_id = {worker.id: worker for worker in report.workers}
    assert by_id[missing.id].rss_bytes is None
    assert by_id[missing.id].phase == "starting"
    assert by_id[partial.id].rss_bytes == (40 + 8) * 1024
    assert by_id[partial.id].phase == "stopping"


def test_every_worker_phase_value_is_a_legal_report_phase() -> None:
    for phase in WorkerPhase:
        worker = _worker("td/account/1", phase=phase.value)
        assert worker.phase == phase.value


# --- publish loop ----------------------------------------------------------


async def test_the_loop_publishes_on_the_period_with_pv(tmp_path: Path) -> None:
    clock = FakeClock()
    supervisor = Supervisor(tmp_path, plane="td", instance="td-1", clock=clock)
    supervisor.allow_reports()
    _hold(supervisor, _spec(), WorkerPhase.RUNNING, ready=True)
    published: list[tuple[str, ProcmanReportEnvelope]] = []

    async def publish(subject: str, envelope: ProcmanReportEnvelope) -> None:
        published.append((subject, envelope))

    task = asyncio.create_task(
        publish_reports(
            supervisor,
            plane="td",
            instance="td-1",
            publish=publish,
            clock=clock,
        )
    )
    try:
        await asyncio.sleep(0)
        assert len(published) == 1
        subject, envelope = published[0]
        assert subject == Topics.procman_report("td", "td-1")
        assert envelope.type == PROCMAN_REPORT
        assert envelope.pv == PROTOCOL_VERSION
        assert envelope.source == "td"
        assert envelope.payload.generation == 1
        assert envelope.payload.workers[0].id == "td/account/42"
        clock.advance(REPORT_PERIOD_S - 0.1)
        await asyncio.sleep(0)
        assert len(published) == 1
        clock.advance(0.1)
        await asyncio.sleep(0)
        assert len(published) == 2
        assert published[1][1].payload.generation == 2
    finally:
        await _finish(task)
    clock.advance(REPORT_PERIOD_S)
    await asyncio.sleep(0)
    assert len(published) == 2


async def test_the_loop_stays_quiet_until_reconciled_and_stops_on_close(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    supervisor = Supervisor(tmp_path, plane="td", instance="td", clock=clock)
    calls = 0

    def extras() -> list[ProcmanWorker]:
        nonlocal calls
        calls += 1
        return []

    published: list[ProcmanReportEnvelope] = []

    async def publish(_subject: str, envelope: ProcmanReportEnvelope) -> None:
        published.append(envelope)

    task = asyncio.create_task(
        publish_reports(
            supervisor,
            plane="td",
            instance="td",
            publish=publish,
            clock=clock,
            extra_workers=extras,
        )
    )
    try:
        await asyncio.sleep(0)
        for _ in range(3):
            clock.advance(REPORT_PERIOD_S)
            await asyncio.sleep(0)
        assert published == []
        assert calls == 0
        supervisor.allow_reports()
        clock.advance(REPORT_PERIOD_S)
        await asyncio.sleep(0)
        assert len(published) == 1
        assert calls == 1
        assert published[0].payload.workers == []
        await supervisor.close(CloseMode.STOP)
        clock.advance(REPORT_PERIOD_S)
        await asyncio.sleep(0)
        assert len(published) == 1
        assert task.done()
    finally:
        await _finish(task)


async def test_an_empty_open_report_is_published_and_extras_are_appended(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    supervisor = Supervisor(tmp_path, plane="sts", instance="sts-jp", clock=clock)
    supervisor.allow_reports()
    _hold(
        supervisor,
        _spec("sts/session/live", plane="sts", kind="session"),
        WorkerPhase.RUNNING,
        ready=True,
    )
    calls = 0

    def extras() -> list[ProcmanWorker]:
        nonlocal calls
        calls += 1
        return [
            _worker(
                "sts/session/restarting",
                phase="running",
                ready=False,
                incarnation=2,
                code_ref="old",
            )
        ]

    published: list[ProcmanReportEnvelope] = []

    async def publish(_subject: str, envelope: ProcmanReportEnvelope) -> None:
        published.append(envelope)

    task = asyncio.create_task(
        publish_reports(
            supervisor,
            plane="sts",
            instance="sts-jp",
            publish=publish,
            clock=clock,
            extra_workers=extras,
        )
    )
    try:
        await asyncio.sleep(0)
        assert calls == 1
        workers = published[0].payload.workers
        assert [worker.id for worker in workers] == [
            "sts/session/live",
            "sts/session/restarting",
        ]
        assert published[0].payload.generation == 1
    finally:
        await _finish(task)


async def test_the_loop_refuses_a_subject_for_another_supervisor(
    tmp_path: Path,
) -> None:
    supervisor = Supervisor(tmp_path, plane="td", instance="td")

    async def publish(_subject: str, _envelope: object) -> None:
        raise AssertionError("paused or mismatched reports are not published")

    with pytest.raises(ProcmanError, match="does not match"):
        await publish_reports(
            supervisor,
            plane="md",
            instance="td",
            publish=publish,
            clock=FakeClock(),
        )


# --- real process tree -----------------------------------------------------

_FORK = """
import os, sys, time
size = 8 * 1024 * 1024
held = bytearray(size)
held[::4096] = b"\\x01" * (size // 4096)
child = os.fork()
if child == 0:
    own = bytearray(size)
    own[::4096] = b"\\x01" * (size // 4096)
    with open(sys.argv[1], "w") as handle:
        handle.write(str(os.getpid()))
    time.sleep(60)
    os._exit(0)
with open(sys.argv[2], "w") as handle:
    handle.write(str(os.getpid()))
time.sleep(60)
"""


def _argv(source: str, *args: str) -> tuple[str, ...]:
    return (sys.executable, "-c", textwrap.dedent(source).strip(), *args)


def _ps() -> list[tuple[int, int, str]]:
    listing = subprocess.check_output(
        ["ps", "-ww", "-eo", "pid=,ppid=,args="], text=True
    )
    rows: list[tuple[int, int, str]] = []
    for line in listing.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) < 2:
            continue
        args = parts[2] if len(parts) == 3 else ""
        rows.append((int(parts[0]), int(parts[1]), args))
    return rows


def _read_pss(pid: int) -> int:
    text = Path(f"/proc/{pid}/smaps_rollup").read_text()
    for line in text.splitlines():
        if line.startswith("Pss:"):
            return int(line.split()[1]) * 1024
    raise AssertionError(f"no Pss for {pid}")


def _pid_running(pid: int) -> bool:
    if pid <= 1:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _kill_tree(pid: int) -> None:
    if pid <= 1 or pid == os.getpid():
        return
    children = Path(f"/proc/{pid}/task/{pid}/children")
    try:
        text = children.read_text()
    except OSError:
        text = ""
    for part in text.split():
        try:
            _kill_tree(int(part))
        except ValueError:
            continue
    try:
        os.kill(pid, signal.SIGKILL)
    except OSError:
        pass


async def _cleanup(supervisor: Supervisor, pids: list[int]) -> None:
    task = supervisor._driver
    if task is not None and not task.done():
        task.cancel()
        await asyncio.wait({task})
    for slot in list(supervisor._slots.values()):
        _kill_tree(slot.shim_pid)
        if slot.pid is not None:
            _kill_tree(slot.pid)
    supervisor._slots.clear()
    for pid in pids:
        _kill_tree(pid)


@pytest.mark.integration
async def test_report_pss_covers_a_forked_child_and_leaves_no_orphans(
    tmp_path: Path,
) -> None:
    child_path = tmp_path / "child"
    parent_path = tmp_path / "parent"
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    spec = _spec(
        argv=_argv(_FORK, str(child_path), str(parent_path)),
        start_timeout_s=30,
        hb_timeout_s=None,
    )
    watched: list[int] = []
    try:
        await supervisor.spawn(spec)
        deadline = time.monotonic() + 5
        status = None
        while time.monotonic() < deadline:
            status = await supervisor.status(spec.id)
            if (
                status is not None
                and status.pid is not None
                and child_path.exists()
                and parent_path.exists()
            ):
                break
            await asyncio.sleep(0.02)
        else:
            raise AssertionError((status, child_path.exists(), parent_path.exists()))
        assert status is not None and status.pid is not None
        assert status.rss_bytes is None
        parent = int(parent_path.read_text())
        child = int(child_path.read_text())
        watched.extend((parent, child))
        assert status.pid == parent
        assert any(pid == child and ppid == parent for pid, ppid, _args in _ps())
        parent_pss = _read_pss(parent)
        child_pss = _read_pss(child)
        supervisor.allow_reports()
        report = await supervisor.report()
        assert [worker.id for worker in report.workers] == [spec.id]
        got = report.workers[0].rss_bytes
        assert got is not None
        assert got >= parent_pss
        assert got >= child_pss
        assert abs(got - (parent_pss + child_pss)) < 1024 * 1024
        seen = await supervisor.status(spec.id)
        assert seen is not None and seen.rss_bytes == got
        await supervisor.stop(spec.id)
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            live = {pid for pid, _ppid, _args in _ps()}
            if parent not in live and child not in live:
                break
            await asyncio.sleep(0.05)
        live = {pid for pid, _ppid, _args in _ps()}
        assert parent not in live
        assert child not in live
        assert not _pid_running(parent)
        assert not _pid_running(child)
    finally:
        await _cleanup(supervisor, watched)
