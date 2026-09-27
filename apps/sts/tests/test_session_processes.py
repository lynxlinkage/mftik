"""One OS process per STS session.

The parent keeps the instance subject. A worker is not an instance. These
tests use a fake subprocess except for the one that execs ``mftik_sts.worker``.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
import time
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
from broker_harness import a_broker
from mftik.protocol import (
    STS_ERROR,
    STS_SESSION_CREATE,
    STS_SESSION_LIST,
    STS_SESSION_STOP,
    ListSessionsRequest,
    ListSessionsRequestEnvelope,
    StsCreateSessionRequest,
    StsCreateSessionRequestEnvelope,
    StsCreateSessionResult,
    StsSessionControlRequest,
    StsSessionControlRequestEnvelope,
    StsSessionControlResult,
    Topics,
)
from mftik.strategy import Strategy
from mftik_db.models import Base
from mftik_db.models.user import User
from mftik_db.repositories import StsSessionRepository
from mftik_db.session import build_engine
from mftik_sts.app import run_rpc
from mftik_sts.runtime_env import IncompatibleEnvironment
from mftik_sts.session import SessionManager
from mftik_sts.session import manager as manager_mod
from mftik_sts.spawn import (
    LIFELINE_FD_ENV,
    PARENT_PID_ENV,
    START_FAIL_REASON,
    SubprocessSpawner,
    WorkerSlot,
    _PipeWorker,
)
from mftik_sts.worker import arm_parent_death, set_pdeathsig
from sqlalchemy.ext.asyncio import async_sessionmaker


class Rebuildable(Strategy):
    name = "rebuildable"
    rebuildable = True


class _Broker:
    async def publish(self, *_args: object, **_kwargs: object) -> None:
        return None


class FakeProcess:
    """A subprocess the parent can signal and wait on, without an OS process."""

    def __init__(self, *, returncode: int | None = None) -> None:
        self.returncode = returncode
        self.signals: list[int] = []
        self._exit = asyncio.Event()
        if returncode is not None:
            self._exit.set()

    def send_signal(self, sig: int) -> None:
        self.signals.append(sig)
        if self.returncode is None:
            self.returncode = 0
            self._exit.set()

    def kill(self) -> None:
        self.signals.append(signal.SIGKILL)
        self.returncode = -9
        self._exit.set()

    async def wait(self) -> int:
        await self._exit.wait()
        return int(self.returncode or 0)


class StubbornProcess(FakeProcess):
    """SIGTERM is recorded and does not exit. SIGKILL does."""

    def send_signal(self, sig: int) -> None:
        self.signals.append(sig)


class FakeSpawned:
    def __init__(
        self,
        process: FakeProcess,
        line: str | None,
        gate: asyncio.Event | None = None,
    ) -> None:
        self.process = process
        self._line = line
        self.gate = gate
        self.lifeline: int | None = None

    async def read_result(self) -> str | None:
        if self.gate is not None:
            await self.gate.wait()
        return self._line


class FakeSpawner:
    def __init__(
        self,
        *,
        line: str | None = None,
        boom: bool = False,
        process: FakeProcess | None = None,
        gate: asyncio.Event | None = None,
    ) -> None:
        self.line = line
        self.boom = boom
        self.process = process
        self.gate = gate
        self.calls: list[dict[str, Any]] = []

    async def spawn(
        self,
        *,
        session_id: str,
        role: str,
        request_json: bytes | None,
    ) -> FakeSpawned:
        self.calls.append(
            {
                "session_id": session_id,
                "role": role,
                "request_json": request_json,
            }
        )
        if self.boom:
            raise OSError("exec failed")
        process = self.process or FakeProcess()
        if self.line is None and process.returncode is None:
            process.returncode = 1
            process._exit.set()
        return FakeSpawned(process, self.line, self.gate)


def _request(session_id: str = "s1") -> StsCreateSessionRequest:
    return StsCreateSessionRequest(
        session_id=session_id,
        created_by=1,
        strategy="rebuildable",
        type="Rebuildable",
    )


def _manager(
    spawner: FakeSpawner,
    store: dict[str, SimpleNamespace],
    *,
    rebuild: bool = False,
    load: bool = False,
) -> SessionManager:
    async def persist(**kwargs: Any) -> SimpleNamespace:
        row = SimpleNamespace(status="live", reason=None, **kwargs)
        store[kwargs["session_id"]] = row
        return row

    async def mark(session_id: str, *, status: str, reason: str | None) -> None:
        row = store[session_id]
        row.status = status
        row.reason = reason

    async def load_session(session_id: str) -> SimpleNamespace | None:
        return store.get(session_id)

    return SessionManager(
        _Broker(),  # type: ignore[arg-type]
        persist_live=persist,
        mark_done=mark,
        load_session=load_session if load else None,
        spawner=spawner,  # type: ignore[arg-type]
        strategy_factory=lambda _name: Rebuildable(),
        rebuild_on_worker_exit=rebuild,
        instance="sts",
    )


def _ok_line() -> str:
    return json.dumps(
        {"ok": True, "strategy": "rebuildable", "status": "live", "reason": None}
    )


async def test_slot_is_claimed_before_the_row_is_written() -> None:
    seen: list[bool] = []
    store: dict[str, SimpleNamespace] = {}
    spawner = FakeSpawner(line=_ok_line(), process=FakeProcess())

    async def persist(**kwargs: Any) -> SimpleNamespace:
        seen.append(kwargs["session_id"] in manager._workers)
        seen.append(manager._workers[kwargs["session_id"]].started)
        row = SimpleNamespace(status="live", reason=None, **kwargs)
        store[kwargs["session_id"]] = row
        return row

    manager = SessionManager(
        _Broker(),  # type: ignore[arg-type]
        persist_live=persist,
        mark_done=lambda *a, **k: _done(),
        spawner=spawner,  # type: ignore[arg-type]
        strategy_factory=lambda _name: Rebuildable(),
        instance="sts",
    )
    try:
        result = await manager.create_session(_request())
        assert seen == [True, False]
        assert result.strategy == "rebuildable"
        assert manager._workers["s1"].started is True
        body = json.loads(spawner.calls[0]["request_json"])
        assert body["session_id"] == "s1"
        assert spawner.calls[0]["role"] == "create"
    finally:
        await manager.close_all()


async def _done() -> None:
    return None


@pytest.mark.parametrize("load", [False, True])
async def test_eof_before_a_result_line_fails_and_does_not_rebuild(
    load: bool,
) -> None:
    store: dict[str, SimpleNamespace] = {}
    spawner = FakeSpawner(line=None)
    manager = _manager(spawner, store, rebuild=True, load=load)
    with pytest.raises(RuntimeError, match=START_FAIL_REASON):
        await manager.create_session(_request())
    await asyncio.sleep(0.05)

    assert store["s1"].status == "failed"
    assert store["s1"].reason == START_FAIL_REASON
    assert "s1" not in manager._workers
    assert len(spawner.calls) == 1
    await manager.close_all()


async def test_a_stop_during_start_keeps_the_row_the_worker_wrote() -> None:
    """Stopped while in on_start: the worker wrote ``done`` and left no line.

    The row already says what happened. Marking it ``failed`` would turn an
    operator's stop into a deploy failure.
    """
    store: dict[str, SimpleNamespace] = {}
    gate = asyncio.Event()
    spawner = FakeSpawner(line=None, gate=gate)
    manager = _manager(spawner, store, load=True)
    create = asyncio.create_task(manager.create_session(_request()))
    while not spawner.calls:
        await asyncio.sleep(0)
    store["s1"].status = "done"
    store["s1"].reason = "operator_stop"
    gate.set()
    with pytest.raises(RuntimeError, match=START_FAIL_REASON):
        await create

    assert store["s1"].status == "done"
    assert store["s1"].reason == "operator_stop"
    assert "s1" not in manager._workers
    await manager.close_all()


async def test_a_failed_result_line_is_not_rebuilt() -> None:
    """``start()`` already wrote ``failed``. The error line must not replace it."""
    store: dict[str, SimpleNamespace] = {}
    gate = asyncio.Event()
    spawner = FakeSpawner(
        line=json.dumps({"ok": False, "error": "on_start exploded"}),
        process=FakeProcess(returncode=1),
        gate=gate,
    )
    manager = _manager(spawner, store, rebuild=True, load=True)
    create = asyncio.create_task(manager.create_session(_request()))
    while "s1" not in store:
        await asyncio.sleep(0)
    store["s1"].status = "failed"
    store["s1"].reason = "start failed: on_start exploded"
    gate.set()
    with pytest.raises(RuntimeError, match="on_start exploded"):
        await create
    await asyncio.sleep(0.05)

    assert store["s1"].status == "failed"
    assert store["s1"].reason == "start failed: on_start exploded"
    assert "s1" not in manager._workers
    assert len(spawner.calls) == 1
    await manager.close_all()
    assert store["s1"].reason == "start failed: on_start exploded"


async def test_an_error_line_fails_a_row_the_worker_left_live() -> None:
    """Validation and ``__init__`` raise before ``start()`` writes the row.

    The parent already persisted ``live``. The error line is the worker
    saying it never got as far as ``start()``, so the row is still
    ``live`` and nothing is running it. Leaving it there makes stop 502
    and, after the reaper, a rebuild of a deploy that was rejected.
    """
    store: dict[str, SimpleNamespace] = {}
    spawner = FakeSpawner(
        line=json.dumps(
            {
                "ok": False,
                "error": "report_interval_ms must be positive, got -5",
            }
        ),
        process=FakeProcess(returncode=1),
    )
    manager = _manager(spawner, store, rebuild=True, load=True)
    with pytest.raises(RuntimeError, match="report_interval_ms must be positive"):
        await manager.create_session(_request())
    await asyncio.sleep(0.05)

    assert store["s1"].status == "failed"
    assert store["s1"].reason == (
        f"{START_FAIL_REASON}: report_interval_ms must be positive, got -5"
    )
    assert "s1" not in manager._workers
    assert len(spawner.calls) == 1
    await manager.close_all()
    assert store["s1"].status == "failed"


async def test_exec_failure_drops_the_slot_and_fails_the_row() -> None:
    store: dict[str, SimpleNamespace] = {}
    spawner = FakeSpawner(boom=True)
    manager = _manager(spawner, store)
    with pytest.raises(OSError, match="exec failed"):
        await manager.create_session(_request())

    assert "s1" not in manager._workers
    assert store["s1"].status == "failed"
    assert store["s1"].reason == START_FAIL_REASON
    await manager.close_all()


async def test_quiet_waits_until_the_control_loop_has_retired() -> None:
    """An empty ``_sessions`` is not the exit condition.

    Stop replies from inside the control task, after ``close`` has removed
    the session. The task stays in ``_retiring`` until that reply is done.
    """
    manager = SessionManager(_Broker())  # type: ignore[arg-type]
    manager._sessions["s1"] = SimpleNamespace()  # type: ignore[assignment]
    release = asyncio.Event()

    async def linger() -> None:
        await release.wait()

    task = asyncio.create_task(linger())
    manager._retiring.add(task)
    task.add_done_callback(manager._retiring.discard)
    waiter = asyncio.create_task(manager.wait_until_quiet())
    manager._sessions.pop("s1")
    await asyncio.sleep(0.05)
    assert not waiter.done()
    release.set()
    await waiter


async def test_the_same_id_is_spawned_once_when_rebuilds_overlap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The slot is claimed before the first await, so the second caller bows out."""
    monkeypatch.setattr(manager_mod, "ensure_deployable", lambda *_a, **_k: None)
    release = asyncio.Event()
    listed = asyncio.Event()
    row = SimpleNamespace(
        session_id="aa0001",
        created_by=1,
        strategy="rebuildable",
        type="Rebuildable",
        instance="sts",
        restart="always",
        rebuild_count=0,
        finished_at=datetime.now(UTC),
        st_facts={},
        st_paras={},
        td={},
        md_ids=[],
    )
    spawner = FakeSpawner(line=_ok_line(), process=FakeProcess())

    async def list_sessions(**_kwargs: Any) -> list[SimpleNamespace]:
        listed.set()
        await release.wait()
        return [row]

    async def bump(_session_id: str) -> int:
        row.rebuild_count += 1
        return row.rebuild_count

    manager = SessionManager(
        _Broker(),  # type: ignore[arg-type]
        list_db_sessions=list_sessions,
        bump_rebuild_count=bump,
        spawner=spawner,  # type: ignore[arg-type]
        strategy_factory=lambda _name: Rebuildable(),
        instance="sts",
    )
    first = asyncio.create_task(manager.rebuild_session("aa0001"))
    await listed.wait()
    assert await manager.rebuild_session("aa0001") is False
    release.set()
    assert await first is True
    assert len(spawner.calls) == 1
    assert spawner.calls[0]["role"] == "rebuild"
    assert spawner.calls[0]["request_json"] is None
    assert manager._workers["aa0001"].started is True
    assert manager._workers["aa0001"].strategy_name == "rebuildable"
    await manager.close_all()


async def test_a_rebuild_that_never_reports_is_not_failed_or_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No success line leaves the row interrupted. The next scan may retry.

    ``close_all`` afterwards does not see the slot, so it does not stamp
    the shutdown reason or move ``finished_at``.
    """
    monkeypatch.setattr(manager_mod, "ensure_deployable", lambda *_a, **_k: None)
    row = SimpleNamespace(
        session_id="aa0001",
        created_by=1,
        strategy="rebuildable",
        type="Rebuildable",
        instance="sts",
        restart="always",
        rebuild_count=0,
        status="interrupted",
        finished_at=datetime.now(UTC),
        st_facts={},
        st_paras={},
        td={},
        md_ids=[],
    )
    marked: list[str] = []
    spawner = FakeSpawner(line=None)

    async def list_sessions(**_kwargs: Any) -> list[SimpleNamespace]:
        return [row]

    async def bump(_session_id: str) -> int:
        row.rebuild_count += 1
        return row.rebuild_count

    async def mark(_session_id: str, *, status: str, reason: str | None) -> None:
        marked.append(status)
        row.status = status
        row.reason = reason

    manager = SessionManager(
        _Broker(),  # type: ignore[arg-type]
        list_db_sessions=list_sessions,
        bump_rebuild_count=bump,
        mark_done=mark,
        spawner=spawner,  # type: ignore[arg-type]
        strategy_factory=lambda _name: Rebuildable(),
        rebuild_on_worker_exit=True,
        instance="sts",
    )
    try:
        finished_at = row.finished_at
        assert await manager.rebuild_session("aa0001") is False
        await asyncio.sleep(0.05)
        assert marked == []
        assert row.status == "interrupted"
        assert row.finished_at == finished_at
        assert row.rebuild_count == 1
        assert "aa0001" not in manager._workers
        assert len(spawner.calls) == 1
        await manager.close_all()
        assert marked == []
        assert row.status == "interrupted"
        assert row.finished_at == finished_at
    finally:
        await manager.close_all()


async def test_rebuild_session_bumps_only_the_id_it_was_given(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = {
        "aa0001": SimpleNamespace(
            session_id="aa0001",
            strategy="bad",
            type="bad",
            instance="sts",
            restart="always",
            rebuild_count=0,
            finished_at=datetime.now(UTC),
        ),
        "aa0002": SimpleNamespace(
            session_id="aa0002",
            strategy="also-bad",
            type="also-bad",
            instance="sts",
            restart="always",
            rebuild_count=0,
            finished_at=datetime.now(UTC),
        ),
    }

    def ensure(type_name: str | None, _store: object = None) -> None:
        raise IncompatibleEnvironment(type_name or "", ("numpy",))

    monkeypatch.setattr(manager_mod, "ensure_deployable", ensure)

    async def list_sessions(**_kwargs: Any) -> list[SimpleNamespace]:
        return [rows["aa0001"]]

    async def bump(session_id: str) -> int:
        rows[session_id].rebuild_count += 1
        return rows[session_id].rebuild_count

    manager = SessionManager(
        _Broker(),  # type: ignore[arg-type]
        list_db_sessions=list_sessions,
        bump_rebuild_count=bump,
        strategy_factory=lambda _name: Rebuildable(),
        instance="sts",
    )
    assert await manager.rebuild_session("aa0001") is False
    assert rows["aa0001"].rebuild_count == 1
    assert rows["aa0002"].rebuild_count == 0


async def test_close_all_sends_sigterm_and_does_not_wait_out_on_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(manager_mod, "WORKER_STOP_WAIT_S", 0.05)
    store: dict[str, SimpleNamespace] = {}
    process = StubbornProcess()
    spawner = FakeSpawner(line=_ok_line(), process=process)
    manager = _manager(spawner, store)
    await manager.create_session(_request())
    await manager.close_all()

    assert signal.SIGTERM in process.signals
    assert signal.SIGKILL in process.signals
    assert store["s1"].status == "interrupted"


async def test_spawner_execs_a_worker_in_its_own_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    class _Stdin:
        def write(self, data: bytes) -> None:
            captured["stdin"] = data

        async def drain(self) -> None:
            return None

        def close(self) -> None:
            captured["closed"] = True

    class _Proc:
        def __init__(self) -> None:
            self.stdin = _Stdin()
            self.returncode = None

    async def fake_exec(*args: object, **kwargs: object) -> _Proc:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return _Proc()

    monkeypatch.setattr(
        "mftik_sts.spawn.asyncio.create_subprocess_exec", fake_exec
    )
    spawned = await SubprocessSpawner().spawn(
        session_id="aa0001",
        role="create",
        request_json=b'{"session_id":"aa0001"}',
    )
    try:
        assert await spawned.read_result() is None
    finally:
        os.close(spawned.lifeline)

    kwargs = captured["kwargs"]
    assert kwargs["start_new_session"] is True
    assert kwargs["stdout"] is None
    assert kwargs["stderr"] is None
    assert kwargs["stdin"] is asyncio.subprocess.PIPE
    assert captured["args"][1:] == (
        "-m",
        "mftik_sts.worker",
        "aa0001",
        "create",
    )
    env = kwargs["env"]
    assert env["MFTIK_DB_POOL_SIZE"] == "1"
    assert env[PARENT_PID_ENV] == str(os.getpid())
    assert env[LIFELINE_FD_ENV] == str(kwargs["pass_fds"][1])
    assert len(kwargs["pass_fds"]) == 2
    assert captured["stdin"] == b'{"session_id":"aa0001"}'
    assert captured["closed"] is True


async def test_a_cancelled_result_read_lets_go_of_the_pipe() -> None:
    """Read on the loop: cancelling it closes the fd at once.

    A read in a thread would keep the fd until the worker wrote or died.
    """
    read_fd, write_fd = os.pipe()
    worker = _PipeWorker(None, read_fd, -1)  # type: ignore[arg-type]
    task = asyncio.create_task(worker.read_result())
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    try:
        with pytest.raises(OSError):
            os.fstat(read_fd)
    finally:
        os.close(write_fd)


async def test_the_result_line_arrives_while_the_worker_keeps_the_pipe() -> None:
    read_fd, write_fd = os.pipe()
    worker = _PipeWorker(None, read_fd, -1)  # type: ignore[arg-type]
    try:
        os.write(write_fd, b'{"ok": true}\n')
        line = await asyncio.wait_for(worker.read_result(), timeout=2.0)
    finally:
        os.close(write_fd)
    assert line == '{"ok": true}\n'


def test_parent_pid_must_match(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(PARENT_PID_ENV, str(os.getppid()))
    try:
        arm_parent_death()
    finally:
        if sys.platform == "linux":
            set_pdeathsig(0)


def test_a_different_parent_pid_exits(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(PARENT_PID_ENV, str(os.getppid() + 1))
    try:
        with pytest.raises(SystemExit):
            arm_parent_death()
    finally:
        if sys.platform == "linux":
            set_pdeathsig(0)


def test_a_missing_parent_pid_exits(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(PARENT_PID_ENV, raising=False)
    with pytest.raises(SystemExit):
        arm_parent_death()


@pytest.mark.skipif(sys.platform != "linux", reason="PDEATHSIG is Linux")
def test_linux_arms_pdeathsig(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[int, int]] = []

    class _Prctl:
        argtypes: object = None
        restype: object = None

        def __call__(self, option: int, signum: int, *_rest: int) -> int:
            calls.append((option, int(signum)))
            return 0

    class _Lib:
        def __init__(self) -> None:
            self.prctl = _Prctl()

    monkeypatch.setattr(
        "mftik_sts.worker.ctypes.CDLL", lambda *_a, **_k: _Lib()
    )
    set_pdeathsig(signal.SIGTERM)
    assert calls
    assert calls[0][0] == 1
    assert calls[0][1] == signal.SIGTERM


class _Exits(Strategy):
    name = "exits"

    def __init__(self) -> None:
        super().__init__()
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def on_ready(self) -> None:
        self.exit("oco_filled")

    async def on_stop(self) -> None:
        self.entered.set()
        await self.release.wait()


async def test_a_strategy_exit_writes_the_row_before_the_worker_can_leave(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``exit`` after ``on_ready`` keeps its reason.

    ``close`` has already popped the session and stopped the control loop
    by the time ``on_stop`` runs. The worker's exit condition is that
    ``close`` returned, which is when the row has the strategy's reason.
    """
    monkeypatch.setattr(manager_mod, "ensure_deployable", lambda *_a, **_k: None)
    strategy = _Exits()
    marks: list[tuple[str, str, str | None]] = []

    async def mark(session_id: str, *, status: str, reason: str | None) -> None:
        marks.append((session_id, status, reason))

    async with a_broker("sts-exit") as broker:
        manager = SessionManager(
            broker,
            mark_done=mark,
            strategy_factory=lambda _name: strategy,
            instance="sts",
        )
        created = asyncio.create_task(
            manager.create_session(
                StsCreateSessionRequest(
                    session_id="exit1",
                    created_by=1,
                    strategy="exits",
                    type="Exits",
                )
            )
        )
        try:
            await asyncio.wait_for(strategy.entered.wait(), timeout=5)
            waiter = asyncio.create_task(manager.wait_until_quiet())
            await asyncio.sleep(0.05)
            assert not waiter.done()
            assert marks == []
            strategy.release.set()
            await asyncio.wait_for(waiter, timeout=5)
            assert marks == [("exit1", "done", "oco_filled")]
            result = await created
            assert result.status == "done"
            assert result.reason == "oco_filled"
        finally:
            strategy.release.set()
            await manager.close_all()
            await asyncio.gather(created, return_exceptions=True)


async def test_list_answers_while_create_waits_for_its_result() -> None:
    """A result line that has not arrived does not stall the instance subject."""
    gate = asyncio.Event()
    spawner = FakeSpawner(line=_ok_line(), process=FakeProcess(), gate=gate)
    store: dict[str, SimpleNamespace] = {}

    async def persist(**kwargs: Any) -> SimpleNamespace:
        row = SimpleNamespace(status="live", reason=None, **kwargs)
        store[kwargs["session_id"]] = row
        return row

    async def mark(session_id: str, *, status: str, reason: str | None) -> None:
        row = store[session_id]
        row.status = status
        row.reason = reason

    async with a_broker("sts-create-wait") as broker:
        manager = SessionManager(
            broker,
            persist_live=persist,
            mark_done=mark,
            spawner=spawner,  # type: ignore[arg-type]
            strategy_factory=lambda _name: Rebuildable(),
            instance="sts",
        )
        stop = asyncio.Event()
        rpc = asyncio.create_task(
            run_rpc(broker, manager, stop, subject=Topics.sts("sts"))
        )
        create_task: asyncio.Task[Any] | None = None
        try:
            await asyncio.sleep(0.05)
            create_task = asyncio.create_task(
                broker.request(
                    Topics.sts("sts"),
                    StsCreateSessionRequestEnvelope.wrap(
                        _request(),
                        type=STS_SESSION_CREATE,
                        source="api",
                    ),
                    timeout=5,
                )
            )
            for _ in range(50):
                if spawner.calls:
                    break
                await asyncio.sleep(0.02)
            assert spawner.calls
            assert not create_task.done()
            listed = await broker.request(
                Topics.sts("sts"),
                ListSessionsRequestEnvelope.wrap(
                    ListSessionsRequest(domain="sts"),
                    type=STS_SESSION_LIST,
                    source="api",
                ),
                timeout=2,
            )
            assert listed.type == STS_SESSION_LIST
            assert not create_task.done()
            gate.set()
            reply = await create_task
            created = StsCreateSessionResult.model_validate(reply.payload)
            assert created.status == "live"
            assert store["s1"].status == "live"
        finally:
            gate.set()
            stop.set()
            rpc.cancel()
            await asyncio.gather(rpc, return_exceptions=True)
            if create_task is not None:
                await asyncio.gather(create_task, return_exceptions=True)
            await manager.close_all()


async def test_close_all_while_rebuild_is_paused_does_not_spawn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A rebuild parked on a read has no process yet. Shutdown must not spawn."""
    monkeypatch.setattr(manager_mod, "ensure_deployable", lambda *_a, **_k: None)
    release = asyncio.Event()
    listed = asyncio.Event()
    row = SimpleNamespace(
        session_id="aa0001",
        created_by=1,
        strategy="rebuildable",
        type="Rebuildable",
        instance="sts",
        restart="always",
        rebuild_count=0,
        finished_at=datetime.now(UTC),
        st_facts={},
        st_paras={},
        td={},
        md_ids=[],
    )
    spawner = FakeSpawner(line=_ok_line(), process=FakeProcess())

    async def list_sessions(**_kwargs: Any) -> list[SimpleNamespace]:
        listed.set()
        await release.wait()
        return [row]

    async def bump(_session_id: str) -> int:
        row.rebuild_count += 1
        return row.rebuild_count

    manager = SessionManager(
        _Broker(),  # type: ignore[arg-type]
        list_db_sessions=list_sessions,
        bump_rebuild_count=bump,
        spawner=spawner,  # type: ignore[arg-type]
        strategy_factory=lambda _name: Rebuildable(),
        instance="sts",
    )
    task = asyncio.create_task(manager.rebuild_session("aa0001"))
    try:
        await listed.wait()
        await manager.close_all()
        release.set()
        assert await task is False
        assert spawner.calls == []
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await manager.close_all()


async def test_a_dead_worker_is_judged_from_its_own_row() -> None:
    seen: list[str] = []

    async def load(session_id: str) -> SimpleNamespace:
        seen.append(session_id)
        return SimpleNamespace(status="live")

    manager = SessionManager(
        _Broker(),  # type: ignore[arg-type]
        load_session=load,
    )
    assert await manager._row_is_live("aa0001") is True
    assert seen == ["aa0001"]


async def test_list_uses_the_row_when_the_rebuild_slot_has_no_name() -> None:
    row = SimpleNamespace(
        session_id="aa0001",
        created_by=1,
        created_at=None,
        finished_at=None,
        status="interrupted",
        strategy="rebuildable",
        type="Rebuildable",
        reason=None,
    )

    async def list_sessions(**_kwargs: Any) -> list[SimpleNamespace]:
        return [row]

    manager = SessionManager(
        _Broker(),  # type: ignore[arg-type]
        list_db_sessions=list_sessions,
    )
    manager._workers["aa0001"] = WorkerSlot(session_id="aa0001", role="rebuild")
    listed = await manager.list_sessions(
        ListSessionsRequest(domain="sts", status="interrupted")
    )
    assert listed[0].strategy == "rebuildable"
    assert listed[0].type == "Rebuildable"


async def test_a_real_worker_answers_stop_on_its_control_subject(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Spawn to the result line, then stop.

    The instance subject answers ``not_found`` — the session is not in the
    parent's ``_sessions``. The control subject is the worker, and the reply
    comes back while that process is still alive.
    """
    url = f"sqlite+aiosqlite:///{tmp_path / 'sts.db'}"
    engine = build_engine(url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        db.add(User(id=1, email="owner-1@test.invalid"))
        await db.commit()

    async def persist(**kwargs: Any) -> Any:
        async with maker() as db:
            repo = StsSessionRepository(db)
            existing = await repo.get_by_session_id(kwargs["session_id"])
            if existing is not None:
                return existing
            row = await repo.create_live(**kwargs)
            await db.commit()
            return row

    async with a_broker("sts-worker") as broker:
        monkeypatch.setenv("DATABASE_URL", url)
        monkeypatch.setenv("BROKER_KEY_PREFIX", broker.config.key_prefix)
        monkeypatch.setenv("NATS_URL", broker.config.nats_url)
        monkeypatch.setenv("MFTIK_DATA", str(tmp_path / "data"))
        manager = SessionManager(
            broker,
            persist_live=persist,
            spawner=SubprocessSpawner(),
            instance="sts",
        )
        stop = asyncio.Event()
        rpc = asyncio.create_task(
            run_rpc(broker, manager, stop, subject=Topics.sts("sts"))
        )
        process = None
        try:
            started = time.perf_counter()
            result = await manager.create_session(
                StsCreateSessionRequest(
                    session_id="aa00aa",
                    created_by=1,
                    strategy="noop",
                    type="NoopStrategy",
                    td={"main": {"api_id": 1}},
                    instance="sts",
                )
            )
            elapsed = time.perf_counter() - started
            print(f"STS_SPAWN_TO_RESULT_S={elapsed:.3f}")
            assert result.status == "live"
            assert elapsed < 10.0
            slot = manager.get("aa00aa")
            assert slot is not None
            process = slot.process
            assert process.returncode is None

            missed = await broker.request(
                Topics.sts("sts"),
                StsSessionControlRequestEnvelope.wrap(
                    StsSessionControlRequest(session_id="aa00aa"),
                    type=STS_SESSION_STOP,
                    source="api",
                    session_id="aa00aa",
                ),
                timeout=2.0,
            )
            assert missed.type == STS_ERROR
            assert missed.payload["code"] == "not_found"
            assert manager.get("aa00aa") is not None

            reply = await broker.request(
                Topics.sts_control("aa00aa"),
                StsSessionControlRequestEnvelope.wrap(
                    StsSessionControlRequest(session_id="aa00aa"),
                    type=STS_SESSION_STOP,
                    source="api",
                    session_id="aa00aa",
                ),
                timeout=5.0,
            )
            stopped = StsSessionControlResult.model_validate(reply.payload)
            assert stopped.session_id == "aa00aa"
            # The reply is sent from inside the control task. The process
            # exits only after that task has retired and the broker has
            # closed, so it is still alive here.
            assert process.returncode is None
            assert slot.watcher is not None
            await asyncio.wait_for(slot.watcher, timeout=5.0)
            assert process.returncode == 0
        finally:
            stop.set()
            rpc.cancel()
            await asyncio.gather(rpc, return_exceptions=True)
            await manager.close_all()
            if process is not None and process.returncode is None:
                process.kill()
                await process.wait()
    await engine.dispose()


async def test_closing_the_lifeline_makes_the_worker_exit(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """EOF on the lifeline is the parent disappearing.

    Darwin has no PDEATHSIG, and ``start_new_session`` keeps a terminal
    signal from reaching the worker. Closing the write end is the same
    EOF the kernel delivers when the parent process dies.
    """
    url = f"sqlite+aiosqlite:///{tmp_path / 'sts.db'}"
    engine = build_engine(url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        db.add(User(id=1, email="owner-1@test.invalid"))
        await db.commit()

    async def persist(**kwargs: Any) -> Any:
        async with maker() as db:
            repo = StsSessionRepository(db)
            existing = await repo.get_by_session_id(kwargs["session_id"])
            if existing is not None:
                return existing
            row = await repo.create_live(**kwargs)
            await db.commit()
            return row

    async with a_broker("sts-lifeline") as broker:
        monkeypatch.setenv("DATABASE_URL", url)
        monkeypatch.setenv("BROKER_KEY_PREFIX", broker.config.key_prefix)
        monkeypatch.setenv("NATS_URL", broker.config.nats_url)
        monkeypatch.setenv("MFTIK_DATA", str(tmp_path / "data"))
        manager = SessionManager(
            broker,
            persist_live=persist,
            spawner=SubprocessSpawner(),
            instance="sts",
        )
        process = None
        try:
            await manager.create_session(
                StsCreateSessionRequest(
                    session_id="aa00ab",
                    created_by=1,
                    strategy="noop",
                    type="NoopStrategy",
                    td={"main": {"api_id": 1}},
                    instance="sts",
                )
            )
            slot = manager.get("aa00ab")
            assert isinstance(slot, WorkerSlot)
            assert slot.lifeline_fd is not None
            process = slot.process
            fd = slot.lifeline_fd
            slot.lifeline_fd = None
            os.close(fd)
            assert slot.watcher is not None
            await asyncio.wait_for(slot.watcher, timeout=5.0)
            assert process.returncode == 0
            async with maker() as db:
                row = await StsSessionRepository(db).get_by_session_id("aa00ab")
            assert row is not None
            assert row.status == "interrupted"
            assert row.reason == "STS shut down while this was running"
        finally:
            await manager.close_all()
            if process is not None and process.returncode is None:
                process.kill()
                await process.wait()
    await engine.dispose()
