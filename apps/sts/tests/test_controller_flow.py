"""Start, end, and converge without a process or a database.

The contract file calls a real Supervisor and must not spawn. These tests
use a fake supervisor so converge can be watched: the request file, one
spawn, the running snapshot, and a non-zero death recorded as ``failed``
by the F11 choice (the default ``restart`` is ``never``).
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from mftik.clock import FakeClock
from mftik.intent_gc import owners_in_report
from mftik.procman import CapacityExceeded, WorkerPhase
from mftik.protocol import (
    DEFAULT_READY_TIMEOUT_S,
    DEFAULT_START_TIMEOUT_S,
    STS_ERROR,
    STS_REASON_OPERATOR_STOP,
    STS_SESSION_END,
    STS_SESSION_LIST,
    STS_SESSION_START,
    STS_SESSION_STATUS,
    Envelope,
    IntentOwner,
    ListSessionsRequest,
    ListSessionsResult,
    ProcmanWorker,
    RpcError,
    StsCreateSessionRequest,
    StsCreateSessionResult,
    StsSessionEndRequest,
    StsSessionEndResult,
    StsSessionStatus,
    Topics,
)
from mftik_sts.controller import (
    SESSION_HB_TIMEOUT_S,
    SESSION_START_TIMEOUT_S,
    SESSION_STOP_GRACE_S,
    StsOrchestrator,
    end_handler,
    list_handler,
    start_handler,
)
from mftik_sts.controller.env import forwarded_env
from mftik_sts.controller.spawn import write_session_request
from mftik_sts.controller.worker import ON_READY_BACKSTOP_S
from mftik_sts.rpc.router import control_handler


class FakeSupervisor:
    """Enough of a supervisor for converge. No shim, no sleep."""

    def __init__(self, work_dir: Path, *, instance: str = "sts") -> None:
        self.work_dir = work_dir
        self.plane = "sts"
        self.instance = instance
        self.spawned: list[object] = []
        self.phase: WorkerPhase | None = None
        self.ready = False
        self.pid: int | None = None
        self.exit_code: int | None = None
        self.signal: int | None = None
        self.ready_on_spawn = False
        self.released: list[str] = []

    async def spawn(self, spec: object) -> None:
        self.spawned.append(spec)
        self.phase = WorkerPhase.STARTING
        self.pid = 4242
        self.ready = False
        self.exit_code = None
        self.signal = None
        if self.ready_on_spawn:
            self.phase = WorkerPhase.RUNNING
            self.ready = True

    async def status(self, worker_id: str) -> SimpleNamespace | None:
        del worker_id
        if self.phase is None:
            return None
        return SimpleNamespace(
            phase=self.phase,
            pid=self.pid,
            ready=self.ready,
            exit_code=self.exit_code,
            signal=self.signal,
        )

    async def stop(self, worker_id: str) -> None:
        del worker_id
        # The slot stays readable. A real ``stop`` releases it and the
        # controller then reads the exit file; this double keeps the
        # exit code on ``status`` so a finished stop is exit 0.
        self.phase = WorkerPhase.STOPPED
        self.pid = None
        self.ready = False
        self.exit_code = 0
        self.signal = None

    async def release_slot(self, worker_id: str) -> None:
        self.released.append(worker_id)
        self.phase = None

    async def start(self) -> tuple[object, ...]:
        return ()

    def listed(self) -> list[ProcmanWorker]:
        """What :meth:`Supervisor.report` would contribute for this slot."""
        if self.phase not in (
            WorkerPhase.STARTING,
            WorkerPhase.RUNNING,
            WorkerPhase.STOPPING,
        ):
            return []
        if not self.spawned:
            return []
        spec = self.spawned[-1]
        return [
            ProcmanWorker(
                id=spec.id,  # type: ignore[attr-defined]
                code_ref=spec.code_ref,  # type: ignore[attr-defined]
                rss_bytes=None,
                phase=self.phase.value,
                ready=self.ready,
                incarnation=spec.incarnation,  # type: ignore[attr-defined]
            )
        ]


def _request(session_id: str = "abc123", *, created_by: int = 1) -> object:
    body = StsCreateSessionRequest(
        session_id=session_id, created_by=created_by, strategy="noop"
    )
    return Envelope[dict].wrap(
        body.model_dump(), type=STS_SESSION_START, source="api"
    )


def _orch(
    tmp_path: Path,
    *,
    clock: FakeClock | None = None,
    publish: list[tuple[str, Envelope]] | None = None,
) -> tuple[StsOrchestrator, FakeSupervisor]:
    supervisor = FakeSupervisor(tmp_path)
    written: list[tuple[str, Envelope]] = [] if publish is None else publish

    async def _publish(subject: str, envelope: Envelope) -> None:
        written.append((subject, envelope))

    orchestrator = StsOrchestrator(
        supervisor,  # type: ignore[arg-type]
        clock=clock if clock is not None else FakeClock(),
        publish=_publish,
        code_ref="test",
        argv_for=lambda path: ("stand-in", str(path)),
    )
    return orchestrator, supervisor


async def test_an_unknown_end_is_an_error_reply(tmp_path: Path) -> None:
    orch, _supervisor = _orch(tmp_path)
    end = StsSessionEndRequest(session_id="missing", reason="gone")
    reply = await end_handler(orch)(
        Envelope[dict].wrap(end.model_dump(), type=STS_SESSION_END, source="api")
    )
    assert reply is not None
    assert reply.type == STS_ERROR
    assert RpcError.model_validate(reply.payload).code == "unknown_session"


async def test_ending_twice_replies_the_terminal_status(tmp_path: Path) -> None:
    orch, _supervisor = _orch(tmp_path)
    await start_handler(orch)(_request())
    end = StsSessionEndRequest(
        session_id="abc123", reason=STS_REASON_OPERATOR_STOP
    )
    message = Envelope[dict].wrap(
        end.model_dump(), type=STS_SESSION_END, source="api"
    )
    first = await end_handler(orch)(message)
    second = await end_handler(orch)(message)
    assert first is not None and second is not None
    assert _model(first.payload, StsSessionEndResult).status == "done"
    assert _model(second.payload, StsSessionEndResult).status == "done"


async def test_a_second_start_does_not_spawn_again(tmp_path: Path) -> None:
    orch, supervisor = _orch(tmp_path)
    supervisor.ready_on_spawn = True
    first = await start_handler(orch)(_request())
    await orch.converge("abc123")
    second = await start_handler(orch)(_request())
    await orch.converge("abc123")
    assert first is not None and second is not None
    assert _model(first.payload, StsCreateSessionResult).status == "starting"
    assert _model(second.payload, StsCreateSessionResult).status == "starting"
    assert len(supervisor.spawned) == 1


async def test_an_omitted_code_ref_is_the_current_release(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "mftik_sts.controller.orchestrator.current_release", lambda: "rel-9"
    )
    supervisor = FakeSupervisor(tmp_path)
    orch = StsOrchestrator(
        supervisor,  # type: ignore[arg-type]
        clock=FakeClock(),
        argv_for=lambda path: ("stand-in", str(path)),
    )
    await start_handler(orch)(_request())
    await orch.converge("abc123")
    assert supervisor.spawned[0].code_ref == "rel-9"  # type: ignore[attr-defined]


def _published(
    supervisor: FakeSupervisor, orchestrator: StsOrchestrator
) -> list[ProcmanWorker]:
    """One publication: supervisor live slots, then pending extras."""
    listed = supervisor.listed()
    have = {worker.id for worker in listed}
    extras = orchestrator.extra_workers()
    assert all(worker.id not in have for worker in extras)
    return [*listed, *extras]


async def test_an_accepted_session_is_on_the_next_report(tmp_path: Path) -> None:
    """B4-07 drops an owner missing from two reports. The accept is listed
    before spawn, and STARTING replaces that extra without a second id."""
    orch, supervisor = _orch(tmp_path)
    await start_handler(orch)(_request())
    before = _published(supervisor, orch)
    assert [worker.id for worker in before] == ["sts/session/abc123"]
    assert before[0].phase == "starting"
    assert owners_in_report("sts", before) == frozenset(
        {IntentOwner(sts_instance="sts", session_id="abc123")}
    )

    await orch.converge("abc123")
    after = _published(supervisor, orch)
    assert [worker.id for worker in after] == ["sts/session/abc123"]
    assert orch.extra_workers() == ()
    assert after[0].phase == "starting"
    assert owners_in_report("sts", after) == owners_in_report("sts", before)


async def test_capacity_exceeded_is_the_start_reply(tmp_path: Path) -> None:
    class _Refuse(FakeSupervisor):
        async def spawn(self, spec: object) -> None:
            del spec
            raise CapacityExceeded("max_workers 1")

    supervisor = _Refuse(tmp_path)
    orch = StsOrchestrator(
        supervisor,  # type: ignore[arg-type]
        clock=FakeClock(),
        code_ref="test",
        argv_for=lambda path: ("stand-in", str(path)),
    )
    reply = await control_handler(None, orch)(_request())  # type: ignore[arg-type]
    assert reply is not None
    assert reply.type == STS_ERROR
    error = RpcError.model_validate(reply.payload)
    assert error.code == "capacity_exceeded"
    assert "max_workers" in error.message
    assert orch.extra_workers() == ()
    assert supervisor.spawned == []


async def test_converge_spawns_once_and_observe_publishes_running(
    tmp_path: Path,
) -> None:
    published: list[tuple[str, Envelope]] = []
    orch, supervisor = _orch(tmp_path, publish=published)
    await start_handler(orch)(_request())
    await orch.converge("abc123")
    path = tmp_path / "sessions" / "abc123.json"
    assert path.is_file()
    loaded = StsCreateSessionRequest.model_validate_json(path.read_text())
    assert loaded.session_id == "abc123"
    assert len(supervisor.spawned) == 1
    spec = supervisor.spawned[0]
    assert spec.restart == "never"  # type: ignore[attr-defined]
    assert spec.argv == ("stand-in", str(path))  # type: ignore[attr-defined]
    assert spec.start_timeout_s == (  # type: ignore[attr-defined]
        SESSION_START_TIMEOUT_S
        + DEFAULT_START_TIMEOUT_S
        + DEFAULT_READY_TIMEOUT_S
        + ON_READY_BACKSTOP_S
    )
    assert spec.hb_timeout_s == SESSION_HB_TIMEOUT_S  # type: ignore[attr-defined]
    assert spec.stop_grace_s == SESSION_STOP_GRACE_S  # type: ignore[attr-defined]
    assert spec.code_ref == "test"  # type: ignore[attr-defined]
    assert spec.env == forwarded_env()  # type: ignore[attr-defined]
    assert "DATABASE_URL" not in spec.env  # type: ignore[attr-defined]
    assert "DATABASE_URL_SYNC" not in spec.env  # type: ignore[attr-defined]
    assert "MFTIK_STATUS_FD" not in spec.env  # type: ignore[attr-defined]

    supervisor.phase = WorkerPhase.RUNNING
    supervisor.ready = True
    await orch.observe_all()
    snapshot = _model(published[-1][1].payload, StsSessionStatus)
    assert published[-1][0] == Topics.sts_status("abc123")
    assert published[-1][1].type == STS_SESSION_STATUS
    assert snapshot.status == "running"
    assert snapshot.conditions["phase"] == "running"
    assert snapshot.observed_generation == 1
    assert snapshot.worker_incarnation == 1
    assert snapshot.restart_count == 0


async def test_a_dead_worker_is_failed_without_a_new_spawn(tmp_path: Path) -> None:
    orch, supervisor = _orch(tmp_path)
    supervisor.ready_on_spawn = True
    await start_handler(orch)(_request())
    await orch.converge("abc123")
    supervisor.phase = WorkerPhase.CRASHED
    supervisor.ready = False
    supervisor.pid = None
    supervisor.exit_code = 1
    await orch.observe_all()
    reply = await list_handler(orch)(
        Envelope[dict].wrap(
            ListSessionsRequest(domain="sts", status="failed").model_dump(),
            type=STS_SESSION_LIST,
            source="api",
        )
    )
    assert reply is not None
    listed = _model(reply.payload, ListSessionsResult)
    assert len(listed.sessions) == 1
    assert listed.sessions[0].status == "failed"
    assert listed.sessions[0].reason == "restart_never"
    assert len(supervisor.spawned) == 1
    assert supervisor.released == ["sts/session/abc123"]


async def test_a_clean_exit_while_desired_running_is_done(tmp_path: Path) -> None:
    """Exit 0 while desired is still running is ``done``, not ``failed``.

    The controller cannot see the worker log, so the reason is
    ``worker_exited:0``. Non-zero stays the path above.
    """
    orch, supervisor = _orch(tmp_path)
    supervisor.ready_on_spawn = True
    await start_handler(orch)(_request())
    await orch.converge("abc123")
    supervisor.phase = WorkerPhase.STOPPED
    supervisor.ready = False
    supervisor.pid = None
    supervisor.exit_code = 0
    await orch.observe_all()
    reply = await list_handler(orch)(
        Envelope[dict].wrap(
            ListSessionsRequest(domain="sts", status="done").model_dump(),
            type=STS_SESSION_LIST,
            source="api",
        )
    )
    assert reply is not None
    listed = _model(reply.payload, ListSessionsResult)
    assert len(listed.sessions) == 1
    assert listed.sessions[0].status == "done"
    assert listed.sessions[0].reason == "worker_exited:0"
    assert len(supervisor.spawned) == 1
    assert supervisor.released == ["sts/session/abc123"]


async def test_list_filters_on_the_column_and_the_owner(tmp_path: Path) -> None:
    orch, _supervisor = _orch(tmp_path)
    await start_handler(orch)(_request("aaa111", created_by=1))
    await start_handler(orch)(_request("bbb222", created_by=2))
    reply = await list_handler(orch)(
        Envelope[dict].wrap(
            ListSessionsRequest(domain="sts", created_by=2).model_dump(),
            type=STS_SESSION_LIST,
            source="api",
        )
    )
    assert reply is not None
    listed = _model(reply.payload, ListSessionsResult)
    assert [row.session_id for row in listed.sessions] == ["bbb222"]
    other = await list_handler(orch)(
        Envelope[dict].wrap(
            {"domain": "td"}, type=STS_SESSION_LIST, source="api"
        )
    )
    assert other is not None
    assert _model(other.payload, ListSessionsResult).sessions == []


async def test_a_terminal_session_cannot_be_started_again(tmp_path: Path) -> None:
    orch, _supervisor = _orch(tmp_path)
    await start_handler(orch)(_request())
    await end_handler(orch)(
        Envelope[dict].wrap(
            StsSessionEndRequest(
                session_id="abc123", reason=STS_REASON_OPERATOR_STOP
            ).model_dump(),
            type=STS_SESSION_END,
            source="api",
        )
    )
    reply = await start_handler(orch)(_request())
    assert reply is not None
    assert RpcError.model_validate(reply.payload).code == "session_ended"


def test_the_request_file_replaces_a_temp_file(tmp_path: Path) -> None:
    request = StsCreateSessionRequest(
        session_id="abc123", created_by=1, strategy="noop"
    )
    path = write_session_request(tmp_path, request)
    assert path == tmp_path / "sessions" / "abc123.json"
    assert list((tmp_path / "sessions").glob(".*.tmp")) == []


def _model(payload: object, model: type):
    if hasattr(payload, "model_dump"):
        payload = payload.model_dump()
    return model.model_validate(payload)
