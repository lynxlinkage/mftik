"""Start, end, and converge without a process or a database.

The contract file calls a real Supervisor and must not spawn. These tests
use a fake supervisor so converge can be watched: the request file, one
spawn, the running snapshot, and a death recorded as ``failed`` without
a restart decision.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from mftik.clock import FakeClock
from mftik.procman import WorkerPhase
from mftik.protocol import (
    STS_ERROR,
    STS_REASON_OPERATOR_STOP,
    STS_SESSION_END,
    STS_SESSION_LIST,
    STS_SESSION_START,
    STS_SESSION_STATUS,
    Envelope,
    ListSessionsRequest,
    ListSessionsResult,
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
from mftik_sts.controller.spawn import write_session_request


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
        self.phase = None
        self.pid = None
        self.ready = False
        self.exit_code = 0

    async def release_slot(self, worker_id: str) -> None:
        self.released.append(worker_id)
        self.phase = None

    async def start(self) -> tuple[object, ...]:
        return ()


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
    assert spec.start_timeout_s == SESSION_START_TIMEOUT_S  # type: ignore[attr-defined]
    assert spec.hb_timeout_s == SESSION_HB_TIMEOUT_S  # type: ignore[attr-defined]
    assert spec.stop_grace_s == SESSION_STOP_GRACE_S  # type: ignore[attr-defined]
    assert spec.code_ref == "test"  # type: ignore[attr-defined]

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
    assert listed.sessions[0].reason == "worker_exited:1"
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
