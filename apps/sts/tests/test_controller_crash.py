"""Cleanup and rehang against a fake supervisor and a request double.

No NATS connection. Time moves only when ``FakeClock.advance`` says so.
"""

from __future__ import annotations

import asyncio
import signal
from pathlib import Path
from types import SimpleNamespace

import pytest
from mftik.broker import NoRespondersError, RequestTimeoutError
from mftik.clock import FakeClock
from mftik.intent_gc import gc_owners, owners_in_report
from mftik.procman import WorkerPhase
from mftik.protocol import (
    TD_ORDER_CANCEL_SESSION,
    Envelope,
    StsCreateSessionRequest,
    StsSessionStatus,
    TdCancelSessionRequest,
    TdCancelSessionResult,
    Topics,
)
from mftik_sts.controller import STS_CLEANUP_TIMEOUT_S, StsOrchestrator, start_handler
from mftik_sts.controller.types import Cleanup, session_worker_id
from mftik_sts.exit_codes import STRATEGY_EXCEPTION

pytestmark = pytest.mark.component


class FakeSupervisor:
    def __init__(self, work_dir: Path) -> None:
        self.work_dir = work_dir
        self.plane = "sts"
        self.instance = "sts"
        self.spawned: list[object] = []
        self.phase: WorkerPhase | None = None
        self.ready = False
        self.pid: int | None = None
        self.exit_code: int | None = None
        self.signal: int | None = None
        self.released: list[str] = []

    async def spawn(self, spec: object) -> None:
        self.spawned.append(spec)
        self.phase = WorkerPhase.STARTING
        self.pid = 4242
        self.ready = False
        self.exit_code = None
        self.signal = None

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


class Broker:
    def __init__(self, responder) -> None:  # type: ignore[no-untyped-def]
        self.responder = responder
        self.calls: list[tuple[str, object, float]] = []

    async def request(
        self, subject: str, envelope: object, *, timeout: float
    ) -> object:
        self.calls.append((subject, envelope, timeout))
        return await self.responder(subject, envelope, timeout)


def _request(api_ids: tuple[int, ...] = (7,), *, restart: str = "on_failure") -> object:
    body = StsCreateSessionRequest(
        session_id="abc123",
        created_by=1,
        strategy="noop",
        restart=restart,
        td={str(api_id): {"api_id": api_id} for api_id in api_ids},  # type: ignore[dict-item]
    )
    return Envelope[dict].wrap(
        body.model_dump(), type="sts.session.start", source="api"
    )


def _ok(session_id: str = "abc123"):
    async def responder(subject: str, envelope: object, timeout: float) -> object:
        del subject, envelope, timeout
        return Envelope[TdCancelSessionResult].wrap(
            TdCancelSessionResult(session_id=session_id, ok=True, unconfirmed=[]),
            type=TD_ORDER_CANCEL_SESSION,
            source="td",
        )

    return responder


def _orch(
    tmp_path: Path,
    broker: Broker,
    clock: FakeClock,
    published: list[tuple[str, Envelope[object]]],
) -> tuple[StsOrchestrator, FakeSupervisor]:
    supervisor = FakeSupervisor(tmp_path)

    async def _publish(subject: str, envelope: Envelope[object]) -> None:
        published.append((subject, envelope))

    orchestrator = StsOrchestrator(
        supervisor,  # type: ignore[arg-type]
        clock=clock,
        publish=_publish,
        broker=broker,  # type: ignore[arg-type]
        code_ref="test",
        argv_for=lambda path: ("stand-in", str(path)),
    )
    return orchestrator, supervisor


def _crash(supervisor: FakeSupervisor, *, code: int, ready: bool = True) -> None:
    supervisor.phase = WorkerPhase.CRASHED
    supervisor.ready = ready
    supervisor.pid = None
    supervisor.exit_code = code
    supervisor.signal = None


async def _until_sleeping(clock: FakeClock, task: asyncio.Task[None]) -> None:
    for _ in range(50):
        if task.done() or clock._heap:  # noqa: SLF001
            return
        await asyncio.sleep(0)
    raise AssertionError("the rehang did not reach its backoff")


def _logs(published: list[tuple[str, Envelope[object]]]) -> list[str]:
    found: list[str] = []
    for topic, envelope in published:
        if not topic.startswith("log.sts."):
            continue
        payload = envelope.payload
        if hasattr(payload, "message"):
            found.append(str(payload.message))
    return found


async def _start(orch: StsOrchestrator, *, restart: str = "on_failure") -> None:
    await start_handler(orch)(_request(restart=restart))
    await orch.converge("abc123")


async def test_a_confirmed_cancel_rehanges_after_the_backoff(tmp_path: Path) -> None:
    clock = FakeClock()
    published: list[tuple[str, Envelope[object]]] = []
    broker = Broker(_ok())
    orch, supervisor = _orch(tmp_path, broker, clock, published)
    supervisor.ready = True
    await _start(orch)
    assert len(supervisor.spawned) == 1
    _crash(supervisor, code=STRATEGY_EXCEPTION, ready=True)
    task = asyncio.create_task(orch.observe_all())
    await _until_sleeping(clock, task)
    assert len(supervisor.spawned) == 1
    held = orch._sessions["abc123"]  # noqa: SLF001
    assert held.phase.value == "restarting"
    assert held.cleanup is Cleanup.CONFIRMED
    assert broker.calls[0][0] == Topics.td_order(7)
    assert broker.calls[0][2] == STS_CLEANUP_TIMEOUT_S
    request = TdCancelSessionRequest.model_validate(
        broker.calls[0][1].payload  # type: ignore[attr-defined]
    )
    assert request.session_id == "abc123"
    assert any(item.status == "restarting" for _topic, item in (
        (topic, StsSessionStatus.model_validate(env.payload))
        for topic, env in published
        if env.type == "sts.session.status"
    ))
    logs = _logs(published)
    assert any("class=A" in line and "reason=on_failure" in line for line in logs)
    clock.advance(60)
    await asyncio.sleep(0)
    await task
    assert len(supervisor.spawned) == 2
    assert supervisor.spawned[1].incarnation == 2  # type: ignore[attr-defined]
    assert held.restart_count == 1
    assert held.worker_incarnation == 2


async def test_no_spawn_until_the_cancel_reply_arrives(tmp_path: Path) -> None:
    clock = FakeClock()
    published: list[tuple[str, Envelope[object]]] = []
    gate = asyncio.Event()
    entered = asyncio.Event()

    async def responder(subject: str, envelope: object, timeout: float) -> object:
        entered.set()
        await gate.wait()
        return await _ok()(subject, envelope, timeout)

    broker = Broker(responder)
    orch, supervisor = _orch(tmp_path, broker, clock, published)
    await _start(orch)
    _crash(supervisor, code=STRATEGY_EXCEPTION, ready=True)
    task = asyncio.create_task(orch.observe_all())
    for _ in range(50):
        if entered.is_set():
            break
        await asyncio.sleep(0)
    assert entered.is_set()
    await asyncio.sleep(0)
    assert len(supervisor.spawned) == 1
    assert not task.done()
    gate.set()
    await _until_sleeping(clock, task)
    assert len(supervisor.spawned) == 1
    clock.advance(60)
    await asyncio.sleep(0)
    await task
    assert len(supervisor.spawned) == 2


async def test_unconfirmed_cids_fail_and_are_named(tmp_path: Path) -> None:
    clock = FakeClock()
    published: list[tuple[str, Envelope[object]]] = []

    async def responder(subject: str, envelope: object, timeout: float) -> object:
        del subject, envelope, timeout
        return Envelope[TdCancelSessionResult].wrap(
            TdCancelSessionResult(
                session_id="abc123", ok=False, unconfirmed=["cid-a", "cid-b"]
            ),
            type=TD_ORDER_CANCEL_SESSION,
            source="td",
        )

    orch, supervisor = _orch(tmp_path, Broker(responder), clock, published)
    await _start(orch)
    _crash(supervisor, code=STRATEGY_EXCEPTION, ready=True)
    await orch.observe_all()
    held = orch._sessions["abc123"]  # noqa: SLF001
    assert held.phase.value == "failed"
    assert held.reason == "cleanup_unconfirmed"
    assert len(supervisor.spawned) == 1
    assert supervisor.released == [session_worker_id("abc123")]
    text = " ".join(_logs(published))
    assert "unconfirmed=api_id=7:cid-a,cid-b" in text
    assert "class=A" in text


async def test_no_responders_is_unconfirmed(tmp_path: Path) -> None:
    clock = FakeClock()
    published: list[tuple[str, Envelope[object]]] = []

    async def responder(subject: str, envelope: object, timeout: float) -> object:
        del envelope
        raise NoRespondersError(subject, "req", timeout)

    orch, supervisor = _orch(tmp_path, Broker(responder), clock, published)
    await _start(orch)
    _crash(supervisor, code=1, ready=False)
    await orch.observe_all()
    held = orch._sessions["abc123"]  # noqa: SLF001
    assert held.phase.value == "failed"
    assert "no_responders" in " ".join(_logs(published))
    assert len(supervisor.spawned) == 1


async def test_a_timeout_is_unconfirmed_and_uses_the_cleanup_budget(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    published: list[tuple[str, Envelope[object]]] = []
    seen: list[float] = []

    async def responder(subject: str, envelope: object, timeout: float) -> object:
        del envelope
        seen.append(timeout)
        raise RequestTimeoutError(subject, "req", timeout)

    orch, supervisor = _orch(tmp_path, Broker(responder), clock, published)
    await _start(orch, restart="never")
    _crash(supervisor, code=STRATEGY_EXCEPTION, ready=True)
    await orch.observe_all()
    held = orch._sessions["abc123"]  # noqa: SLF001
    assert held.phase.value == "failed"
    assert held.reason == "restart_never"
    assert seen == [STS_CLEANUP_TIMEOUT_S]
    text = " ".join(_logs(published))
    assert "reason=restart_never" in text
    assert "unconfirmed=api_id=7:timeout" in text
    assert len(supervisor.spawned) == 1


async def test_cancels_run_in_parallel_and_one_miss_fails_the_session(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    published: list[tuple[str, Envelope[object]]] = []
    peak = 0
    current = 0

    async def responder(subject: str, envelope: object, timeout: float) -> object:
        nonlocal peak, current
        del envelope, timeout
        current += 1
        peak = max(peak, current)
        await asyncio.sleep(0)
        current -= 1
        api_id = int(subject.rsplit(".", 1)[-1])
        if api_id == 8:
            return Envelope[TdCancelSessionResult].wrap(
                TdCancelSessionResult(
                    session_id="abc123", ok=False, unconfirmed=["cid-z"]
                ),
                type=TD_ORDER_CANCEL_SESSION,
                source="td",
            )
        return Envelope[TdCancelSessionResult].wrap(
            TdCancelSessionResult(session_id="abc123", ok=True, unconfirmed=[]),
            type=TD_ORDER_CANCEL_SESSION,
            source="td",
        )

    body = StsCreateSessionRequest(
        session_id="abc123",
        created_by=1,
        strategy="noop",
        restart="on_failure",
        td={"a": {"api_id": 7}, "b": {"api_id": 8}},  # type: ignore[dict-item]
    )
    message = Envelope[dict].wrap(
        body.model_dump(), type="sts.session.start", source="api"
    )
    orch, supervisor = _orch(tmp_path, Broker(responder), clock, published)
    await start_handler(orch)(message)
    await orch.converge("abc123")
    _crash(supervisor, code=STRATEGY_EXCEPTION, ready=True)
    await orch.observe_all()
    assert peak == 2
    held = orch._sessions["abc123"]  # noqa: SLF001
    assert held.phase.value == "failed"
    assert "api_id=8:cid-z" in " ".join(_logs(published))
    assert len(supervisor.spawned) == 1


async def test_a_restarting_session_stays_on_the_report(tmp_path: Path) -> None:
    clock = FakeClock()
    published: list[tuple[str, Envelope[object]]] = []
    broker = Broker(_ok())
    orch, supervisor = _orch(tmp_path, broker, clock, published)
    await _start(orch)
    _crash(supervisor, code=STRATEGY_EXCEPTION, ready=True)
    task = asyncio.create_task(orch.observe_all())
    await _until_sleeping(clock, task)
    extras = orch.extra_workers()
    assert [worker.id for worker in extras] == [session_worker_id("abc123")]
    owners = owners_in_report("sts", extras)
    first = gc_owners(owners, (), None, report=owners, report_generation=1)
    assert first.release == frozenset()
    second = gc_owners(
        owners, first.absent, first.generation, report=owners, report_generation=2
    )
    assert second.release == frozenset()
    missed = gc_owners(owners, (), None, report=frozenset(), report_generation=1)
    gone = gc_owners(
        owners,
        missed.absent,
        missed.generation,
        report=frozenset(),
        report_generation=2,
    )
    assert owners <= gone.release
    clock.advance(60)
    await asyncio.sleep(0)
    await task


async def test_a_kill_stop_cleans_up_and_does_not_rehang(tmp_path: Path) -> None:
    clock = FakeClock()
    published: list[tuple[str, Envelope[object]]] = []
    broker = Broker(_ok())
    orch, supervisor = _orch(tmp_path, broker, clock, published)
    await _start(orch)

    async def stop(worker_id: str) -> None:
        del worker_id
        supervisor.phase = WorkerPhase.STOPPED
        supervisor.pid = None
        supervisor.ready = True
        supervisor.exit_code = None
        supervisor.signal = signal.SIGKILL

    supervisor.stop = stop  # type: ignore[method-assign]
    from mftik.protocol import STS_REASON_OPERATOR_STOP, StsSessionEndRequest
    from mftik_sts.controller import end_handler

    reply = await end_handler(orch)(
        Envelope[dict].wrap(
            StsSessionEndRequest(
                session_id="abc123", reason=STS_REASON_OPERATOR_STOP
            ).model_dump(),
            type="sts.session.end",
            source="api",
        )
    )
    assert reply is not None
    held = orch._sessions["abc123"]  # noqa: SLF001
    assert held.phase.value == "failed"
    assert held.reason == "crash_class_b"
    assert len(broker.calls) == 1
    assert len(supervisor.spawned) == 1
    assert "class=B" in " ".join(_logs(published))
