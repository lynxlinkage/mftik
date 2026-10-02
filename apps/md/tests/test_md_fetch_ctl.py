"""The MD process owns one fetch worker and restarts it after it was ready."""

from __future__ import annotations

import importlib.metadata
import logging
from dataclasses import replace
from pathlib import Path

import pytest
from mftik.clock import FakeClock
from mftik.procman import (
    OOM_SCORE_ADJ,
    CapacityExceeded,
    CloseMode,
    ObservedWorker,
    ProcmanError,
    ReattachObservation,
    RestartDecision,
    WorkerPhase,
    WorkerSpec,
    WorkerStatus,
)
from mftik_md.app import INSTANCE, _code_ref, _work_dir
from mftik_md.defaults import (
    FETCH_HB_TIMEOUT_S,
    FETCH_MIN_BACKOFF_S,
    FETCH_RECONCILE_PERIOD_S,
    FETCH_RESTART_MAX,
    FETCH_RESTART_WINDOW_S,
    FETCH_START_TIMEOUT_S,
    FETCH_STOP_GRACE_S,
)
from mftik_md.fetch_ctl import (
    FETCH_WORKER_ID,
    FetchController,
    fetch_close_mode,
    fetch_worker_argv,
    forwarded_env,
)


class FakeSupervisor:
    """Records spawn and record_restart. Holds whatever status the test sets.

    A successful spawn replaces the slot with ``STARTING``, which is what
    the real supervisor does. A raised :class:`ProcmanError` leaves it.
    """

    def __init__(self) -> None:
        self.plane = "md"
        self.instance = "md"
        self.spawned: list[WorkerSpec] = []
        self.recorded: list[tuple[str, WorkerPhase]] = []
        self.status_of: WorkerStatus | None = None
        self.fail: ProcmanError | None = None

    async def status(self, worker_id: str) -> WorkerStatus | None:
        assert worker_id == FETCH_WORKER_ID
        return self.status_of

    async def spawn(self, spec: WorkerSpec) -> None:
        if self.fail is not None:
            raise self.fail
        self.spawned.append(spec)
        self.status_of = _status(spec, WorkerPhase.STARTING)

    async def record_restart(
        self, worker_id: str, decision: RestartDecision
    ) -> None:
        self.recorded.append((worker_id, decision.phase))
        if self.status_of is not None and self.status_of.spec.id == worker_id:
            self.status_of = replace(self.status_of, phase=decision.phase)


def _controller(
    supervisor: FakeSupervisor | None = None,
    *,
    clock: FakeClock | None = None,
) -> tuple[FetchController, FakeSupervisor, FakeClock]:
    supervisor = supervisor or FakeSupervisor()
    clock = clock or FakeClock()
    controller = FetchController(
        supervisor,  # type: ignore[arg-type]
        clock=clock,
        code_ref="vtest",
        env={},
    )
    return controller, supervisor, clock


def _status(spec: WorkerSpec, phase: WorkerPhase) -> WorkerStatus:
    return WorkerStatus(
        spec=spec,
        phase=phase,
        pid=spec.incarnation + 10,
        ready=phase is WorkerPhase.CRASHED,
        exit_code=1 if phase is WorkerPhase.CRASHED else None,
        signal=None,
        rss_bytes=None,
    )


def _observed(
    observed: ObservedWorker, incarnation: int | None
) -> ReattachObservation:
    return ReattachObservation(
        id=FETCH_WORKER_ID,
        observed=observed,
        spec=None,
        incarnation=incarnation,
        status=None,
        exit_record=None,
    )


def test_the_spec_is_one_on_failure_fetch_worker() -> None:
    controller, _supervisor, _clock = _controller()
    spec = controller.spec(1)
    assert spec.id == "md/fetch"
    assert spec.plane == "md"
    assert spec.kind == "fetch"
    assert spec.restart == "on_failure"
    assert spec.incarnation == 1
    assert spec.code_ref == "vtest"
    assert spec.oom_score_adj == OOM_SCORE_ADJ[("md", "fetch")] == 300
    assert spec.start_timeout_s == FETCH_START_TIMEOUT_S
    assert spec.hb_timeout_s == FETCH_HB_TIMEOUT_S
    assert spec.stop_grace_s == FETCH_STOP_GRACE_S
    assert spec.rlimit_data_bytes is None
    assert spec.argv == fetch_worker_argv()
    assert spec.env == {}


def test_sigterm_detaches() -> None:
    assert fetch_close_mode() is CloseMode.DETACH


def test_work_dir_is_the_plane_and_the_instance(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("WORK_DIR", str(tmp_path))
    assert _work_dir() == tmp_path / "md" / INSTANCE


def test_code_ref_is_the_installed_distribution() -> None:
    assert _code_ref() == importlib.metadata.version("mftik")
    assert _code_ref()


def test_forwarded_env_keeps_the_bus(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("NATS_URL", "nats://localhost:4222")
    monkeypatch.setenv("DATABASE_URL", "x")
    monkeypatch.delenv("BROKER_KEY_PREFIX", raising=False)
    env = forwarded_env()
    assert env["NATS_URL"] == "nats://localhost:4222"
    assert "DATABASE_URL" not in env
    assert "BROKER_KEY_PREFIX" not in env


def test_the_md_process_does_not_construct_a_fetch_session() -> None:
    text = (
        Path(__file__).resolve().parents[1] / "src" / "mftik_md" / "app.py"
    ).read_text()
    assert "FetchSession" not in text
    assert "fetch_close_mode" in text


async def test_a_running_worker_is_adopted() -> None:
    controller, supervisor, _clock = _controller()
    await controller.reconcile([_observed(ObservedWorker.RUNNING, 1)])
    assert supervisor.spawned == []


async def test_a_missing_worker_is_spawned() -> None:
    controller, supervisor, _clock = _controller()
    await controller.reconcile([])
    assert [spec.incarnation for spec in supervisor.spawned] == [1]


async def test_another_workers_observation_does_not_stop_it() -> None:
    """Connection workers are not this controller's. Their ids are ignored."""
    controller, supervisor, _clock = _controller()
    other = ReattachObservation(
        id="md/conn/public/0",
        observed=ObservedWorker.RUNNING,
        spec=None,
        incarnation=1,
        status=None,
        exit_record=None,
    )
    await controller.reconcile([other])
    assert [spec.id for spec in supervisor.spawned] == [FETCH_WORKER_ID]
    assert [spec.incarnation for spec in supervisor.spawned] == [1]


async def test_an_absent_worker_restarts_at_the_next_incarnation() -> None:
    controller, supervisor, _clock = _controller()
    await controller.reconcile([_observed(ObservedWorker.ABSENT, 4)])
    assert [spec.incarnation for spec in supervisor.spawned] == [5]


async def test_a_crash_after_ready_waits_out_backoff_then_spawns() -> None:
    controller, supervisor, clock = _controller()
    spec = controller.spec(1)
    supervisor.status_of = _status(spec, WorkerPhase.CRASHED)

    await controller.pass_once()
    assert supervisor.spawned == []
    assert supervisor.recorded == [(FETCH_WORKER_ID, WorkerPhase.BACKOFF)]

    await controller.pass_once()
    assert supervisor.spawned == []

    clock.advance(FETCH_MIN_BACKOFF_S)
    await controller.pass_once()
    assert [item.incarnation for item in supervisor.spawned] == [2]


async def test_a_death_before_ready_is_not_spawned() -> None:
    controller, supervisor, clock = _controller()
    spec = controller.spec(1)
    supervisor.status_of = _status(spec, WorkerPhase.FAILED)

    await controller.pass_once()
    assert supervisor.recorded == [(FETCH_WORKER_ID, WorkerPhase.FAILED)]
    assert supervisor.spawned == []

    clock.advance(FETCH_RESTART_WINDOW_S)
    await controller.pass_once()
    assert supervisor.spawned == []


async def test_lost_is_left_alone() -> None:
    controller, supervisor, clock = _controller()
    spec = controller.spec(2)
    supervisor.status_of = _status(spec, WorkerPhase.LOST)
    await controller.reconcile([_observed(ObservedWorker.LOST, 2)])
    assert supervisor.spawned == []
    assert supervisor.recorded == []

    supervisor.status_of = None
    clock.advance(FETCH_RECONCILE_PERIOD_S)
    await controller.pass_once()
    assert supervisor.spawned == []


async def test_a_capacity_refusal_is_retried(
    caplog: pytest.LogCaptureFixture,
) -> None:
    controller, supervisor, _clock = _controller()
    supervisor.fail = CapacityExceeded("max_workers exceeded")
    caplog.set_level(logging.ERROR, logger="md.fetch")

    await controller.reconcile([])
    assert supervisor.spawned == []
    assert "capacity_exceeded" in caplog.text

    supervisor.fail = None
    await controller.pass_once()
    assert [spec.incarnation for spec in supervisor.spawned] == [1]


async def test_the_window_stops_further_restarts() -> None:
    controller, supervisor, clock = _controller()
    for incarnation in range(1, FETCH_RESTART_MAX + 1):
        spec = controller.spec(incarnation)
        supervisor.status_of = _status(spec, WorkerPhase.CRASHED)
        await controller.pass_once()
        assert supervisor.recorded[-1][1] is WorkerPhase.BACKOFF
        delay = FETCH_MIN_BACKOFF_S * (2 ** (incarnation - 1))
        clock.advance(delay)
        await controller.pass_once()
        assert supervisor.spawned[-1].incarnation == incarnation + 1

    spec = controller.spec(FETCH_RESTART_MAX + 1)
    supervisor.status_of = _status(spec, WorkerPhase.CRASHED)
    await controller.pass_once()
    assert supervisor.recorded[-1][1] is WorkerPhase.FATAL
    clock.advance(FETCH_RESTART_WINDOW_S)
    await controller.pass_once()
    assert len(supervisor.spawned) == FETCH_RESTART_MAX


def test_restart_numbers_are_the_provisional_stand_ins() -> None:
    """The plan does not give these. The names are the ones under 「需要決定」."""
    assert FETCH_RESTART_MAX == 5
    assert FETCH_RESTART_WINDOW_S == 600.0
    assert FETCH_MIN_BACKOFF_S == 1.0
