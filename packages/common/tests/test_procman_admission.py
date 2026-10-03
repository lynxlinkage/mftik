"""B3-05: admit or refuse a start before anything is launched.

The decision is pure (no process, no ``/proc``). Supervisor tests fake
``spawn_shim``. One integration test starts a real shim.
"""

from __future__ import annotations

import asyncio
import os
import threading
from pathlib import Path

import pytest
from mftik.procman import (
    SHIM_VMRSS_BYTES,
    AdmissionBudget,
    AdmissionReason,
    AdmissionWorker,
    CapacityExceeded,
    InvalidWorkerSpec,
    ProcmanError,
    Supervisor,
    WorkerPhase,
    WorkerSpec,
    decide_admission,
    socket_path,
    supervisor_state_path,
)
from mftik.procman.supervisor import _Slot
from test_procman_supervisor import (
    _SLEEP,
    _argv,
    _cleanup,
    _dead_pid,
    _peer,
    _ps_family,
    _reap_workdir,
    _until,
)
from test_procman_supervisor import (
    _spec as _sleep_spec,
)

_MIB = 1024 * 1024


def _spec(**overrides: object) -> WorkerSpec:
    raw: dict[str, object] = {
        "id": "td/account/42",
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


def _budget(**overrides: object) -> AdmissionBudget:
    raw: dict[str, object] = {
        "max_workers": None,
        "memory_budget_mb": None,
        "estimate_mb": {},
    }
    raw.update(overrides)
    return AdmissionBudget(**raw)  # type: ignore[arg-type]


def _worker(
    worker_id: str,
    phase: WorkerPhase | None,
    *,
    kind: str | None = "account",
    rss_bytes: int | None = None,
) -> AdmissionWorker:
    return AdmissionWorker(
        id=worker_id, kind=kind, phase=phase, rss_bytes=rss_bytes
    )


def _decide(
    budget: AdmissionBudget | None,
    *,
    held: tuple[AdmissionWorker, ...] = (),
    spawning: tuple[AdmissionWorker, ...] = (),
    candidate: WorkerSpec | None = None,
):
    return decide_admission(
        budget=budget,
        held=held,
        spawning=spawning,
        candidate=candidate if candidate is not None else _spec(),
    )


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


def _no_launch(monkeypatch: pytest.MonkeyPatch) -> None:
    def explode(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("spawn or /proc ran")

    monkeypatch.setattr("mftik.procman.supervisor.spawn_shim", explode)
    monkeypatch.setattr("mftik.procman.supervisor._measure_pss", explode)
    monkeypatch.setattr("mftik.procman.supervisor._tree_pss_bytes", explode)


# --- budget ---------------------------------------------------------------


def test_the_shim_figure_is_the_b3_01_measurement() -> None:
    """§4.7: VmRSS 14576 kB, and kB on Linux is 1024 bytes."""
    assert SHIM_VMRSS_BYTES == 14576 * 1024


def test_a_budget_rejects_anything_that_is_not_a_positive_int() -> None:
    with pytest.raises(InvalidWorkerSpec):
        _budget(max_workers=True)
    with pytest.raises(InvalidWorkerSpec):
        _budget(max_workers=0)
    with pytest.raises(InvalidWorkerSpec):
        _budget(max_workers=-1)
    with pytest.raises(InvalidWorkerSpec):
        _budget(memory_budget_mb=1.5)
    with pytest.raises(InvalidWorkerSpec):
        _budget(memory_budget_mb=True)
    with pytest.raises(InvalidWorkerSpec):
        _budget(estimate_mb={"account": True})
    with pytest.raises(InvalidWorkerSpec):
        _budget(estimate_mb={"account": 0})
    with pytest.raises(InvalidWorkerSpec):
        _budget(estimate_mb={"": 1})
    with pytest.raises(InvalidWorkerSpec):
        _budget(estimate_mb="account")  # type: ignore[arg-type]


def test_a_budget_keeps_the_callers_estimates_at_arms_length() -> None:
    estimates = {"account": 30}
    budget = _budget(max_workers=2, memory_budget_mb=64, estimate_mb=estimates)
    estimates["account"] = 1
    estimates["conn"] = 2
    assert dict(budget.estimate_mb) == {"account": 30}
    with pytest.raises(TypeError):
        budget.estimate_mb["account"] = 9  # type: ignore[index]
    assert budget.max_workers == 2
    assert budget.memory_budget_mb == 64


def test_none_and_an_empty_budget_are_unlimited() -> None:
    held = tuple(
        _worker(f"td/account/{index}", WorkerPhase.RUNNING) for index in range(4)
    )
    assert _decide(None, held=held).admitted
    assert _decide(_budget(), held=held).admitted


# --- counting -------------------------------------------------------------


@pytest.mark.parametrize(
    "phase",
    [WorkerPhase.STARTING, WorkerPhase.RUNNING, WorkerPhase.STOPPING],
)
def test_only_a_live_phase_counts_toward_max_workers(phase: WorkerPhase) -> None:
    decision = _decide(
        _budget(max_workers=1),
        held=(_worker("td/account/1", phase),),
        candidate=_spec(id="td/account/2"),
    )
    assert decision.admitted is False
    assert decision.reason is AdmissionReason.WORKERS
    assert "2 workers" in decision.message
    assert "max_workers is 1" in decision.message


@pytest.mark.parametrize(
    "phase",
    [
        WorkerPhase.FAILED,
        WorkerPhase.CRASHED,
        WorkerPhase.BACKOFF,
        WorkerPhase.FATAL,
        WorkerPhase.STOPPED,
        WorkerPhase.LOST,
    ],
)
def test_a_phase_that_is_not_alive_does_not_count(phase: WorkerPhase) -> None:
    decision = _decide(
        _budget(max_workers=1),
        held=(_worker("td/account/1", phase),),
        candidate=_spec(id="td/account/2"),
    )
    assert decision.admitted
    assert decision.reason is None


def test_an_in_flight_spawn_counts_and_the_same_id_is_not_counted_twice() -> None:
    budget = _budget(max_workers=2)
    spawning = (_worker("td/account/1", None),)
    assert _decide(
        budget, spawning=spawning, candidate=_spec(id="td/account/2")
    ).admitted
    refused = _decide(
        _budget(max_workers=1),
        spawning=spawning,
        candidate=_spec(id="td/account/2"),
    )
    assert refused.reason is AdmissionReason.WORKERS
    both = (
        _worker("td/account/1", WorkerPhase.STARTING, rss_bytes=10),
        _worker("td/account/9", WorkerPhase.FAILED),
    )
    # The same id is live and in flight. It counts once, plus the candidate.
    once = _decide(
        budget,
        held=both,
        spawning=spawning,
        candidate=_spec(id="td/account/2"),
    )
    assert once.admitted


@pytest.mark.parametrize(
    "phase",
    [
        WorkerPhase.FAILED,
        WorkerPhase.CRASHED,
        WorkerPhase.BACKOFF,
        WorkerPhase.FATAL,
        WorkerPhase.STOPPED,
        WorkerPhase.LOST,
        WorkerPhase.RUNNING,
    ],
)
def test_replacing_a_held_id_is_not_refused_or_counted_twice(
    phase: WorkerPhase,
) -> None:
    """A restart is a start the plan does not refuse, even over the cap."""
    held = (
        _worker("td/account/1", phase, rss_bytes=50 * _MIB),
        _worker("td/account/9", WorkerPhase.RUNNING),
    )
    decision = _decide(
        _budget(max_workers=1, memory_budget_mb=1, estimate_mb={"account": 1}),
        held=held,
        candidate=_spec(id="td/account/1", incarnation=2),
    )
    assert decision.admitted
    alone = _decide(
        _budget(max_workers=1),
        held=(_worker("td/account/1", phase),),
        candidate=_spec(id="td/account/1", incarnation=2),
    )
    assert alone.admitted


def test_a_missing_kind_is_refused_and_not_taken_as_zero() -> None:
    budget = _budget(memory_budget_mb=1024, estimate_mb={"account": 1})
    missing = _decide(budget, candidate=_spec(kind="fetch"))
    assert missing.admitted is False
    assert missing.reason is AdmissionReason.UNKNOWN_KIND
    assert "fetch" in missing.message
    assert "zero" in missing.message
    unmeasured = _decide(
        budget,
        held=(_worker("td/account/1", WorkerPhase.RUNNING, kind="fetch"),),
        candidate=_spec(id="td/account/2"),
    )
    assert unmeasured.reason is AdmissionReason.UNKNOWN_KIND
    assert "td/account/1" in unmeasured.message
    dead = _decide(
        budget,
        held=(_worker("td/account/1", WorkerPhase.FAILED, kind="fetch"),),
        candidate=_spec(id="td/account/2"),
    )
    assert dead.admitted
    inflight = _decide(
        budget,
        spawning=(_worker("td/account/1", None, kind=None),),
        candidate=_spec(id="td/account/2"),
    )
    assert inflight.reason is AdmissionReason.UNKNOWN_KIND
    assert "td/account/1" in inflight.message


def test_a_measured_rss_is_used_and_zero_is_a_measurement() -> None:
    """The estimate is the fallback. A stored Pss, including 0, wins."""
    fits = _decide(
        _budget(memory_budget_mb=40, estimate_mb={"conn": 100, "account": 1}),
        held=(_worker("md/conn/0", WorkerPhase.RUNNING, kind="conn", rss_bytes=1024),),
        candidate=_spec(id="td/account/2", kind="account"),
    )
    assert fits.admitted
    overflows = _decide(
        _budget(memory_budget_mb=40, estimate_mb={"conn": 1, "account": 1}),
        held=(
            _worker(
                "md/conn/0",
                WorkerPhase.RUNNING,
                kind="conn",
                rss_bytes=100 * _MIB,
            ),
        ),
        candidate=_spec(id="td/account/2", kind="account"),
    )
    assert overflows.reason is AdmissionReason.MEMORY
    zero = _decide(
        _budget(memory_budget_mb=30, estimate_mb={"conn": 100, "account": 1}),
        held=(_worker("md/conn/0", WorkerPhase.RUNNING, kind="conn", rss_bytes=0),),
        candidate=_spec(id="td/account/2", kind="account"),
    )
    assert zero.admitted


def test_one_shim_is_added_for_every_counted_worker() -> None:
    one = _MIB + SHIM_VMRSS_BYTES
    assert one <= 20 * _MIB
    assert 2 * one > 20 * _MIB
    budget = _budget(memory_budget_mb=20, estimate_mb={"account": 1})
    assert _decide(budget, candidate=_spec(id="td/account/1")).admitted
    second = _decide(
        budget,
        held=(_worker("td/account/1", WorkerPhase.RUNNING),),
        candidate=_spec(id="td/account/2"),
    )
    assert second.reason is AdmissionReason.MEMORY
    assert "20" in second.message
    spawning = _decide(
        budget,
        spawning=(_worker("td/account/1", None),),
        candidate=_spec(id="td/account/2"),
    )
    assert spawning.reason is AdmissionReason.MEMORY


def test_equal_to_the_budget_is_inside_it_and_one_byte_over_is_not() -> None:
    estimate = _MIB
    budget_mb = 40
    budget_bytes = budget_mb * _MIB
    rss = budget_bytes - estimate - 2 * SHIM_VMRSS_BYTES
    assert rss > 0
    budget = _budget(memory_budget_mb=budget_mb, estimate_mb={"account": 1})
    held = (_worker("td/account/1", WorkerPhase.RUNNING, rss_bytes=rss),)
    exact = _decide(budget, held=held, candidate=_spec(id="td/account/2"))
    assert exact.admitted
    over = _decide(
        budget,
        held=(_worker("td/account/1", WorkerPhase.RUNNING, rss_bytes=rss + 1),),
        candidate=_spec(id="td/account/2"),
    )
    assert over.reason is AdmissionReason.MEMORY
    assert str(budget_bytes) in over.message
    assert str(budget_bytes + 1) in over.message


def test_the_worker_limit_is_reported_before_a_missing_kind() -> None:
    decision = _decide(
        _budget(max_workers=1, memory_budget_mb=1, estimate_mb={}),
        held=(_worker("td/account/1", WorkerPhase.RUNNING),),
        candidate=_spec(id="td/account/2", kind="fetch"),
    )
    assert decision.reason is AdmissionReason.WORKERS


def test_a_bad_admission_argument_is_refused() -> None:
    with pytest.raises(TypeError):
        decide_admission(
            budget={"max_workers": 1},  # type: ignore[arg-type]
            held=(),
            spawning=(),
            candidate=_spec(),
        )
    with pytest.raises(TypeError):
        _decide(_budget(max_workers=1), held=("nope",))  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        _decide(_budget(max_workers=1), held=(_worker("td/account/1", None),))


# --- supervisor, no processes ---------------------------------------------


def _supervisor(tmp_path: Path, **budget: object) -> Supervisor:
    return Supervisor(
        tmp_path,
        plane="td",
        instance="td",
        budget=_budget(**budget) if budget else None,
    )


async def test_a_refused_start_does_not_launch_or_reserve(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _no_launch(monkeypatch)

    def ran(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("fence or supervisor.json ran")

    monkeypatch.setattr("mftik.procman.supervisor._fence_pid", ran)
    monkeypatch.setattr("mftik.procman.supervisor._merge_spawn_intent", ran)
    supervisor = _supervisor(tmp_path, max_workers=1)
    held = _spec(id="td/account/1")
    failed = _spec(id="td/account/9", incarnation=3)
    _hold(supervisor, held, WorkerPhase.RUNNING)
    failed_slot = _hold(supervisor, failed, WorkerPhase.FAILED)
    refused = _spec(id="td/account/2")
    with pytest.raises(CapacityExceeded) as exc:
        await supervisor.spawn(refused)
    assert exc.value.code == "capacity_exceeded"
    assert "max_workers" in str(exc.value)
    assert refused.id not in supervisor._spawning
    assert refused.id not in supervisor._spawning.kinds
    assert not socket_path(tmp_path, refused.id).exists()
    assert not supervisor_state_path(tmp_path).exists()
    assert supervisor._slots[held.id].phase is WorkerPhase.RUNNING
    assert supervisor._slots[failed.id] is failed_slot


async def test_a_restart_at_the_cap_is_not_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []

    def admitted(spec: WorkerSpec, *, work_dir: Path) -> None:
        del work_dir
        calls.append(spec.id)
        raise OSError("admitted")

    def walked(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("/proc ran")

    monkeypatch.setattr("mftik.procman.supervisor.spawn_shim", admitted)
    monkeypatch.setattr("mftik.procman.supervisor._measure_pss", walked)
    monkeypatch.setattr("mftik.procman.supervisor._tree_pss_bytes", walked)
    supervisor = _supervisor(tmp_path, max_workers=1)
    running = _spec(id="td/account/9")
    failed = _spec(id="td/account/1")
    _hold(supervisor, running, WorkerPhase.RUNNING)
    _hold(
        supervisor,
        failed,
        WorkerPhase.FAILED,
        pid=_dead_pid(),
        worker_start_ticks=9,
        released=True,
    )
    with pytest.raises(OSError, match="admitted"):
        await supervisor.spawn(_spec(id=failed.id, incarnation=2))
    assert calls == [failed.id]
    assert failed.id not in supervisor._spawning
    assert supervisor._slots[running.id].phase is WorkerPhase.RUNNING


async def test_a_live_slot_is_still_refused_before_admission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _no_launch(monkeypatch)
    supervisor = _supervisor(tmp_path, max_workers=1)
    spec = _spec()
    _hold(supervisor, spec, WorkerPhase.RUNNING)
    with pytest.raises(ProcmanError, match="will not spawn over") as exc:
        await supervisor.spawn(_spec(incarnation=2))
    assert not isinstance(exc.value, CapacityExceeded)
    assert not supervisor_state_path(tmp_path).exists()


async def test_replacing_a_lost_slot_is_not_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A ``LOST`` slot whose pid is gone is a restart. The cap does not block it."""
    calls: list[str] = []

    def admitted(spec: WorkerSpec, *, work_dir: Path) -> None:
        del work_dir
        calls.append(spec.id)
        raise OSError("admitted")

    monkeypatch.setattr("mftik.procman.supervisor.spawn_shim", admitted)
    supervisor = _supervisor(tmp_path, max_workers=1)
    running = _spec(id="td/account/9")
    lost = _spec(id="td/account/1")
    _hold(supervisor, running, WorkerPhase.RUNNING)
    _hold(
        supervisor,
        lost,
        WorkerPhase.LOST,
        exit_code=None,
        pid=_dead_pid(),
        worker_start_ticks=9,
        released=True,
        shim_pid=0,
    )
    with pytest.raises(OSError, match="admitted"):
        await supervisor.spawn(_spec(id=lost.id, incarnation=2))
    assert calls == [lost.id]
    assert lost.id not in supervisor._spawning
    assert supervisor._slots[running.id].phase is WorkerPhase.RUNNING


async def test_a_live_lost_pid_is_fenced_and_not_a_capacity_refusal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Admission admits the replacement. The fence still refuses a live pid."""
    _no_launch(monkeypatch)
    supervisor = _supervisor(tmp_path, max_workers=1)
    _hold(supervisor, _spec(id="td/account/9"), WorkerPhase.RUNNING)
    pid = os.getpid()
    ticks = int(Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19])
    lost = _spec(id="td/account/1")
    _hold(
        supervisor,
        lost,
        WorkerPhase.LOST,
        exit_code=None,
        pid=pid,
        worker_start_ticks=ticks,
    )
    with pytest.raises(ProcmanError, match=f"worker pid {pid} is still alive") as exc:
        await supervisor.spawn(_spec(id=lost.id, incarnation=2))
    assert not isinstance(exc.value, CapacityExceeded)
    assert supervisor._slots[lost.id].phase is WorkerPhase.LOST
    assert lost.id not in supervisor._spawning
    assert not supervisor_state_path(tmp_path).exists()


async def test_release_slot_drops_the_worker_from_the_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The count is ``_slots``. A released id is a new start, and a refusal
    does not rewrite ``supervisor.json``."""
    calls: list[str] = []

    def admitted(spec: WorkerSpec, *, work_dir: Path) -> None:
        del work_dir
        calls.append(spec.id)
        raise OSError("admitted")

    monkeypatch.setattr("mftik.procman.supervisor.spawn_shim", admitted)
    supervisor = _supervisor(tmp_path, max_workers=1)
    live = _spec(id="td/account/1")
    held = _spec(id="td/account/2")
    _hold(supervisor, live, WorkerPhase.RUNNING)
    _hold(
        supervisor,
        held,
        WorkerPhase.FAILED,
        released=True,
        pid=_dead_pid(),
        worker_start_ticks=9,
        shim_pid=0,
    )
    await supervisor.release_slot(held.id)
    assert held.id not in supervisor._slots
    path = supervisor_state_path(tmp_path)
    written = path.read_bytes()
    with pytest.raises(CapacityExceeded) as exc:
        await supervisor.spawn(_spec(id=held.id, incarnation=2))
    assert exc.value.code == "capacity_exceeded"
    assert held.id not in supervisor._spawning
    assert path.read_bytes() == written
    assert calls == []
    supervisor._slots.pop(live.id)
    with pytest.raises(OSError, match="admitted"):
        await supervisor.spawn(_spec(id="td/account/3"))
    assert calls == ["td/account/3"]


async def test_the_incarnation_check_still_runs_before_admission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _no_launch(monkeypatch)
    supervisor = _supervisor(tmp_path, max_workers=1)
    spec = _spec()
    slot = _hold(supervisor, spec, WorkerPhase.CRASHED)
    _hold(supervisor, _spec(id="td/account/9"), WorkerPhase.RUNNING)
    with pytest.raises(ProcmanError, match="incarnation"):
        await supervisor.spawn(spec)
    assert supervisor._slots[spec.id] is slot


async def test_a_second_new_id_sees_the_reservation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two concurrent starts cannot both pass. The check holds the lock."""
    started = threading.Event()
    release = threading.Event()
    calls: list[str] = []

    def block(spec: WorkerSpec, *, work_dir: Path) -> None:
        del work_dir
        calls.append(spec.id)
        if len(calls) > 1:
            raise AssertionError("second launch")
        started.set()
        if not release.wait(timeout=5):
            raise TimeoutError("release")
        raise OSError("admitted, then stopped")

    monkeypatch.setattr("mftik.procman.supervisor.spawn_shim", block)
    supervisor = _supervisor(tmp_path, max_workers=1)
    first = _spec(id="td/account/1")
    second = _spec(id="td/account/2")
    task = asyncio.create_task(supervisor.spawn(first))
    assert await asyncio.to_thread(started.wait, 2)
    with pytest.raises(CapacityExceeded):
        await supervisor.spawn(second)
    assert calls == [first.id]
    assert second.id not in supervisor._spawning
    assert first.id in supervisor._spawning
    release.set()
    with pytest.raises(OSError, match="admitted"):
        await task
    assert first.id not in supervisor._spawning
    assert first.id not in supervisor._spawning.kinds


async def test_memory_and_an_unknown_kind_refuse_before_launch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _no_launch(monkeypatch)
    tight = _supervisor(
        tmp_path, memory_budget_mb=1, estimate_mb={"account": 1}
    )
    with pytest.raises(CapacityExceeded, match="memory_budget_mb") as exc:
        await tight.spawn(_spec())
    assert exc.value.code == "capacity_exceeded"
    assert "td/account/42" not in tight._spawning
    unknown = _supervisor(tmp_path, memory_budget_mb=1024, estimate_mb={})
    with pytest.raises(ProcmanError, match="fetch") as missing:
        await unknown.spawn(_spec(kind="fetch"))
    assert not isinstance(missing.value, CapacityExceeded)


async def test_a_measured_pss_admits_what_the_estimate_would_refuse(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []

    def admitted(spec: WorkerSpec, *, work_dir: Path) -> None:
        del work_dir
        calls.append(spec.id)
        raise OSError("admitted")

    def walked(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("/proc ran")

    monkeypatch.setattr("mftik.procman.supervisor.spawn_shim", admitted)
    monkeypatch.setattr("mftik.procman.supervisor._measure_pss", walked)
    monkeypatch.setattr("mftik.procman.supervisor._tree_pss_bytes", walked)
    supervisor = Supervisor(
        tmp_path,
        plane="td",
        instance="td",
        budget=_budget(
            memory_budget_mb=40, estimate_mb={"conn": 100, "account": 1}
        ),
    )
    _hold(
        supervisor,
        _spec(id="td/account/1", kind="conn"),
        WorkerPhase.RUNNING,
        rss_bytes=1024,
    )
    with pytest.raises(OSError, match="admitted"):
        await supervisor.spawn(_spec(id="td/account/2", kind="account"))
    assert calls == ["td/account/2"]


async def test_no_budget_still_launches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []

    def admitted(spec: WorkerSpec, *, work_dir: Path) -> None:
        del work_dir
        calls.append(spec.id)
        raise OSError("admitted")

    monkeypatch.setattr("mftik.procman.supervisor.spawn_shim", admitted)
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    for index in range(3):
        _hold(supervisor, _spec(id=f"td/account/{index}"), WorkerPhase.RUNNING)
    with pytest.raises(OSError, match="admitted"):
        await supervisor.spawn(_spec(id="td/account/9"))
    assert calls == ["td/account/9"]


def test_a_supervisor_rejects_a_budget_it_would_have_to_invent(
    tmp_path: Path,
) -> None:
    with pytest.raises(TypeError, match="AdmissionBudget"):
        Supervisor(
            tmp_path,
            plane="td",
            instance="td",
            budget={"max_workers": 1},  # type: ignore[arg-type]
        )


# --- real shim ------------------------------------------------------------


@pytest.mark.integration
async def test_the_worker_that_fits_starts_and_the_next_is_refused(
    tmp_path: Path,
) -> None:
    """One real shim. The next start is ``capacity_exceeded`` and leaves no orphan."""
    supervisor = Supervisor(
        tmp_path,
        plane="td",
        instance="td",
        budget=_budget(max_workers=1),
    )
    first = _sleep_spec(
        _argv(_SLEEP), id="td/account/1", hb_timeout_s=None, start_timeout_s=30
    )
    second = _sleep_spec(
        _argv(_SLEEP), id="td/account/2", hb_timeout_s=None, start_timeout_s=30
    )
    try:
        await supervisor.spawn(first)
        status = await _until(
            supervisor,
            first.id,
            lambda item: item is not None and item.pid is not None,
        )
        with pytest.raises(CapacityExceeded) as exc:
            await supervisor.spawn(second)
        assert exc.value.code == "capacity_exceeded"
        assert "max_workers" in str(exc.value)
        assert await supervisor.status(second.id) is None
        assert second.id not in supervisor._spawning
        assert not socket_path(tmp_path, second.id).exists()
        shims, workers = _ps_family(tmp_path)
        assert shims == {_peer(socket_path(tmp_path, first.id))}
        assert workers == {status.pid}
    finally:
        await _cleanup(supervisor)
        _reap_workdir(tmp_path)
        assert _ps_family(tmp_path) == (set(), set())
