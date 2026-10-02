"""Applying a reconcile pass: paper spawn, capacity, and the boot view."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest
from mftik.procman import (
    CapacityExceeded,
    ObservedWorker,
    ProcmanError,
    ReattachObservation,
    WorkerPhase,
    WorkerStatus,
)
from mftik_td.account.heartbeat import BEAT_PERIOD_S
from mftik_td.controller.defaults import ACCOUNT_HB_TIMEOUT_S
from mftik_td.controller.types import (
    ActionKind,
    BoundAccount,
    OrchestratorAction,
    account_worker_id,
)
from mftik_td.controller.worker import account_worker_spec
from mftik_td.supervise import (
    account_views,
    account_worker_argv,
    apply_reconcile,
)


class _Supervisor:
    def __init__(
        self,
        work_dir: Path,
        statuses: dict[str, WorkerStatus] | None = None,
    ) -> None:
        self.work_dir = work_dir
        self.spawned: list[str] = []
        self._statuses = statuses or {}

    async def status(self, worker_id: str) -> WorkerStatus | None:
        return self._statuses.get(worker_id)

    async def spawn(self, spec) -> None:
        if spec.id == account_worker_id(1):
            raise CapacityExceeded("full")
        if spec.id == account_worker_id(2):
            raise ProcmanError("pid 9 is still alive")
        self.spawned.append(spec.id)

    async def stop(self, worker_id: str) -> None:
        raise ProcmanError(f"cannot stop {worker_id}")

    async def release_slot(self, worker_id: str) -> None:
        return None


def _account(api_id: int, venue: str) -> BoundAccount:
    return BoundAccount(api_id=api_id, venue=venue, instance="td")


def _spawn(api_id: int) -> OrchestratorAction:
    return OrchestratorAction(
        kind=ActionKind.SPAWN, api_id=api_id, incarnation=1
    )


def test_the_beat_period_is_inside_the_heartbeat_timeout() -> None:
    assert BEAT_PERIOD_S < ACCOUNT_HB_TIMEOUT_S


def test_the_worker_argv_names_the_module() -> None:
    argv = account_worker_argv(7, 2, True)
    assert argv[1:3] == ("-m", "mftik_td.account")
    assert "--api-id" in argv
    assert "7" in argv
    assert argv[-1] == "true"


async def test_one_capacity_refusal_does_not_stop_the_pass(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    supervisor = _Supervisor(tmp_path)
    accounts = (
        _account(1, "Paper"),
        _account(2, "Paper"),
        _account(4, "Gate"),
        _account(3, "Paper"),
    )
    actions = (
        _spawn(1),
        OrchestratorAction(kind=ActionKind.STOP, api_id=9),
        _spawn(2),
        _spawn(4),
        _spawn(3),
        OrchestratorAction(kind=ActionKind.PUSH_TRADING, api_id=3, active=True),
    )

    await apply_reconcile(
        supervisor,
        actions,
        accounts,
        code_ref="test",
        cancel_on_disconnect={3: False},
    )

    assert supervisor.spawned == [account_worker_id(3)]
    text = "\n".join(record.getMessage() for record in caplog.records)
    assert "api_id=1" in text
    assert "capacity_exceeded" in text
    refused = [
        record.getMessage()
        for record in caplog.records
        if "api_id=2" in record.getMessage()
    ]
    assert refused
    assert all("capacity_exceeded" not in line for line in refused)
    assert "api_id=4" in text
    assert "Gate" in text


async def test_a_held_slot_replaces_the_boot_view(tmp_path: Path) -> None:
    """``live_start_ticks`` is None, so the boot view would say the pid is gone.

    ``status`` of a live phase is what the pass actually reconciles.
    """
    account = _account(7, "Paper")
    spec = account_worker_spec(
        account,
        incarnation=2,
        argv=account_worker_argv(7, 2, False),
        code_ref="test",
        start_timeout_s=ACCOUNT_HB_TIMEOUT_S,
        hb_timeout_s=ACCOUNT_HB_TIMEOUT_S,
        stop_grace_s=ACCOUNT_HB_TIMEOUT_S,
    )
    status = WorkerStatus(
        spec=spec,
        phase=WorkerPhase.RUNNING,
        pid=10,
        ready=True,
        exit_code=None,
        signal=None,
        rss_bytes=None,
    )
    observation = ReattachObservation(
        id=account_worker_id(7),
        observed=ObservedWorker.RUNNING,
        spec=spec,
        incarnation=1,
        status=None,
        exit_record=None,
    )
    held = _Supervisor(tmp_path, {account_worker_id(7): status})
    views = await account_views(held, (observation,), (account,))
    assert len(views) == 1
    assert views[0].pid_gone is False
    assert views[0].observed is ObservedWorker.RUNNING
    assert views[0].incarnation == 2

    gone = _Supervisor(tmp_path)
    assert await account_views(gone, (observation,), (account,)) == ()
