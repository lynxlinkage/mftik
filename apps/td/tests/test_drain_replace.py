"""Applying a drain-replace: no stop on a failed drain, one spawn after."""

from __future__ import annotations

import asyncio
from pathlib import Path

from mftik.procman import (
    ObservedWorker,
    ProcmanError,
    RestartIntensity,
    Supervisor,
    WorkerPhase,
    WorkerStatus,
)
from mftik.protocol import (
    TD_ACCOUNT_TRADING,
    TD_TRADING_DRAIN,
    Envelope,
    IntentOwner,
    TdAccountTrading,
    TdIntentPut,
    TdTradingDrainResult,
)
from mftik_td.controller import TdOrchestrator, intent_book
from mftik_td.controller.types import (
    AccountView,
    ActionKind,
    BoundAccount,
    OrchestratorAction,
    account_worker_id,
)
from mftik_td.controller.worker import account_worker_spec
from mftik_td.supervise import apply_reconcile, run_drain_replace

API = 7


def _account(api_id: int = API) -> BoundAccount:
    return BoundAccount(api_id=api_id, venue="Paper", instance="td")


def _orch(tmp_path: Path) -> TdOrchestrator:
    return TdOrchestrator(
        Supervisor(tmp_path, plane="td", instance="td"),
        intensity=RestartIntensity(max_restarts=2, window_s=30, min_backoff_s=0.5),
        code_ref="test",
    )


def _status(
    account: BoundAccount, incarnation: int, *, ready: bool = True
) -> WorkerStatus:
    spec = account_worker_spec(
        account,
        incarnation=incarnation,
        argv=("python",),
        code_ref="test",
        start_timeout_s=1,
        hb_timeout_s=1,
        stop_grace_s=1,
    )
    return WorkerStatus(
        spec=spec,
        phase=WorkerPhase.RUNNING,
        pid=10,
        ready=ready,
        exit_code=None,
        signal=None,
        rss_bytes=None,
    )


class _Supervisor:
    def __init__(self, status: WorkerStatus | None) -> None:
        self.status_now = status
        self.stopped: list[str] = []
        self.spawned: list[object] = []
        self.fail_stop = False
        self.fail_spawn = False

    async def status(self, worker_id: str) -> WorkerStatus | None:
        del worker_id
        return self.status_now

    async def stop(self, worker_id: str) -> None:
        self.stopped.append(worker_id)
        if self.fail_stop:
            raise ProcmanError(f"cannot stop {worker_id}")
        self.status_now = None

    async def spawn(self, spec: object) -> None:
        self.spawned.append(spec)
        if self.fail_spawn:
            raise ProcmanError("spawn failed")
        self.status_now = WorkerStatus(
            spec=spec,  # type: ignore[arg-type]
            phase=WorkerPhase.RUNNING,
            pid=11,
            ready=True,
            exit_code=None,
            signal=None,
            rss_bytes=None,
        )

    async def release_slot(self, worker_id: str) -> None:
        raise AssertionError(worker_id)


class _Broker:
    def __init__(
        self, *, drained: bool = True, block: asyncio.Event | None = None
    ) -> None:
        self.drained = drained
        self.block = block
        self.started = asyncio.Event()
        self.types: list[str] = []
        self.aborts: list[bool] = []
        self.pushed: list[bool] = []

    async def request(self, subject: str, envelope, *, timeout: float | None = None):
        del subject, timeout
        self.types.append(envelope.type)
        if envelope.type == TD_TRADING_DRAIN:
            abort = bool(envelope.payload.abort)
            self.aborts.append(abort)
            self.started.set()
            if self.block is not None and not abort:
                await self.block.wait()
            return Envelope[TdTradingDrainResult].wrap(
                TdTradingDrainResult(
                    api_id=envelope.payload.api_id,
                    drained=False if abort else self.drained,
                ),
                type=TD_TRADING_DRAIN,
                source="td",
            )
        if envelope.type == TD_ACCOUNT_TRADING:
            active = envelope.payload.active
            self.pushed.append(active)
            return Envelope[TdAccountTrading].wrap(
                TdAccountTrading(api_id=envelope.payload.api_id, active=active),
                type=TD_ACCOUNT_TRADING,
                source="td",
            )
        raise AssertionError(envelope.type)


def _view(api_id: int = API) -> AccountView:
    return AccountView(
        api_id=api_id,
        observed=ObservedWorker.RUNNING,
        pid_gone=False,
        incarnation=1,
    )


def test_reconcile_skips_an_account_being_replaced(tmp_path: Path) -> None:
    orch = _orch(tmp_path)
    view = _view()
    assert any(
        action.kind is ActionKind.STOP
        for action in orch.reconcile((), (), (view,))
    )
    orch.draining.add(API)
    assert orch.reconcile((), (), (view,)) == ()
    other = _account(8)
    actions = orch.reconcile(
        (_account(), other),
        (
            TdIntentPut(
                session_id="s",
                owner=IntentOwner(sts_instance="sts", session_id="s"),
                api_ids=[API, 8],
            ),
        ),
        (view, _view(8)),
        publish=True,
    )
    assert all(action.api_id != API for action in actions)
    assert OrchestratorAction(
        kind=ActionKind.PUSH_TRADING, api_id=8, active=True
    ) in actions


async def test_a_drain_that_does_not_finish_does_not_stop(tmp_path: Path) -> None:
    account = _account()
    supervisor = _Supervisor(_status(account, 2))
    broker = _Broker(drained=False)
    actions = (
        OrchestratorAction(kind=ActionKind.EXTEND_DEADMAN, api_id=API),
        OrchestratorAction(kind=ActionKind.DRAIN, api_id=API),
        OrchestratorAction(kind=ActionKind.STOP, api_id=API),
    )
    drained = await apply_reconcile(
        supervisor,
        actions,
        (account,),
        code_ref="test",
        cancel_on_disconnect={API: False},
        broker=broker,
    )
    assert drained == {API: False}
    assert supervisor.stopped == []
    assert broker.types == [TD_TRADING_DRAIN]


async def test_a_finished_drain_stops(tmp_path: Path) -> None:
    account = _account()
    supervisor = _Supervisor(_status(account, 2))
    drained = await apply_reconcile(
        supervisor,
        (
            OrchestratorAction(kind=ActionKind.EXTEND_DEADMAN, api_id=API),
            OrchestratorAction(kind=ActionKind.DRAIN, api_id=API),
            OrchestratorAction(kind=ActionKind.STOP, api_id=API),
        ),
        (account,),
        code_ref="test",
        cancel_on_disconnect={},
        broker=_Broker(drained=True),
    )
    assert drained[API] is True
    assert supervisor.stopped == [account_worker_id(API)]


async def test_replace_spawns_the_next_incarnation_and_pushes(
    tmp_path: Path,
) -> None:
    book = intent_book()
    book.clear()
    account = _account()
    supervisor = _Supervisor(_status(account, 2))
    broker = _Broker()
    orch = _orch(tmp_path)
    try:
        book.put(
            TdIntentPut(
                session_id="s",
                owner=IntentOwner(sts_instance="sts", session_id="s"),
                api_ids=[API],
            )
        )
        result = await run_drain_replace(
            supervisor,
            orch,
            broker,
            account,
            cancel_on_disconnect={API: False},
        )
    finally:
        book.clear()
    assert result.ok is True
    assert result.incarnation == 3
    assert supervisor.stopped == [account_worker_id(API)]
    assert len(supervisor.spawned) == 1
    assert supervisor.spawned[0].incarnation == 3  # type: ignore[attr-defined]
    assert broker.pushed == [True]
    assert orch.draining == set()


async def test_a_second_replace_is_refused(tmp_path: Path) -> None:
    account = _account()
    release = asyncio.Event()
    broker = _Broker(block=release)
    supervisor = _Supervisor(_status(account, 2))
    orch = _orch(tmp_path)
    first = asyncio.create_task(
        run_drain_replace(
            supervisor,
            orch,
            broker,
            account,
            cancel_on_disconnect={API: False},
        )
    )
    await broker.started.wait()
    second = await run_drain_replace(
        supervisor,
        orch,
        broker,
        account,
        cancel_on_disconnect={API: False},
    )
    assert second.ok is False
    assert second.reason == "drain_in_progress"
    release.set()
    done = await first
    assert done.ok is True
    assert orch.draining == set()


async def test_not_running_does_not_hang(tmp_path: Path) -> None:
    broker = _Broker()
    result = await run_drain_replace(
        _Supervisor(None),
        _orch(tmp_path),
        broker,
        _account(),
        cancel_on_disconnect={},
    )
    assert result.ok is False
    assert result.reason == "not_running"
    assert broker.types == []


async def test_a_failed_stop_resumes_the_worker(tmp_path: Path) -> None:
    account = _account()
    supervisor = _Supervisor(_status(account, 2))
    supervisor.fail_stop = True
    broker = _Broker()
    result = await run_drain_replace(
        supervisor,
        _orch(tmp_path),
        broker,
        account,
        cancel_on_disconnect={},
    )
    assert result.ok is False
    assert result.reason == "stop_failed"
    assert supervisor.spawned == []
    assert True in broker.aborts


async def test_a_failed_spawn_does_not_claim_the_old_worker(
    tmp_path: Path,
) -> None:
    account = _account()
    supervisor = _Supervisor(_status(account, 2))
    supervisor.fail_spawn = True
    result = await run_drain_replace(
        supervisor,
        _orch(tmp_path),
        _Broker(),
        account,
        cancel_on_disconnect={},
    )
    assert result.ok is False
    assert result.reason == "spawn_failed"
    assert "still serving" not in result.reason
