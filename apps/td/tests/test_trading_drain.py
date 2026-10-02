"""Drain-replace on the trading layer: refuse, wait, resume (F27)."""

from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from pathlib import Path

import pytest
from mftik.broker.errors import RequestTimeoutError
from mftik.clock import FakeClock
from mftik.exchange.models import OrderType, Side
from mftik.procman import (
    ProcmanError,
    RestartIntensity,
    Supervisor,
    WorkerPhase,
    WorkerStatus,
)
from mftik.protocol import (
    TD_TRADING_DRAIN,
    Envelope,
    OrderCancel,
    OrderSubmit,
    RejectCode,
    TdTradingDrainResult,
)
from mftik_td.account import AccountWorker
from mftik_td.account.trading import TradingLayer
from mftik_td.controller import TdOrchestrator
from mftik_td.controller.defaults import QUIESCE_LEASE_S
from mftik_td.controller.types import BoundAccount
from mftik_td.controller.worker import account_worker_spec
from mftik_td.supervise import run_drain_replace

API = 7


async def _until(ready) -> None:  # noqa: ANN001
    for _ in range(10):
        if ready():
            return
        await asyncio.sleep(0)
    raise AssertionError("the drain step did not become ready")


class _Resident:
    api_id = API


class _Oms:
    def view(self) -> object:
        return type("View", (), {"orders": {}})()


class _Book:
    def __init__(self) -> None:
        self.oms = _Oms()
        self.ledger = object()
        self.private = object()
        self.starts = 0
        self.destroys = 0
        self.destroyed = False

    async def start(self) -> None:
        self.starts += 1

    async def destroy(self) -> None:
        self.destroys += 1
        self.destroyed = True


class _Orders:
    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.submits: list[str] = []
        self.cancels: list[str] = []

    async def submit(self, request: object) -> str:
        self.submits.append(str(request))
        await self.release.wait()
        return "placed"

    async def cancel(self, request: object) -> str:
        self.cancels.append(str(request))
        return "cancelled"

    async def cancel_session(self, request: object) -> str:
        return "session"


def _submit(api_id: int = API) -> OrderSubmit:
    return OrderSubmit(
        session_id="sess",
        api_id=api_id,
        universal_ticker="Bybit_Spot_BTCUSDT",
        side=Side.BUY,
        type=OrderType.LIMIT,
        qty=Decimal("0.01"),
        price=Decimal("60000"),
        client_order_id="cid-1",
    )


def _cancel(api_id: int = API) -> OrderCancel:
    return OrderCancel(
        session_id="sess", api_id=api_id, client_order_id="cid-1"
    )


async def test_a_submit_is_refused_while_draining_and_a_cancel_is_not() -> None:
    worker = AccountWorker(API, venue="Bybit")
    worker.trading._draining = True  # noqa: SLF001 — the flag's other setter waits
    submit = await worker.orders.submit(_submit())
    assert submit.accepted is False
    assert submit.error_code == RejectCode.TD_DRAINING
    cancel = await worker.orders.cancel(_cancel())
    assert cancel.error_code == RejectCode.TD_VENUE_NOT_CONNECTED
    wrong = await worker.orders.submit(_submit(api_id=8))
    assert wrong.error_code == RejectCode.TD_WRONG_API_ID


async def test_a_cancel_is_refused_once_nothing_is_in_flight() -> None:
    worker = AccountWorker(API, venue="Bybit")
    worker.trading._draining = True  # noqa: SLF001
    worker.trading._quiesced = True  # noqa: SLF001
    cancel = await worker.orders.cancel(_cancel())
    assert cancel.accepted is False
    assert cancel.error_code == RejectCode.TD_DRAINING


@pytest.mark.component
async def test_an_in_flight_submit_finishes_before_drained() -> None:
    clock = FakeClock()
    book = _Book()
    layer = TradingLayer(
        _Resident(),  # type: ignore[arg-type]
        session=book,  # type: ignore[arg-type]
        clock=clock,
        replace_timeout_s=30,
    )
    await layer.activate()
    orders = _Orders()
    layer.arm(orders)  # type: ignore[arg-type]
    inflight = asyncio.create_task(orders.submit("one"))
    await _until(lambda: layer._busy == 1)  # noqa: SLF001
    drain = asyncio.create_task(layer.drain_for_replace())
    await _until(lambda: layer.draining and not drain.done())
    assert await orders.cancel("c") == "cancelled"
    assert orders.cancels == ["c"]
    orders.release.set()
    assert await drain is True
    assert await inflight == "placed"
    assert layer.refusing_cancels is True
    assert layer.active is True
    assert book.destroys == 0
    orders.release.clear()
    later = asyncio.create_task(orders.submit("two"))
    await asyncio.sleep(0)
    assert layer._busy == 0  # noqa: SLF001 — a new submit is not counted
    orders.release.set()
    await later


@pytest.mark.component
async def test_a_drain_timeout_resumes_service() -> None:
    clock = FakeClock()
    book = _Book()
    layer = TradingLayer(
        _Resident(),  # type: ignore[arg-type]
        session=book,  # type: ignore[arg-type]
        clock=clock,
        replace_timeout_s=5,
    )
    await layer.activate()
    orders = _Orders()
    layer.arm(orders)  # type: ignore[arg-type]
    inflight = asyncio.create_task(orders.submit("one"))
    await _until(lambda: layer._busy == 1)  # noqa: SLF001
    drain = asyncio.create_task(layer.drain_for_replace())
    # The deadline is captured after the flag flips. Advancing before
    # the sleep is queued moves the clock and the wait is scheduled
    # past it, so this waits until that sleep exists.
    await _until(lambda: layer.draining and bool(clock._heap))  # noqa: SLF001
    clock.advance(5)
    await asyncio.sleep(0)
    assert await drain is False
    assert layer.draining is False
    assert layer.refusing_cancels is False
    assert layer.active is True
    assert book.destroys == 0
    orders.release.set()
    await inflight
    orders.release.clear()
    again = asyncio.create_task(orders.submit("two"))
    await asyncio.sleep(0)
    assert layer._busy == 1  # noqa: SLF001
    orders.release.set()
    assert await again == "placed"


def _layer(clock: FakeClock, book: _Book) -> TradingLayer:
    return TradingLayer(
        _Resident(),  # type: ignore[arg-type]
        session=book,  # type: ignore[arg-type]
        clock=clock,
        replace_timeout_s=30,
    )


async def _serve_again(layer: TradingLayer, orders: _Orders) -> None:
    """A submit and a cancel both enter the handler after the quiesce."""
    orders.release.clear()
    submit = asyncio.create_task(orders.submit("again"))
    await asyncio.sleep(0)
    assert layer._busy == 1  # noqa: SLF001
    orders.release.set()
    assert await submit == "placed"
    assert await orders.cancel("c") == "cancelled"
    assert orders.cancels == ["c"]


@pytest.mark.component
async def test_a_quiesce_lease_resumes_when_nothing_stops_the_worker(
    caplog: pytest.LogCaptureFixture,
) -> None:
    clock = FakeClock()
    book = _Book()
    layer = _layer(clock, book)
    await layer.activate()
    orders = _Orders()
    layer.arm(orders)  # type: ignore[arg-type]
    with caplog.at_level(logging.WARNING):
        assert await layer.drain_for_replace() is True
        await _until(lambda: bool(clock._heap))  # noqa: SLF001
        assert layer.refusing_cancels is True
        clock.advance(QUIESCE_LEASE_S)
        await asyncio.sleep(0)
    assert layer.draining is False
    assert layer.refusing_cancels is False
    assert layer.active is True
    assert book.destroys == 0
    assert "quiesce lease expired" in caplog.text
    await _serve_again(layer, orders)


@pytest.mark.component
async def test_a_stop_inside_the_lease_does_not_resume(
    caplog: pytest.LogCaptureFixture,
) -> None:
    clock = FakeClock()
    book = _Book()
    layer = _layer(clock, book)
    await layer.activate()
    assert await layer.drain_for_replace() is True
    await _until(lambda: bool(clock._heap))  # noqa: SLF001
    with caplog.at_level(logging.WARNING):
        # deactivate is the process stop: it sets ``_closing`` and
        # cancels the lease before the session is destroyed.
        await layer.deactivate()
        clock.advance(QUIESCE_LEASE_S)
        await asyncio.sleep(0)
    assert "quiesce lease expired" not in caplog.text
    assert layer.draining is True
    assert layer.refusing_cancels is True
    assert layer.active is False
    assert book.destroys == 1


@pytest.mark.component
async def test_a_second_drain_inside_the_lease_returns_immediately() -> None:
    clock = FakeClock()
    layer = _layer(clock, _Book())
    await layer.activate()
    assert await layer.drain_for_replace() is True
    await _until(lambda: bool(clock._heap))  # noqa: SLF001
    # No advance. A wait on the lease would sit on the fake clock.
    assert await layer.drain_for_replace() is True
    assert layer.draining is True
    assert layer.refusing_cancels is True
    assert clock.monotonic() == 0


class _Held:
    """One worker slot. ``stop`` drops it. Spawn is not part of a refused drain."""

    def __init__(self, account: BoundAccount) -> None:
        spec = account_worker_spec(
            account,
            incarnation=2,
            argv=("python",),
            code_ref="test",
            start_timeout_s=1,
            hb_timeout_s=1,
            stop_grace_s=1,
        )
        self.status_now: WorkerStatus | None = WorkerStatus(
            spec=spec,
            phase=WorkerPhase.RUNNING,
            pid=10,
            ready=True,
            exit_code=None,
            signal=None,
            rss_bytes=None,
        )
        self.stopped: list[str] = []

    async def status(self, worker_id: str) -> WorkerStatus | None:
        del worker_id
        return self.status_now

    async def stop(self, worker_id: str) -> None:
        self.stopped.append(worker_id)
        raise ProcmanError(f"cannot stop {worker_id}")

    async def spawn(self, spec: object) -> None:
        raise AssertionError(spec)


class _LateBroker:
    """The drain RPC times out. The worker still runs it, then the abort."""

    def __init__(self, layer: TradingLayer) -> None:
        self.layer = layer
        self.handler: asyncio.Task[bool] | None = None

    async def request(self, subject: str, envelope, *, timeout: float | None = None):
        del subject
        if envelope.type != TD_TRADING_DRAIN:
            raise AssertionError(envelope.type)
        if envelope.payload.abort:
            assert self.handler is not None
            # Same subject, one at a time: the drain finishes, then abort.
            assert await self.handler is True
            await self.layer.resume_after_drain()
            return Envelope[TdTradingDrainResult].wrap(
                TdTradingDrainResult(api_id=envelope.payload.api_id, drained=False),
                type=TD_TRADING_DRAIN,
                source="td",
            )
        self.handler = asyncio.create_task(self.layer.drain_for_replace())
        raise RequestTimeoutError("td.account.7", "late", float(timeout or 0))


@pytest.mark.component
async def test_a_timed_out_drain_aborts_a_quiesce_that_lands_later(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    book = _Book()
    layer = _layer(clock, book)
    await layer.activate()
    orders = _Orders()
    layer.arm(orders)  # type: ignore[arg-type]
    inflight = asyncio.create_task(orders.submit("one"))
    await _until(lambda: layer._busy == 1)  # noqa: SLF001
    account = BoundAccount(api_id=API, venue="Bybit", instance="td")
    broker = _LateBroker(layer)
    supervisor = _Held(account)
    orch = TdOrchestrator(
        Supervisor(tmp_path, plane="td", instance="td"),
        intensity=RestartIntensity(max_restarts=2, window_s=30, min_backoff_s=0.5),
        code_ref="test",
    )
    replace = asyncio.create_task(
        run_drain_replace(
            supervisor,
            orch,
            broker,
            account,
            cancel_on_disconnect={API: False},
        )
    )
    await _until(lambda: layer.draining and broker.handler is not None)
    # The controller has already timed out and is waiting on the abort,
    # which waits for this in-flight call to finish and the drain to quiesce.
    orders.release.set()
    result = await replace
    assert await inflight == "placed"
    assert result.ok is False
    assert result.reason == "not_drained"
    assert supervisor.stopped == []
    assert layer.draining is False
    assert layer.refusing_cancels is False
    assert layer.active is True
    assert orch.draining == set()
    await _serve_again(layer, orders)
