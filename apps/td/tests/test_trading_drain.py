"""Drain-replace on the trading layer: refuse, wait, resume (F27)."""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
from mftik.clock import FakeClock
from mftik.exchange.models import OrderType, Side
from mftik.protocol import OrderCancel, OrderSubmit, RejectCode
from mftik_td.account import AccountWorker
from mftik_td.account.trading import TradingLayer

API = 7


async def _until(ready) -> None:  # noqa: ANN001
    for _ in range(10):
        if ready():
            return
        await asyncio.sleep(0)
    raise AssertionError("the drain step did not become ready")


class _Resident:
    api_id = API


class _Book:
    def __init__(self) -> None:
        self.oms = object()
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
