"""SDK settled read against an in-process paper account worker.

A subprocess paper worker uses ``PaperRemotePrivateClient``, which has
no fetch-by-client-order-id on purpose. The chase loop's forced recon
then waits ``UNKNOWN_FORCE_RECON_S`` (10s) before it can settle the
book, which is this tier's whole budget before the worker has even
started. ``PaperPrivateClient`` can answer the chase. The test holds
that answer until ``StrategyOms.view(settled=True)`` is parked, then
lets it through.

``Session.start`` is not called. Its sweep and chase loops sleep, and
its order stream would race the UNKNOWN this test forces onto the book.
The worker is up the same way the handler tests mark it: connected,
started, active, serving ``td.account.{api_id}``.
"""

from __future__ import annotations

import asyncio
import contextlib
from decimal import Decimal
from types import SimpleNamespace

import pytest
from broker_harness import a_broker, subjects_under
from mftik.broker import Broker
from mftik.broker.handler import serve
from mftik.exchange import PaperExchange
from mftik.exchange.models import OrderStatus, Side, limit_order
from mftik.protocol import Topics
from mftik.strategy.oms import StrategyOms
from mftik_td.account import AccountWorker
from mftik_td.account.handlers import account_subject_handler
from mftik_td.account.session import Session

API = 8
CID = "cid-unknown"


async def _until_subscribed(broker: Broker, subject: str) -> None:
    connection = broker.transport.nc  # type: ignore[attr-defined]
    full = broker.transport._rpc_subject(subject)  # type: ignore[attr-defined]
    deadline = asyncio.get_running_loop().time() + 2.0
    while asyncio.get_running_loop().time() < deadline:
        if full in subjects_under(connection, broker.config.key_prefix):
            await connection.flush(timeout=1)
            return
        await asyncio.sleep(0)
    raise TimeoutError(full)


def _strategy_oms(broker: Broker) -> StrategyOms:
    oms = StrategyOms()
    session = SimpleNamespace(
        broker=broker,
        td_api_ids=[API],
        session_id="abc123",
        strategy=SimpleNamespace(name="quiet", registry_key="quiet"),
    )
    oms.bind(SimpleNamespace(session=session))  # type: ignore[arg-type]
    return oms


@pytest.mark.integration
async def test_the_sdk_settled_read_returns_after_the_unknown_order_resolves() -> None:
    exchange = PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")},
        tick_interval=60,
    )
    exchange.register_api(
        "paper-key",
        "paper-secret",
        balances={"USDT": Decimal("1000000")},
    )
    await exchange.start()
    try:
        async with a_broker("settled-view") as broker:
            await _run(broker, exchange)
    finally:
        await exchange.stop()


async def _run(broker: Broker, exchange: PaperExchange) -> None:
    private = exchange.private(
        api_key="paper-key",
        api_secret="paper-secret",
        auto_register=False,
    )
    await private.connect()
    placed = await private.place_order(
        limit_order(
            ticker="Paper_Spot_BTCUSDT",
            side=Side.BUY,
            qty=Decimal("0.01"),
            price=Decimal("49000"),
            client_order_id=CID,
        )
    )
    assert placed.status is OrderStatus.NEW
    session = Session(api_id=API, broker=broker, private=private)
    unknown = placed.model_copy(update={"status": OrderStatus.UNKNOWN})
    session.oms.handle_order(unknown)
    session._started = True
    worker = AccountWorker(API, venue="Paper", session=session)
    worker.trading._active = True

    entered = asyncio.Event()
    release = asyncio.Event()
    original = private.fetch_order_by_client_order_id

    async def gated(client_order_id: str, *, ticker: object = None) -> object:
        entered.set()
        await release.wait()
        return await original(client_order_id, ticker=ticker)  # type: ignore[arg-type]

    private.fetch_order_by_client_order_id = gated  # type: ignore[method-assign]

    stop = asyncio.Event()
    subject = Topics.td_account(API)
    server = asyncio.create_task(
        serve(broker, subject, account_subject_handler(worker), stop=stop)
    )
    view_task: asyncio.Task[object] | None = None
    try:
        await _until_subscribed(broker, subject)
        view_task = asyncio.create_task(_strategy_oms(broker).view(API, settled=True))
        await asyncio.wait_for(entered.wait(), timeout=2)
        assert not view_task.done()
        release.set()
        view = await asyncio.wait_for(view_task, timeout=2)
        assert CID in view.orders
        assert view.orders[CID].status is OrderStatus.NEW
        assert all(
            order.status is not OrderStatus.UNKNOWN for order in view.orders.values()
        )
    finally:
        release.set()
        if view_task is not None and not view_task.done():
            view_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await view_task
        stop.set()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(server, timeout=2)
        await worker.trading.deactivate()
