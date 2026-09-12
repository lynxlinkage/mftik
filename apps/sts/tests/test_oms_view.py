"""Strategy-side OMS reads — resolve must return None, not raise.

``td: {}`` is a legal MD-only run. Several attached accounts must be named.
``td_sole()`` raises in both cases; ``view`` / ``order`` must not.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.exchange.models import Order, OrderStatus, OrderType, Side
from mftik.exchange.oms import OmsView
from mftik.protocol import (
    TD_OMS_ORDER,
    TD_OMS_VIEW,
    Envelope,
    TdOmsOrderRequest,
    TdOmsViewRequest,
    Topics,
)
from mftik.strategy.oms import StrategyOms


@pytest.fixture
async def broker() -> Broker:
    async with a_broker() as client:
        yield client


def _oms(broker: Broker, *api_ids: int) -> StrategyOms:
    oms = StrategyOms()
    session = SimpleNamespace(
        broker=broker,
        td_api_ids=list(api_ids),
        session_id="s-oms",
        strategy=SimpleNamespace(name="quiet"),
    )
    oms.bind(SimpleNamespace(session=session))
    return oms


def _order(cid: str) -> Order:
    return Order(
        client_order_id=cid,
        universal_ticker="Paper_Spot_BTCUSDT",
        side=Side.BUY,
        type=OrderType.LIMIT,
        qty=Decimal("1"),
        price=Decimal("10"),
        status=OrderStatus.NEW,
    )


class _Book:
    def __init__(self, broker: Broker) -> None:
        self._broker = broker
        self._orders: dict[int, dict[str, Order]] = {}
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task] = []

    def write(self, api_id: int, cid: str) -> None:
        self._orders.setdefault(api_id, {})[cid] = _order(cid)

    async def start(self, *api_ids: int) -> None:
        for api_id in api_ids:
            self._tasks.append(
                asyncio.create_task(self._serve(api_id), name=f"oms-{api_id}")
            )
            await asyncio.sleep(0)

    async def close(self) -> None:
        self._stop.set()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _serve(self, api_id: int) -> None:
        async for req in self._broker.serve(
            Topics.td_account(api_id), stop=self._stop
        ):
            if req.envelope.type == TD_OMS_VIEW:
                TdOmsViewRequest.model_validate(req.envelope.payload or {})
                await req.reply(
                    Envelope[OmsView].wrap(
                        OmsView(orders=dict(self._orders.get(api_id, {}))),
                        type=TD_OMS_VIEW,
                        source="td",
                    )
                )
                continue
            payload = TdOmsOrderRequest.model_validate(req.envelope.payload or {})
            order = self._orders.get(api_id, {}).get(payload.client_order_id)
            if order is None:
                await req.reply(Envelope[dict].wrap({}, type=TD_OMS_ORDER, source="td"))
            else:
                await req.reply(
                    Envelope[Order].wrap(order, type=TD_OMS_ORDER, source="td")
                )


@pytest.mark.asyncio
async def test_a_single_account_needs_no_api_id(broker: Broker) -> None:
    book = _Book(broker)
    book.write(7, "1")
    await book.start(7)
    try:
        oms = _oms(broker, 7)
        view = await oms.view()
        assert set(view.orders) == {"1"}
    finally:
        await book.close()


@pytest.mark.asyncio
async def test_no_account_reads_as_an_empty_book(broker: Broker) -> None:
    book = _Book(broker)
    book.write(7, "1")
    await book.start(7)
    try:
        oms = _oms(broker)
        assert (await oms.view()).orders == {}
        assert await oms.order("1") is None
    finally:
        await book.close()


@pytest.mark.asyncio
async def test_multiple_accounts_must_be_named(broker: Broker) -> None:
    book = _Book(broker)
    book.write(7, "a")
    book.write(8, "b")
    await book.start(7, 8)
    try:
        oms = _oms(broker, 7, 8)
        assert (await oms.view()).orders == {}
        assert set((await oms.view(7)).orders) == {"a"}
        assert set((await oms.view(8)).orders) == {"b"}
        assert await oms.order("a") is None
        assert (await oms.order("a", 7)) is not None
    finally:
        await book.close()


@pytest.mark.asyncio
async def test_order_is_a_direct_read_of_the_authority(broker: Broker) -> None:
    book = _Book(broker)
    book.write(7, "cid-new")
    await book.start(7)
    try:
        oms = _oms(broker, 7)
        assert await oms.order("cid-new") is not None
    finally:
        await book.close()
