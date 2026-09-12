"""StrategyOms.wait_cids — live-book condition wait, woken by fan-out."""

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
    TD_ORDER_REJECT,
    TD_ORDER_UPDATE,
    Envelope,
    OrderReject,
    TdOmsOrderRequest,
    TdOmsViewRequest,
    Topics,
    UntypedEnvelope,
)
from mftik.strategy import Strategy
from mftik.strategy.oms import WAIT_CIDS_TIMEOUT_S, StrategyOms
from mftik_sts.session.session import StsSession


@pytest.fixture
async def broker() -> Broker:
    async with a_broker() as client:
        yield client


def _oms(broker: Broker, *api_ids: int) -> StrategyOms:
    oms = StrategyOms()
    session = SimpleNamespace(
        broker=broker,
        td_api_ids=list(api_ids),
        session_id="s-wait",
        strategy=SimpleNamespace(name="quiet"),
    )
    oms.bind(SimpleNamespace(session=session))
    return oms


def _order(cid: str, status: OrderStatus = OrderStatus.NEW) -> Order:
    return Order(
        client_order_id=cid,
        universal_ticker="Paper_Spot_BTCUSDT",
        side=Side.BUY,
        type=OrderType.LIMIT,
        qty=Decimal("1"),
        price=Decimal("10"),
        status=status,
    )


class _Book:
    def __init__(self, broker: Broker) -> None:
        self._broker = broker
        self._orders: dict[int, dict[str, Order]] = {}
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task] = []

    def write(
        self,
        api_id: int,
        cid: str,
        status: OrderStatus = OrderStatus.NEW,
    ) -> None:
        self._orders.setdefault(api_id, {})[cid] = _order(cid, status)

    def drop(self, api_id: int, cid: str) -> None:
        self._orders.get(api_id, {}).pop(cid, None)

    async def start(self, *api_ids: int) -> None:
        for api_id in api_ids:
            self._tasks.append(
                asyncio.create_task(self._serve(api_id), name=f"wait-{api_id}")
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
                await req.reply(
                    Envelope[dict].wrap({}, type=TD_OMS_ORDER, source="td")
                )
            else:
                await req.reply(
                    Envelope[Order].wrap(order, type=TD_OMS_ORDER, source="td")
                )


def _not_pending(order: Order) -> bool:
    return order.status is not OrderStatus.PENDING_NEW


@pytest.mark.asyncio
async def test_wait_cids_is_true_when_the_book_already_matches(
    broker: Broker,
) -> None:
    book = _Book(broker)
    book.write(7, "cid-1", OrderStatus.NEW)
    await book.start(7)
    try:
        oms = _oms(broker, 7)
        assert await oms.wait_cids(
            7, "cid-1", until=_not_pending, timeout=1.0
        )
    finally:
        await book.close()


@pytest.mark.asyncio
async def test_a_missing_cid_is_already_gone(broker: Broker) -> None:
    """Terminal orders leave the live book — there is nothing to wait for."""
    book = _Book(broker)
    await book.start(7)
    try:
        oms = _oms(broker, 7)
        assert await oms.wait_cids(7, "gone", until=_not_pending, timeout=1.0)
        assert await oms.wait_cids(7, [], until=_not_pending, timeout=1.0)
    finally:
        await book.close()


@pytest.mark.asyncio
async def test_wait_cids_times_out_while_still_pending(broker: Broker) -> None:
    book = _Book(broker)
    book.write(7, "cid-1", OrderStatus.PENDING_NEW)
    await book.start(7)
    try:
        oms = _oms(broker, 7)
        assert (
            await oms.wait_cids(7, "cid-1", until=_not_pending, timeout=0.1)
            is False
        )
    finally:
        await book.close()


@pytest.mark.asyncio
async def test_a_signal_wakes_wait_cids(broker: Broker) -> None:
    book = _Book(broker)
    book.write(7, "cid-1", OrderStatus.PENDING_NEW)
    await book.start(7)
    try:
        oms = _oms(broker, 7)

        async def flip() -> None:
            await asyncio.sleep(0.05)
            book.write(7, "cid-1", OrderStatus.NEW)
            oms.signal(7, "cid-1", _order("cid-1", OrderStatus.NEW))

        asyncio.create_task(flip())
        assert await oms.wait_cids(
            7, ["cid-1"], until=_not_pending, timeout=1.0
        )
    finally:
        await book.close()


@pytest.mark.asyncio
async def test_a_dropped_terminal_uses_the_fan_out_order(
    broker: Broker,
) -> None:
    """FILLED is popped from the book; the event payload still satisfies."""
    book = _Book(broker)
    book.write(7, "cid-1", OrderStatus.PENDING_NEW)
    await book.start(7)
    try:
        oms = _oms(broker, 7)

        async def fill() -> None:
            await asyncio.sleep(0.05)
            filled = _order("cid-1", OrderStatus.FILLED)
            book.drop(7, "cid-1")
            oms.signal(7, "cid-1", filled)

        asyncio.create_task(fill())
        assert await oms.wait_cids(
            7,
            "cid-1",
            until=lambda o: o.status is OrderStatus.FILLED,
            timeout=1.0,
        )
    finally:
        await book.close()


@pytest.mark.asyncio
async def test_every_cid_must_match(broker: Broker) -> None:
    book = _Book(broker)
    book.write(7, "a", OrderStatus.NEW)
    book.write(7, "b", OrderStatus.PENDING_NEW)
    await book.start(7)
    try:
        oms = _oms(broker, 7)

        async def flip_b() -> None:
            await asyncio.sleep(0.05)
            book.write(7, "b", OrderStatus.UNKNOWN)
            oms.signal(7, "b", _order("b", OrderStatus.UNKNOWN))

        asyncio.create_task(flip_b())
        assert await oms.wait_cids(
            7, ["a", "b"], until=_not_pending, timeout=1.0
        )
    finally:
        await book.close()


@pytest.mark.asyncio
async def test_wait_cids_rejects_a_negative_timeout(broker: Broker) -> None:
    oms = _oms(broker, 7)
    with pytest.raises(ValueError, match="timeout"):
        await oms.wait_cids(7, "x", until=_not_pending, timeout=-1)


@pytest.mark.asyncio
async def test_td_fan_out_wakes_wait_cids(broker: Broker) -> None:
    """Session dispatch signals OMS after the strategy hook returns."""
    book = _Book(broker)
    book.write(7, "cid-1", OrderStatus.PENDING_NEW)
    await book.start(7)
    try:
        strategy = Strategy()
        sts = StsSession(
            session_id="sts-wait-cids",
            broker=broker,
            created_by=1,
            strategy=strategy,
            td_api_ids=[7],
            heartbeat_interval=0.1,
        )

        async def publish_new() -> None:
            await asyncio.sleep(0.05)
            book.write(7, "cid-1", OrderStatus.NEW)
            await sts._on_td_global(
                7,
                UntypedEnvelope.wrap(
                    _order("cid-1", OrderStatus.NEW).model_dump(mode="json"),
                    type=TD_ORDER_UPDATE,
                    source="td",
                ),
            )

        asyncio.create_task(publish_new())
        assert await strategy.oms.wait_cids(
            7, "cid-1", until=_not_pending, timeout=1.0
        )
    finally:
        await book.close()


@pytest.mark.asyncio
async def test_a_raising_hook_still_wakes_wait_cids(broker: Broker) -> None:
    book = _Book(broker)
    book.write(7, "cid-1", OrderStatus.PENDING_NEW)
    await book.start(7)
    try:

        class Broken(Strategy):
            name = "broken"

            async def on_order_update(self, api_id: int, order: Order) -> None:
                raise RuntimeError("hook failed")

        strategy = Broken()
        sts = StsSession(
            session_id="sts-wait-raise",
            broker=broker,
            created_by=1,
            strategy=strategy,
            td_api_ids=[7],
            heartbeat_interval=0.1,
        )

        async def publish_new() -> None:
            await asyncio.sleep(0.05)
            book.write(7, "cid-1", OrderStatus.NEW)
            await sts._on_td_global(
                7,
                UntypedEnvelope.wrap(
                    _order("cid-1", OrderStatus.NEW).model_dump(mode="json"),
                    type=TD_ORDER_UPDATE,
                    source="td",
                ),
            )

        asyncio.create_task(publish_new())
        assert await strategy.oms.wait_cids(
            7, "cid-1", until=_not_pending, timeout=1.0
        )
    finally:
        await book.close()


@pytest.mark.asyncio
async def test_an_order_reject_clears_a_pending_cid(broker: Broker) -> None:
    book = _Book(broker)
    book.write(7, "cid-1", OrderStatus.PENDING_NEW)
    await book.start(7)
    try:
        strategy = Strategy()
        sts = StsSession(
            session_id="sts-wait-reject",
            broker=broker,
            created_by=1,
            strategy=strategy,
            td_api_ids=[7],
            heartbeat_interval=0.1,
        )

        async def reject() -> None:
            await asyncio.sleep(0.05)
            book.drop(7, "cid-1")
            await sts._on_td_global(
                7,
                UntypedEnvelope.wrap(
                    OrderReject(
                        api_id=7,
                        client_order_id="cid-1",
                        reason="post only",
                    ).model_dump(mode="json"),
                    type=TD_ORDER_REJECT,
                    source="td",
                ),
            )

        asyncio.create_task(reject())
        assert await strategy.oms.wait_cids(
            7, "cid-1", until=_not_pending, timeout=1.0
        )
    finally:
        await book.close()


def test_wait_timeout_constant_leaves_room_for_cancel() -> None:
    """on_stop budget is 10s; the helper must not consume all of it."""
    assert WAIT_CIDS_TIMEOUT_S == 5.0
