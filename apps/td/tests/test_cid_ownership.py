"""Cancel ownership — cross-session cancels are logged, never blocked."""

from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import Any

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.exchange import PaperExchange, Side
from mftik.exchange.models import OrderStatus, OrderType, limit_order
from mftik.protocol import (
    STS_LEASE_HEARTBEAT,
    STS_ORDER_CANCEL,
    STS_ORDER_SUBMIT,
    Envelope,
    LeaseHeartbeat,
    OrderAck,
    OrderCancel,
    OrderSubmit,
    TdAttachRequest,
    Topics,
)
from mftik_td.session import PaperSessionFactory, SessionManager

API_ID = 42


@pytest.fixture
async def broker() -> Broker:
    async with a_broker() as client:
        yield client


@pytest.fixture
async def paper() -> PaperExchange:
    async with PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")},
        tick_interval=0.05,
        seed=7,
    ) as ex:
        yield ex


@pytest.fixture
def factory(broker: Broker, paper: PaperExchange) -> PaperSessionFactory:
    return PaperSessionFactory(broker, paper)


async def _lease_publisher(
    broker: Broker, session_id: str, stop: asyncio.Event
) -> None:
    token = 0
    topic = Topics.sts_td_session(session_id)
    while not stop.is_set():
        token += 1
        await broker.publish(
            topic,
            Envelope[LeaseHeartbeat].wrap(
                LeaseHeartbeat(session_id=session_id, token=token),
                type=STS_LEASE_HEARTBEAT,
                source="sts",
                session_id=session_id,
            ),
        )
        try:
            await asyncio.wait_for(stop.wait(), timeout=0.1)
        except TimeoutError:
            continue


async def _order_rpc(broker: Broker, envelope: Envelope[Any]) -> bool:
    """Send an order request the way StrategyOms does and return the ack."""
    reply = await broker.request(
        Topics.td_order(API_ID), envelope, timeout=2.0
    )
    return OrderAck.model_validate(reply.payload).accepted


async def _submit(broker: Broker, session_id: str, cid: str) -> bool:
    return await _order_rpc(
        broker,
        Envelope[OrderSubmit].wrap(
            OrderSubmit(
                session_id=session_id,
                api_id=API_ID,
                universal_ticker="Paper_Spot_BTCUSDT",
                side=Side.BUY,
                type=OrderType.LIMIT,
                qty=Decimal("0.01"),
                price=Decimal("1000"),
                client_order_id=cid,
            ),
            type=STS_ORDER_SUBMIT,
            source="sts",
            session_id=session_id,
        ),
    )


async def _cancel(broker: Broker, session_id: str, cid: str) -> bool:
    return await _order_rpc(
        broker,
        Envelope[OrderCancel].wrap(
            OrderCancel(
                session_id=session_id, api_id=API_ID, client_order_id=cid
            ),
            type=STS_ORDER_CANCEL,
            source="sts",
            session_id=session_id,
        ),
    )


def _order_by_cid(manager: SessionManager, cid: str):
    session = manager.get(API_ID)
    assert session is not None
    for order in session.oms.view().orders.values():
        if order.client_order_id == cid:
            return order
    return None


@pytest.fixture
async def two_sessions(broker: Broker, factory: PaperSessionFactory):
    """Manager with sts-a and sts-b both attached to ``API_ID``."""
    manager = SessionManager(factory, broker, lease_grace=2.0)
    stop = asyncio.Event()
    tasks = [
        asyncio.create_task(_lease_publisher(broker, "sts-a", stop)),
        asyncio.create_task(_lease_publisher(broker, "sts-b", stop)),
    ]
    for sid in ("sts-a", "sts-b"):
        await manager.attach(
            TdAttachRequest(
                session_id=sid, api_id=API_ID, timeout=2.0, created_by=1
            )
        )
    yield manager, stop, tasks
    stop.set()
    await asyncio.gather(*tasks, return_exceptions=True)
    await manager.close_all()


async def test_foreign_cancel_is_warned_not_blocked(
    broker: Broker, two_sessions, caplog
) -> None:
    """Cross-session cancels go through — blocking would strand orphans."""
    manager, _, _ = two_sessions

    cid = "1001"
    await _submit(broker, "sts-a", cid)
    await asyncio.sleep(0.2)
    assert manager._accounts[API_ID].cid_owner[cid] == "sts-a"
    assert _order_by_cid(manager, cid) is not None

    # sts-b sees the cid on td.oms.{api_id} and cancels it.
    with caplog.at_level(logging.WARNING, logger="mftik_td.session.manager"):
        await _cancel(broker, "sts-b", cid)
        await asyncio.sleep(0.2)

    order = _order_by_cid(manager, cid)
    assert order is None or order.status is OrderStatus.CANCELED

    warned = [
        r for r in caplog.records if "cross-session cancel" in r.getMessage()
    ]
    assert warned, "expected a warning for the cross-session cancel"
    assert "owner=sts-a (live)" in warned[0].getMessage()

    # The operator-facing log stream carries it too.
    lines = await broker.fetch_log_buffer(Topics.log_td(API_ID))
    assert any("cross-session cancel" in raw and cid in raw for raw in lines)


async def test_detached_owner_is_reported_as_detached(
    broker: Broker, two_sessions, caplog
) -> None:
    """The orphan case the warning exists for."""
    manager, _, _ = two_sessions

    cid = "1500"
    await _submit(broker, "sts-a", cid)
    await asyncio.sleep(0.2)
    await manager.detach(session_id="sts-a", api_id=API_ID)

    with caplog.at_level(logging.WARNING, logger="mftik_td.session.manager"):
        await _cancel(broker, "sts-b", cid)
        await asyncio.sleep(0.2)

    order = _order_by_cid(manager, cid)
    assert order is None or order.status is OrderStatus.CANCELED

    warned = [
        r for r in caplog.records if "cross-session cancel" in r.getMessage()
    ]
    assert warned
    assert "owner=sts-a (detached)" in warned[0].getMessage()


async def test_owner_can_cancel(broker: Broker, two_sessions) -> None:
    manager, _, _ = two_sessions

    cid = "1002"
    await _submit(broker, "sts-a", cid)
    await asyncio.sleep(0.2)
    assert _order_by_cid(manager, cid) is not None

    await _cancel(broker, "sts-a", cid)
    await asyncio.sleep(0.2)

    order = _order_by_cid(manager, cid)
    assert order is None or order.status is OrderStatus.CANCELED
    # Terminal status prunes the ownership entry.
    assert cid not in manager._accounts[API_ID].cid_owner


async def test_unowned_cid_is_cancelable(broker: Broker, two_sessions) -> None:
    """Recon-discovered orders have no owner and stay cancelable by anyone."""
    manager, _, _ = two_sessions
    session = manager.get(API_ID)
    assert session is not None

    cid = "2001"
    await session.private.place_order(limit_order(
        ticker="Paper_Spot_BTCUSDT",
        side=Side.BUY,
        qty=Decimal("0.01"),
        price=Decimal("1000"),
        client_order_id=cid,
    ))
    await asyncio.sleep(0.1)
    assert cid not in manager._accounts[API_ID].cid_owner

    await _cancel(broker, "sts-b", cid)
    await asyncio.sleep(0.2)

    order = _order_by_cid(manager, cid)
    assert order is None or order.status is OrderStatus.CANCELED


async def test_detach_keeps_owner_for_provenance(
    broker: Broker, two_sessions
) -> None:
    """Ownership outlives the link so the warning can still name the owner."""
    manager, _, _ = two_sessions

    cid = "3001"
    await _submit(broker, "sts-a", cid)
    await asyncio.sleep(0.2)
    acct = manager._accounts[API_ID]
    assert acct.cid_owner[cid] == "sts-a"

    await manager.detach(session_id="sts-a", api_id=API_ID)
    assert acct.cid_owner[cid] == "sts-a"
    assert "sts-a" not in acct.links
