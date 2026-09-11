"""An ``api_id`` is held in-process; a second process is a boot-probe refusal.

The KV claim is gone. ``self._accounts`` is the only map. A second attach
on the same manager is a refcount. A second *process* of the same instance
name is refused at boot by ``refuse_if_serving`` — not a lock, and a
same-window dual-open is still accepted.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.exchange import PaperExchange
from mftik.health import InstanceAlreadyServing, refuse_if_serving
from mftik.protocol import (
    STS_LEASE_HEARTBEAT,
    Envelope,
    HealthStatus,
    HealthStatusEnvelope,
    LeaseHeartbeat,
    TdAttachRequest,
    Topics,
)
from mftik_td.session import PaperSessionFactory, SessionManager

API_ID = 3


@pytest.fixture
async def broker() -> Broker:
    async with a_broker("td-own") as client:
        yield client


@pytest.fixture
async def paper() -> PaperExchange:
    async with PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")},
        tick_interval=0.05,
        seed=3,
        volatility_bps=0,
    ) as ex:
        yield ex


def _manager(broker: Broker, paper: PaperExchange) -> SessionManager:
    factory = PaperSessionFactory(broker, paper)
    factory.bind_api(API_ID, api_key="key-3", api_secret="sec-3")
    return SessionManager(factory, broker, lease_grace=2.0, instance="td")


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
            await asyncio.wait_for(stop.wait(), timeout=0.05)
        except TimeoutError:
            continue


@pytest.mark.asyncio
async def test_a_second_attach_on_one_manager_is_a_refcount(
    broker: Broker, paper: PaperExchange
) -> None:
    manager = _manager(broker, paper)
    stop = asyncio.Event()
    first = asyncio.create_task(_lease_publisher(broker, "sts-a", stop))
    second = asyncio.create_task(_lease_publisher(broker, "sts-b", stop))
    try:
        await manager.attach(
            TdAttachRequest(
                session_id="sts-a", api_id=API_ID, timeout=2.0, created_by=1
            )
        )
        result = await manager.attach(
            TdAttachRequest(
                session_id="sts-b", api_id=API_ID, timeout=2.0, created_by=1
            )
        )
        assert manager.active_api_ids == [API_ID]
        assert result.refcount == 2
        assert manager.refcount(API_ID) == 2
    finally:
        stop.set()
        await asyncio.gather(first, second, return_exceptions=True)
        await manager.close_all()


@pytest.mark.asyncio
async def test_closing_the_account_lets_another_manager_attach(
    broker: Broker, paper: PaperExchange
) -> None:
    first = _manager(broker, paper)
    second = _manager(broker, paper)
    stop = asyncio.Event()
    pub = asyncio.create_task(_lease_publisher(broker, "hand-over", stop))
    try:
        await first.attach(
            TdAttachRequest(
                session_id="hand-over", api_id=API_ID, timeout=2.0, created_by=1
            )
        )
        await first.close_all()

        await second.attach(
            TdAttachRequest(
                session_id="hand-over", api_id=API_ID, timeout=2.0, created_by=1
            )
        )
        assert second.active_api_ids == [API_ID]
    finally:
        stop.set()
        await pub
        await second.close_all()


@pytest.mark.asyncio
async def test_boot_probe_refuses_a_second_process_of_the_same_instance(
    broker: Broker,
) -> None:
    stop = asyncio.Event()

    async def serve() -> None:
        async for req in broker.serve(Topics.td("td"), stop=stop):
            await req.reply(
                HealthStatusEnvelope.wrap(
                    HealthStatus(status="ok", service="td", instance="td"),
                    type="td.health",
                    source="td",
                )
            )

    task = asyncio.create_task(serve())
    await asyncio.sleep(0.05)
    try:
        with pytest.raises(InstanceAlreadyServing) as refused:
            await refuse_if_serving(
                broker, domain="td", instance="td", timeout=1.0
            )
        assert refused.value.subject == Topics.td("td")
        assert "td" in str(refused.value)
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
