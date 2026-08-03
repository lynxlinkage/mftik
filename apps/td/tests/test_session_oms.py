from __future__ import annotations

import asyncio
from decimal import Decimal

import fakeredis.aioredis
import pytest
from mft.broker import Broker, BrokerConfig
from mft.exchange import PaperExchange, Side
from mft.exchange.models import OrderStatus
from mft.protocol import (
    STS_LEASE_HEARTBEAT,
    Envelope,
    LeaseHeartbeat,
    TdAttachRequest,
    Topics,
)
from mft_td.session import PaperSessionFactory, SessionManager


@pytest.fixture
async def broker() -> Broker:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test"),
        redis_client=redis,
    )
    await client.connect()
    yield client
    await client.close()
    await redis.aclose()


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


@pytest.mark.asyncio
async def test_oms_updates_from_session_callbacks(
    broker: Broker, factory: PaperSessionFactory
) -> None:
    factory.bind_api(1, api_key="key-1", api_secret="sec-1")
    session = await factory.create(1)
    await session.start()

    private = session.private
    order = await private.place_limit_order(
        symbol="BTCUSDT",
        side=Side.BUY,
        qty=Decimal("0.01"),
        price=Decimal("1000"),
    )
    await asyncio.sleep(0.05)

    view = session.oms.view()
    # The book is keyed by client_order_id, not the venue's id.
    assert order.client_order_id in view.orders
    assert view.orders[order.client_order_id].status is OrderStatus.NEW
    assert private.api_key == "key-1"

    await session.destroy()


@pytest.mark.asyncio
async def test_attach_refcount_destroy(
    broker: Broker, factory: PaperSessionFactory
) -> None:
    manager = SessionManager(factory, broker, lease_grace=2.0)
    stop = asyncio.Event()
    pubs = [
        asyncio.create_task(_lease_publisher(broker, "sts-a", stop)),
        asyncio.create_task(_lease_publisher(broker, "sts-b", stop)),
    ]

    r1 = await manager.attach(
        TdAttachRequest(session_id="sts-a", api_id=42, timeout=2.0, created_by=1)
    )
    r2 = await manager.attach(
        TdAttachRequest(session_id="sts-b", api_id=42, timeout=2.0, created_by=1)
    )
    assert r1.refcount == 1
    assert r2.refcount == 2
    session = manager.get(42)
    assert session is not None
    assert session.private.api_key == "paper-key-42"

    await manager.detach(session_id="sts-a", api_id=42)
    assert manager.get(42) is not None

    await manager.detach(session_id="sts-b", api_id=42)
    assert manager.get(42) is None
    assert session.destroyed

    stop.set()
    await asyncio.gather(*pubs)


@pytest.mark.asyncio
async def test_paper_factory_isolates_api_keys(
    broker: Broker, paper: PaperExchange
) -> None:
    factory = PaperSessionFactory(broker, paper)
    factory.bind_api(1, "alice-key", "alice-secret")
    factory.bind_api(2, "bob-key", "bob-secret")

    s1 = await factory.create(1)
    s2 = await factory.create(2)
    assert s1.private.api_key == "alice-key"
    assert s2.private.api_key == "bob-key"
    await s1.destroy()
    await s2.destroy()
