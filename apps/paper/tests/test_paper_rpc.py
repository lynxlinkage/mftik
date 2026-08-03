from __future__ import annotations

import asyncio
from decimal import Decimal

import fakeredis.aioredis
import pytest
from mft.broker import Broker, BrokerConfig
from mft.exchange import PaperExchange, Side
from mft.exchange.models import OrderStatus, OrderType, PlaceOrderRequest
from mft.exchange.paper.remote import PaperRemotePrivateClient
from mft_paper.app import RedisEventBridge
from mft_paper.rpc import dispatch


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


@pytest.mark.asyncio
async def test_remote_private_place_cancel(broker: Broker) -> None:
    bridge = RedisEventBridge(broker)
    exchange = PaperExchange(
        tick_interval=10.0,
        on_order=bridge.on_order,
        on_fill=bridge.on_fill,
        on_balance=bridge.on_balance,
    )
    exchange.register_api(
        "paper-key-1",
        "paper-secret-1",
        balances={"BTC": Decimal("1"), "USDT": Decimal("100000")},
    )
    exchange.register_api(
        "paper-key-2",
        "paper-secret-2",
        balances={"BTC": Decimal("10"), "USDT": Decimal("500000")},
    )
    await exchange.place_order(
        "paper-key-2",
        PlaceOrderRequest(
            symbol="BTCUSDT",
            side=Side.SELL,
            type=OrderType.LIMIT,
            qty=Decimal("10"),
            price=Decimal("50001"),
            client_order_id="seed-ask",
        ),
    )
    await exchange.start()

    stop = asyncio.Event()
    rpc_task = asyncio.create_task(_serve(broker, exchange, stop))

    private = PaperRemotePrivateClient(
        broker, api_key="paper-key-1", api_secret="paper-secret-1"
    )
    await private.connect()

    # Resting below ask — does not cross.
    order = await private.place_limit_order(
        symbol="BTCUSDT",
        side=Side.BUY,
        qty=Decimal("1"),
        price=Decimal("50000"),
    )
    assert order.status is OrderStatus.NEW
    assert order.client_order_id

    canceled = await private.cancel_by_client_order_id(order.client_order_id)
    assert canceled.status is OrderStatus.CANCELED

    # Cross the seeded ask — fills against paper-key-2.
    filled = await private.place_limit_order(
        symbol="BTCUSDT",
        side=Side.BUY,
        qty=Decimal("1"),
        price=Decimal("50001"),
    )
    assert filled.status is OrderStatus.FILLED

    bals = {b.asset: b.free for b in await private.fetch_balances()}
    assert bals["BTC"] == Decimal("2")

    await private.close()
    stop.set()
    rpc_task.cancel()
    await asyncio.gather(rpc_task, return_exceptions=True)
    await exchange.stop()


async def _serve(
    broker: Broker, exchange: PaperExchange, stop: asyncio.Event
) -> None:
    from mft.protocol import Topics

    async for req in broker.serve(Topics.PAPER, stop=stop):
        await dispatch(req, exchange=exchange)
