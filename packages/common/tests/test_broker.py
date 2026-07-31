from __future__ import annotations

import asyncio

import fakeredis.aioredis
import pytest
from mft.broker import Broker, BrokerConfig, IncomingRequest, RequestTimeoutError
from mft.protocol import Envelope, UntypedEnvelope


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
async def test_pubsub_roundtrip(broker: Broker) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    received: asyncio.Future[UntypedEnvelope] = loop.create_future()

    async def reader() -> None:
        async for env in broker.subscribe("topic.demo", stop=stop):
            if not received.done():
                received.set_result(env)
            break
        stop.set()

    task = asyncio.create_task(reader())
    await asyncio.sleep(0.05)

    sent = Envelope[dict].wrap(
        {"n": 1},
        type="demo",
        source="test",
    )
    await broker.publish("topic.demo", sent)

    got = await asyncio.wait_for(received, timeout=2)
    stop.set()
    await task

    assert got.type == "demo"
    assert got.payload == {"n": 1}
    assert got.id == sent.id


@pytest.mark.asyncio
async def test_request_reply(broker: Broker) -> None:
    stop = asyncio.Event()

    async def server() -> None:
        async for req in broker.serve("orders.place", stop=stop):
            await req.reply(
                Envelope[dict].wrap(
                    {"ok": True, "echo": req.envelope.payload},
                    type="orders.place.result",
                    source="td",
                )
            )
            break
        stop.set()

    task = asyncio.create_task(server())
    await asyncio.sleep(0.05)

    response = await broker.request(
        "orders.place",
        Envelope[dict].wrap(
            {"symbol": "BTCUSDT", "qty": 1},
            type="orders.place",
            source="sts",
        ),
        timeout=2,
    )
    await task

    assert response.type == "orders.place.result"
    assert response.payload == {
        "ok": True,
        "echo": {"symbol": "BTCUSDT", "qty": 1},
    }


@pytest.mark.asyncio
async def test_request_timeout(broker: Broker) -> None:
    with pytest.raises(RequestTimeoutError):
        await broker.request(
            "nobody.home",
            Envelope[dict].wrap({}, type="ping", source="test"),
            timeout=0.2,
        )


@pytest.mark.asyncio
async def test_serve_handler(broker: Broker) -> None:
    stop = asyncio.Event()

    async def handler(req: IncomingRequest) -> None:
        await req.reply(
            Envelope[dict].wrap(
                {"pong": True},
                type="pong",
                source="server",
            )
        )
        stop.set()

    task = asyncio.create_task(
        broker.serve_handler("ping", handler, stop=stop)
    )
    await asyncio.sleep(0.05)

    response = await broker.request(
        "ping",
        Envelope[dict].wrap({}, type="ping", source="client"),
        timeout=2,
    )
    await task

    assert response.payload == {"pong": True}
