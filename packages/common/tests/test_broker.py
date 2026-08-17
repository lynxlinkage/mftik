from __future__ import annotations

import asyncio

import fakeredis.aioredis
import pytest
from mftik.broker import (
    BidirectionalStream,
    Broker,
    BrokerConfig,
    IncomingRequest,
    RequestTimeoutError,
)
from mftik.protocol import Envelope, UntypedEnvelope


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
async def test_publish_log_buffers_for_late_subscribers(broker: Broker) -> None:
    topic = "log.sts.late"
    first = Envelope[dict].wrap(
        {"level": "info", "message": "before connect"},
        type="log",
        source="sts",
        session_id="late",
    )
    await broker.publish_log(topic, first)

    buffered = await broker.fetch_log_buffer(topic)
    assert len(buffered) == 1
    assert '"before connect"' in buffered[0]

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    received: asyncio.Future[UntypedEnvelope] = loop.create_future()

    async def reader() -> None:
        async for env in broker.subscribe(topic, stop=stop):
            if not received.done():
                received.set_result(env)
            break
        stop.set()

    task = asyncio.create_task(reader())
    await asyncio.sleep(0.05)

    second = Envelope[dict].wrap(
        {"level": "info", "message": "live"},
        type="log",
        source="sts",
        session_id="late",
    )
    await broker.publish_log(topic, second)
    got = await asyncio.wait_for(received, timeout=2)
    await task
    assert got.payload == {"level": "info", "message": "live"}
    assert len(await broker.fetch_log_buffer(topic)) == 2


@pytest.mark.asyncio
async def test_publish_log_trims_to_maxlen(broker: Broker) -> None:
    topic = "log.sts.trim"
    for i in range(5):
        await broker.publish_log(
            topic,
            Envelope[dict].wrap(
                {"level": "info", "message": f"line-{i}"},
                type="log",
                source="sts",
                session_id="trim",
            ),
            maxlen=3,
        )

    buffered = await broker.fetch_log_buffer(topic)
    assert len(buffered) == 3
    assert '"line-2"' in buffered[0]
    assert '"line-4"' in buffered[-1]


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


@pytest.mark.asyncio
async def test_bistream_roundtrip(broker: Broker) -> None:
    up, down = broker.bistream_pair("session-1")
    loop = asyncio.get_running_loop()
    got: asyncio.Future[UntypedEnvelope] = loop.create_future()

    async def reader() -> None:
        async with down:
            async for env in down:
                if not got.done():
                    got.set_result(env)
                break

    task = asyncio.create_task(reader())
    await asyncio.sleep(0.05)

    async with up:
        sent = Envelope[dict].wrap(
            {"hello": "sts"},
            type="session.msg",
            source="td",
        )
        await up.send(sent)

    received = await asyncio.wait_for(got, timeout=2)
    await task

    assert received.type == "session.msg"
    assert received.payload == {"hello": "sts"}
    assert received.id == sent.id


@pytest.mark.asyncio
async def test_bistream_peer_swap(broker: Broker) -> None:
    up_topic, down_topic = BidirectionalStream.topics("x")
    a = broker.bistream(tx=up_topic, rx=down_topic)
    b = a.peer()
    assert b.tx == a.rx
    assert b.rx == a.tx


@pytest.mark.asyncio
async def test_psubscribe_receives_channel_and_envelope(broker: Broker) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    received: asyncio.Future[tuple[str, UntypedEnvelope]] = loop.create_future()

    async def reader() -> None:
        async for channel, env in broker.psubscribe("log.*", stop=stop):
            if not received.done():
                received.set_result((channel, env))
            break
        stop.set()

    task = asyncio.create_task(reader())
    await asyncio.sleep(0.05)

    sent = Envelope[dict].wrap(
        {"level": "info", "message": "pattern"},
        type="log",
        source="sts",
        session_id="abc",
    )
    await broker.publish("log.sts.abc", sent)

    channel, got = await asyncio.wait_for(received, timeout=2)
    stop.set()
    await task

    assert channel == "log.sts.abc"
    assert got.id == sent.id
    assert got.payload == {"level": "info", "message": "pattern"}
