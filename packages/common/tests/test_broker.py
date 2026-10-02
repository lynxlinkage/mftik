from __future__ import annotations

import asyncio

import pytest
from broker_harness import session_loop
from mftik.broker import (
    Broker,
    IncomingRequest,
    RequestTimeoutError,
)
from mftik.protocol import (
    Envelope,
    Topics,
    UntypedEnvelope,
)

pytestmark = session_loop


@pytest.mark.real_sleep(
    reason="this test calls asyncio.sleep while waiting for a real side effect"
)
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


@pytest.mark.real_sleep(
    reason="this test calls asyncio.sleep while waiting for a real side effect"
)
async def test_publish_log_is_live_fan_out(broker: Broker) -> None:
    """Late replay left the broker. ``publish_log`` is ``publish``."""
    topic = "log.sts.late"
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

    sent = Envelope[dict].wrap(
        {"level": "info", "message": "live"},
        type="log",
        source="sts",
        session_id="late",
    )
    await broker.publish_log(topic, sent)
    got = await asyncio.wait_for(received, timeout=2)
    await task
    assert got.payload == {"level": "info", "message": "live"}


@pytest.mark.real_sleep(
    reason="this test calls asyncio.sleep while waiting for a real side effect"
)
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


@pytest.mark.real_sleep(
    reason="NATS no-responders grace is a real asyncio.sleep"
)
async def test_request_timeout(broker: Broker) -> None:
    with pytest.raises(RequestTimeoutError):
        await broker.request(
            "nobody.home",
            Envelope[dict].wrap({}, type="ping", source="test"),
            timeout=0.2,
        )


@pytest.mark.real_sleep(
    reason="this test calls asyncio.sleep while waiting for a real side effect"
)
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

    task = asyncio.create_task(broker.serve_handler("ping", handler, stop=stop))
    await asyncio.sleep(0.05)

    response = await broker.request(
        "ping",
        Envelope[dict].wrap({}, type="ping", source="client"),
        timeout=2,
    )
    await task

    assert response.payload == {"pong": True}


@pytest.mark.real_sleep(
    reason="this test calls asyncio.sleep while waiting for a real side effect"
)
async def test_psubscribe_receives_channel_and_envelope(broker: Broker) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    received: asyncio.Future[tuple[str, UntypedEnvelope]] = loop.create_future()

    async def reader() -> None:
        # ``log.*.*``, not ``log.*``: one wildcard per segment. Redis globs the
        # whole channel name and matches either, so this test used to pass with
        # the short form on a pattern no other transport can read.
        async for channel, env in broker.psubscribe(Topics.log_pattern(), stop=stop):
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
