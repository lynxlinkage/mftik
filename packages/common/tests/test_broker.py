from __future__ import annotations

import asyncio

import pytest
from broker_harness import a_broker
from mftik.broker import (
    Broker,
    IncomingRequest,
    LeasedSessionLink,
    RequestTimeoutError,
)
from mftik.protocol import (
    STS_LEASE_HEARTBEAT,
    Envelope,
    LeaseHeartbeat,
    Topics,
    UntypedEnvelope,
)


@pytest.fixture
async def broker() -> Broker:
    async with a_broker() as client:
        yield client


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
async def test_fetch_log_buffer_trims_to_maxlen(broker: Broker) -> None:
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

    buffered = await broker.fetch_log_buffer(topic, maxlen=3)
    assert len(buffered) == 3
    assert '"line-4"' in buffered[-1]


#: The ring STS and the API both ask for, as ``_STATUS_BUFFER`` in each. Named
#: here because the test below is only interesting at a length a caller really
#: uses: everything else in this file asks for a handful of lines, and a
#: transport whose own per-subject cap sat at 100 answered all of those
#: correctly while halving this one.
STATUS_RING = 200


@pytest.mark.asyncio
async def test_a_ring_the_size_production_asks_for_is_the_size_it_gets(
    broker: Broker,
) -> None:
    """Replay ``maxlen`` is exact, not "up to", and not "up to some cap of ours"."""
    topic = Topics.status_sts()
    for i in range(STATUS_RING + 5):
        await broker.publish_log(
            topic,
            Envelope[dict].wrap(
                {"level": "info", "message": f"line-{i}"},
                type="log",
                source="sts",
            ),
            maxlen=STATUS_RING,
            ttl_seconds=3600,
        )

    buffered = await broker.fetch_log_buffer(topic, maxlen=STATUS_RING)
    assert len(buffered) == STATUS_RING
    # The newest, so a UI opening late sees the end of the story and not a
    # window from the middle of it.
    assert f'"line-{STATUS_RING + 4}"' in buffered[-1]


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

    task = asyncio.create_task(broker.serve_handler("ping", handler, stop=stop))
    await asyncio.sleep(0.05)

    response = await broker.request(
        "ping",
        Envelope[dict].wrap({}, type="ping", source="client"),
        timeout=2,
    )
    await task

    assert response.payload == {"pong": True}


@pytest.mark.asyncio
async def test_leased_link_acks_and_expires(broker: Broker) -> None:
    stop = asyncio.Event()
    ready = asyncio.Event()
    expired = asyncio.Event()
    acks: list[int] = []

    def ack(hb: LeaseHeartbeat) -> Envelope[dict]:
        acks.append(hb.token)
        return Envelope[dict].wrap(
            {"token": hb.token}, type="lease.ack", source="md"
        )

    async def on_expired() -> None:
        expired.set()

    link = LeasedSessionLink(
        broker,
        rx="sts.md.e1",
        tx="md.e1",
        stop=stop,
        grace=0.4,
        ready=ready,
        ack=ack,
        on_expired=on_expired,
        watch_interval=0.1,
        name="test-lease",
    )
    task = asyncio.create_task(link.run())
    await asyncio.sleep(0.05)
    await broker.publish(
        "sts.md.e1",
        Envelope[LeaseHeartbeat].wrap(
            LeaseHeartbeat(session_id="e1", token=7),
            type=STS_LEASE_HEARTBEAT,
            source="sts",
        ),
    )
    await asyncio.wait_for(ready.wait(), timeout=2)
    assert link.last_token == 7
    await asyncio.wait_for(expired.wait(), timeout=2)
    stop.set()
    await asyncio.gather(task, return_exceptions=True)
    assert acks == [7]


@pytest.mark.asyncio
async def test_state_projection_tracks_writes(broker: Broker) -> None:
    name = "td.oms.1"
    await broker.state_put(name, "cid-1", {"status": "new"})
    proj = broker.state_projection(name)
    await proj.start()
    try:
        assert proj.get("cid-1") == {"status": "new"}
        await broker.state_put(name, "cid-1", {"status": "filled"})
        await broker.state_drop(name, "cid-1")
        for _ in range(40):
            if proj.get("cid-1") is None and "cid-1" not in proj.all():
                break
            await asyncio.sleep(0.05)
        assert proj.all() == {} or proj.get("cid-1") is None
    finally:
        await proj.close()


@pytest.mark.asyncio
async def test_a_dead_projection_is_not_live(broker: Broker) -> None:
    """A watch that dies must not keep serving the last map it saw."""
    name = "td.oms.fail"

    async def dying_watch(_name: str, *, stop=None):
        raise RuntimeError("watch died")
        yield "", None

    broker.state_watch = dying_watch  # type: ignore[method-assign]
    await broker.state_put(name, "cid-1", {"status": "new"})
    proj = broker.state_projection(name)
    await proj.start()
    try:
        for _ in range(40):
            if not proj.live:
                break
            await asyncio.sleep(0.05)
        assert not proj.live
    finally:
        await proj.close()


@pytest.mark.asyncio
async def test_a_non_dict_field_is_dropped_from_the_projection(
    broker: Broker,
) -> None:
    """Keeping the last dict would freeze a field the writer has replaced."""
    name = "td.oms.nondict"
    await broker.state_put(name, "cid-1", {"status": "new"})
    proj = broker.state_projection(name)
    await proj.start()
    try:
        await broker.transport.state_put_many(name, {"cid-1": "42"})
        for _ in range(40):
            if proj.get("cid-1") is None:
                break
            await asyncio.sleep(0.05)
        assert proj.get("cid-1") is None
    finally:
        await proj.close()


@pytest.mark.asyncio
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
