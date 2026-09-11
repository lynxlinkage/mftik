"""The NATS transport's own machinery.

Everything here is about how NATS keeps a promise rather than the promise
itself. The promises are in ``test_broker*.py``.

The hot path is core NATS. A request nobody serves fails at once. A
message published while nobody is subscribed is gone. ``connect()`` does
not ensure streams or buckets.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable

import pytest
from broker_harness import a_broker, inject_raw_request
from mftik.broker import Broker, BrokerConfig
from mftik.broker.errors import RequestTimeoutError
from mftik.broker.transport.nats import (
    _NO_RESPONDERS_CEILING_S,
    NatsTransport,
    _check_subject,
    _iter_until_stopped,
)
from mftik.protocol import Envelope, Topics

SUBJECT = "demo"


@pytest.fixture
async def broker() -> Broker:
    async with a_broker("test-nats") as client:
        yield client


def _envelope(n: int = 1) -> Envelope[dict]:
    return Envelope[dict].wrap({"n": n}, type="demo", source="test")


def _transport(broker: Broker) -> NatsTransport:
    transport = broker.transport
    assert isinstance(transport, NatsTransport)
    return transport


@pytest.mark.asyncio
async def test_a_request_is_answered_by_its_handler(broker: Broker) -> None:
    stop = asyncio.Event()

    async def serve() -> None:
        async for req in broker.serve(SUBJECT, stop=stop):
            await req.reply(
                Envelope[dict].wrap({"pong": 7}, type="demo.reply", source="test")
            )
            return

    task = asyncio.create_task(serve())
    await asyncio.sleep(0.3)
    try:
        reply = await broker.request(SUBJECT, _envelope(7), timeout=5)
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert reply.payload == {"pong": 7}


@pytest.mark.asyncio
async def test_a_live_request_is_not_stored_anywhere(broker: Broker) -> None:
    """Which is how ``probe`` leaves nothing behind, and it must stay true."""
    subject = Topics.health("md", "md-jp-1")
    with pytest.raises(RequestTimeoutError):
        await broker.probe(subject, _envelope(), timeout=0.2)
    with pytest.raises(RequestTimeoutError):
        await broker.request(subject, _envelope(), timeout=0.2)


@pytest.mark.asyncio
async def test_a_request_to_nobody_fails_at_once_rather_than_waiting(
    broker: Broker,
) -> None:
    started = asyncio.get_running_loop().time()
    with pytest.raises(RequestTimeoutError):
        await broker.request("nobody.here", _envelope(), timeout=5.0)
    spent = asyncio.get_running_loop().time() - started
    assert spent < _NO_RESPONDERS_CEILING_S * 2


@pytest.mark.asyncio
async def test_a_probe_does_not_wait_for_a_plane_to_turn_up(broker: Broker) -> None:
    started = asyncio.get_running_loop().time()
    with pytest.raises(RequestTimeoutError):
        await broker.probe("health.md.gone", _envelope(), timeout=5.0)
    spent = asyncio.get_running_loop().time() - started
    assert spent < _NO_RESPONDERS_CEILING_S


@pytest.mark.asyncio
async def test_a_request_waits_out_an_owner_that_is_still_arriving(
    broker: Broker,
) -> None:
    subject = "td.order.handover"
    stop = asyncio.Event()

    async def owner() -> None:
        await asyncio.sleep(0.4)
        async for req in broker.serve(subject, stop=stop):
            await req.reply(_envelope(7))
            break
        stop.set()

    task = asyncio.create_task(owner())
    reply = await broker.request(subject, _envelope(), timeout=2.0)
    await asyncio.wait_for(task, timeout=2)
    assert reply.payload == {"n": 7}


def test_a_topic_that_would_not_survive_being_a_subject_is_refused() -> None:
    for bad in ("", "md. s-1", "md.*", "md.>", "md..s-1"):
        with pytest.raises(ValueError, match="NATS subject"):
            _check_subject(bad)
    assert _check_subject("md.s-1") == "md.s-1"


@pytest.mark.asyncio
async def test_every_subscriber_has_its_own_interest(broker: Broker) -> None:
    stop = asyncio.Event()
    topic = Topics.md_session("s-1")
    first: list[int] = []
    second: list[int] = []

    ready_a = asyncio.Event()
    ready_b = asyncio.Event()

    async def reader(into: list[int], ready: asyncio.Event) -> None:
        async for envelope in broker.subscribe(topic, stop=stop, ready=ready):
            into.append(envelope.payload["n"])
            if len(into) == 2:
                return

    tasks = [
        asyncio.create_task(reader(first, ready_a)),
        asyncio.create_task(reader(second, ready_b)),
    ]
    await asyncio.wait_for(asyncio.gather(ready_a.wait(), ready_b.wait()), timeout=5)
    await broker.publish(topic, _envelope(1))
    await broker.publish(topic, _envelope(2))
    try:
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=10)
    finally:
        stop.set()

    assert first == [1, 2]
    assert second == [1, 2]


@pytest.mark.asyncio
async def test_a_subscriber_does_not_receive_what_it_missed(broker: Broker) -> None:
    stop = asyncio.Event()
    topic = Topics.md_session("s-2")
    await broker.publish(topic, _envelope(1))

    seen: list[int] = []
    ready = asyncio.Event()

    async def reader() -> None:
        async for envelope in broker.subscribe(topic, stop=stop, ready=ready):
            seen.append(envelope.payload["n"])

    task = asyncio.create_task(reader())
    await asyncio.wait_for(ready.wait(), timeout=5)
    await broker.publish(topic, _envelope(2))
    await asyncio.sleep(0.2)
    stop.set()
    await asyncio.wait_for(task, timeout=10)

    assert seen == [2]


@pytest.mark.asyncio
async def test_a_cross_connection_subscribe_is_visible_before_publish() -> None:
    async with a_broker("xconn") as publisher:
        other = Broker(
            BrokerConfig(
                nats_url=publisher.config.nats_url,
                key_prefix=publisher.config.key_prefix,
            )
        )
        await other.connect()
        stop = asyncio.Event()
        ready = asyncio.Event()
        topic = Topics.md_session("s-xconn")
        seen: list[int] = []

        async def reader() -> None:
            async for envelope in other.subscribe(topic, stop=stop, ready=ready):
                seen.append(envelope.payload["n"])
                return

        task = asyncio.create_task(reader())
        try:
            await asyncio.wait_for(ready.wait(), timeout=5)
            await publisher.publish(topic, _envelope(7))
            await asyncio.wait_for(task, timeout=5)
        finally:
            stop.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await other.close()

        assert seen == [7]


@pytest.mark.asyncio
async def test_a_stopped_serve_loop_still_hands_over_what_it_had_taken() -> None:
    inbound: asyncio.Queue[tuple[str, str | None]] = asyncio.Queue()
    for n in range(5):
        inbound.put_nowait((f"work-{n}", None))
    stop = asyncio.Event()

    seen: list[str] = []
    async for raw, _reply in _iter_until_stopped(inbound, stop=stop):
        seen.append(raw)
        stop.set()

    assert seen == [f"work-{n}" for n in range(5)]


_ARM_TIMEOUT_S = 10.0
_NUDGE_INTERVAL_S = 0.05


def _leftovers(before: frozenset[asyncio.Task]) -> frozenset[asyncio.Task]:
    return frozenset(asyncio.all_tasks()) - before - {asyncio.current_task()}


async def _arm(
    task: asyncio.Task, read: asyncio.Event, nudge: Callable[[], Awaitable[None]]
) -> None:
    clock = asyncio.get_running_loop()
    deadline = clock.time() + _ARM_TIMEOUT_S
    while not read.is_set():
        if task.done():
            await task
            raise AssertionError("the loop ended before it read anything")
        if clock.time() > deadline:
            raise AssertionError(f"the loop read nothing within {_ARM_TIMEOUT_S}s")
        await nudge()
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(read.wait(), _NUDGE_INTERVAL_S)


@pytest.mark.asyncio
async def test_a_cancelled_subscribe_loop_leaves_nothing_pending() -> None:
    inbound: asyncio.Queue[tuple[str, str | None]] = asyncio.Queue()
    stop = asyncio.Event()
    read = asyncio.Event()
    before = frozenset(asyncio.all_tasks())

    async def subscribe_loop() -> None:
        async for _item in _iter_until_stopped(inbound, stop=stop):
            read.set()

    async def nudge() -> None:
        inbound.put_nowait(("wake", None))

    task = asyncio.create_task(subscribe_loop())
    await _arm(task, read, nudge)

    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert _leftovers(before) == frozenset()


@pytest.mark.asyncio
async def test_a_cancelled_read_does_not_swallow_the_message_it_had_won() -> None:
    inbound: asyncio.Queue[tuple[str, str | None]] = asyncio.Queue()
    stop = asyncio.Event()
    read = asyncio.Event()
    seen: list[str] = []

    async def subscribe_loop() -> None:
        async for raw, _reply in _iter_until_stopped(inbound, stop=stop):
            seen.append(raw)
            read.set()

    async def nudge() -> None:
        inbound.put_nowait(("wake", None))

    task = asyncio.create_task(subscribe_loop())
    await _arm(task, read, nudge)
    taken, queued = len(seen), inbound.qsize()

    inbound.put_nowait(("late", None))
    await asyncio.sleep(0)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert (len(seen) - taken) + (inbound.qsize() - queued) == 1


@pytest.mark.parametrize("loop_name", ["subscribe", "serve"])
@pytest.mark.asyncio
async def test_a_cancelled_plane_loop_leaves_nothing_pending(
    broker: Broker, loop_name: str
) -> None:
    stop = asyncio.Event()
    read = asyncio.Event()
    topic = "md.teardown" if loop_name == "subscribe" else "td.teardown"
    before = frozenset(asyncio.all_tasks())

    async def plane_loop() -> None:
        if loop_name == "subscribe":
            async for _env in broker.subscribe(topic, stop=stop):
                read.set()
        else:
            async for _req in broker.serve(topic, stop=stop):
                read.set()

    async def nudge() -> None:
        if loop_name == "subscribe":
            await broker.publish(topic, _envelope())
        else:
            await inject_raw_request(broker, topic, _envelope().to_json())

    task = asyncio.create_task(plane_loop())
    await _arm(task, read, nudge)

    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert _leftovers(before) == frozenset()


@pytest.mark.asyncio
async def test_connect_does_not_need_jetstream() -> None:
    """A NATS without ``-js`` is the production shape."""
    async with a_broker("no-js") as broker:
        stop = asyncio.Event()

        async def serve() -> None:
            async for req in broker.serve("ping", stop=stop):
                await req.reply(_envelope(1))
                return

        task = asyncio.create_task(serve())
        await asyncio.sleep(0.1)
        try:
            reply = await broker.request("ping", _envelope(), timeout=2)
        finally:
            stop.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        assert reply.payload == {"n": 1}
