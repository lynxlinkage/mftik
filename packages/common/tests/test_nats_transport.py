"""The NATS transport's own machinery.

Like ``test_redis_transport.py`` next door: everything here is about how NATS
keeps a promise rather than the promise itself, so it runs only on the NATS
pass. The promises are in ``test_broker*.py`` and hold either way.

Three of these are worth reading before changing anything in the transport,
because each one bit during the port and none of them fails loudly:

* a stream whose subjects covered the core request space answers the requester
  with a publish acknowledgement, which parses as a reply;
* a per-message TTL below one second is refused and the message dropped, so a
  lease asked for in milliseconds would never be written at all;
* a KV key may not contain a colon, and every lease name in this system is
  spelled with them.
"""

from __future__ import annotations

import asyncio

import pytest
from broker_harness import a_broker, only_on
from mftik.broker import Broker
from mftik.broker.errors import RequestTimeoutError
from mftik.broker.transport.nats import (
    FANOUT_MAX_MSGS_PER_SUBJECT,
    MIN_TTL_SECONDS,
    NatsTransport,
    _check_subject,
    _kv_key,
    _ttl_seconds,
)
from mftik.protocol import Envelope, Topics

pytestmark = only_on("nats")

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


# --- the reply a stream would have sent instead --------------------------------


@pytest.mark.asyncio
async def test_a_request_is_answered_by_its_handler_and_not_by_jetstream(
    broker: Broker,
) -> None:
    """A stream is a subscriber too, and it answers.

    ``post`` needs a work-queue stream, and the obvious way to build one is to
    give it the same subject space ``request`` uses. Do that and JetStream
    receives every core request — and replies to it, on the requester's own
    reply subject, with ``{"stream": ..., "seq": n}``. The caller parses that as
    its answer and the handler's real reply arrives second, to an inbox nobody
    is reading. Nothing errors; the control plane simply starts returning
    nonsense.

    So the two spaces are separate, and this is what says so.
    """
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
    assert "stream" not in reply.payload


@pytest.mark.asyncio
async def test_a_live_request_is_not_stored_anywhere(broker: Broker) -> None:
    """Which is how ``probe`` leaves nothing behind, and it must stay true.

    Redis caps and expires a probe queue because it cannot refuse an unserved
    request. Core NATS never wrote one down, and the work-queue stream must not
    have picked it up on the way past.
    """
    subject = Topics.health("md", "md-jp-1")
    with pytest.raises(RequestTimeoutError):
        await broker.probe(subject, _envelope(), timeout=0.2)
    with pytest.raises(RequestTimeoutError):
        await broker.request(subject, _envelope(), timeout=0.2)

    transport = _transport(broker)
    info = await transport.js.stream_info(transport._post_stream)  # noqa: SLF001
    assert info.state.messages == 0


@pytest.mark.asyncio
async def test_a_request_to_nobody_fails_at_once_rather_than_waiting(
    broker: Broker,
) -> None:
    """No responders is an answer, and it arrives in milliseconds.

    Redis cannot tell "nobody is serving" from "the answer is slow", so a
    request to a down plane costs its caller the whole timeout. Core NATS knows,
    and the control plane learns that a plane is down long before its own
    deadline — which is why this raises the same error rather than a new one.
    """
    started = asyncio.get_running_loop().time()
    with pytest.raises(RequestTimeoutError):
        await broker.request("nobody.here", _envelope(), timeout=5.0)
    assert asyncio.get_running_loop().time() - started < 2.0


# --- leases: whole seconds, and a race Redis loses ----------------------------


def test_a_ttl_below_a_second_is_rounded_up_to_one() -> None:
    """The floor is the server's, and going under it is not a rounding error.

    JetStream refuses a ``Nats-TTL`` below one second and *discards the message*
    (ADR-43), so a lease asked for in milliseconds would not be written at all —
    a claim silently never taken, which is the one failure a lease exists to
    prevent. Rounding up rather than down for the same reason: a lease that
    expired early is two processes holding one account.
    """
    assert _ttl_seconds(0.02) == MIN_TTL_SECONDS == 1
    assert _ttl_seconds(0) == 1
    assert _ttl_seconds(1.2) == 2
    assert _ttl_seconds(30) == 30


@pytest.mark.asyncio
async def test_a_lease_expires_on_its_own(broker: Broker) -> None:
    """One second, because that is the shortest this transport can express."""
    await broker.lease_put("sts:alive:s-1", ttl=1, owner="p1")
    assert await broker.lease_held("sts:alive:s-1") is True
    await asyncio.sleep(2.5)
    assert await broker.lease_held("sts:alive:s-1") is False


@pytest.mark.asyncio
async def test_holding_a_lease_a_rival_took_is_refused(broker: Broker) -> None:
    """The race Redis documents losing, closed here.

    Redis extends whatever lease is there, because it has no compare-and-set to
    lean on — so a holder whose lease lapsed while a rival took it renews the
    *rival's* claim and carries on believing it holds the resource. Here the
    extension is conditional on the revision that was read, so the answer is no.
    """
    assert await broker.lease_take("td:account:7", ttl=30, owner="p1") is True
    # p1 loses it and p2 takes over, which is what a lapse and a re-take looks
    # like from the store's point of view.
    assert await broker.lease_release("td:account:7", owner="p1") is True
    assert await broker.lease_take("td:account:7", ttl=30, owner="p2") is True

    assert await broker.lease_hold("td:account:7", owner="p1", ttl=30) is False
    assert await broker.lease_owner("td:account:7") == "p2"


@pytest.mark.asyncio
async def test_a_lapsed_lease_is_not_re_created_by_holding_it(
    broker: Broker,
) -> None:
    """Whoever let one expire goes back through ``lease_take``, where a rival
    gets to say no."""
    assert await broker.lease_hold("td:account:9", owner="p1", ttl=30) is False
    assert await broker.lease_held("td:account:9") is False


# --- names that have to survive being NATS names ------------------------------


def test_a_lease_name_full_of_colons_becomes_a_key_nats_will_take() -> None:
    """Every lease name here is a Redis key tail, and KV forbids the colon.

    ``[-/_=.a-zA-Z0-9]`` is the whole allowed set. The colons become dots, which
    is what the segments always meant.
    """
    assert _kv_key("sts:alive:s-1") == "sts.alive.s-1"
    assert _kv_key("backfill:lock:42") == "backfill.lock.42"
    assert _kv_key("td.oms.7.o-1") == "td.oms.7.o-1"


def test_a_name_nats_still_cannot_take_is_refused_by_name() -> None:
    """Rather than deep inside the client, on a value that came from a venue.

    A symbol or an api_key can reach these, and an error naming the value is
    worth far more than one naming the header it failed in.
    """
    for bad in ("has space", "star*", "arrow>", "back\\slash"):
        with pytest.raises(ValueError, match="NATS KV key"):
            _kv_key(bad)


def test_a_topic_that_would_not_survive_being_a_subject_is_refused() -> None:
    """Redis channels take any bytes; a NATS subject is parsed.

    A stray wildcard changes which messages a subscription gets, and an empty
    segment makes it undeliverable — neither of which should surface at the
    publish, several layers from whatever built the topic.
    """
    for bad in ("", "md. s-1", "md.*", "md.>", "md..s-1"):
        with pytest.raises(ValueError, match="NATS subject"):
            _check_subject(bad)
    assert _check_subject("md.s-1") == "md.s-1"


@pytest.mark.asyncio
async def test_two_feeds_that_would_share_a_stream_name_are_refused(
    broker: Broker,
) -> None:
    """A stream name cannot hold a dot, so the dots become underscores.

    That is not injective — ``a.b`` and ``a_b`` both arrive at ``a_b`` — and two
    feeds sharing a tape stream means one warm-up reading the other's prints.
    Nothing in the real naming can collide, and this is what keeps that true.
    """
    transport = _transport(broker)
    transport._tape_stream("aggtrade.Gate_Spot_ETH")  # noqa: SLF001
    with pytest.raises(ValueError, match="both name the NATS tape"):
        transport._tape_stream("aggtrade.Gate.Spot_ETH")  # noqa: SLF001


# --- fan-out: one consumer per subscriber -------------------------------------


@pytest.mark.asyncio
async def test_every_subscriber_is_a_consumer_of_its_own(broker: Broker) -> None:
    """The fan-out shape: one stored copy, each reader at its own position.

    A slow subscriber cannot cost a fast one anything and neither can see the
    other's progress, which is what makes this a broadcast rather than a queue.
    """
    stop = asyncio.Event()
    topic = Topics.md_session("s-1")
    first: list[int] = []
    second: list[int] = []

    async def reader(into: list[int]) -> None:
        async for envelope in broker.subscribe(topic, stop=stop):
            into.append(envelope.payload["n"])
            if len(into) == 2:
                return

    tasks = [
        asyncio.create_task(reader(first)),
        asyncio.create_task(reader(second)),
    ]
    await asyncio.sleep(0.5)
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
    """Best effort, on a store that could have replayed it.

    The message is *there* — that is the point of fan-out being JetStream, and
    what makes a subject addressable by offset later. But a subscriber starts at
    NEW, because the broker promises that a message published while nobody was
    listening is gone, and half the callers here are built on it.
    """
    stop = asyncio.Event()
    topic = Topics.md_session("s-2")
    await broker.publish(topic, _envelope(1))

    seen: list[int] = []

    async def reader() -> None:
        async for envelope in broker.subscribe(topic, stop=stop):
            seen.append(envelope.payload["n"])

    task = asyncio.create_task(reader())
    await asyncio.sleep(0.5)
    await broker.publish(topic, _envelope(2))
    await asyncio.sleep(0.5)
    stop.set()
    await asyncio.wait_for(task, timeout=10)

    assert seen == [2]


@pytest.mark.asyncio
async def test_a_fan_out_subject_keeps_a_bounded_tail(broker: Broker) -> None:
    """Which is what makes ``publish_log`` and ``publish`` the same write.

    Every subject holds its last few messages rather than only the log topics,
    so the stream's size follows how many subjects are live instead of how fast
    the busiest one prints — a feed cannot grow it.
    """
    topic = Topics.log_sts("abc")
    for n in range(FANOUT_MAX_MSGS_PER_SUBJECT + 20):
        await broker.publish_log(topic, _envelope(n))

    buffered = await broker.fetch_log_buffer(topic)
    assert len(buffered) == FANOUT_MAX_MSGS_PER_SUBJECT


# --- the tape: a stream per feed ----------------------------------------------


@pytest.mark.asyncio
async def test_each_feed_gets_its_own_retention(broker: Broker) -> None:
    """Why the tape is a stream per feed rather than one stream for all of them.

    The bounds are per feed in the interface — so many records, so much age —
    and a stream's limits are the stream's. One stream could only ever hold the
    loosest of them.
    """
    transport = _transport(broker)
    await broker.tape_append(
        "aggtrade.Gate_Spot_A", {"price": "1"}, maxlen=10, ttl_seconds=60
    )
    await broker.tape_append(
        "aggtrade.Gate_Spot_B", {"price": "1"}, maxlen=5000, ttl_seconds=7200
    )

    a = await transport.js.stream_info(transport._tape_stream("aggtrade.Gate_Spot_A"))  # noqa: SLF001
    b = await transport.js.stream_info(transport._tape_stream("aggtrade.Gate_Spot_B"))  # noqa: SLF001
    assert (a.config.max_msgs, a.config.max_age) == (10, 60)
    assert (b.config.max_msgs, b.config.max_age) == (5000, 7200)


@pytest.mark.asyncio
async def test_the_newest_records_are_found_by_arithmetic(broker: Broker) -> None:
    """The other reason for a stream per feed.

    A warm-up asks for the newest N records, and a consumer answers by starting
    at a sequence and reading forward — it cannot be asked to skip. On a shared
    stream, finding where this feed's last N began would mean walking past every
    other feed's prints, which on a busy node is hundreds of thousands of them.
    """
    feed = "aggtrade.Gate_Spot_ETHUSDT"
    for n in range(50):
        await broker.tape_append(
            feed, {"trade_id": str(n)}, maxlen=1000, ttl_seconds=3600
        )

    rows = await broker.tape_tail(feed, count=5)
    assert [f["trade_id"] for _ms, f in rows] == ["45", "46", "47", "48", "49"]
    assert all(ms > 0 for ms, _f in rows)


@pytest.mark.asyncio
async def test_a_coverage_record_carries_its_own_expiry(broker: Broker) -> None:
    """A feed can be subscribed and print nothing at all.

    A dead instrument, a venue outage — the appends that would otherwise renew
    the record never come, so the write states its own TTL rather than leaning
    on the tape's.
    """
    transport = _transport(broker)
    feed = "aggtrade.Gate_Spot_QUIET"
    await broker.tape_mark_recording(feed, since_ms=1, ttl_seconds=1800)

    bucket = await transport._bucket("tapecov")  # noqa: SLF001
    status = await bucket.status()
    msg = await transport.js.get_msg(
        status.stream_info.config.name, subject=f"$KV.{status.bucket}.{_kv_key(feed)}"
    )
    assert (msg.headers or {}).get("Nats-TTL") == "1800"
