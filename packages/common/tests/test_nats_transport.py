"""The NATS transport's own machinery.

Everything here is about how NATS keeps a promise rather than the promise
itself. The promises are in ``test_broker*.py``.

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
import contextlib
from collections.abc import Awaitable, Callable

import nats.js.api as js_api
import nats.js.errors
import pytest
from broker_harness import a_broker, inject_raw_request
from mftik.broker import Broker, BrokerConfig
from mftik.broker.errors import (
    RequestTimeoutError,
    StateReadIncompleteError,
    StreamShapeError,
)
from mftik.broker.transport.nats import (
    _NO_RESPONDERS_CEILING_S,
    FANOUT_MAX_MSGS_PER_SUBJECT,
    MIN_TTL_SECONDS,
    NatsTransport,
    _check_subject,
    _iter_until_stopped,
    _kv_key,
    _sanitize,
    _ttl_seconds,
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


# --- the reply a stream would have sent instead --------------------------------


@pytest.mark.asyncio
async def test_a_request_is_answered_by_its_handler_and_not_by_jetstream(
    broker: Broker,
) -> None:
    """A stream is a subscriber too, and it answers.

    A JetStream stream whose filter covered the RPC subject space would
    receive every core request — and reply to it, on the requester's own
    reply subject, with ``{"stream": ..., "seq": n}``. The caller parses that
    as its answer. Fan-out and logs stay on their own spaces so that cannot
    happen.
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

    Core NATS never wrote one down. There is no work-queue stream to pick it
    up on the way past.
    """
    subject = Topics.health("md", "md-jp-1")
    with pytest.raises(RequestTimeoutError):
        await broker.probe(subject, _envelope(), timeout=0.2)
    with pytest.raises(RequestTimeoutError):
        await broker.request(subject, _envelope(), timeout=0.2)

    # This broker's streams, not the server's. ``streams_info`` lists every
    # stream the server holds, so an unscoped assertion fails on any NATS that
    # another run has touched — which is the shared one CI would grow into, and
    # a developer's own the moment they point two checkouts at it.
    transport = _transport(broker)
    mine = _sanitize(broker.config.key_prefix)
    names = [
        info.config.name
        for info in await transport.js.streams_info()
        if info.config.name and info.config.name.startswith(mine)
    ]
    assert names, "the fan-out stream should exist under this broker's prefix"
    assert not any(name.endswith("_post") for name in names)


@pytest.mark.asyncio
async def test_a_request_to_nobody_fails_at_once_rather_than_waiting(
    broker: Broker,
) -> None:
    """No responders is an answer, and it arrives in milliseconds.

    Redis cannot tell "nobody is serving" from "the answer is slow", so a
    request to a down plane costs its caller the whole timeout. Core NATS knows,
    and the control plane learns that a plane is down long before its own
    deadline — which is why this raises the same error rather than a new one.

    "Milliseconds" is bounded by :data:`_NO_RESPONDERS_CEILING_S` rather than by
    a number written here, so the two cannot drift apart: whatever that is, it is
    what a caller with a five second budget spends before being told no.
    """
    started = asyncio.get_running_loop().time()
    with pytest.raises(RequestTimeoutError):
        await broker.request("nobody.here", _envelope(), timeout=5.0)
    spent = asyncio.get_running_loop().time() - started
    assert spent < _NO_RESPONDERS_CEILING_S * 2


@pytest.mark.asyncio
async def test_a_probe_does_not_wait_for_a_plane_to_turn_up(broker: Broker) -> None:
    """Where ``probe`` parts company with ``request``.

    An order gains from waiting out an owner handover because the caller wants
    the order placed. A probe gains nothing: "down" is the answer it exists to
    collect, and one that arrives after the dashboard stopped asking tells nobody
    anything. So it spends the boot-race grace and no more, whatever budget its
    caller happened to bring.
    """
    started = asyncio.get_running_loop().time()
    with pytest.raises(RequestTimeoutError):
        await broker.probe("health.md.gone", _envelope(), timeout=5.0)
    spent = asyncio.get_running_loop().time() - started
    assert spent < _NO_RESPONDERS_CEILING_S


@pytest.mark.asyncio
async def test_a_request_waits_out_an_owner_that_is_still_arriving(
    broker: Broker,
) -> None:
    """The handover, which a fixed grace of ~100ms used to fail through.

    An account's order subject is served by whichever TD process holds the
    account, and during a handover there is a window with nobody on it. Under
    Redis the request parks and the new owner takes it; here it comes straight
    back saying no responders — so what covers the window is asking again, and
    for long enough. ``ORDER_ACK_TIMEOUT_S`` is two seconds and a handover is not
    reliably inside a tenth of one.
    """
    subject = "td.order.handover"
    stop = asyncio.Event()

    async def owner() -> None:
        # Late enough that the old fixed grace would have given up, and well
        # inside what an order ack allows.
        await asyncio.sleep(0.4)
        async for req in broker.serve(subject, stop=stop):
            await req.reply(_envelope(7))
            break
        stop.set()

    task = asyncio.create_task(owner())
    reply = await broker.request(subject, _envelope(), timeout=2.0)
    await asyncio.wait_for(task, timeout=2)
    assert reply.payload == {"n": 7}


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


# --- fan-out: each subscriber has its own interest ----------------------------


@pytest.mark.asyncio
async def test_every_subscriber_has_its_own_interest(broker: Broker) -> None:
    """The fan-out shape: each subscriber has its own interest.

    A slow subscriber cannot cost a fast one anything and neither can see the
    other's progress, which is what makes this a broadcast rather than a queue.
    """
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
    """Best effort, on a store that could have replayed it.

    The stream still captures the subject — that is what makes it
    addressable by offset later. A live subscriber starts at now, because
    the broker promises that a message published while nobody was
    listening is gone, and half the callers here are built on it.
    """
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
async def test_a_publish_does_not_wait_for_the_stream(broker: Broker) -> None:
    """Live fan-out waits for this process's server, not the stream leader.

    ``js.publish`` is replaced so a local-leader ack cannot hide inside
    the same call. The message must still land on the fan-out stream —
    capture is the tail, not the thing the publisher sits on.
    """
    transport = _transport(broker)
    original = transport._js.publish  # noqa: SLF001

    async def boom(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("live publish must not wait for js.publish")

    transport._js.publish = boom  # noqa: SLF001
    topic = Topics.md_session("s-core")
    try:
        await broker.publish(topic, _envelope(1))
    finally:
        transport._js.publish = original  # noqa: SLF001

    await transport.nc.flush()
    subject = transport._fanout_subject(topic)  # noqa: SLF001
    deadline = asyncio.get_running_loop().time() + 5
    held = 0
    while True:
        held = (await transport._subject_counts(transport._fanout_stream, subject)).get(  # noqa: SLF001
            subject, 0
        )
        if held >= 1:
            break
        if asyncio.get_running_loop().time() >= deadline:
            pytest.fail(f"fan-out stream held {held} after 5s; expected the publish")
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_a_cross_connection_subscribe_is_visible_before_publish() -> None:
    """``nc.subscribe`` without a flush misses a publish on another connection.

    Production is that shape: MD publishes, STS subscribes. Same-connection
    tests cannot see it — SUB and PUB share one ``_pending`` list.
    """
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
async def test_a_fan_out_subject_keeps_a_bounded_tail(broker: Broker) -> None:
    """Fan-out is capped per subject so a busy feed cannot grow the stream.

    Publish no longer waits for the stream ack, so the count is allowed to
    catch up. The bound is what we are asserting, not the race.
    """
    topic = Topics.log_sts("abc")
    transport = _transport(broker)
    for n in range(FANOUT_MAX_MSGS_PER_SUBJECT + 20):
        await broker.publish(topic, _envelope(n))
    await transport.nc.flush()

    subject = transport._fanout_subject(topic)  # noqa: SLF001

    async def held() -> int:
        return (await transport._subject_counts(transport._fanout_stream, subject)).get(  # noqa: SLF001
            subject, 0
        )

    deadline = asyncio.get_running_loop().time() + 5
    count = 0
    while True:
        count = await held()
        if count == FANOUT_MAX_MSGS_PER_SUBJECT:
            break
        if asyncio.get_running_loop().time() >= deadline:
            pytest.fail(
                f"held {count} after 5s; expected {FANOUT_MAX_MSGS_PER_SUBJECT}"
            )
        await asyncio.sleep(0.05)


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
async def test_a_coverage_record_is_durable(broker: Broker) -> None:
    """Coverage is a fact about the tape, not a TTL that needs renewing."""
    transport = _transport(broker)
    feed = "aggtrade.Gate_Spot_QUIET"
    await broker.tape_mark_recording(feed, since_ms=1, ttl_seconds=1800)

    bucket = await transport._bucket("tapecov")  # noqa: SLF001
    status = await bucket.status()
    msg = await transport.js.get_msg(
        status.stream_info.config.name, subject=f"$KV.{status.bucket}.{_kv_key(feed)}"
    )
    assert (msg.headers or {}).get("Nats-TTL") is None
    coverage = await broker.tape_coverage(feed)
    assert coverage["continuous_since_ms"] == "1"


def test_kv_shape_is_read_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NATS_KV_REPLICAS", "3")
    monkeypatch.setenv("NATS_KV_PLACEMENT_CLUSTER", "jp")
    config = BrokerConfig.from_env()
    assert config.kv_replicas == 3
    assert config.kv_placement_cluster == "jp"


@pytest.mark.asyncio
async def test_an_unplaced_stream_is_asked_to_move_to_the_declared_cluster() -> None:
    """A live stream on the wrong cluster is moved, or left with a warning.

    The pin is the KV rule — try, warn, keep booting. This is
    ``_ensure_stream_placement`` itself, not ``_reshape`` or ``connect``.
    """
    transport = NatsTransport(BrokerConfig(kv_placement_cluster="jp"))
    updates: list[dict] = []

    async def fake_js_api(
        op: str,
        payload: dict | None = None,
        *,
        timeout: float | None = None,
    ) -> dict:
        if op.startswith("STREAM.INFO."):
            return {"config": {"name": "mftik_ps", "num_replicas": 1}}
        if op.startswith("STREAM.UPDATE."):
            assert payload is not None
            updates.append(payload)
            raise nats.js.errors.ServerError(
                code=500,
                err_code=10052,
                description="no cluster named jp",
            )
        raise AssertionError(op)

    transport._js_api = fake_js_api  # noqa: SLF001
    await transport._ensure_stream_placement("mftik_ps")  # noqa: SLF001

    assert updates
    assert updates[0]["placement"] == {"cluster": "jp"}


@pytest.mark.asyncio
async def test_a_stream_already_on_the_declared_cluster_is_left_alone() -> None:
    transport = NatsTransport(BrokerConfig(kv_placement_cluster="jp"))
    calls: list[str] = []

    async def fake_js_api(
        op: str,
        payload: dict | None = None,
        *,
        timeout: float | None = None,
    ) -> dict:
        calls.append(op)
        return {"config": {"name": "mftik_ps", "placement": {"cluster": "jp"}}}

    transport._js_api = fake_js_api  # noqa: SLF001
    await transport._ensure_stream_placement("mftik_ps")  # noqa: SLF001

    assert calls == ["STREAM.INFO.mftik_ps"]


@pytest.mark.asyncio
async def test_kv_shape_is_a_no_op_without_replicas_or_cluster() -> None:
    transport = NatsTransport(BrokerConfig())
    calls: list[str] = []

    async def fake(
        op: str,
        payload: dict | None = None,
        *,
        timeout: float | None = None,
    ) -> dict:
        calls.append(op)
        raise AssertionError(op)

    transport._js_api = fake  # noqa: SLF001
    await transport._ensure_kv_shape("state")  # noqa: SLF001
    assert calls == []


@pytest.mark.asyncio
async def test_kv_shape_pins_after_a_replica_update() -> None:
    """Replica update first, placement second — NATS refuses both at once."""
    transport = NatsTransport(
        BrokerConfig(kv_replicas=3, kv_placement_cluster="jp")
    )
    updates: list[dict] = []

    async def fake(
        op: str,
        payload: dict | None = None,
        *,
        timeout: float | None = None,
    ) -> dict:
        if op.startswith("STREAM.INFO."):
            return {"config": {"name": "KV_x", "num_replicas": 1}}
        if op.startswith("STREAM.UPDATE."):
            assert payload is not None
            updates.append(payload)
            return {}
        raise AssertionError(op)

    transport._js_api = fake  # noqa: SLF001
    await transport._ensure_kv_shape("x")  # noqa: SLF001

    assert len(updates) == 2
    assert updates[0]["num_replicas"] == 3
    assert updates[1]["placement"] == {"cluster": "jp"}


@pytest.mark.asyncio
async def test_kv_shape_skips_placement_when_replicas_fail() -> None:
    transport = NatsTransport(
        BrokerConfig(kv_replicas=3, kv_placement_cluster="jp")
    )
    updates = 0

    async def fake(
        op: str,
        payload: dict | None = None,
        *,
        timeout: float | None = None,
    ) -> dict:
        nonlocal updates
        if op.startswith("STREAM.INFO."):
            return {"config": {"name": "KV_x", "num_replicas": 1}}
        if op.startswith("STREAM.UPDATE."):
            updates += 1
            raise nats.js.errors.ServerError(
                code=500, err_code=10123, description="cannot scale"
            )
        raise AssertionError(op)

    transport._js_api = fake  # noqa: SLF001
    await transport._ensure_kv_shape("x")  # noqa: SLF001
    assert updates == 1


@pytest.mark.asyncio
async def test_kv_shape_reuses_the_info_it_already_has() -> None:
    """A matching replica count must not refetch STREAM.INFO to pin."""
    transport = NatsTransport(
        BrokerConfig(kv_replicas=3, kv_placement_cluster="jp")
    )
    infos = 0

    async def fake(
        op: str,
        payload: dict | None = None,
        *,
        timeout: float | None = None,
    ) -> dict:
        nonlocal infos
        if op.startswith("STREAM.INFO."):
            infos += 1
            return {"config": {"name": "KV_x", "num_replicas": 3}}
        if op.startswith("STREAM.UPDATE."):
            return {}
        raise AssertionError(op)

    transport._js_api = fake  # noqa: SLF001
    await transport._ensure_kv_shape("x")  # noqa: SLF001
    assert infos == 1


@pytest.mark.asyncio
async def test_an_existing_bucket_is_bound_when_create_disagrees(
    broker: Broker,
) -> None:
    """``create_key_value`` is STREAM.CREATE, and a live KV often disagrees.

    Production buckets pick up ``allow_msg_ttl`` and a replica count the
    client did not send. CREATE then answers 10058. Binding and using the
    stream that is already there is what a restart has to do.
    """
    transport = _transport(broker)
    name = "oms.bind"
    await broker.state_put_many(name, {"k": "1"})

    bucket_name = _sanitize(f"{transport.config.key_prefix}_state")
    stream = f"KV_{bucket_name}"
    raw = await transport._js_api(f"STREAM.INFO.{stream}")  # noqa: SLF001
    cfg = dict(raw["config"])
    cfg["description"] = "shape-drift"
    await transport._js_api(f"STREAM.UPDATE.{stream}", cfg)  # noqa: SLF001

    transport._kv.clear()  # noqa: SLF001
    transport._kv_status.clear()  # noqa: SLF001

    assert await broker.state_all(name) == {"k": "1"}


@pytest.mark.asyncio
async def test_a_stopped_serve_loop_still_hands_over_what_it_had_taken() -> None:
    """Everything queued when the stop event fires, whichever won the race.

    The stop event is delivered through the inbound queue rather than raced
    against it, so a loop that is told to stop after taking the first of a
    batch still hands the rest over. The ordering that mattered is the one
    where a message *wins* the race: the loop yields it and then finds its
    own condition false.
    """
    inbound: asyncio.Queue[tuple[str, str | None]] = asyncio.Queue()
    for n in range(5):
        inbound.put_nowait((f"work-{n}", None))
    stop = asyncio.Event()

    seen: list[str] = []
    async for raw, _reply in _iter_until_stopped(inbound, stop=stop):
        seen.append(raw)
        # Told to stop having taken the first of a batch — a plane is usually
        # asked to shut down while its subject is busy, not while it is idle.
        stop.set()

    assert seen == [f"work-{n}" for n in range(5)]


_ARM_TIMEOUT_S = 10.0
_NUDGE_INTERVAL_S = 0.05


def _leftovers(before: frozenset[asyncio.Task]) -> frozenset[asyncio.Task]:
    """Unfinished tasks that were not already running when the loop started.

    A set difference rather than a count, so a failure can name what is still
    pending, and so the answer does not depend on the order ``all_tasks`` happens
    to hand its contents back. ``all_tasks`` returns only unfinished tasks, which
    is exactly the set asyncio complains about at collection time.
    """
    return frozenset(asyncio.all_tasks()) - before - {asyncio.current_task()}


async def _arm(
    task: asyncio.Task, read: asyncio.Event, nudge: Callable[[], Awaitable[None]]
) -> None:
    """Wait until the loop under test has read one message, nudging until it has.

    Nudged repeatedly rather than once because a fan-out subscription only sees
    what is published after its consumer exists, and that consumer is built
    inside ``task``.

    Bounded, and watching the task as well as the event, because the alternative
    is a test that hangs: a loop that raises on its way up would otherwise leave
    this spinning, and with no ``pytest-timeout`` here a regression would stall
    CI instead of failing it.
    """
    clock = asyncio.get_running_loop()
    deadline = clock.time() + _ARM_TIMEOUT_S
    while not read.is_set():
        if task.done():
            # Awaited rather than asserted on, so the failure is the loop's own
            # traceback instead of a report that something went wrong nearby.
            await task
            raise AssertionError("the loop ended before it read anything")
        if clock.time() > deadline:
            raise AssertionError(f"the loop read nothing within {_ARM_TIMEOUT_S}s")
        await nudge()
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(read.wait(), _NUDGE_INTERVAL_S)


@pytest.mark.asyncio
async def test_a_cancelled_subscribe_loop_leaves_nothing_pending() -> None:
    """A stopped session cancels the task running the `async for`, mid-read.

    That read used to be a task of its own, raced against the stop event under
    `asyncio.wait`, and `asyncio.wait` does not cancel what it was waiting on
    when it is cancelled — it drops its own callbacks and leaves the rest. So the
    read was left pending, and asyncio said so once the collector reached it:
    `Task was destroyed but it is pending`, one per subscribe loop per teardown,
    in every plane on this transport (issue #81). Redis is quiet because its
    serve loop is a poll with a timeout and has no second waiter to abandon.

    Asserted on the task set rather than on log output, because that warning is
    emitted from `__del__` and when it runs is the collector's business. Asserted
    on the whole set rather than on reads specifically, so it still holds for the
    stop tap that replaced the race, and for whatever replaces that.
    """
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
    """Cancellation must not consume a message it has no way to deliver.

    The race this loop used to run left a window one instant wide: the read
    completes, and the cancellation arrives before the loop is scheduled to take
    what it returned. Cancelling something that already holds a result does
    nothing, so the message was neither in the queue nor with the consumer.
    Exactly the loss the handover after the loop prevents, in the one path
    that did not go through it.

    Asserted as conservation rather than as a location, because either place is
    fine and which one it lands in is the scheduler's business: handed to the
    consumer, or left in the queue for whoever reads next.
    """
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
    # One pass, which is the window: long enough for the read to have completed,
    # too short for the loop to have been told that it did.
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
    """The same property through the two doors a plane actually goes in by.

    `subscribe` and `serve` each wrap the helper above, and this is what the
    counts in issue #81 were: one orphan per loop in flight, so an STS session
    holding two contributed two and MD's one contributed one. Worth asserting
    here as well as on the helper, because the leak is only visible to whoever
    owns the outermost `async for`, and a future rearrangement of these two could
    park a waiter somewhere the helper's `finally` cannot reach.
    """
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
            # Core NATS stores nothing; a request without a reply would
            # wait out its timeout. The serve loop only needs a wake.
            await inject_raw_request(broker, topic, _envelope().to_json())

    task = asyncio.create_task(plane_loop())
    await _arm(task, read, nudge)

    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert _leftovers(before) == frozenset()


@pytest.mark.asyncio
async def test_a_trim_that_cannot_find_the_horizon_keeps_the_tape(
    broker: Broker,
) -> None:
    """A slow sweep must not be read as "this feed is all past its window".

    The horizon is found with a consumer and a fetch, and a fetch that times out
    returns nothing — which is exactly what a feed whose last print predates the
    window returns. Conflating them purged every record and reported it as an
    ordinary trim, so a single slow second cost a strategy its whole warm-up.
    """
    transport = _transport(broker)
    feed = "aggtrade.Gate_Spot_SLOWSWEEP"
    for n in range(10):
        await broker.tape_append(feed, {"trade_id": str(n)}, maxlen=100, ttl_seconds=60)

    async def _cannot_tell(*_args: object, **_kwargs: object) -> None:
        return None

    transport._first_seq_at_or_after = _cannot_tell  # type: ignore[method-assign] # noqa: SLF001
    # A horizon inside the feed's history, so there is genuinely something to
    # keep and something to drop.
    dropped = await broker.tape_trim_before(feed, min_id_ms=1)

    assert dropped == 0
    assert len(await broker.tape_tail(feed, count=20)) == 10


@pytest.mark.asyncio
async def test_a_feed_that_stopped_printing_is_still_trimmed_to_nothing(
    broker: Broker,
) -> None:
    """The other half of the pair, so the careful branch above is not a no-op.

    A dead instrument's tape does go, and it goes because the newest record is
    asked for its age directly rather than because a read found nothing.
    """
    feed = "aggtrade.Gate_Spot_DEADINSTRUMENT"
    for n in range(5):
        await broker.tape_append(feed, {"trade_id": str(n)}, maxlen=100, ttl_seconds=60)

    # A horizon in the far future, so every record is behind it.
    dropped = await broker.tape_trim_before(feed, min_id_ms=4_000_000_000_000)

    assert dropped == 5
    assert await broker.tape_tail(feed, count=20) == []


# --- the coverage record a live feed keeps ------------------------------------


#: A coverage TTL short enough to cross its own half-life inside a test. Four
#: hours is what MD asks for.
_SHORT_COVERAGE_TTL_S = 2


async def _coverage_seq(broker: Broker, feed: str) -> int | None:
    """Which stream message currently holds this feed's coverage record.

    A renewal republishes the whole record, so the sequence advancing is the
    renewal having happened — and it is the only sign of one from outside, since
    a renewed record holds the same fields it did before.
    """
    transport = _transport(broker)
    bucket = await transport._bucket("tapecov")  # noqa: SLF001
    status = await bucket.status()
    assert status.stream_info.config.name is not None
    try:
        msg = await transport.js.get_msg(
            status.stream_info.config.name,
            subject=f"$KV.{status.bucket}.{_kv_key(feed)}",
        )
    except nats.js.errors.Error:
        return None
    return msg.seq


@pytest.mark.asyncio
async def test_a_feed_recording_past_its_ttl_keeps_its_coverage(
    broker: Broker,
) -> None:
    """Renewed by the appends, or a live feed outlives its own description.

    Coverage is one KV entry with a per-message TTL, written when recording
    starts and not again until it stops. A feed recording for longer than that
    TTL — four hours, against a session that runs for days — used to lose it
    while still printing. The next recorder then reads no ``stopped_ms`` and no
    prior ``continuous_since_ms``, judges the interruption unmeasurable, and
    restarts continuity, so hours of intact tape fall behind the mark. That is
    the exact outcome ``tape_mark_recording`` exists to avoid, reached by an
    expiry instead of by a gap.
    """
    feed = "aggtrade.Gate_Spot_LONGRUN"
    ttl = _SHORT_COVERAGE_TTL_S
    await broker.tape_mark_recording(feed, since_ms=1_000, ttl_seconds=ttl)
    marked = await _coverage_seq(broker, feed)

    await broker.tape_append(feed, {"trade_id": "0"}, maxlen=100, ttl_seconds=ttl)
    assert await _coverage_seq(broker, feed) == marked

    await asyncio.sleep(ttl + 0.2)
    await broker.tape_append(feed, {"trade_id": "1"}, maxlen=100, ttl_seconds=ttl)
    assert await _coverage_seq(broker, feed) == marked
    coverage = await broker.tape_coverage(feed)
    assert coverage["continuous_since_ms"] == "1000"
    assert coverage["recording"] == "1"


@pytest.mark.asyncio
async def test_a_feed_nobody_marked_recording_gets_no_coverage_invented(
    broker: Broker,
) -> None:
    """An empty record would claim the feed is described and say nothing about it.

    Which reads the same as coverage that expired, so a renewal writing one would
    turn "never recorded" into "recorded, and nothing is known" — and
    ``tape_mark_recording`` treats those two the same way for good reason. A feed
    appending without a mark has nothing to keep alive.
    """
    feed = "aggtrade.Gate_Spot_UNMARKED"
    ttl = _SHORT_COVERAGE_TTL_S
    await broker.tape_append(feed, {"trade_id": "0"}, maxlen=100, ttl_seconds=ttl)
    await asyncio.sleep(ttl / 2 + 0.1)
    await broker.tape_append(feed, {"trade_id": "1"}, maxlen=100, ttl_seconds=ttl)

    assert await broker.tape_coverage(feed) == {}
    assert await _coverage_seq(broker, feed) is None


# --- what a removed field leaves behind ---------------------------------------


@pytest.mark.asyncio
async def _state_subjects(broker: Broker, name: str) -> set[str]:
    """Subjects the state stream still holds for ``name``.

    After a drop the live fields remain and the delete markers must not:
    a leftover DEL is one extra subject ``_kv_scan`` transfers per order
    this account has ever worked.
    """
    transport = _transport(broker)
    stream, head = await transport._state_stream()  # noqa: SLF001
    prefix = f"{head}{transport._state_key(name, '')}"  # noqa: SLF001
    return set(await transport._subject_counts(stream, f"{prefix}>"))  # noqa: SLF001


@pytest.mark.asyncio
async def test_a_dropped_field_leaves_nothing_where_it_was(broker: Broker) -> None:
    """A pull read skips the delete marker a watcher needs.

    KV delete writes a marker so :meth:`Broker.state_watch` can see the field
    go. The marker is then purged so the stream holds only the live set.
    """
    name = "oms.dropped"
    await broker.state_put_many(name, {f"o{n}": str(n) for n in range(6)})
    await broker.state_drop(name, "o0", "o1", "o2")

    assert await broker.state_all(name) == {"o3": "3", "o4": "4", "o5": "5"}
    assert len(await _state_subjects(broker, name)) == 3


@pytest.mark.asyncio
async def test_dropping_a_missing_field_does_not_mint_a_marker(
    broker: Broker,
) -> None:
    """nats-py ``delete`` never raises; an unchecked call would grow the bucket."""
    name = "oms.ghost"
    await broker.state_put_many(name, {"o1": "1"})
    before = await _state_subjects(broker, name)

    assert await broker.state_drop(name, "ghost") == 0
    assert await _state_subjects(broker, name) == before


@pytest.mark.asyncio
async def test_replacing_the_book_per_fill_does_not_grow_the_bucket(
    broker: Broker,
) -> None:
    """The writer caches the last field set, so a replace does not scan first.

    TD replaces the whole order book per fill. The last-written set is what
    tells it which fields to drop, and ``state_all`` still answers with the
    live book after twenty of those.
    """
    name = "oms.churn"
    for n in range(20):
        await broker.state_replace(name, {f"o{n}": str(n)})

    assert await broker.state_all(name) == {"o19": "19"}
    assert len(await _state_subjects(broker, name)) == 1


@pytest.mark.asyncio
async def test_clearing_a_name_leaves_nothing_on_the_stream(broker: Broker) -> None:
    """Every field is deleted so a watcher sees each one go."""
    name = "oms.cleared"
    await broker.state_put_many(name, {f"o{n}": str(n) for n in range(8)})
    await broker.state_clear(name)

    assert await broker.state_all(name) == {}
    assert await _state_subjects(broker, name) == set()


# --- a read that could not finish ---------------------------------------------


@pytest.mark.asyncio
async def test_a_state_read_that_comes_up_short_raises(broker: Broker) -> None:
    """A book missing rows reads exactly like a book that small.

    Which is the whole problem: a strategy asking for its open orders cannot tell
    the difference, and the difference is placing an order twice or hedging a
    position that is already flat. So a scan that cannot deliver what the stream
    says it holds says so.
    """
    transport = _transport(broker)
    name = "oms.short"
    await broker.state_put_many(name, {"o1": "1", "o2": "2"})

    real = transport._subject_counts  # noqa: SLF001

    async def overcount(stream: str, pattern: str) -> dict[str, int]:
        # A key the stream does not hold, which is what losing a message looks
        # like from the reader's side.
        return {**await real(stream, pattern), f"{pattern[:-1]}ghost": 1}

    transport._subject_counts = overcount  # type: ignore[method-assign]  # noqa: SLF001

    with pytest.raises(StateReadIncompleteError, match="2 of 3"):
        await broker.state_all(name)


@pytest.mark.asyncio
async def test_a_key_that_went_mid_read_is_not_an_error(broker: Broker) -> None:
    """The other side of it, or a concurrent drop would raise at every reader.

    A field removed between the count and the fetch makes the smaller answer the
    current one. Told apart from a read that failed by asking the stream again:
    only a count that still disagrees means messages went missing rather than
    keys.
    """
    transport = _transport(broker)
    name = "oms.raced"
    await broker.state_put_many(name, {"o1": "1", "o2": "2"})

    real = transport._subject_counts  # noqa: SLF001
    calls = 0

    async def overcount_once(stream: str, pattern: str) -> dict[str, int]:
        nonlocal calls
        calls += 1
        counts = await real(stream, pattern)
        if calls == 1:
            return {**counts, f"{pattern[:-1]}ghost": 1}
        return counts

    transport._subject_counts = overcount_once  # type: ignore[method-assign]  # noqa: SLF001

    assert await broker.state_all(name) == {"o1": "1", "o2": "2"}


# --- the consumer a read builds -----------------------------------------------


@pytest.mark.asyncio
async def test_a_finished_read_leaves_no_consumer_on_the_server(
    broker: Broker,
) -> None:
    """``unsubscribe`` tears down the client's inboxes and nothing else.

    nats-py says so in as many words, so the consumer went on existing on the
    server until ``inactive_threshold`` reaped it thirty seconds later. Every
    read here builds one and TD reads its book per fill, so the hot path was
    leaving a consumer per print and carrying thirty seconds' worth at a time.
    """
    transport = _transport(broker)
    name = "oms.consumers"
    await broker.state_put_many(name, {"o1": "1"})
    stream, _head = await transport._state_stream()  # noqa: SLF001

    async def consumers() -> int:
        info = await transport.js.stream_info(stream)
        return info.state.consumer_count

    before = await consumers()
    for _ in range(5):
        await broker.state_all(name)

    assert await consumers() == before


@pytest.mark.asyncio
async def test_a_stream_shape_the_server_will_not_take_is_named_not_raw(
    broker: Broker,
) -> None:
    """A refused reshape says which field refused, from inside ``connect``.

    This is #88's failure written down. Moving ``allow_msg_ttl`` off the fan-out
    stream is fine on a fresh server and impossible on one that has already run
    the previous build: ``add_stream`` refuses because the shape differs, and
    the ``update_stream`` behind it refuses because a TTL flag cannot be turned
    off. ``_ensure_stream`` caught only the first, so the second left
    ``connect()`` as a bare ``ServerError`` — every plane failing to boot, and
    only on servers that already held the stream.

    Asserted on the message rather than only the type, because the whole point
    is that whoever meets this at deploy time is told the field.
    """
    transport = _transport(broker)
    name = _sanitize(f"{broker.config.key_prefix}_shapecheck")
    subject = f"{broker.config.key_prefix}.shapecheck.>"
    await transport.js.add_stream(
        js_api.StreamConfig(name=name, subjects=[subject], allow_msg_ttl=True)
    )

    with pytest.raises(StreamShapeError) as caught:
        await transport._ensure_stream(
            js_api.StreamConfig(name=name, subjects=[subject], allow_msg_ttl=False)
        )

    message = str(caught.value)
    assert name in message
    assert "allow_msg_ttl" in message

    await transport.js.delete_stream(name)


@pytest.mark.asyncio
async def test_a_wider_retention_is_applied_rather_than_refused(
    broker: Broker,
) -> None:
    """Raising a limit on a stream that exists is the case reshaping is for.

    MD hands ``_ensure_tape_stream`` the retention its environment names, so an
    operator who raises ``MD_TAPE_MAXLEN`` changes the declared shape of every
    tape stream at once. Refusing all of them would leave MD unable to record a
    feed it has recorded before until someone deleted the streams by hand, which
    is a worse answer than widening them.
    """
    transport = _transport(broker)
    name = _sanitize(f"{broker.config.key_prefix}_widecheck")
    subject = f"{broker.config.key_prefix}.widecheck.>"
    await transport.js.add_stream(
        js_api.StreamConfig(name=name, subjects=[subject], max_msgs=10)
    )

    await transport._ensure_stream(
        js_api.StreamConfig(name=name, subjects=[subject], max_msgs=100)
    )

    assert (await transport.js.stream_info(name)).config.max_msgs == 100
    await transport.js.delete_stream(name)
