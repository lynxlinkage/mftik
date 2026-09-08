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
import contextlib
from collections.abc import Awaitable, Callable

import pytest
from broker_harness import a_broker, only_on
from mftik.broker import Broker
from mftik.broker.errors import RequestTimeoutError
from mftik.broker.transport.nats import (
    _NO_RESPONDERS_CEILING_S,
    FANOUT_MAX_MSGS_PER_SUBJECT,
    MIN_TTL_SECONDS,
    NatsTransport,
    _check_subject,
    _iter_until_stopped,
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

    Written with plain ``publish``, because that is where the stream's own cap is
    the *only* bound. ``publish_log`` adds a purge to whatever its caller asked
    to keep, so it can only ever show a number the caller chose.
    """
    topic = Topics.log_sts("abc")
    for n in range(FANOUT_MAX_MSGS_PER_SUBJECT + 20):
        await broker.publish(topic, _envelope(n))

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


@pytest.mark.asyncio
async def test_a_stopped_serve_loop_still_hands_over_what_it_had_taken() -> None:
    """Everything queued when the stop event fires, whichever won the race.

    ``_pump_posted`` acknowledges a posted message as it hands it to the queue,
    so a message dropped here is a backfill or an account sweep the work queue
    will not offer to anybody again. The ordering that mattered is the one where
    a message *wins* the race: the loop yields it and then finds its own
    condition false, and the rest of the batch used to go out with it.
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
    nothing, so the message was neither in the queue nor with the consumer — and
    on `serve` that is a posted request `_pump_posted` had already acknowledged,
    which makes it a backfill hand-off or an account-history sweep that simply
    stopped existing. Exactly the loss the handover after the loop prevents, in
    the one path that did not go through it.

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
            await broker.post(topic, _envelope())

    task = asyncio.create_task(plane_loop())
    await _arm(task, read, nudge)

    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert _leftovers(before) == frozenset()


@pytest.mark.asyncio
async def test_a_ring_longer_than_the_stream_can_hold_is_refused(
    broker: Broker,
) -> None:
    """Loudly, because the stream has already discarded by the time we know.

    ``maxlen`` is enforced by a purge that keeps the newest N, and a purge
    cannot bring back what the stream's own per-subject cap dropped on the way
    in. So the only honest answers are "hold that many" and "no" — and this used
    to be neither: a request above the cap skipped the purge and was served
    however many the stream happened to be keeping.
    """
    with pytest.raises(ValueError, match="per-subject ceiling"):
        await broker.publish_log(
            Topics.status_sts(),
            _envelope(),
            maxlen=FANOUT_MAX_MSGS_PER_SUBJECT + 1,
        )


@pytest.mark.asyncio
async def test_a_log_line_carries_the_expiry_its_caller_asked_for(
    broker: Broker,
) -> None:
    """``ttl_seconds`` was accepted and dropped on the floor.

    The fan-out stream has one ``max_age`` for every subject on it, so the
    caller's number had nowhere to go and every line lived the stream's full
    day. A per-message TTL is where it goes — the same mechanism a lease uses,
    which is why the stream is created with ``allow_msg_ttl``.
    """
    transport = _transport(broker)
    topic = "log.sts.expiry"
    await broker.publish_log(topic, _envelope(), maxlen=10, ttl_seconds=1800)

    msg = await transport.js.get_msg(
        transport._fanout_stream,  # noqa: SLF001
        subject=transport._fanout_subject(topic),  # noqa: SLF001
    )
    assert (msg.headers or {}).get("Nats-TTL") == "1800"


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
