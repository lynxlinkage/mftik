"""The broker every test runs against, on whichever transport is selected.

Sixty-odd test modules across six packages want the same thing: a connected
:class:`Broker` that nothing else is using, closed after. One copy here beats
sixty that drift — the same reasoning as ``db_harness`` next door (see the
``pythonpath`` note in the root ``pyproject.toml``).

``MFTIK_TEST_BROKER`` chooses the transport, exactly as ``MFTIK_TEST_LOOP``
chooses the event loop and ``TEST_POSTGRES_URL`` chooses the database. It
defaults to ``nats``, because that is what a node runs on, and a suite on a
different transport from the node cannot see a transport-specific regression.
``redis`` runs the same tests against the other one; CI runs both.

Neither is a fake. There was a fake — fakeredis — and it is gone, for the
reason the database suite gave up on sqlite-only years earlier: an in-process
imitation agrees with the real server right up to the behaviour you are trying
to test. JetStream has no in-process imitation at all, and writing one would
have meant asserting against our own guess at what a consumer does.

**Isolation.** fakeredis handed every test a private store, and a real server
does not, so each broker here gets a ``key_prefix`` nobody else has and drops
everything under it on the way out. Under NATS that prefix names the streams
and buckets; under Redis it is the first segment of every key.
"""

from __future__ import annotations

import os
import socket
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, closing, suppress
from urllib.parse import urlsplit

import pytest
from mftik.broker import Broker, BrokerConfig
from mftik.broker.transport.nats import _sanitize

#: Which transport ``a_broker`` builds, and the environment variable that says.
TRANSPORT_ENV = "MFTIK_TEST_BROKER"

#: Short enough to disappear into a test, long enough not to spin.
#:
#: Redis only, and only for its blocking pop: a serving loop cannot be
#: cancelled out of one, so a domain shutting down waits out at most one poll —
#: a second in production, and a second per test teardown here. That second was
#: once the single largest cost in the suite. NATS needs none of it, because a
#: subscription *is* cancellable and a serve loop there stops when it is told.
TEST_POLL_SECONDS = 0.05


def transport_name() -> str:
    """The transport this run is exercising."""
    return os.getenv(TRANSPORT_ENV, "nats").strip().lower()


def only_on(transport: str) -> pytest.MarkDecorator:
    """Skip unless this run is on ``transport``.

    For the tests that describe one store's internals rather than the broker's
    promises — a Redis list being capped, a NATS consumer being ephemeral. Those
    are worth testing and cannot be written twice, so they are written once and
    run on the pass that can see them. Anything about what a *caller* is
    promised belongs in a test with no marker at all.
    """
    return pytest.mark.skipif(
        transport_name() != transport,
        reason=(
            f"describes the {transport} transport; this run is on {transport_name()}"
        ),
    )


#: The shortest lease TTL the selected transport can express, in seconds.
#:
#: Redis takes an expiry in milliseconds, so a test that wants to watch a claim
#: lapse can cut it to 20ms and sleep. NATS' per-message TTL is whole seconds
#: with a one second floor (ADR-43), and a shorter request is rounded up rather
#: than honoured — so the same test has to wait out a real second there.
#:
#: A test that needs a lapse asks for this instead of naming a number, which is
#: what keeps it a test about lapsing rather than about one store's resolution.
MIN_LEASE_TTL = 0.02 if transport_name() == "redis" else 1.0


def test_config(key_prefix: str) -> BrokerConfig:
    """A broker config for one test, on the selected transport."""
    return BrokerConfig(
        transport=transport_name(),
        nats_url=os.getenv("NATS_URL", "nats://localhost:4222"),
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/15"),
        key_prefix=key_prefix,
        serve_poll_seconds=TEST_POLL_SECONDS,
        # Long enough that nothing is reaped mid-test, short enough that a
        # crashed run does not leave consumers on the server for the afternoon.
        consumer_idle_seconds=60.0,
    )


@asynccontextmanager
async def a_broker(key_prefix: str = "test") -> AsyncIterator[Broker]:
    """A connected broker nothing else shares, cleaned up on the way out.

    ``key_prefix`` is a label rather than the isolation: a suffix nobody else
    has is appended, so two modules asking for ``"test"`` still cannot see each
    other. Pass something meaningful only when a failure would be easier to read
    with a name on it.
    """
    prefix = f"{key_prefix}-{uuid.uuid4().hex[:10]}"
    client = Broker(test_config(prefix))
    await client.connect()
    try:
        yield client
    finally:
        with suppress(Exception):
            await _drop_everything(client, prefix)
        await client.close()


async def _drop_everything(broker: Broker, prefix: str) -> None:
    """Remove what this broker created, so the next test starts empty.

    Reaches through :attr:`Broker.transport` on purpose. A test suite is allowed
    to — it is describing the transport rather than going around it — and the
    alternative is a teardown hook on a production class that only tests call.
    """
    transport = broker.transport
    if transport_name() == "redis":
        keys = [key async for key in transport.redis.scan_iter(f"{prefix}:*")]
        if keys:
            await transport.redis.delete(*keys)
        return

    # NATS: the prefix names the streams, and a KV bucket is a stream called
    # ``KV_<bucket>``, so deleting by name covers the buckets too. Spelled
    # through the transport's own sanitiser rather than guessed at, because a
    # prefix a test chose may not survive being a stream name unchanged.
    stem = _sanitize(prefix)
    wanted = (stem, f"KV_{stem}")
    for info in await transport.js.streams_info():
        name = info.config.name or ""
        if name.startswith(wanted):
            with suppress(Exception):
                await transport.js.delete_stream(name)


# --- reaching past the broker, on purpose -------------------------------------
#
# A handful of tests need to describe something the broker's own vocabulary
# cannot say: what is sitting on a subject nobody has served yet, a message that
# will not parse, a tape record stamped an hour ago. Each is a real promise with
# a real caller behind it, and each is spelled differently on the two
# transports.
#
# They live here rather than in the tests so the assertions stay transport-blind:
# a test says "posted work waits for its consumer" once, and this is the part
# that knows how each store spells it.


async def inject_raw_request(broker: Broker, subject: str, raw: str) -> None:
    """Leave ``raw`` as durable work on ``subject``, bypassing the broker.

    For the one thing ``post`` cannot express: a request that will not parse.
    ``Broker.post`` takes an envelope and serializes it, so a test about what a
    serve loop does with unreadable bytes has to put the bytes there itself.
    """
    transport = broker.transport
    if transport_name() == "redis":
        await transport.redis.rpush(transport._rpc_queue(subject), raw)  # noqa: SLF001
        return
    await transport.js.publish(transport._post_subject(subject), raw.encode())  # noqa: SLF001


async def queued_requests(broker: Broker, subject: str) -> list[str]:
    """Durable work waiting on ``subject``, without serving it.

    Serving would consume it, and what these tests assert is precisely that
    something is *still there* — that a detach was posted rather than awaited,
    and that its handler has not run yet.
    """
    transport = broker.transport
    if transport_name() == "redis":
        return list(
            await transport.redis.lrange(transport._rpc_queue(subject), 0, -1)  # noqa: SLF001
        )

    # NATS: read the work-queue stream directly, message by message, with no
    # consumer anywhere in it.
    #
    # Not a consumer, on purpose and not for tidiness. A work-queue stream
    # refuses a consumer that does not acknowledge — "consumer in pull mode
    # requires explicit ack policy on workqueue stream" — and one that *does*
    # acknowledge deletes the message, which is precisely what serving it would
    # have done. Walking the subject by sequence is the only way to look without
    # taking.
    import nats.js.errors
    from mftik.broker.transport.nats import NatsTransport

    assert isinstance(transport, NatsTransport)
    post_subject = transport._post_subject(subject)  # noqa: SLF001
    stream = transport._post_stream  # noqa: SLF001

    rows: list[str] = []
    seq = 0
    while True:
        try:
            msg = await transport.js.get_msg(
                stream, seq=seq + 1, subject=post_subject, next=True
            )
        except nats.js.errors.NotFoundError:
            return rows
        assert msg.seq is not None
        seq = msg.seq
        rows.append((msg.data or b"").decode())


async def append_tape_at(
    broker: Broker,
    feed: str,
    fields: dict[str, str],
    *,
    recorded_ms: int,
    ttl_seconds: int = 3600,
) -> None:
    """Append a tape record stamped ``recorded_ms``.

    Gaps are the point. A recording's stamp is assigned by the store at write
    time, so a test that let it do that could only ever produce gaps as long as
    it was willing to sleep — and the behaviour worth testing is a deploy-sized
    hole against a two hour window.
    """
    await broker.transport.tape_append(
        feed, fields, maxlen=10_000, ttl_seconds=ttl_seconds, recorded_ms=recorded_ms
    )


def server_address() -> tuple[str, int]:
    """Host and port of the server this run needs, for a reachability check."""
    config = test_config("probe")
    url = config.redis_url if transport_name() == "redis" else config.nats_url
    parts = urlsplit(url)
    default = 6379 if transport_name() == "redis" else 4222
    return parts.hostname or "localhost", parts.port or default


def server_is_up(timeout: float = 1.0) -> bool:
    """Whether the selected transport's server is accepting connections.

    A bare TCP connect, because this is called from ``pytest_sessionstart``
    where there is no event loop yet and the question is only whether anything
    is listening. A server that answers the socket and then refuses every
    request will fail the tests, which is the right place for that to surface.
    """
    host, port = server_address()
    with closing(socket.socket()) as sock:
        sock.settimeout(timeout)
        return sock.connect_ex((host, port)) == 0
