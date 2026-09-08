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
