"""The broker every test runs against.

Sixty-odd test modules across six packages want the same thing: a connected
:class:`Broker` that nothing else is using, closed after. One copy here beats
sixty that drift — the same reasoning as ``db_harness`` next door (see the
``pythonpath`` note in the root ``pyproject.toml``).

This is not a fake. There was one — fakeredis — and it is gone, for the
reason the database suite gave up on sqlite-only years earlier: an in-process
imitation agrees with the real server right up to the behaviour you are trying
to test. JetStream has no in-process imitation at all, and writing one would
have meant asserting against our own guess at what a consumer does.

**Isolation.** fakeredis handed every test a private store, and a real server
does not, so each broker here gets a ``key_prefix`` nobody else has and drops
everything under it on the way out. That prefix names the streams and buckets.
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

#: The shortest lease TTL NATS can express, in seconds.
#:
#: Per-message TTL is whole seconds with a one second floor (ADR-43), and a
#: shorter request is rounded up rather than honoured. A test that watches a
#: claim lapse asks for this instead of naming a number, which is what keeps
#: it a test about lapsing rather than about the store's resolution.
MIN_LEASE_TTL = 1.0


def test_config(key_prefix: str) -> BrokerConfig:
    """A broker config for one test."""
    return BrokerConfig(
        nats_url=os.getenv("NATS_URL", "nats://localhost:4222"),
        key_prefix=key_prefix,
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

    The prefix names the streams, and a KV bucket is a stream called
    ``KV_<bucket>``, so deleting by name covers the buckets too. Spelled
    through the transport's own sanitiser rather than guessed at, because a
    prefix a test chose may not survive being a stream name unchanged.
    """
    transport = broker.transport
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
# a real caller behind it.
#
# They live here rather than in the tests so the assertions stay above the
# store: a test says "bytes that will not parse still reach serve" once, and
# this is the part that knows how the store spells it.


async def inject_raw_request(broker: Broker, subject: str, raw: str) -> None:
    """Publish ``raw`` onto the core RPC subject, bypassing the broker.

    For the one thing ``request`` cannot express: bytes that will not parse.
    A serve loop must already be listening — core NATS stores nothing.
    """
    transport = broker.transport
    await transport.nc.publish(transport._rpc_subject(subject), raw.encode())  # noqa: SLF001


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
    """Host and port of the NATS server this run needs, for a reachability check."""
    config = test_config("probe")
    parts = urlsplit(config.nats_url)
    return parts.hostname or "localhost", parts.port or 4222


def server_is_up(timeout: float = 1.0) -> bool:
    """Whether the NATS server is accepting connections.

    A bare TCP connect, because this is called from ``pytest_sessionstart``
    where there is no event loop yet and the question is only whether anything
    is listening. A server that answers the socket and then refuses every
    request will fail the tests, which is the right place for that to surface.
    """
    host, port = server_address()
    with closing(socket.socket()) as sock:
        sock.settimeout(timeout)
        return sock.connect_ex((host, port)) == 0
