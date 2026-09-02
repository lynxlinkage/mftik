"""The broker every fakeredis-backed test runs against.

Fifty-odd test modules across six packages want the same thing: a
:class:`Broker` on a private in-memory Redis, connected, and closed after.
One copy here beats fifty that drift — the same reasoning as ``db_harness``
next door (see the ``pythonpath`` note in the root ``pyproject.toml``).

It also sets the poll interval, which is the reason this file exists rather
than the fifty copies continuing to work. A serving loop cannot be
cancelled out of a blocking ``BLPOP``, so a domain shutting down waits out
at most one poll — a second in production, and a second per test teardown
here. That second was the single largest cost in the suite; measured
against the whole of ``packages apps``, dropping it took the run from 238s
to 116s with nothing else changed.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import fakeredis.aioredis
from mftik.broker import Broker, BrokerConfig

#: Short enough to disappear into a test, long enough not to spin.
#:
#: fakeredis honours a fractional ``BLPOP`` timeout, so this is a real wait
#: of 50ms and not a busy loop. Production keeps the default second: nothing
#: there is waiting on a domain's shutdown, and a poll that often would be
#: twenty pointless round trips a second per served subject.
TEST_POLL_SECONDS = 0.05


@asynccontextmanager
async def a_broker(key_prefix: str = "test") -> AsyncIterator[Broker]:
    """A connected broker on its own in-memory Redis, closed on the way out.

    ``key_prefix`` keeps two modules' queues apart when they would otherwise
    collide on a shared name; the Redis itself is private to each call, so
    it matters only for readability in a failure.
    """
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(
            redis_url="redis://fake",
            key_prefix=key_prefix,
            serve_poll_seconds=TEST_POLL_SECONDS,
        ),
        redis_client=redis,
    )
    await client.connect()
    try:
        yield client
    finally:
        await client.close()
        await redis.aclose()
