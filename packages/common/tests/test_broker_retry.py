"""A dead pooled connection must cost a retry, not a session.

Redis closes a client connection that has idled past its ``timeout`` (300s in
production). The client only finds out when it borrows that connection again
and the health-check ping fails — and with no retry configured, redis-py hands
that ConnectionError to whoever asked. In STS the asker is a feed pump, and its
only reading of an exception is ``session can no longer run``: on 2026-08-18 a
cross_arb session was marked failed 2ms into ``pubsub.subscribe()`` for exactly
this, with nothing wrong with it or with Redis, which had been up 13 days.

So these test the policy, not the plumbing: what the client does with a
ConnectionError, and what it deliberately does not do with a TimeoutError.

The TimeoutError half was written on a wrong premise — that one cannot arise
without a ``socket_timeout``. It can: a blocking command carries a read
deadline of its own, and on 2026-08-18 that deadline ended STS's RPC serve
loop. The policy did not change, but its reason did, and the loops that issue
blocking pops now handle the exception themselves — see
``test_broker_serve_survives``.
"""

from __future__ import annotations

import pytest
from mftik.broker.client import build_redis
from mftik.broker.config import BrokerConfig
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

CONFIG = BrokerConfig(redis_url="redis://default:secret@redis.invalid:6379/0")


def _connection():
    """One connection as the pool would build it. No server is contacted."""
    return build_redis(CONFIG).connection_pool.make_connection()


async def _failing(error: BaseException, *, times: int, calls: list[int]):
    async def do() -> str:
        calls.append(1)
        if len(calls) <= times:
            raise error
        return "PONG"

    return do


async def _swallow(_error: BaseException) -> None:
    """Stand in for ``_disconnect_raise``, which drops the dead connection."""


@pytest.mark.asyncio
async def test_a_connection_the_server_closed_is_retried_not_raised() -> None:
    """The regression: this is the exception that failed a healthy session."""
    conn = _connection()
    calls: list[int] = []
    do = await _failing(
        RedisConnectionError("Connection closed by server."),
        times=1,
        calls=calls,
    )

    assert await conn.retry.call_with_retry(do, _swallow) == "PONG"
    # Not "it did not raise" — it was actually sent again on a fresh checkout.
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_a_timeout_is_never_re_sent() -> None:
    """The narrowing, and it is about what a re-sent pop would take.

    A retry re-sends, and ``request`` carries new orders — reason enough on its
    own to keep this list short. But the case against retrying a *timeout* is
    sharper than that: the only commands that can time out here are the
    blocking pops, and a re-sent BLPOP takes the next element rather than the
    one whose reply was lost. That element is gone, and gone silently, which is
    worse than the failed poll a caller can see and repeat.
    """
    conn = _connection()
    calls: list[int] = []
    do = await _failing(RedisTimeoutError("timed out"), times=1, calls=calls)

    with pytest.raises(RedisTimeoutError):
        await conn.retry.call_with_retry(do, _swallow)
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_a_redis_that_is_really_down_still_gives_up() -> None:
    """Retrying is not waiting forever — the caller still learns it failed."""
    conn = _connection()
    calls: list[int] = []
    do = await _failing(
        RedisConnectionError("Connection closed by server."),
        times=99,
        calls=calls,
    )

    with pytest.raises(RedisConnectionError):
        await conn.retry.call_with_retry(do, _swallow)
    assert len(calls) == CONFIG.command_retries + 1


def test_the_health_check_still_fires_before_the_server_would_hang_up() -> None:
    """The two settings are one mechanism: find it, then replace it.

    A check that idles longer than the server's ``timeout`` never sees a live
    connection go stale, and the retry would be carrying the whole load.
    """
    assert CONFIG.health_check_interval < 300
    assert CONFIG.command_retries > 0
