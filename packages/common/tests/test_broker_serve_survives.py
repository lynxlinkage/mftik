"""What must not end a serve loop.

``Broker.serve`` is a domain's control plane. When it returns, the process
stays up: sessions keep trading, the heartbeat keeps ticking, and every
request piles into a Redis list nobody is reading. On 2026-08-18 STS spent
seven hours in that state after one ``BLPOP`` raised ``TimeoutError`` on a
socket that stalled — a failure that costs one poll, ended a control plane,
and left no line in any log.

So these test what the loop survives, and that surviving means going back to
work rather than merely not raising.
"""

from __future__ import annotations

import asyncio
from typing import Any

import fakeredis.aioredis
import pytest
from mftik.broker import Broker, BrokerConfig, IncomingRequest
from mftik.broker import client as broker_client
from mftik.protocol import Envelope
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

SUBJECT = "demo"


@pytest.fixture
async def broker() -> Broker:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test-serve"),
        redis_client=redis,
    )
    await client.connect()
    yield client
    await client.close()
    await redis.aclose()


@pytest.fixture(autouse=True)
def _no_retry_delay(monkeypatch: pytest.MonkeyPatch) -> None:
    """The wait after a failed poll is there to pace a log, not a test."""
    monkeypatch.setattr(broker_client, "_SERVE_POLL_RETRY_S", 0.0)


def _envelope(n: int) -> Envelope[dict[str, Any]]:
    return Envelope[dict].wrap({"n": n}, type="demo", source="test")


def _raise_once(
    broker: Broker, error: BaseException, *, on_key: str | None = None
) -> list[str]:
    """Fail one ``blpop`` the way a stalled socket does, then behave.

    ``on_key`` picks which poll fails, because the loop under test and the
    caller waiting on its reply both poll the same client — without it a test
    aimed at one of them can be satisfied by the other.
    """
    real = broker.redis.blpop
    fired: list[str] = []

    async def flaky(*args: Any, **kwargs: Any) -> Any:
        key = args[0] if args else kwargs.get("keys")
        if not fired and on_key in (None, key):
            fired.append(str(key))
            raise error
        return await real(*args, **kwargs)

    broker.redis.blpop = flaky
    return fired


async def _first_request(broker: Broker, stop: asyncio.Event) -> IncomingRequest:
    async for req in broker.serve(SUBJECT, stop=stop):
        return req
    raise AssertionError("serve ended without yielding")


@pytest.mark.parametrize(
    "error",
    [
        RedisTimeoutError("Timeout reading from redis:6379"),
        RedisConnectionError("Connection closed by server."),
    ],
    ids=["read-timeout", "connection-error"],
)
@pytest.mark.asyncio
async def test_a_failed_poll_costs_a_poll_and_not_the_loop(
    broker: Broker, error: BaseException
) -> None:
    """The regression, both ways it arrives.

    A read deadline expiring and a Redis that outlived its retries look the
    same from here: the poll failed and the next one may well work.
    """
    stop = asyncio.Event()
    queue = f"{broker.config.key_prefix}:rpc:{SUBJECT}"
    fired = _raise_once(broker, error, on_key=queue)
    task = asyncio.create_task(_first_request(broker, stop))

    await broker.post(SUBJECT, _envelope(1))
    req = await asyncio.wait_for(task, timeout=5)

    assert fired == [queue]
    assert req.envelope.payload == {"n": 1}
    stop.set()


@pytest.mark.asyncio
async def test_an_unreadable_request_is_dropped_rather_than_served(
    broker: Broker,
) -> None:
    """BLPOP already took it, so the only choice left is which one dies."""
    stop = asyncio.Event()
    queue = f"{broker.config.key_prefix}:rpc:{SUBJECT}"
    await broker.redis.rpush(queue, "{not an envelope")
    await broker.post(SUBJECT, _envelope(2))

    req = await asyncio.wait_for(_first_request(broker, stop), timeout=5)

    assert req.envelope.payload == {"n": 2}
    stop.set()


@pytest.mark.asyncio
async def test_a_failed_reply_poll_is_not_an_answer(broker: Broker) -> None:
    """``request`` keeps asking until its own deadline, not until Redis slips.

    The caller's contract is the timeout it passed. A poll that raised has
    said nothing about whether a reply is coming, so it must not be allowed
    to surface as some other exception in its place.
    """
    stop = asyncio.Event()

    async def responder() -> None:
        async for req in broker.serve(SUBJECT, stop=stop):
            await req.reply(
                Envelope[dict].wrap(
                    {"pong": True}, type="demo.reply", source="test"
                )
            )
            return

    server = asyncio.create_task(responder())
    await asyncio.sleep(0.05)

    ask = _envelope(3)
    reply_key = f"{broker.config.key_prefix}:rpc:reply:{ask.id}"
    fired = _raise_once(
        broker,
        RedisTimeoutError("Timeout reading from redis:6379"),
        on_key=reply_key,
    )

    reply = await broker.request(SUBJECT, ask, timeout=5)

    assert fired == [reply_key]
    assert reply.payload == {"pong": True}
    stop.set()
    await server
