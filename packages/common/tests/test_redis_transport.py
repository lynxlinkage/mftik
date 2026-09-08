"""The Redis transport's own machinery.

Everything here describes how Redis keeps a promise rather than the promise
itself — a list being capped, a key expiring, a blocking pop that stalled. The
promises are in ``test_broker*.py`` and hold on either transport; these are the
mechanics behind them on this one, and they run only on the Redis pass.

Two of them are regressions with dates on them, both a failed poll ending a
loop. What the retry policy around them is and why it is narrow is next door in
``test_broker_retry.py``, which needs no server and so runs on either pass.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from broker_harness import a_broker, only_on
from mftik.broker import Broker
from mftik.broker.errors import RequestTimeoutError
from mftik.broker.transport.redis import (
    PROBE_QUEUE_MAXLEN,
    PROBE_QUEUE_TTL_SECONDS,
    RedisTransport,
    _ms,
)
from mftik.protocol import Envelope, HealthCheck, Topics
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

pytestmark = only_on("redis")

SUBJECT = "demo"


@pytest.fixture
async def broker() -> Broker:
    async with a_broker("test-redis") as client:
        yield client


def _envelope(n: int) -> Envelope[dict[str, Any]]:
    return Envelope[dict].wrap({"n": n}, type="demo", source="test")


def _probe_envelope() -> Envelope[HealthCheck]:
    return Envelope[HealthCheck].wrap(HealthCheck(), type="md.health", source="api")


def _queue(broker: Broker, subject: str) -> str:
    transport = broker.transport
    assert isinstance(transport, RedisTransport)
    return transport._rpc_queue(subject)  # noqa: SLF001


# --- a failed poll costs a poll ----------------------------------------------


@pytest.fixture(autouse=True)
def _no_retry_delay(monkeypatch: pytest.MonkeyPatch) -> None:
    """The wait after a failed poll is there to pace a log, not a test."""
    from mftik.broker.transport import redis as redis_transport

    monkeypatch.setattr(redis_transport, "_SERVE_POLL_RETRY_S", 0.0)


def _raise_once(
    broker: Broker, error: BaseException, *, on_key: str | None = None
) -> list[str]:
    """Fail one ``blpop`` the way a stalled socket does, then behave.

    ``on_key`` picks which poll fails, because the loop under test and the
    caller waiting on its reply both poll the same client — without it a test
    aimed at one of them can be satisfied by the other.
    """
    real = broker.transport.redis.blpop
    fired: list[str] = []

    async def flaky(*args: Any, **kwargs: Any) -> Any:
        key = args[0] if args else kwargs.get("keys")
        if not fired and on_key in (None, key):
            fired.append(str(key))
            raise error
        return await real(*args, **kwargs)

    broker.transport.redis.blpop = flaky
    return fired


async def _first_request(broker: Broker, stop: asyncio.Event) -> Any:
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

    A read deadline expiring and a Redis that outlived its retries look the same
    from here: the poll failed and the next one may well work.
    """
    stop = asyncio.Event()
    queue = _queue(broker, SUBJECT)
    fired = _raise_once(broker, error, on_key=queue)
    task = asyncio.create_task(_first_request(broker, stop))

    await broker.post(SUBJECT, _envelope(1))
    req = await asyncio.wait_for(task, timeout=5)

    assert fired == [queue]
    assert req.envelope.payload == {"n": 1}
    stop.set()


@pytest.mark.asyncio
async def test_a_failed_reply_poll_is_not_an_answer(broker: Broker) -> None:
    """``request`` keeps asking until its own deadline, not until Redis slips.

    The caller's contract is the timeout it passed. A poll that raised has said
    nothing about whether a reply is coming, so it must not be allowed to
    surface as some other exception in its place.
    """
    stop = asyncio.Event()

    async def responder() -> None:
        async for req in broker.serve(SUBJECT, stop=stop):
            await req.reply(
                Envelope[dict].wrap({"pong": True}, type="demo.reply", source="test")
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


# --- what a probe leaves behind ----------------------------------------------


@pytest.mark.asyncio
async def test_probing_a_dead_instance_does_not_grow_without_end(
    broker: Broker,
) -> None:
    """Redis has no way to refuse an unserved request, so the queue is capped.

    ``request`` would leave one record per probe in a list forever — roughly 17k
    a day at a five-second refresh, per down instance. The cap is what makes a
    dashboard safe to point at something that is not there. NATS keeps the same
    promise by storing nothing at all.
    """
    subject = Topics.health("md", "md-jp-1")
    queue = _queue(broker, subject)

    for _ in range(PROBE_QUEUE_MAXLEN * 3):
        with pytest.raises(RequestTimeoutError):
            await broker.probe(subject, _probe_envelope(), timeout=0.02)

    assert await broker.transport.redis.llen(queue) == PROBE_QUEUE_MAXLEN


@pytest.mark.asyncio
async def test_a_probe_queue_expires_so_a_forgotten_key_goes_away(
    broker: Broker,
) -> None:
    """The cap bounds a queue being written to; this is what removes it."""
    subject = Topics.health("md", "md-jp-1")
    queue = _queue(broker, subject)

    with pytest.raises(RequestTimeoutError):
        await broker.probe(subject, _probe_envelope(), timeout=0.02)

    ttl = await broker.transport.redis.ttl(queue)
    assert 0 < ttl <= PROBE_QUEUE_TTL_SECONDS


@pytest.mark.asyncio
async def test_a_probe_reply_key_is_not_left_behind_on_timeout(
    broker: Broker,
) -> None:
    """The other half of leaving nothing: the caller cleans up after itself."""
    envelope = _probe_envelope()
    reply_key = f"{broker.config.key_prefix}:rpc:reply:{envelope.id}"

    with pytest.raises(RequestTimeoutError):
        await broker.probe(Topics.health("td", "td-jp-1"), envelope, timeout=0.02)

    assert await broker.transport.redis.exists(reply_key) == 0


# --- sub-second leases, which only this transport can express ----------------


@pytest.mark.asyncio
async def test_a_lease_can_expire_inside_a_test(broker: Broker) -> None:
    """Redis takes a TTL in milliseconds, so a test need not sleep a second.

    NATS cannot: its per-message TTL is whole seconds with a one second floor,
    so the same test there has to wait one out. Production leases are thirty
    seconds and neither transport is near the difference.
    """
    await broker.lease_put("sts:alive:s-1", ttl=0.05, owner="p1")
    assert await broker.lease_held("sts:alive:s-1") is True
    await asyncio.sleep(0.2)
    assert await broker.lease_held("sts:alive:s-1") is False


def test_a_fractional_ttl_survives_the_conversion() -> None:
    assert _ms(0.05) == 50
    assert _ms(30) == 30_000


def test_a_ttl_too_small_to_express_becomes_the_smallest_one() -> None:
    """Redis rejects a zero TTL outright, and rounding is a poor way to learn it."""
    assert _ms(0.0001) == 1
    assert _ms(0) == 1


# --- what removes a tape nobody writes to anymore -----------------------------


@pytest.mark.asyncio
async def test_appending_renews_the_ttl_on_both_tape_keys(broker: Broker) -> None:
    """Without this a tape outlives the last strategy that ever wanted it.

    A key expiry, because a Redis stream does not age out on its own — which is
    also why the MINID trim next to it is the real retention policy rather than
    this. NATS puts the age on the stream instead and needs no renewal, so the
    same promise is tested there by reading the stream's own limits.
    """
    transport = broker.transport
    assert isinstance(transport, RedisTransport)
    feed = "aggtrade.BinanceUM_Perp_BTCUSDT"

    await broker.tape_mark_recording(feed, since_ms=1, ttl_seconds=3600)
    await broker.tape_append(feed, {"price": "1"}, maxlen=100, ttl_seconds=1800)

    assert 0 < await transport.redis.ttl(transport.tape_key(feed)) <= 1800
    assert 0 < await transport.redis.ttl(transport.tape_coverage_key(feed)) <= 1800
