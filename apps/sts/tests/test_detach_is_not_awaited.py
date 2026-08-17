"""Stopping a session must not wait on the domains it is detaching from.

The lease is what actually ends an attach: TD and MD each watch this session's
heartbeat and run the identical teardown when it stops. The detach message is
promptness and a reason on the row, not the mechanism — so a domain that is
slow, busy or gone can cost a stop nothing.
"""

from __future__ import annotations

import asyncio
import time

import fakeredis.aioredis
import pytest
from mftik.broker import Broker, BrokerConfig
from mftik.protocol import (
    MD_SESSION_DETACH,
    TD_SESSION_DETACH,
    MdDetachRequest,
    TdDetachRequest,
    Topics,
    UntypedEnvelope,
)
from mftik_sts.session.session import StsSession
from mftik_sts.strategy import Strategy


@pytest.fixture
async def broker() -> Broker:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test-detach"),
        redis_client=redis,
    )
    await client.connect()
    yield client
    await client.close()
    await redis.aclose()


async def _queued(broker: Broker, subject: str) -> list[UntypedEnvelope]:
    """Everything sitting on a subject's RPC queue, without serving it."""
    key = broker._rpc_queue(subject)  # noqa: SLF001
    rows = await broker.redis.lrange(key, 0, -1)
    return [UntypedEnvelope.from_json(row) for row in rows]


def _session(broker: Broker, **kwargs) -> StsSession:
    return StsSession(
        session_id=kwargs.pop("session_id", "d-1"),
        broker=broker,
        created_by=1,
        strategy=Strategy(),
        heartbeat_interval=0.05,
        **kwargs,
    )


async def test_stop_does_not_wait_for_a_domain_that_never_answers(
    broker: Broker,
) -> None:
    """Nothing is serving TD or MD here — the old code took ten seconds."""
    session = _session(broker, td_api_ids=[1, 2], md_ids=["ticker.Paper_Spot_BTCUSDT"])
    await session.start()

    started = time.monotonic()
    await session.stop()
    elapsed = time.monotonic() - started

    # Generous against a slow machine and still an order of magnitude under
    # the two five-second attempts per attach this replaced.
    assert elapsed < 2.0, f"stop took {elapsed:.1f}s"


async def test_the_detaches_are_left_on_the_queue_for_whoever_serves_it(
    broker: Broker,
) -> None:
    """Not waiting is not the same as not sending.

    A Redis list holds the message until a consumer takes it, so a domain that
    is down during the stop still gets the detach when it comes back — which
    is the difference between this and publishing on a stream nobody reads.
    """
    session = _session(
        broker,
        session_id="d-2",
        td_api_ids=[7],
        md_ids=["ticker.Paper_Spot_BTCUSDT"],
    )
    await session.start()
    await session.stop()

    td = await _queued(broker, Topics.TD)
    md = await _queued(broker, Topics.MD)

    assert [env.type for env in td] == [TD_SESSION_DETACH]
    assert [env.type for env in md] == [MD_SESSION_DETACH]

    td_payload = TdDetachRequest.model_validate(td[0].payload)
    assert td_payload.session_id == "d-2"
    assert td_payload.api_id == 7
    # The reason is the whole point of sending it at all: it separates a clean
    # stop from a process that vanished and let its lease expire.
    assert td_payload.reason == "sts_stop"
    assert MdDetachRequest.model_validate(md[0].payload).session_id == "d-2"

    # Posted, not requested: nobody is waiting on a reply inbox.
    assert td[0].reply_to is None
    assert md[0].reply_to is None


async def test_one_detach_per_attached_api_id(broker: Broker) -> None:
    session = _session(broker, session_id="d-3", td_api_ids=[1, 2, 3])
    await session.start()
    await session.stop()

    td = await _queued(broker, Topics.TD)
    api_ids = sorted(
        TdDetachRequest.model_validate(env.payload).api_id for env in td
    )
    assert api_ids == [1, 2, 3]
    # No md attach, so nothing was sent to MD.
    assert await _queued(broker, Topics.MD) == []


async def test_a_broker_that_cannot_take_the_detach_still_stops(
    broker: Broker, monkeypatch
) -> None:
    """The lease covers it, so a failed post is a log line and not a hang."""

    async def boom(subject, envelope):  # noqa: ANN001, ANN202
        raise RuntimeError("redis is down")

    session = _session(broker, session_id="d-4", td_api_ids=[1])
    await session.start()
    monkeypatch.setattr(broker, "post", boom)

    started = time.monotonic()
    await session.stop()

    assert time.monotonic() - started < 2.0
    assert session.destroyed


async def test_the_heartbeat_stops_which_is_what_ends_the_attach(
    broker: Broker,
) -> None:
    """The detach is the courtesy; this is the mechanism."""
    session = _session(broker, session_id="d-5", td_api_ids=[1])
    await session.start()

    seen: list[str] = []
    stop = asyncio.Event()

    async def listen() -> None:
        async for env in broker.subscribe(
            Topics.sts_td_session("d-5"), stop=stop
        ):
            seen.append(env.type)

    task = asyncio.create_task(listen())
    await asyncio.sleep(0.2)
    assert seen, "no heartbeat while running"

    await session.stop()
    seen.clear()
    await asyncio.sleep(0.2)
    stop.set()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert seen == [], "heartbeat outlived the session"
