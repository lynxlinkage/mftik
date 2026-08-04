"""Status fan-out on ``status.sts`` — what the UI listens to.

Each event is a full snapshot rather than a delta, and every event is written
to the replay buffer, so a page that connects late still learns the current
state of everything it missed.
"""

from __future__ import annotations

import asyncio
import json

import fakeredis.aioredis
import pytest
from mft.broker import Broker, BrokerConfig
from mft.protocol import (
    STS_SESSION_STATUS,
    StsCreateSessionRequest,
    Topics,
)
from mft_sts.impl import register
from mft_sts.session import SessionManager
from mft_sts.strategy import Strategy


class Idle(Strategy):
    """Does nothing on its own — the test drives every transition."""

    name = "idle_status"


class FailsOnReady(Strategy):
    name = "fails_on_ready"

    async def on_ready(self) -> None:
        self.fail("no tradable account attached")


@pytest.fixture
async def broker() -> Broker:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test"),
        redis_client=redis,
    )
    await client.connect()
    yield client
    await client.close()
    await redis.aclose()


def _manager(broker: Broker, strategy: type[Strategy]) -> SessionManager:
    register(strategy)
    return SessionManager(
        broker, heartbeat_interval=0.1, strategy_factory=lambda name: strategy()
    )


async def _events(broker: Broker) -> list[dict]:
    """Every status event so far, from the replay buffer."""
    raw = await broker.fetch_log_buffer(Topics.status_sts())
    return [json.loads(line) for line in raw]


async def _wait_for(broker: Broker, count: int) -> list[dict]:
    for _ in range(100):
        events = await _events(broker)
        if len(events) >= count:
            return events
        await asyncio.sleep(0.02)
    raise AssertionError(
        f"expected {count} status events, saw {len(await _events(broker))}"
    )


@pytest.mark.asyncio
async def test_the_lifecycle_is_announced_as_snapshots(broker: Broker) -> None:
    manager = _manager(broker, Idle)
    await manager.create_session(
        StsCreateSessionRequest(
            session_id="st-1", created_by=9, strategy="idle_status"
        )
    )
    await manager.pause("st-1")
    await manager.resume("st-1")
    await manager.stop_session("st-1")

    events = await _wait_for(broker, 4)
    assert [e["type"] for e in events] == [STS_SESSION_STATUS] * 4
    assert [
        (e["payload"]["status"], e["payload"]["paused"]) for e in events
    ] == [
        ("live", False),
        ("live", True),
        ("live", False),
        ("done", False),
    ]
    # A snapshot names its session and strategy, so a consumer never has to
    # remember what an earlier event said.
    for e in events:
        assert e["payload"]["session_id"] == "st-1"
        assert e["payload"]["strategy"] == "idle_status"
        assert e["payload"]["created_by"] == 9
        assert e["session_id"] == "st-1"


@pytest.mark.asyncio
async def test_a_failure_is_announced_with_its_reason(broker: Broker) -> None:
    manager = _manager(broker, FailsOnReady)
    await manager.create_session(
        StsCreateSessionRequest(
            session_id="st-2", created_by=1, strategy="fails_on_ready"
        )
    )
    events = await _wait_for(broker, 1)

    # The strategy failed inside on_ready, so it never was live: one event.
    assert len(events) == 1
    payload = events[0]["payload"]
    assert payload["status"] == "failed"
    assert payload["reason"] == "no tradable account attached"
    assert payload["finished_at"] is not None


@pytest.mark.asyncio
async def test_a_live_session_carries_no_reason_or_finish_time(
    broker: Broker,
) -> None:
    manager = _manager(broker, Idle)
    await manager.create_session(
        StsCreateSessionRequest(
            session_id="st-3", created_by=1, strategy="idle_status"
        )
    )
    payload = (await _wait_for(broker, 1))[0]["payload"]

    assert payload["status"] == "live"
    assert payload["reason"] is None
    assert payload["finished_at"] is None
    await manager.close_all()


@pytest.mark.asyncio
async def test_subscribers_get_the_same_events_live(broker: Broker) -> None:
    """The buffer is a replay of the fan-out, not a substitute for it."""
    manager = _manager(broker, Idle)
    seen: list[dict] = []
    stop = asyncio.Event()

    async def listen() -> None:
        async for env in broker.subscribe(Topics.status_sts(), stop=stop):
            seen.append(env.payload)
            if len(seen) >= 2:
                stop.set()

    task = asyncio.create_task(listen())
    await asyncio.sleep(0.1)

    await manager.create_session(
        StsCreateSessionRequest(
            session_id="st-4", created_by=1, strategy="idle_status"
        )
    )
    await manager.stop_session("st-4")

    try:
        await asyncio.wait_for(task, timeout=3)
    except TimeoutError:
        stop.set()
        raise
    assert [p["status"] for p in seen] == ["live", "done"]


@pytest.mark.asyncio
async def test_a_broken_channel_does_not_take_the_session_down(
    broker: Broker,
) -> None:
    """Status is a notification, not a dependency of running strategies."""
    manager = _manager(broker, Idle)

    original = broker.publish_log

    async def exploding_publish_log(topic, envelope, **kwargs):
        # Only the status channel breaks — session logs share this method and
        # breaking those would fail the session for an unrelated reason.
        if topic == Topics.status_sts():
            raise RuntimeError("redis gone")
        return await original(topic, envelope, **kwargs)

    broker.publish_log = exploding_publish_log  # type: ignore[method-assign]
    result = await manager.create_session(
        StsCreateSessionRequest(
            session_id="st-5", created_by=1, strategy="idle_status"
        )
    )

    assert result.session_id == "st-5"
    assert manager.get("st-5") is not None
    await manager.close_all()
