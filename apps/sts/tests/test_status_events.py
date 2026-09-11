"""Status fan-out on ``status.sts`` — what the UI listens to.

Each event is a full snapshot rather than a delta. A late socket reads the
current rows (REST), then this subject — the broker no longer keeps a ring.
"""

from __future__ import annotations

import asyncio

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.protocol import (
    STS_SESSION_STATUS,
    StsCreateSessionRequest,
    Topics,
)
from mftik.strategy import Strategy
from mftik_sts.impl import register
from mftik_sts.session import SessionManager


class Idle(Strategy):
    """Does nothing on its own — the test drives every transition."""

    name = "idle_status"


class FailsOnReady(Strategy):
    name = "fails_on_ready"

    async def on_ready(self) -> None:
        self.fail("no tradable account attached")


@pytest.fixture
async def broker() -> Broker:
    async with a_broker() as client:
        yield client


def _manager(broker: Broker, strategy: type[Strategy]) -> SessionManager:
    register(strategy)
    return SessionManager(
        broker, heartbeat_interval=0.1, strategy_factory=lambda name: strategy()
    )


async def _listen(broker: Broker, topic: str, count: int) -> tuple[
    list, asyncio.Event, asyncio.Task
]:
    seen: list = []
    stop = asyncio.Event()

    async def listen() -> None:
        async for env in broker.subscribe(topic, stop=stop):
            seen.append(env)
            if len(seen) >= count:
                stop.set()

    task = asyncio.create_task(listen())
    await asyncio.sleep(0.05)
    return seen, stop, task


@pytest.mark.asyncio
async def test_the_lifecycle_is_announced_as_snapshots(broker: Broker) -> None:
    manager = _manager(broker, Idle)
    status, status_stop, status_task = await _listen(
        broker, Topics.status_sts(), 2
    )
    logs, log_stop, log_task = await _listen(broker, Topics.log_sts("st-1"), 1)

    await manager.create_session(
        StsCreateSessionRequest(
            session_id="st-1",
            created_by=9,
            strategy="idle_status",
            type="private::Tiny",
        )
    )
    await manager.stop_session("st-1")

    await asyncio.wait_for(status_task, timeout=3)
    events = [e.model_dump() for e in status]
    assert [e["type"] for e in events] == [STS_SESSION_STATUS] * 2
    assert [e["payload"]["status"] for e in events] == ["live", "done"]
    for e in events:
        assert "paused" not in e["payload"]
        assert e["payload"]["session_id"] == "st-1"
        assert e["payload"]["strategy"] == "idle_status"
        assert e["payload"]["type"] == "private::Tiny"
        assert e["payload"]["created_by"] == 9
        assert e["session_id"] == "st-1"

    await asyncio.wait_for(log_task, timeout=3)
    started = [
        e for e in logs if "session started" in (e.payload or {}).get("message", "")
    ]
    assert started
    assert started[0].payload["type"] == "private::Tiny"
    log_stop.set()
    status_stop.set()


@pytest.mark.asyncio
async def test_a_failure_is_announced_with_its_reason(broker: Broker) -> None:
    manager = _manager(broker, FailsOnReady)
    seen, stop, task = await _listen(broker, Topics.status_sts(), 1)

    await manager.create_session(
        StsCreateSessionRequest(
            session_id="st-2",
            created_by=1,
            strategy="fails_on_ready",
            type="private::Tiny",
        )
    )
    await asyncio.wait_for(task, timeout=3)

    assert len(seen) == 1
    payload = seen[0].payload
    assert payload["status"] == "failed"
    assert payload["reason"] == "no tradable account attached"
    assert payload["type"] == "private::Tiny"
    assert payload["finished_at"] is not None
    stop.set()


@pytest.mark.asyncio
async def test_a_live_session_carries_no_reason_or_finish_time(
    broker: Broker,
) -> None:
    manager = _manager(broker, Idle)
    seen, stop, task = await _listen(broker, Topics.status_sts(), 1)
    await manager.create_session(
        StsCreateSessionRequest(
            session_id="st-3", created_by=1, strategy="idle_status"
        )
    )
    await asyncio.wait_for(task, timeout=3)
    payload = seen[0].payload

    assert payload["status"] == "live"
    assert payload["reason"] is None
    assert payload["finished_at"] is None
    stop.set()
    await manager.close_all()


@pytest.mark.asyncio
async def test_subscribers_get_the_same_events_live(broker: Broker) -> None:
    manager = _manager(broker, Idle)
    seen, stop, task = await _listen(broker, Topics.status_sts(), 2)

    await manager.create_session(
        StsCreateSessionRequest(
            session_id="st-4", created_by=1, strategy="idle_status"
        )
    )
    await manager.stop_session("st-4")

    await asyncio.wait_for(task, timeout=3)
    assert [p.payload["status"] for p in seen] == ["live", "done"]
    stop.set()


@pytest.mark.asyncio
async def test_a_broken_channel_does_not_take_the_session_down(
    broker: Broker,
) -> None:
    """Status is a notification, not a dependency of running strategies."""
    manager = _manager(broker, Idle)

    original = broker.publish

    async def exploding_publish(topic, envelope):
        if topic == Topics.status_sts():
            raise RuntimeError("nats gone")
        return await original(topic, envelope)

    broker.publish = exploding_publish  # type: ignore[method-assign]
    result = await manager.create_session(
        StsCreateSessionRequest(
            session_id="st-5", created_by=1, strategy="idle_status"
        )
    )

    assert result.session_id == "st-5"
    assert manager.get("st-5") is not None
    await manager.close_all()
