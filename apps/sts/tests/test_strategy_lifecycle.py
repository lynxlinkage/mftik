from __future__ import annotations

import fakeredis.aioredis
import pytest
from mft.broker import Broker, BrokerConfig
from mft.protocol import StsCreateSessionRequest
from mft_sts.impl import register, resolve
from mft_sts.session import SessionManager
from mft_sts.strategy import Strategy


class RecordingStrategy(Strategy):
    name = "recording"

    def __init__(self) -> None:
        super().__init__()
        self.events: list[str] = []

    async def on_start(self) -> None:
        self.events.append("on_start")

    async def on_ready(self) -> None:
        self.events.append("on_ready")

    async def on_stop(self) -> None:
        self.events.append("on_stop")

    async def on_pause(self) -> None:
        await super().on_pause()
        self.events.append("on_pause")

    async def on_resume(self) -> None:
        await super().on_resume()
        self.events.append("on_resume")


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


@pytest.mark.asyncio
async def test_session_strategy_process_control_1_1(broker: Broker) -> None:
    register(RecordingStrategy)
    instances: list[RecordingStrategy] = []

    def factory(name: str | None) -> Strategy:
        assert name == "recording"
        strat = RecordingStrategy()
        instances.append(strat)
        return strat

    manager = SessionManager(
        broker, heartbeat_interval=0.1, strategy_factory=factory
    )
    result = await manager.create_session(
        StsCreateSessionRequest(
            session_id="life-1",
            created_by=1,
            strategy="recording",
        )
    )
    assert result.strategy == "recording"
    assert result.session_id == "life-1"

    session = manager.get(result.session_id)
    assert session is not None
    assert len(instances) == 1
    strat = instances[0]
    assert session.strategy is strat
    assert strat.session is session
    assert strat.events == ["on_start", "on_ready"]

    await manager.pause(result.session_id)
    assert strat.paused
    assert strat.events == ["on_start", "on_ready", "on_pause"]

    await manager.resume(result.session_id)
    assert not strat.paused
    assert strat.events == ["on_start", "on_ready", "on_pause", "on_resume"]

    await manager.close(result.session_id)
    assert strat.events == [
        "on_start",
        "on_ready",
        "on_pause",
        "on_resume",
        "on_stop",
    ]


@pytest.mark.asyncio
async def test_unknown_strategy_rejected(broker: Broker) -> None:
    manager = SessionManager(broker, heartbeat_interval=0.1)
    with pytest.raises(KeyError, match="unknown strategy"):
        await manager.create_session(
            StsCreateSessionRequest(
                session_id="bad",
                created_by=1,
                strategy="missing",
            )
        )


@pytest.mark.asyncio
async def test_default_strategy_is_noop(broker: Broker) -> None:
    manager = SessionManager(broker, heartbeat_interval=0.1)
    result = await manager.create_session(
        StsCreateSessionRequest(
            session_id="noop-1",
            created_by=1,
            strategy="noop",
        )
    )
    assert result.strategy == "noop"
    session = manager.get(result.session_id)
    assert session is not None
    assert session.strategy.name == "noop"
    assert resolve("noop").name == "noop"
    await manager.close_all()
