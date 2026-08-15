from __future__ import annotations

import asyncio

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


class ExitSoonStrategy(Strategy):
    name = "exit_soon"

    def __init__(self) -> None:
        super().__init__()
        self.events: list[str] = []
        self.exit_reason: str | None = None

    async def on_start(self) -> None:
        self.events.append("on_start")

    async def on_ready(self) -> None:
        self.events.append("on_ready")
        self.exit("natural_end")

    async def on_stop(self) -> None:
        self.events.append("on_stop")


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
    assert resolve("NoopStrategy").name == "noop"
    await manager.close_all()


@pytest.mark.asyncio
async def test_strategy_exit_triggers_on_stop(broker: Broker) -> None:
    register(ExitSoonStrategy)
    instances: list[ExitSoonStrategy] = []

    def factory(name: str | None) -> Strategy:
        s = ExitSoonStrategy()
        instances.append(s)
        return s

    manager = SessionManager(
        broker, heartbeat_interval=0.1, strategy_factory=factory
    )
    await manager.create_session(
        StsCreateSessionRequest(
            session_id="exit-1",
            created_by=1,
            strategy="exit_soon",
        )
    )
    strat = instances[0]
    for _ in range(50):
        if manager.get("exit-1") is None and "on_stop" in strat.events:
            break
        await asyncio.sleep(0.02)
    assert manager.get("exit-1") is None
    assert strat.events == ["on_start", "on_ready", "on_stop"]


@pytest.mark.asyncio
async def test_noop_pause_cancels_and_resume_rearms(broker: Broker) -> None:
    manager = SessionManager(broker, heartbeat_interval=0.1)
    await manager.create_session(
        StsCreateSessionRequest(
            session_id="noop-pause",
            created_by=1,
            strategy="noop",
            td=[1],
        )
    )
    session = manager.get("noop-pause")
    assert session is not None
    strat = session.strategy
    assert strat._tick_token is not None  # type: ignore[attr-defined]
    assert strat._tick_token.active  # type: ignore[attr-defined]

    await manager.pause("noop-pause")
    assert not strat._tick_token.active  # type: ignore[attr-defined]

    await manager.resume("noop-pause")
    assert strat._tick_token.active  # type: ignore[attr-defined]

    await manager.close_all()


@pytest.mark.asyncio
async def test_strategy_can_reach_the_symbol_plane(broker: Broker) -> None:
    """TD does not validate filters, so the strategy needs them to round."""
    register(RecordingStrategy)
    manager = SessionManager(broker, heartbeat_interval=0.1)
    result = await manager.create_session(
        StsCreateSessionRequest(
            session_id="sym-1", created_by=1, strategy="recording"
        )
    )
    session = manager.get(result.session_id)
    assert session is not None

    plane = session.strategy.symbols
    # A recording view over the session's client, not the client itself: what
    # the plane answers decides how an order is rounded, so the session event
    # log has to see it (see ``mft_sts.symbols``).
    assert plane.client is session.symbols
    assert hasattr(plane, "get")
    assert hasattr(plane, "filter")

    await manager.close(result.session_id)


def test_unbound_strategy_has_no_plane() -> None:
    assert RecordingStrategy().symbols is None
