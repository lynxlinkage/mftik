from __future__ import annotations

import asyncio

import fakeredis.aioredis
import pytest
from mft.broker import Broker, BrokerConfig
from mft.protocol import (
    TD_ERROR,
    TD_HEALTH,
    HealthCheck,
    HealthCheckEnvelope,
    HealthStatus,
    RpcError,
    Topics,
)
from mft_td.rpc import dispatch


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
async def test_td_health_reply(broker: Broker) -> None:
    stop = asyncio.Event()

    async def server() -> None:
        async for req in broker.serve(Topics.TD, stop=stop):
            await dispatch(req)
            break
        stop.set()

    task = asyncio.create_task(server())
    await asyncio.sleep(0.05)

    reply = await broker.request(
        Topics.TD,
        HealthCheckEnvelope.wrap(
            HealthCheck(),
            type=TD_HEALTH,
            source="api",
        ),
        timeout=2,
    )
    await task

    assert reply.type == TD_HEALTH
    assert reply.source == "td"
    status = HealthStatus.model_validate(reply.payload)
    assert status.status == "ok"
    assert status.service == "td"


@pytest.mark.asyncio
async def test_td_unknown_type_error(broker: Broker) -> None:
    stop = asyncio.Event()

    async def server() -> None:
        async for req in broker.serve(Topics.TD, stop=stop):
            await dispatch(req)
            break
        stop.set()

    task = asyncio.create_task(server())
    await asyncio.sleep(0.05)

    reply = await broker.request(
        Topics.TD,
        HealthCheckEnvelope.wrap(
            HealthCheck(note="nope"),
            type="td.not_a_method",
            source="api",
        ),
        timeout=2,
    )
    await task

    assert reply.type == TD_ERROR
    err = RpcError.model_validate(reply.payload)
    assert err.code == "unknown_type"
    assert "td.not_a_method" in err.message
