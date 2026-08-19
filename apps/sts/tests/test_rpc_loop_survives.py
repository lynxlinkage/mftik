"""STS's RPC loop must come back from anything short of shutdown.

On 2026-08-18 it did not. One ``BLPOP`` raised, ``run_rpc``'s ``try`` covered
the dispatch inside the loop but not the iteration itself, and the coroutine
returned. STS went on running three live sessions for seven hours with nothing
able to list, pause or stop one of them, and the only sign was that the health
probe every dashboard polls started timing out.

``Broker.serve`` now survives the failures it can name; this is the layer
behind that, for the ones it cannot.
"""

from __future__ import annotations

import asyncio
from typing import Any

import fakeredis.aioredis
import pytest
from mftik.broker import Broker, BrokerConfig
from mftik.protocol import (
    STS_HEALTH,
    HealthCheck,
    HealthCheckEnvelope,
    HealthStatus,
    Topics,
)
from mftik_sts import app as sts_app


@pytest.fixture
async def broker() -> Broker:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test-stsrpc"),
        redis_client=redis,
    )
    await client.connect()
    yield client
    await client.close()
    await redis.aclose()


async def _health(broker: Broker) -> HealthStatus:
    reply = await broker.request(
        Topics.STS,
        HealthCheckEnvelope.wrap(HealthCheck(), type=STS_HEALTH, source="test"),
        timeout=5,
    )
    return HealthStatus.model_validate(reply.payload)


@pytest.mark.asyncio
async def test_the_loop_rebuilds_itself_after_an_unexpected_failure(
    broker: Broker, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sts_app, "RPC_RESTART_DELAY_SECONDS", 0.0)

    real_serve = broker.serve
    failures: list[str] = []

    def flaky(*args: Any, **kwargs: Any) -> Any:
        if not failures:
            failures.append("boom")
            raise RuntimeError("something serve does not handle")
        return real_serve(*args, **kwargs)

    monkeypatch.setattr(broker, "serve", flaky)

    stop = asyncio.Event()
    rpc = asyncio.create_task(run_rpc_under_test(broker, stop))
    try:
        status = await asyncio.wait_for(_health(broker), timeout=5)
    finally:
        stop.set()
        await asyncio.wait_for(rpc, timeout=5)

    assert failures == ["boom"]
    assert status.status == "ok"


async def run_rpc_under_test(broker: Broker, stop: asyncio.Event) -> None:
    # ``sessions`` is only ever passed through to the handlers, and health —
    # the one call an operator makes to ask whether the subject is being
    # served at all — is the handler that does not touch it.
    await sts_app.run_rpc(broker, None, stop)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_shutdown_still_ends_the_loop(broker: Broker) -> None:
    """A loop that will not stop is the other way to lose a restart."""
    stop = asyncio.Event()
    rpc = asyncio.create_task(run_rpc_under_test(broker, stop))
    await asyncio.sleep(0.05)

    stop.set()
    await asyncio.wait_for(rpc, timeout=5)

    assert rpc.done() and rpc.exception() is None
