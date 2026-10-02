"""A second process of the same instance name is a boot-probe refusal.

The KV claim is gone, and RM-06 took the in-process account map with it. What
survives is the process-level answer: a second TD calling itself ``td`` is
refused at boot by ``refuse_if_serving`` (F36) — not a lock, and a same-window
dual-open is still accepted.
"""

from __future__ import annotations

import asyncio

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.health import InstanceAlreadyServing, refuse_if_serving
from mftik.protocol import (
    HealthStatus,
    HealthStatusEnvelope,
    Topics,
)


@pytest.fixture
async def broker() -> Broker:
    async with a_broker("td-own") as client:
        yield client


@pytest.mark.real_sleep(
    reason="this test calls asyncio.sleep while waiting for a real side effect"
)
@pytest.mark.asyncio
async def test_boot_probe_refuses_a_second_process_of_the_same_instance(
    broker: Broker,
) -> None:
    stop = asyncio.Event()

    async def serve() -> None:
        async for req in broker.serve(Topics.td("td"), stop=stop):
            await req.reply(
                HealthStatusEnvelope.wrap(
                    HealthStatus(status="ok", service="td", instance="td"),
                    type="td.health",
                    source="td",
                )
            )

    task = asyncio.create_task(serve())
    await asyncio.sleep(0.05)
    try:
        with pytest.raises(InstanceAlreadyServing) as refused:
            await refuse_if_serving(
                broker, domain="td", instance="td", timeout=1.0
            )
        assert refused.value.subject == Topics.td("td")
        assert "td" in str(refused.value)
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
