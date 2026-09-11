"""Boot ``probe`` refuses a second process of the same instance name."""

from __future__ import annotations

import asyncio

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.health import InstanceAlreadyServing, refuse_if_serving
from mftik.protocol import HealthStatus, HealthStatusEnvelope, Topics


@pytest.fixture
async def broker() -> Broker:
    async with a_broker("test-probe") as client:
        yield client


@pytest.mark.asyncio
async def test_a_quiet_subject_is_not_already_serving(broker: Broker) -> None:
    await refuse_if_serving(broker, domain="sts", instance="sts-tw", timeout=0.3)


@pytest.mark.asyncio
async def test_a_responder_is_named_and_refused(broker: Broker) -> None:
    stop = asyncio.Event()

    async def serve() -> None:
        async for req in broker.serve(Topics.sts("sts-tw"), stop=stop):
            await req.reply(
                req.envelope.model_copy(
                    update={"source": "sts-tw", "type": "sts.health"}
                )
            )
            break

    task = asyncio.create_task(serve())
    await asyncio.sleep(0.05)
    try:
        with pytest.raises(InstanceAlreadyServing) as refused:
            await refuse_if_serving(
                broker, domain="sts", instance="sts-tw", timeout=1.0
            )
        assert refused.value.subject == Topics.sts("sts-tw")
        assert "sts-tw" in str(refused.value)
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_health_on_the_control_subject_counts_as_a_responder(
    broker: Broker,
) -> None:
    """Planes already answer ``{domain}.health`` on their instance subject."""
    stop = asyncio.Event()

    async def serve() -> None:
        async for req in broker.serve(Topics.td("td-jp-1"), stop=stop):
            await req.reply(
                HealthStatusEnvelope.wrap(
                    HealthStatus(
                        status="ok", service="td", instance="td-jp-1"
                    ),
                    type="td.health",
                    source="td",
                )
            )

    task = asyncio.create_task(serve())
    await asyncio.sleep(0.05)
    try:
        with pytest.raises(InstanceAlreadyServing) as refused:
            await refuse_if_serving(
                broker, domain="td", instance="td-jp-1", timeout=1.0
            )
        assert refused.value.source == "td-jp-1"
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
