"""What a role actually changes on the wire, not just what it returns.

``control_subjects`` is unit-tested next door. This drives the real
``run_rpc`` against a real (fake) Redis, because the claim that matters is
about a request nobody took: it has to still be *there*, waiting for a peer,
rather than answered or dropped.
"""

from __future__ import annotations

import asyncio

import pytest
from broker_harness import a_broker
from mftik import Role, control_subjects
from mftik.broker import Broker
from mftik.broker.errors import RequestTimeoutError
from mftik.protocol import (
    MD_HEALTH,
    HealthCheck,
    HealthCheckEnvelope,
    HealthStatus,
    Topics,
)
from mftik_md import app as md_app

INSTANCE = "md-jp-1"


@pytest.fixture
async def broker():
    async with a_broker("md-role") as client:
        yield client


def _probe() -> HealthCheckEnvelope:
    return HealthCheckEnvelope.wrap(
        HealthCheck(), type=MD_HEALTH, source="api"
    )


async def _serving(broker: Broker, role: Role):
    """Run ``run_rpc`` for every subject ``role`` grants, and stop cleanly."""
    stop = asyncio.Event()
    tasks = [
        asyncio.create_task(
            md_app.run_rpc(broker, None, stop, subject=subject)  # type: ignore[arg-type]
        )
        for subject in control_subjects("md", INSTANCE, role)
    ]
    await asyncio.sleep(0.05)
    return stop, tasks


async def _stop(stop: asyncio.Event, tasks: list[asyncio.Task]) -> None:
    stop.set()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def _asks(broker: Broker, subject: str, *, timeout: float = 1.0) -> bool:
    """Whether ``subject`` answered a health probe inside ``timeout``."""
    try:
        reply = await broker.request(subject, _probe(), timeout=timeout)
    except RequestTimeoutError:
        return False
    return HealthStatus.model_validate(reply.payload).status == "ok"


async def _queued(broker: Broker, subject: str) -> int:
    return int(
        await broker.redis.llen(f"{broker.config.key_prefix}:rpc:{subject}")
    )


@pytest.mark.asyncio
async def test_active_answers_its_own_name_and_the_shared_pool(
    broker: Broker,
) -> None:
    stop, tasks = await _serving(broker, Role.ACTIVE)
    try:
        assert await _asks(broker, Topics.md(INSTANCE)) is True
        assert await _asks(broker, Topics.MD) is True
    finally:
        await _stop(stop, tasks)


@pytest.mark.asyncio
async def test_named_answers_its_own_name_only(broker: Broker) -> None:
    """And the anycast request is *waiting*, not lost.

    That distinction is the whole reason a named instance is safe to run
    alongside an active one: work nobody addressed stays in the list for the
    peer that will take it.
    """
    stop, tasks = await _serving(broker, Role.NAMED)
    try:
        assert await _asks(broker, Topics.md(INSTANCE)) is True
        assert await _asks(broker, Topics.MD, timeout=0.2) is False
        assert await _queued(broker, Topics.MD) == 1
    finally:
        await _stop(stop, tasks)


@pytest.mark.asyncio
async def test_standby_answers_neither(broker: Broker) -> None:
    """Including its own name.

    A warming MD and the one it is replacing are both ``md-jp-1``, so gating
    only the shared pool would still let it take a named attach for feeds it
    has not opened.
    """
    stop, tasks = await _serving(broker, Role.STANDBY)
    try:
        assert tasks == [], "standby builds no serve loop at all"
        assert await _asks(broker, Topics.md(INSTANCE), timeout=0.2) is False
        assert await _asks(broker, Topics.MD, timeout=0.2) is False
        assert await _queued(broker, Topics.md(INSTANCE)) == 1
    finally:
        await _stop(stop, tasks)
