"""What a role actually changes on the wire, not just what it returns.

``control_subjects`` is unit-tested next door. This drives the real ``run_rpc``
against a real broker, because the claim that matters is about a request a
gated instance must not take — and "did not answer" alone cannot show that,
since nobody answering looks identical.

So the gated instance is run *alongside an active peer*. An anycast request
that the peer answers is one the gated instance did not swallow, which is the
property that makes a warming MD safe to run next to the one it is replacing.

This used to be asserted by looking for the unanswered request still sitting in
a Redis list. That was Redis' own behaviour rather than a promise of the broker,
and it is no longer one: ``request`` fails fast when nobody is serving, and
``post`` is what waits. See ``docs/Broker.md``.
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

#: An active instance to run beside the gated one, so the anycast pool has
#: somebody in it. Its own name is a subject the gated instance never serves,
#: which is what makes "who answered" readable from which subject did.
PEER = "md-jp-2"


@pytest.fixture
async def broker():
    async with a_broker("md-role") as client:
        yield client


def _probe() -> HealthCheckEnvelope:
    return HealthCheckEnvelope.wrap(
        HealthCheck(), type=MD_HEALTH, source="api"
    )


async def _serving(broker: Broker, role: Role, *, instance: str = INSTANCE):
    """Run ``run_rpc`` for every subject ``role`` grants, and stop cleanly."""
    stop = asyncio.Event()
    tasks = [
        asyncio.create_task(
            md_app.run_rpc(broker, None, stop, subject=subject)  # type: ignore[arg-type]
        )
        for subject in control_subjects("md", instance, role)
    ]
    await asyncio.sleep(0.3)
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
async def test_named_answers_its_own_name_and_leaves_the_pool_alone(
    broker: Broker,
) -> None:
    """The whole reason a named instance is safe beside an active one.

    It answers attaches addressed to it and takes nothing from the shared pool.
    The peer answering the anycast request is what says so — a named instance
    that had subscribed to it could have taken that one instead, and a test that
    only checked for silence would have read the two the same way.
    """
    stop, tasks = await _serving(broker, Role.NAMED)
    peer_stop, peer_tasks = await _serving(broker, Role.ACTIVE, instance=PEER)
    try:
        assert await _asks(broker, Topics.md(INSTANCE)) is True
        assert await _asks(broker, Topics.MD) is True
        assert await _asks(broker, Topics.md(PEER)) is True
    finally:
        await _stop(peer_stop, peer_tasks)
        await _stop(stop, tasks)


@pytest.mark.asyncio
async def test_standby_answers_neither(broker: Broker) -> None:
    """Including its own name.

    A warming MD and the one it is replacing are both ``md-jp-1``, so gating
    only the shared pool would still let it take a named attach for feeds it has
    not opened. The peer is here for the same reason as above: the anycast pool
    still works, so the standby's silence is gating rather than an empty bus.
    """
    stop, tasks = await _serving(broker, Role.STANDBY)
    peer_stop, peer_tasks = await _serving(broker, Role.ACTIVE, instance=PEER)
    try:
        assert tasks == [], "standby builds no serve loop at all"
        assert await _asks(broker, Topics.md(INSTANCE), timeout=0.5) is False
        assert await _asks(broker, Topics.MD) is True
    finally:
        await _stop(peer_stop, peer_tasks)
        await _stop(stop, tasks)
