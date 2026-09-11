"""A session notices a TD that stopped acknowledging.

Same fuse as MD: three missed intervals after the first ack. A quiet TD
stops the strategy — there is no book in a cache to keep trading against.
"""

from __future__ import annotations

import asyncio

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.protocol import (
    TD_LEASE_ACK,
    Envelope,
    LeaseAck,
    Topics,
)
from mftik.strategy import Strategy
from mftik_sts.session.session import StsSession

GRACE = 0.25
SESSION = "watchdog-td"
API_ID = 7


class Quiet(Strategy):
    name = "quiet"


@pytest.fixture
async def broker() -> Broker:
    async with a_broker("sts-td-watchdog") as client:
        yield client


def _session(broker: Broker, **over) -> StsSession:
    kwargs = {
        "session_id": SESSION,
        "broker": broker,
        "created_by": 1,
        "strategy": Quiet(),
        "td_api_ids": [API_ID],
        "heartbeat_interval": 0.05,
        "md_ack_grace": GRACE,
    }
    kwargs.update(over)
    return StsSession(**kwargs)


async def _ack(broker: Broker, api_id: int, token: int = 1) -> None:
    await broker.publish(
        Topics.td_session(api_id, SESSION),
        Envelope[LeaseAck].wrap(
            LeaseAck(api_id=api_id, session_id=SESSION, token=token),
            type=TD_LEASE_ACK,
            source="td",
            session_id=SESSION,
        ),
    )


async def _arm(
    session: StsSession, broker: Broker, api_id: int, timeout: float = 3.0
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    token = 0
    while asyncio.get_running_loop().time() < deadline:
        token += 1
        await _ack(broker, api_id, token)
        if api_id in session._td_acks:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"session never heard an acknowledgement from api_id={api_id}")


async def _acking(broker: Broker, api_ids: list[int], stop: asyncio.Event) -> None:
    token = 0
    while not stop.is_set():
        token += 1
        for api_id in api_ids:
            await _ack(broker, api_id, token)
        await asyncio.sleep(0.05)


async def _exit_reason(session: StsSession, timeout: float = 3.0) -> str | None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if session.exit_reason is not None:
            return session.exit_reason
        await asyncio.sleep(0.02)
    return None


@pytest.mark.asyncio
async def test_a_session_that_never_heard_a_td_is_not_failed(
    broker: Broker,
) -> None:
    session = _session(broker)
    await session.start()
    try:
        await asyncio.sleep(GRACE * 3)
        assert session.exit_reason is None
    finally:
        await session.stop()


@pytest.mark.asyncio
async def test_a_session_whose_td_goes_quiet_is_failed(broker: Broker) -> None:
    session = _session(broker)
    await session.start()
    try:
        await _arm(session, broker, API_ID)

        reason = await _exit_reason(session)

        assert reason is not None, "a dead TD must not be invisible"
        assert str(API_ID) in reason
        assert session.exit_failed is True
    finally:
        await session.stop()


@pytest.mark.asyncio
async def test_a_live_td_never_trips_the_watchdog(broker: Broker) -> None:
    session = _session(broker)
    stop = asyncio.Event()
    pub = asyncio.create_task(_acking(broker, [API_ID], stop))
    await session.start()
    try:
        await asyncio.sleep(GRACE * 4)
        assert session.exit_reason is None
    finally:
        stop.set()
        await pub
        await session.stop()
