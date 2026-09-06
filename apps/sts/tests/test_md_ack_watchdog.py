"""PI-8 — a session notices an MD that stopped acknowledging.

The lease was one-directional in effect. MD and TD watch this session's
heartbeat and tear down when it stops; the acknowledgements coming back were
stored in `_md_ack_token` and read nowhere — four writes, no reads, in STS or
in its tests. So an MD that died just stopped delivering, `on_best_quote`
quietly never fired again, and nothing anywhere said so.

Splitting a session's feeds across instances turns that from a gap into a
blocker. Losing *every* feed stops a strategy, because a strategy receiving
nothing does nothing. Losing *some* leaves it acting on the rest — `CrossArb`
quoting one venue against a hedge price that has stopped moving. Half a picture
is more dangerous than none.
"""

from __future__ import annotations

import asyncio

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.protocol import (
    MD_LEASE_ACK,
    Envelope,
    MdLeaseAck,
    Topics,
)
from mftik.strategy import Strategy
from mftik_sts.session.session import StsSession

GRACE = 0.25
SESSION = "watchdog-sts"


class Quiet(Strategy):
    name = "quiet"


@pytest.fixture
async def broker() -> Broker:
    async with a_broker("sts-watchdog") as client:
        yield client


def _session(broker: Broker, **over) -> StsSession:
    kwargs = {
        "session_id": SESSION,
        "broker": broker,
        "created_by": 1,
        "strategy": Quiet(),
        "md_ids": ["bestquote.Paper_Spot_BTCUSDT"],
        "heartbeat_interval": 0.05,
        "md_ack_grace": GRACE,
    }
    kwargs.update(over)
    return StsSession(**kwargs)


async def _ack(broker: Broker, instance: str | None, token: int = 1) -> None:
    await broker.publish(
        Topics.md_session(SESSION),
        Envelope[MdLeaseAck].wrap(
            MdLeaseAck(session_id=SESSION, token=token, instance=instance),
            type=MD_LEASE_ACK,
            source="md",
            session_id=SESSION,
        ),
    )


async def _acking(
    broker: Broker, instances: list[str], stop: asyncio.Event
) -> None:
    token = 0
    while not stop.is_set():
        token += 1
        for instance in instances:
            await _ack(broker, instance, token)
        await asyncio.sleep(0.05)


async def _exit_reason(session: StsSession, timeout: float = 3.0) -> str | None:
    """Wait for the session to ask to exit, or return None."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if session.exit_reason is not None:
            return session.exit_reason
        await asyncio.sleep(0.02)
    return None


@pytest.mark.asyncio
async def test_a_session_that_never_heard_an_md_is_not_failed(
    broker: Broker,
) -> None:
    """The window every deploy passes through.

    A session starts heartbeating before MD has attached to hear it. A watchdog
    armed at start rather than at the first acknowledgement would fire in that
    window on every single deploy.
    """
    session = _session(broker)
    await session.start()
    try:
        await asyncio.sleep(GRACE * 3)
        assert session.exit_reason is None
    finally:
        await session.stop()


@pytest.mark.asyncio
async def test_a_session_whose_md_goes_quiet_is_failed(broker: Broker) -> None:
    session = _session(broker)
    await session.start()
    try:
        await _ack(broker, "md-jp-1")
        await asyncio.sleep(0.05)

        reason = await _exit_reason(session)

        assert reason is not None, "a dead MD must not be invisible"
        assert "md-jp-1" in reason, "the reason names which instance went quiet"
        assert session.exit_failed is True
    finally:
        await session.stop()


@pytest.mark.asyncio
async def test_a_live_feed_never_trips_the_watchdog(broker: Broker) -> None:
    session = _session(broker)
    stop = asyncio.Event()
    pub = asyncio.create_task(_acking(broker, ["md-jp-1"], stop))
    await session.start()
    try:
        await asyncio.sleep(GRACE * 4)
        assert session.exit_reason is None
    finally:
        stop.set()
        await pub
        await session.stop()


@pytest.mark.asyncio
async def test_one_instance_going_quiet_fails_the_session(
    broker: Broker,
) -> None:
    """The case the whole ticket is for.

    ``md-jp-2`` keeps acknowledging throughout. A watchdog on a single
    timestamp would be kept fresh by it and never notice that ``md-jp-1`` —
    holding, say, the hedge venue — had stopped. The session would go on
    quoting against a price that no longer moves.
    """
    session = _session(broker)
    stop = asyncio.Event()
    await session.start()
    try:
        await _ack(broker, "md-jp-1")
        await _ack(broker, "md-jp-2")
        await asyncio.sleep(0.05)
        pub = asyncio.create_task(_acking(broker, ["md-jp-2"], stop))

        reason = await _exit_reason(session)

        assert reason is not None
        assert "md-jp-1" in reason
        assert "md-jp-2" not in reason, (
            "the instance still talking is not the one at fault"
        )
    finally:
        stop.set()
        await pub
        await session.stop()


@pytest.mark.asyncio
async def test_an_md_that_does_not_name_itself_is_still_watched(
    broker: Broker,
) -> None:
    """A rolling upgrade has an MD on each side of the field being added."""
    session = _session(broker)
    await session.start()
    try:
        await _ack(broker, None)
        await asyncio.sleep(0.05)

        reason = await _exit_reason(session)

        assert reason is not None
        assert "md" in reason
    finally:
        await session.stop()
