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
from decimal import Decimal

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.exchange.models import BestQuote
from mftik.protocol import (
    MD_BEST_QUOTE,
    MD_LEASE_ACK,
    Envelope,
    MdLeaseAck,
    Topics,
    UntypedEnvelope,
)
from mftik.strategy import Strategy
from mftik_sts.session.session import StsSession

GRACE = 0.25
SESSION = "watchdog-sts"
FEED_A = "bestquote.Paper_Spot_BTCUSDT"
FEED_B = "bestquote.Gate_Spot_ETHUSDT"
TICKER_A = "Paper_Spot_BTCUSDT"
TICKER_B = "Gate_Spot_ETHUSDT"


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
        "md_ids": [FEED_A],
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


async def _arm(
    session: StsSession, broker: Broker, instance: str | None, timeout: float = 3.0
) -> None:
    """Acknowledge until the session has heard it, then stop.

    ``start()`` returns before its feed subscription exists: the pump is a task,
    and the subscription is made the first time that task runs. A single publish
    into that window is delivered to nobody — on either transport, against a
    real server — and every test below arms the watchdog with a single publish.

    Repeating until ``_md_acks`` shows the instance is what makes the arming a
    fact instead of a race, and stopping the moment it does is what leaves the
    grace period starting where the test thinks it does.
    """
    deadline = asyncio.get_running_loop().time() + timeout
    token = 0
    while asyncio.get_running_loop().time() < deadline:
        token += 1
        await _ack(broker, instance, token)
        if (instance or "md") in session._md_acks:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"session never heard an acknowledgement from {instance!r}")


async def _acking(
    broker: Broker, instances: list[str], stop: asyncio.Event
) -> None:
    token = 0
    while not stop.is_set():
        token += 1
        for instance in instances:
            await _ack(broker, instance, token)
        await asyncio.sleep(0.05)


async def _print(broker: Broker, ticker: str = TICKER_A) -> None:
    await broker.publish(
        Topics.md_session(SESSION),
        UntypedEnvelope.wrap(
            BestQuote(
                universal_ticker=ticker,
                bid=Decimal("100"),
                bid_qty=Decimal("1"),
                ask=Decimal("101"),
                ask_qty=Decimal("2"),
            ).model_dump(mode="json"),
            type=MD_BEST_QUOTE,
            source="md",
            session_id=SESSION,
        ),
    )


async def _printing(broker: Broker, ticker: str, stop: asyncio.Event) -> None:
    while not stop.is_set():
        await _print(broker, ticker)
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
        await _arm(session, broker, "md-jp-1")

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
        await _arm(session, broker, "md-jp-1")
        await _arm(session, broker, "md-jp-2")
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
        await _arm(session, broker, None)

        reason = await _exit_reason(session)

        assert reason is not None
        assert "md" in reason
    finally:
        await session.stop()


@pytest.mark.asyncio
async def test_a_live_print_stream_never_trips_the_watchdog(
    broker: Broker,
) -> None:
    """Ticks are the same liveness fact as a lease ack.

    Under a burst the pump may apply prints for longer than the grace
    without running the ack handler. The peer is still delivering; the
    watchdog must not call that MD death.
    """
    session = _session(broker)
    stop = asyncio.Event()
    await session.start()
    try:
        await _arm(session, broker, "md-jp-1")
        pub = asyncio.create_task(_printing(broker, TICKER_A, stop))
        await asyncio.sleep(GRACE * 4)
        assert session.exit_reason is None
    finally:
        stop.set()
        await pub
        await session.stop()


@pytest.mark.asyncio
async def test_prints_from_one_instance_do_not_keep_another_alive(
    broker: Broker,
) -> None:
    """A print refreshes only the MD that owns that feed."""
    session = _session(
        broker,
        md={"md-jp-1": [FEED_A], "md-jp-2": [FEED_B]},
    )
    stop = asyncio.Event()
    await session.start()
    try:
        await _arm(session, broker, "md-jp-1")
        await _arm(session, broker, "md-jp-2")
        pub = asyncio.create_task(_printing(broker, TICKER_B, stop))

        reason = await _exit_reason(session)

        assert reason is not None
        assert "md-jp-1" in reason
        assert "md-jp-2" not in reason, (
            "the instance still printing is not the one at fault"
        )
    finally:
        stop.set()
        await pub
        await session.stop()


@pytest.mark.asyncio
async def test_prints_do_not_arm_the_watchdog(broker: Broker) -> None:
    """Quiet books still arm on the first lease ack, not on a tick.

    A session that has only seen prints has not heard an acknowledgement.
    Arming from the tape would fail a deploy whose MD has not acked yet
    the moment the book went quiet again.
    """
    session = _session(broker)
    stop = asyncio.Event()
    pub = asyncio.create_task(_printing(broker, TICKER_A, stop))
    await session.start()
    try:
        await asyncio.sleep(GRACE * 3)
        assert session.exit_reason is None
        assert session._md_acks == {}
    finally:
        stop.set()
        await pub
        await session.stop()
