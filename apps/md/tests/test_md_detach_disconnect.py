"""Detaching must not be held up by the venue connection it is closing.

MD serves its control plane one request at a time. A venue socket takes
seconds to close — the server may never answer the close frame, and there is
one connection per traffic class to get through — so a detach that disconnects
inline holds every other attach, list and detach behind it.
"""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal

import fakeredis.aioredis
import pytest
from mftik.broker import Broker, BrokerConfig
from mftik.exchange import PaperExchange
from mftik.protocol import (
    STS_LEASE_HEARTBEAT,
    Envelope,
    LeaseHeartbeat,
    MdAttachRequest,
    Topics,
)
from mftik_md.session import PaperPublicFactory, SessionManager
from mftik_md.session.venue import VenueSession

FEED = "orderbook.Paper_Spot_BTCUSDT"

#: Long enough that an inline disconnect could not hide inside timing noise.
SLOW_CLOSE_S = 3.0


@pytest.fixture
async def broker() -> Broker:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test-md-detach"),
        redis_client=redis,
    )
    await client.connect()
    yield client
    await client.close()
    await redis.aclose()


@pytest.fixture
async def paper() -> PaperExchange:
    async with PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")},
        tick_interval=0.05,
        seed=1,
        volatility_bps=0,
    ) as ex:
        yield ex


async def _heartbeat(broker: Broker, session_id: str, stop: asyncio.Event) -> None:
    token = 0
    topic = Topics.sts_md_session(session_id)
    while not stop.is_set():
        token += 1
        await broker.publish(
            topic,
            Envelope[LeaseHeartbeat].wrap(
                LeaseHeartbeat(session_id=session_id, token=token),
                type=STS_LEASE_HEARTBEAT,
                source="sts",
                session_id=session_id,
            ),
        )
        try:
            await asyncio.wait_for(stop.wait(), timeout=0.1)
        except TimeoutError:
            continue


async def _attached(
    broker: Broker, paper: PaperExchange, session_id: str
) -> tuple[SessionManager, asyncio.Event, asyncio.Task]:
    sessions = SessionManager(
        PaperPublicFactory(broker, paper), broker, lease_grace=5.0
    )
    stop = asyncio.Event()
    task = asyncio.create_task(_heartbeat(broker, session_id, stop))
    await sessions.attach(
        MdAttachRequest(
            session_id=session_id,
            created_by=1,
            subscriptions=[FEED],
            timeout=5.0,
        )
    )
    return sessions, stop, task


def _make_venue_slow_to_close(sessions: SessionManager) -> asyncio.Event:
    """Stand in for a venue whose close frame is never answered."""
    closed = asyncio.Event()
    venue = next(iter(sessions._venues.values()))  # noqa: SLF001
    real_stop = venue.stop

    async def slow_stop() -> None:
        await asyncio.sleep(SLOW_CLOSE_S)
        await real_stop()
        closed.set()

    venue.stop = slow_stop  # type: ignore[method-assign]
    return closed


@pytest.mark.asyncio
async def test_detach_returns_before_the_venue_is_disconnected(
    broker: Broker, paper: PaperExchange
) -> None:
    sessions, stop, task = await _attached(broker, paper, "md-slow-1")
    closed = _make_venue_slow_to_close(sessions)

    started = time.monotonic()
    await sessions.detach(session_id="md-slow-1", reason="sts_stop")
    elapsed = time.monotonic() - started

    assert elapsed < SLOW_CLOSE_S / 2, f"detach took {elapsed:.1f}s"
    # The session is already gone as far as anything else can tell...
    assert sessions._venues == {}  # noqa: SLF001
    assert not closed.is_set()
    # ...and the disconnect really does finish, on its own.
    await asyncio.wait_for(closed.wait(), timeout=SLOW_CLOSE_S + 2)

    stop.set()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_a_closing_venue_does_not_block_the_next_attach(
    broker: Broker, paper: PaperExchange
) -> None:
    """The point of the change: one detach must not hold the control plane."""
    sessions, stop, task = await _attached(broker, paper, "md-slow-2")
    _make_venue_slow_to_close(sessions)
    await sessions.detach(session_id="md-slow-2", reason="sts_stop")

    stop2 = asyncio.Event()
    task2 = asyncio.create_task(_heartbeat(broker, "md-slow-3", stop2))
    started = time.monotonic()
    result = await sessions.attach(
        MdAttachRequest(
            session_id="md-slow-3",
            created_by=1,
            subscriptions=[FEED],
            timeout=5.0,
        )
    )
    elapsed = time.monotonic() - started

    assert result.subscriptions == [FEED]
    assert elapsed < SLOW_CLOSE_S / 2, f"attach waited {elapsed:.1f}s"
    # A fresh venue, not the one still closing.
    assert isinstance(
        next(iter(sessions._venues.values())), VenueSession  # noqa: SLF001
    )

    for event, running in ((stop, task), (stop2, task2)):
        event.set()
        running.cancel()
    await asyncio.gather(task, task2, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_shutdown_waits_for_the_disconnects_it_started(
    broker: Broker, paper: PaperExchange
) -> None:
    """The one moment something is waiting: the process is about to end."""
    sessions, stop, task = await _attached(broker, paper, "md-slow-4")
    closed = _make_venue_slow_to_close(sessions)

    await sessions.detach(session_id="md-slow-4", reason="sts_stop")
    assert not closed.is_set()

    await sessions.close_all()
    assert closed.is_set(), "close_all returned with a socket still closing"

    stop.set()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
