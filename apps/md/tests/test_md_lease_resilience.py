"""What an MD attach survives when the lease loop's transport fails.

Same shape as TD's, with more ways in: this loop also carries the feed
subscribe / unsubscribe messages, and those open venue sockets. A websocket
that will not come up must not take the lease down with it — the loop is
where the ACKs come from, and where the only expiry lives.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

import fakeredis.aioredis
import pytest
from mftik.broker import Broker, BrokerConfig
from mftik.exchange import PaperExchange
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import (
    MD_LEASE_ACK,
    MD_SUBSCRIBE,
    STS_LEASE_HEARTBEAT,
    Envelope,
    LeaseHeartbeat,
    MdAttachRequest,
    MdSubscribe,
    Topics,
)
from mftik_md.session import PaperPublicFactory, SessionManager

FEED = Topics.md_feed("orderbook", UniversalTicker.parse("Paper_Spot_BTCUSDT"))
SID = "md-lease-1"


@pytest.fixture
async def broker() -> Broker:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test-md-lease"),
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
        seed=2,
        volatility_bps=0,
    ) as ex:
        yield ex


@pytest.fixture
def sessions(broker: Broker, paper: PaperExchange) -> SessionManager:
    return SessionManager(
        PaperPublicFactory(broker, paper), broker, lease_grace=2.0
    )


async def _lease_publisher(
    broker: Broker, session_id: str, stop: asyncio.Event
) -> None:
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
            await asyncio.wait_for(stop.wait(), timeout=0.05)
        except TimeoutError:
            continue


async def _until(predicate, timeout: float = 5.0) -> bool:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.02)
    return predicate()


@pytest.mark.asyncio
async def test_a_failed_subscription_is_retried_not_fatal(
    broker: Broker, sessions: SessionManager
) -> None:
    sts_topic = Topics.sts_md_session(SID)
    original = broker.subscribe
    broken: list[str] = []

    def flaky_subscribe(topics, *, stop=None):
        if topics == sts_topic and not broken:
            broken.append(topics)

            async def failing():
                raise ConnectionError("redis gone")
                yield  # pragma: no cover

            return failing()
        return original(topics, stop=stop)

    broker.subscribe = flaky_subscribe  # type: ignore[method-assign]
    stop = asyncio.Event()
    pub = asyncio.create_task(_lease_publisher(broker, SID, stop))
    try:
        result = await sessions.attach(
            MdAttachRequest(
                session_id=SID,
                created_by=1,
                subscriptions=[FEED],
                timeout=5.0,
            )
        )
        assert broken, "the test never exercised a failing subscription"
        assert result.subscriptions == [FEED]
    finally:
        broker.subscribe = original  # type: ignore[method-assign]
        stop.set()
        await asyncio.gather(pub, return_exceptions=True)
        await sessions.close_all()


@pytest.mark.asyncio
async def test_a_feed_that_will_not_open_does_not_end_the_lease(
    broker: Broker, sessions: SessionManager
) -> None:
    """STS asks for a feed and does not get one, which is visible. A lease
    loop that died over it would not be."""
    ack_topic = Topics.md_session(SID)
    acked: list[int] = []

    async def collect_acks(stop: asyncio.Event) -> None:
        async for env in broker.subscribe(ack_topic, stop=stop):
            if env.type == MD_LEASE_ACK:
                acked.append(1)

    stop = asyncio.Event()
    pub = asyncio.create_task(_lease_publisher(broker, SID, stop))
    collector = asyncio.create_task(collect_acks(stop))
    try:
        await sessions.attach(
            MdAttachRequest(
                session_id=SID, created_by=1, subscriptions=[], timeout=5.0
            )
        )
        acked.clear()

        await broker.publish(
            Topics.sts_md_session(SID),
            Envelope[MdSubscribe].wrap(
                MdSubscribe(session_id=SID, feed="not-a-parsable-feed"),
                type=MD_SUBSCRIBE,
                source="sts",
                session_id=SID,
            ),
        )

        assert await _until(lambda: len(acked) >= 2), (
            "ACKs stopped after a bad feed — the lease loop did not survive"
        )
        assert SID in sessions._links  # noqa: SLF001
    finally:
        stop.set()
        await asyncio.gather(pub, collector, return_exceptions=True)
        await sessions.close_all()
