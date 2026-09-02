"""What an attach survives when the lease loop's transport fails.

The loop is the only thing that answers STS for a link, and — because the
watchdog lives inside it — the only thing that can expire the lease. A Redis
blip that ended it used to leave the link attached with nobody reading for
it: no ack, no expiry, no detach, and no log to say so, because nothing
awaits the task. The td row stayed ``live`` until someone noticed.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.exchange import PaperExchange
from mftik.protocol import (
    STS_DETACH,
    STS_LEASE_HEARTBEAT,
    TD_LEASE_ACK,
    Envelope,
    LeaseHeartbeat,
    StsDetach,
    TdAttachRequest,
    Topics,
)
from mftik_td.session import PaperSessionFactory, SessionManager

SID = "sess-lease-1"
API_ID = 3
SIBLING_API_ID = 4
LEASE_GRACE = 1.0


@dataclass
class FakeStore:
    rows: dict[tuple[str, int], SimpleNamespace] = field(default_factory=dict)

    async def persist_live(
        self, *, session_id: str, created_by: int, api_id: int
    ) -> SimpleNamespace:
        row = self.rows.get((session_id, api_id))
        if row is not None:
            row.status = "live"
            row.finished_at = None
            return row
        row = SimpleNamespace(
            session_id=session_id,
            created_by=created_by,
            created_at=datetime.now(UTC),
            finished_at=None,
            status="live",
            api_id=api_id,
        )
        self.rows[(session_id, api_id)] = row
        return row

    async def mark_done(
        self, *, session_id: str, api_id: int
    ) -> SimpleNamespace | None:
        row = self.rows.get((session_id, api_id))
        if row is None or row.status != "live":
            return None
        row.status = "done"
        row.finished_at = datetime.now(UTC)
        return row

    async def list_sessions(
        self,
        *,
        status: str | None = "live",
        created_by: int | None = None,
        limit: int = 100,
    ) -> list[SimpleNamespace]:
        out = [
            row
            for row in self.rows.values()
            if (status is None or row.status == status)
            and (created_by is None or row.created_by == created_by)
        ]
        return out[:limit]

    def status(self, api_id: int = API_ID, session_id: str = SID) -> str | None:
        row = self.rows.get((session_id, api_id))
        return None if row is None else row.status


@pytest.fixture
async def broker() -> Broker:
    async with a_broker("test-lease") as client:
        yield client


@pytest.fixture
async def paper() -> PaperExchange:
    async with PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")},
        tick_interval=0.05,
        seed=11,
        volatility_bps=0,
    ) as ex:
        yield ex


@pytest.fixture
def store() -> FakeStore:
    return FakeStore()


@pytest.fixture
def sessions(
    broker: Broker, paper: PaperExchange, store: FakeStore
) -> SessionManager:
    factory = PaperSessionFactory(broker, paper)
    factory.bind_api(API_ID, api_key="key-3", api_secret="sec-3")
    factory.bind_api(SIBLING_API_ID, api_key="key-4", api_secret="sec-4")
    return SessionManager(
        factory,
        broker,
        persist_live=store.persist_live,
        mark_done=store.mark_done,
        list_db_sessions=store.list_sessions,
        lease_grace=LEASE_GRACE,
    )


async def _lease_publisher(
    broker: Broker, session_id: str, stop: asyncio.Event
) -> None:
    token = 0
    topic = Topics.sts_td_session(session_id)
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
    """Wait for ``predicate`` to hold, so tests never race the event loop."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.02)
    return predicate()


@pytest.mark.asyncio
async def test_a_failed_subscription_is_retried_not_fatal(
    broker: Broker, sessions: SessionManager, store: FakeStore
) -> None:
    """The first read raising must not cost the attach.

    A subscription that breaks is a transport failure, not STS going away,
    and the difference is the whole bug: one is worth resubscribing to.
    """
    sts_topic = Topics.sts_td_session(SID)
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
            TdAttachRequest(
                api_id=API_ID, session_id=SID, created_by=1, timeout=5.0
            )
        )
        assert broken, "the test never exercised a failing subscription"
        assert result.refcount == 1
        assert store.status() == "live"
    finally:
        broker.subscribe = original  # type: ignore[method-assign]
        stop.set()
        await asyncio.gather(pub, return_exceptions=True)
        await sessions.close_all()


@pytest.mark.asyncio
async def test_an_ack_that_cannot_be_published_does_not_end_the_lease(
    broker: Broker, sessions: SessionManager, store: FakeStore
) -> None:
    """STS heartbeats again in a second; the loop must live to hear it.

    Proved by a later ACK arriving rather than by the link still being
    attached — a loop that died leaves the link attached too, which is the
    whole problem.
    """
    ack_topic = Topics.td_session(API_ID, SID)
    original = broker.publish
    refused: list[str] = []
    acked: list[str] = []

    async def refusing_publish(topic, envelope):
        if topic == ack_topic and len(refused) < 3:
            refused.append(topic)
            raise ConnectionError("redis gone")
        return await original(topic, envelope)

    async def collect_acks(stop: asyncio.Event) -> None:
        async for env in broker.subscribe(ack_topic, stop=stop):
            if env.type == TD_LEASE_ACK:
                acked.append(env.type)

    broker.publish = refusing_publish  # type: ignore[method-assign]
    stop = asyncio.Event()
    pub = asyncio.create_task(_lease_publisher(broker, SID, stop))
    collector = asyncio.create_task(collect_acks(stop))
    try:
        await sessions.attach(
            TdAttachRequest(
                api_id=API_ID, session_id=SID, created_by=1, timeout=5.0
            )
        )
        assert await _until(lambda: len(refused) == 3)
        assert await _until(lambda: bool(acked)), (
            "no ACK after the transport recovered — the loop did not survive"
        )
        assert store.status() == "live"
    finally:
        broker.publish = original  # type: ignore[method-assign]
        stop.set()
        await asyncio.gather(pub, collector, return_exceptions=True)
        await sessions.close_all()


@pytest.mark.asyncio
async def test_a_lease_loop_that_ends_on_its_own_detaches(
    broker: Broker, sessions: SessionManager, store: FakeStore
) -> None:
    """The ending nothing else accounts for.

    Cancelled from outside with nobody having asked for a teardown: the loop
    is gone, so no ack, no expiry and no detach will ever come from it. It
    must close the attach on its way out rather than leave a live row.
    """
    stop = asyncio.Event()
    pub = asyncio.create_task(_lease_publisher(broker, SID, stop))
    try:
        await sessions.attach(
            TdAttachRequest(
                api_id=API_ID, session_id=SID, created_by=1, timeout=5.0
            )
        )
        link = sessions._accounts[API_ID].links[SID]  # noqa: SLF001
        link.tasks[0].cancel()

        assert await _until(lambda: store.status() == "done")
        assert sessions.get(API_ID) is None
    finally:
        stop.set()
        await asyncio.gather(pub, return_exceptions=True)
        await sessions.close_all()


@pytest.mark.asyncio
async def test_a_dead_link_does_not_outlive_its_sibling(
    broker: Broker, sessions: SessionManager, store: FakeStore
) -> None:
    """The incident, in miniature.

    Two api_ids share one session and one ``sts.td.{session}`` channel, and
    each loop acts only on its own api_id. When one loop was gone, the detach
    addressed to it was read by the surviving sibling, filtered out, and
    dropped — so that row stayed live for hours while its partner closed
    cleanly. Losing a loop must not depend on the other one to be noticed.
    """
    stop = asyncio.Event()
    pub = asyncio.create_task(_lease_publisher(broker, SID, stop))
    try:
        for api_id in (API_ID, SIBLING_API_ID):
            await sessions.attach(
                TdAttachRequest(
                    api_id=api_id, session_id=SID, created_by=1, timeout=5.0
                )
            )

        sibling = sessions._accounts[SIBLING_API_ID].links[SID]  # noqa: SLF001
        sibling.tasks[0].cancel()

        assert await _until(lambda: store.status(SIBLING_API_ID) == "done")
        # The survivor is untouched, and still the only one STS is talking to.
        assert store.status(API_ID) == "live"

        await broker.publish(
            Topics.sts_td_session(SID),
            Envelope[StsDetach].wrap(
                StsDetach(session_id=SID, api_id=API_ID),
                type=STS_DETACH,
                source="sts",
                session_id=SID,
            ),
        )
        assert await _until(lambda: store.status(API_ID) == "done")
    finally:
        stop.set()
        await asyncio.gather(pub, return_exceptions=True)
        await sessions.close_all()
