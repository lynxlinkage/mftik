"""Detach as a request that is answered, rather than a message that is sent.

Published on the session stream, a detach is read by one subscriber per link
and acted on only by the one whose api_id it names — so a link whose reader
has stopped never sees its own detach, and the sibling that did read it drops
it. Nothing anywhere learns that the attach was not closed. On the RPC
subject it is served by a task that outlives every lease, and it replies.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import fakeredis.aioredis
import pytest
from mftik.broker import Broker, BrokerConfig
from mftik.exchange import PaperExchange
from mftik.protocol import (
    STS_LEASE_HEARTBEAT,
    TD_ERROR,
    TD_SESSION_DETACH,
    Envelope,
    LeaseHeartbeat,
    RpcError,
    TdAttachRequest,
    TdDetachRequest,
    TdDetachRequestEnvelope,
    TdDetachResult,
    Topics,
)
from mftik_td.rpc import dispatch
from mftik_td.session import PaperSessionFactory, SessionManager

SID = "sess-detach-rpc"
API_ID = 3
SIBLING_API_ID = 4


@dataclass
class FakeStore:
    rows: dict[tuple[str, int], SimpleNamespace] = field(default_factory=dict)

    async def persist_live(
        self, *, session_id: str, created_by: int, api_id: int
    ) -> SimpleNamespace:
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
        return [
            row
            for row in self.rows.values()
            if status is None or row.status == status
        ][:limit]

    def status(self, api_id: int = API_ID) -> str | None:
        row = self.rows.get((SID, api_id))
        return None if row is None else row.status


@pytest.fixture
async def broker() -> Broker:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test-detach-rpc"),
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
        seed=13,
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
        lease_grace=2.0,
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


async def _serve(
    broker: Broker, sessions: SessionManager, stop: asyncio.Event
) -> None:
    async for req in broker.serve(Topics.TD, stop=stop):
        await dispatch(req, sessions=sessions)


def _detach(api_id: int = API_ID) -> TdDetachRequestEnvelope:
    return TdDetachRequestEnvelope.wrap(
        TdDetachRequest(session_id=SID, api_id=api_id),
        type=TD_SESSION_DETACH,
        source="sts",
        session_id=SID,
    )


@pytest.mark.asyncio
async def test_a_detach_request_closes_the_attach_and_answers(
    broker: Broker, sessions: SessionManager, store: FakeStore
) -> None:
    srv_stop = asyncio.Event()
    server = asyncio.create_task(_serve(broker, sessions, srv_stop))
    stop = asyncio.Event()
    pub = asyncio.create_task(_lease_publisher(broker, SID, stop))
    try:
        await sessions.attach(
            TdAttachRequest(
                api_id=API_ID, session_id=SID, created_by=1, timeout=5.0
            )
        )

        reply = await broker.request(Topics.TD, _detach(), timeout=5.0)

        assert reply.type == TD_SESSION_DETACH
        assert TdDetachResult.model_validate(reply.payload).refcount == 0
        assert store.status() == "done"
        assert sessions.get(API_ID) is None
    finally:
        stop.set()
        srv_stop.set()
        server.cancel()
        await asyncio.gather(pub, server, return_exceptions=True)
        await sessions.close_all()


@pytest.mark.asyncio
async def test_a_detach_does_not_need_its_own_lease_loop(
    broker: Broker, sessions: SessionManager, store: FakeStore
) -> None:
    """The failure this moved for.

    With the link's reader gone, a detach on the session stream reaches only
    the sibling, which is not allowed to act on another api_id's attach. The
    request-reply path does not go through either loop.
    """
    srv_stop = asyncio.Event()
    server = asyncio.create_task(_serve(broker, sessions, srv_stop))
    stop = asyncio.Event()
    pub = asyncio.create_task(_lease_publisher(broker, SID, stop))
    try:
        for api_id in (API_ID, SIBLING_API_ID):
            await sessions.attach(
                TdAttachRequest(
                    api_id=api_id, session_id=SID, created_by=1, timeout=5.0
                )
            )
        # Take the reader away without letting it tidy up after itself.
        link = sessions._accounts[SIBLING_API_ID].links[SID]  # noqa: SLF001
        link.stop.set()
        await asyncio.gather(*link.tasks, return_exceptions=True)

        reply = await broker.request(
            Topics.TD, _detach(SIBLING_API_ID), timeout=5.0
        )

        assert reply.type == TD_SESSION_DETACH
        assert store.status(SIBLING_API_ID) == "done"
        assert store.status(API_ID) == "live"
    finally:
        stop.set()
        srv_stop.set()
        server.cancel()
        await asyncio.gather(pub, server, return_exceptions=True)
        await sessions.close_all()


@pytest.mark.asyncio
async def test_detaching_twice_is_not_an_error(
    broker: Broker, sessions: SessionManager, store: FakeStore
) -> None:
    """What makes the retry safe: a reply lost after the work was done must
    cost nothing but a second call."""
    srv_stop = asyncio.Event()
    server = asyncio.create_task(_serve(broker, sessions, srv_stop))
    stop = asyncio.Event()
    pub = asyncio.create_task(_lease_publisher(broker, SID, stop))
    try:
        await sessions.attach(
            TdAttachRequest(
                api_id=API_ID, session_id=SID, created_by=1, timeout=5.0
            )
        )
        first = await broker.request(Topics.TD, _detach(), timeout=5.0)
        second = await broker.request(Topics.TD, _detach(), timeout=5.0)

        assert first.type == TD_SESSION_DETACH
        assert second.type == TD_SESSION_DETACH
        assert TdDetachResult.model_validate(second.payload).refcount == 0
        assert store.status() == "done"
    finally:
        stop.set()
        srv_stop.set()
        server.cancel()
        await asyncio.gather(pub, server, return_exceptions=True)
        await sessions.close_all()


@pytest.mark.asyncio
async def test_a_malformed_detach_is_refused_not_ignored(
    broker: Broker, sessions: SessionManager
) -> None:
    srv_stop = asyncio.Event()
    server = asyncio.create_task(_serve(broker, sessions, srv_stop))
    try:
        reply = await broker.request(
            Topics.TD,
            Envelope[dict].wrap(
                {"session_id": SID},  # no api_id
                type=TD_SESSION_DETACH,
                source="sts",
                session_id=SID,
            ),
            timeout=5.0,
        )
        assert reply.type == TD_ERROR
        assert RpcError.model_validate(reply.payload).code == "invalid_payload"
    finally:
        srv_stop.set()
        server.cancel()
        await asyncio.gather(server, return_exceptions=True)
        await sessions.close_all()
