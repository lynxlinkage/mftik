from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import fakeredis.aioredis
import pytest
from mft.broker import Broker, BrokerConfig
from mft.exchange import PaperExchange
from mft.protocol import (
    STS_LEASE_HEARTBEAT,
    TD_ERROR,
    TD_SESSION_ATTACH,
    TD_SESSION_LIST,
    Envelope,
    LeaseHeartbeat,
    ListSessionsRequest,
    ListSessionsRequestEnvelope,
    ListSessionsResult,
    RpcError,
    TdAttachRequest,
    TdAttachRequestEnvelope,
    TdAttachResult,
    Topics,
)
from mft_td.rpc import dispatch
from mft_td.session import PaperSessionFactory, SessionManager


@dataclass
class FakeStore:
    rows: dict[tuple[str, int], SimpleNamespace] = field(default_factory=dict)

    async def persist_live(
        self, *, session_id: str, created_by: int, api_id: int
    ) -> SimpleNamespace:
        key = (session_id, api_id)
        existing = self.rows.get(key)
        if existing is not None:
            return existing
        row = SimpleNamespace(
            session_id=session_id,
            created_by=created_by,
            created_at=datetime.now(UTC),
            finished_at=None,
            status="live",
            api_id=api_id,
        )
        self.rows[key] = row
        return row

    async def mark_done(
        self, *, session_id: str, api_id: int
    ) -> SimpleNamespace | None:
        row = self.rows.get((session_id, api_id))
        if row is None:
            return None
        row.status = "done"
        row.finished_at = datetime.now(UTC)
        return row

    async def list_sessions(
        self,
        *,
        status: str | None = "live",
        created_by: int | None = None,
    ) -> list[SimpleNamespace]:
        out = []
        for row in self.rows.values():
            if status is not None and row.status != status:
                continue
            if created_by is not None and row.created_by != created_by:
                continue
            out.append(row)
        return out


async def _lease_publisher(
    broker: Broker, session_id: str, stop: asyncio.Event, *, interval: float = 0.1
) -> None:
    token = 0
    topic = Topics.sts_session(session_id)
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
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except TimeoutError:
            continue


@pytest.fixture
async def broker() -> Broker:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test"),
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
        seed=3,
    ) as ex:
        yield ex


@pytest.fixture
def store() -> FakeStore:
    return FakeStore()


@pytest.fixture
def manager(broker: Broker, paper: PaperExchange, store: FakeStore) -> SessionManager:
    factory = PaperSessionFactory(broker, paper)
    return SessionManager(
        factory,
        broker,
        persist_live=store.persist_live,
        mark_done=store.mark_done,
        list_db_sessions=store.list_sessions,
        lease_grace=2.0,
    )


@pytest.mark.asyncio
async def test_attach_waits_for_sts_lease(
    broker: Broker, manager: SessionManager, store: FakeStore
) -> None:
    stop = asyncio.Event()
    pub = asyncio.create_task(_lease_publisher(broker, "s-attach", stop))

    result = await manager.attach(
        TdAttachRequest(
            session_id="s-attach",
            api_id=7,
            timeout=2.0,
            created_by=1,
        )
    )
    stop.set()
    await pub

    assert result.session_id == "s-attach"
    assert result.api_id == 7
    assert result.refcount == 1
    assert ("s-attach", 7) in store.rows
    assert store.rows[("s-attach", 7)].status == "live"
    assert manager.get(7) is not None

    await manager.close_all()


@pytest.mark.asyncio
async def test_attach_timeout(broker: Broker, manager: SessionManager) -> None:
    with pytest.raises(TimeoutError):
        await manager.attach(
            TdAttachRequest(
                session_id="missing",
                api_id=1,
                timeout=0.3,
                created_by=1,
            )
        )
    assert manager.get(1) is None


@pytest.mark.asyncio
async def test_attach_refcount_same_api(
    broker: Broker, manager: SessionManager
) -> None:
    stop = asyncio.Event()
    pubs = [
        asyncio.create_task(_lease_publisher(broker, "s1", stop)),
        asyncio.create_task(_lease_publisher(broker, "s2", stop)),
    ]

    r1 = await manager.attach(
        TdAttachRequest(session_id="s1", api_id=3, timeout=2.0, created_by=1)
    )
    r2 = await manager.attach(
        TdAttachRequest(session_id="s2", api_id=3, timeout=2.0, created_by=1)
    )
    assert r1.refcount == 1
    assert r2.refcount == 2
    assert manager.get(3) is not None

    await manager.detach(session_id="s1", api_id=3, reason="sts_stop")
    assert manager.get(3) is not None
    await manager.detach(session_id="s2", api_id=3, reason="sts_stop")
    assert manager.get(3) is None

    stop.set()
    await asyncio.gather(*pubs)


@pytest.mark.asyncio
async def test_rpc_attach_and_list(
    broker: Broker, manager: SessionManager
) -> None:
    stop_lease = asyncio.Event()
    stop_serve = asyncio.Event()
    pub = asyncio.create_task(_lease_publisher(broker, "rpc-sts", stop_lease))

    async def server() -> None:
        async for req in broker.serve(Topics.TD, stop=stop_serve):
            await dispatch(req, sessions=manager)
            if req.envelope.type == TD_SESSION_LIST:
                break
        stop_serve.set()

    serve_task = asyncio.create_task(server())
    await asyncio.sleep(0.05)

    create_reply = await broker.request(
        Topics.TD,
        TdAttachRequestEnvelope.wrap(
            TdAttachRequest(
                session_id="rpc-sts",
                api_id=3,
                timeout=2.0,
                created_by=9,
            ),
            type=TD_SESSION_ATTACH,
            source="api",
        ),
        timeout=3,
    )
    created = TdAttachResult.model_validate(create_reply.payload)
    assert create_reply.type == TD_SESSION_ATTACH
    assert created.session_id == "rpc-sts"
    assert created.api_id == 3

    list_reply = await broker.request(
        Topics.TD,
        ListSessionsRequestEnvelope.wrap(
            ListSessionsRequest(domain="td", status="live"),
            type=TD_SESSION_LIST,
            source="api",
        ),
        timeout=2,
    )
    await serve_task
    stop_lease.set()
    await pub

    listed = ListSessionsResult.model_validate(list_reply.payload)
    assert any(s.session_id == created.session_id for s in listed.sessions)

    await manager.close_all()


@pytest.mark.asyncio
async def test_rpc_attach_timeout_error(
    broker: Broker, manager: SessionManager
) -> None:
    stop = asyncio.Event()

    async def server() -> None:
        async for req in broker.serve(Topics.TD, stop=stop):
            await dispatch(req, sessions=manager)
            break
        stop.set()

    task = asyncio.create_task(server())
    await asyncio.sleep(0.05)

    reply = await broker.request(
        Topics.TD,
        TdAttachRequestEnvelope.wrap(
            TdAttachRequest(
                session_id="gone",
                api_id=2,
                timeout=0.25,
                created_by=1,
            ),
            type=TD_SESSION_ATTACH,
            source="api",
        ),
        timeout=3,
    )
    await task

    assert reply.type == TD_ERROR
    err = RpcError.model_validate(reply.payload)
    assert err.code == "timeout"
