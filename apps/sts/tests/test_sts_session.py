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
    STS_HEALTH,
    STS_SESSION_CREATE,
    STS_SESSION_LIST,
    HealthCheck,
    HealthCheckEnvelope,
    HealthStatus,
    ListSessionsRequest,
    ListSessionsRequestEnvelope,
    ListSessionsResult,
    StsCreateSessionRequest,
    StsCreateSessionRequestEnvelope,
    StsCreateSessionResult,
    TdAccountRef,
    TdAttachRequest,
    Topics,
)
from mftik_sts.rpc import dispatch
from mftik_sts.session import SessionManager as StsSessionManager
from mftik_td.session import PaperSessionFactory
from mftik_td.session import SessionManager as TdSessionManager


@dataclass
class FakeStsStore:
    rows: dict[str, SimpleNamespace] = field(default_factory=dict)

    async def persist_live(
        self,
        *,
        session_id: str,
        created_by: int,
        strategy: str | None = None,
        td_api_ids: list[int] | None = None,
        md_ids: list[str] | None = None,
        st_paras: dict | None = None,
        cid_slot: int | None = None,
        restart: str = "always",
        **_extra: object,
    ) -> SimpleNamespace:
        existing = self.rows.get(session_id)
        if existing is not None:
            return existing
        row = SimpleNamespace(
            session_id=session_id,
            created_by=created_by,
            created_at=datetime.now(UTC),
            finished_at=None,
            status="live",
            strategy=strategy,
            td_api_ids=list(td_api_ids or []),
            md_ids=list(md_ids or []),
            st_paras=dict(st_paras or {}),
            cid_slot=cid_slot,
            restart=restart,
            rebuild_count=0,
            reason=None,
        )
        self.rows[session_id] = row
        return row

    async def mark_done(
        self,
        session_id: str,
        *,
        status: str = "done",
        reason: str | None = None,
    ) -> SimpleNamespace | None:
        row = self.rows.get(session_id)
        if row is None:
            return None
        row.status = status
        row.reason = reason
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


@dataclass
class FakeTdStore:
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
def sts_store() -> FakeStsStore:
    return FakeStsStore()


@pytest.fixture
def sts_manager(broker: Broker, sts_store: FakeStsStore) -> StsSessionManager:
    return StsSessionManager(
        broker,
        persist_live=sts_store.persist_live,
        mark_done=sts_store.mark_done,
        list_db_sessions=sts_store.list_sessions,
        heartbeat_interval=0.1,
    )


@pytest.mark.asyncio
async def test_sts_session_lives_independently(
    sts_manager: StsSessionManager, sts_store: FakeStsStore
) -> None:
    result = await sts_manager.create_session(
        StsCreateSessionRequest(
            session_id="s1",
            created_by=1,
            strategy="noop",
            td={"paper": TdAccountRef(api_id=1)},
        )
    )
    assert result.session_id == "s1"
    assert sts_store.rows["s1"].strategy == "noop"
    session = sts_manager.get("s1")
    assert session is not None

    await asyncio.sleep(0.25)
    assert sts_manager.get("s1") is not None

    await sts_manager.close("s1")
    assert sts_manager.get("s1") is None
    assert sts_store.rows["s1"].status == "done"


@pytest.mark.asyncio
async def test_sts_lease_enables_td_attach(
    broker: Broker, sts_manager: StsSessionManager
) -> None:
    await sts_manager.create_session(
        StsCreateSessionRequest(
            session_id="lease1",
            created_by=2,
            strategy="noop",
            td={"paper": TdAccountRef(api_id=11)},
        )
    )
    session = sts_manager.get("lease1")
    assert session is not None
    assert session.td["paper"].api_id == 11

    paper = PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")}, tick_interval=0.05, seed=1
    )
    await paper.start()
    td_store = FakeTdStore()
    td_factory = PaperSessionFactory(broker, paper)
    td_manager = TdSessionManager(
        td_factory,
        broker,
        persist_live=td_store.persist_live,
        mark_done=td_store.mark_done,
        list_db_sessions=td_store.list_sessions,
        lease_grace=2.0,
    )

    td = await td_manager.attach(
        TdAttachRequest(
            session_id="lease1",
            api_id=11,
            timeout=2.0,
            created_by=2,
        )
    )
    assert td.session_id == "lease1"
    assert td.api_id == 11
    assert td.refcount == 1
    assert ("lease1", 11) in td_store.rows

    await td_manager.close_all()
    await sts_manager.close_all()
    await paper.stop()


@pytest.mark.asyncio
async def test_sts_rpc_create_list_health(
    broker: Broker, sts_manager: StsSessionManager
) -> None:
    stop = asyncio.Event()
    seen_list = asyncio.Event()

    async def server() -> None:
        async for req in broker.serve(Topics.STS, stop=stop):
            await dispatch(req, sessions=sts_manager)
            if req.envelope.type == STS_SESSION_LIST:
                seen_list.set()
                break
        stop.set()

    task = asyncio.create_task(server())
    await asyncio.sleep(0.05)

    health = await broker.request(
        Topics.STS,
        HealthCheckEnvelope.wrap(
            HealthCheck(), type=STS_HEALTH, source="api"
        ),
        timeout=2,
    )
    assert HealthStatus.model_validate(health.payload).service == "sts"

    create_reply = await broker.request(
        Topics.STS,
        StsCreateSessionRequestEnvelope.wrap(
            StsCreateSessionRequest(
                session_id="rpc1",
                created_by=5,
                strategy="noop",
                td={"paper": TdAccountRef(api_id=1)},
            ),
            type=STS_SESSION_CREATE,
            source="api",
        ),
        timeout=2,
    )
    created = StsCreateSessionResult.model_validate(create_reply.payload)
    assert created.session_id == "rpc1"

    list_reply = await broker.request(
        Topics.STS,
        ListSessionsRequestEnvelope.wrap(
            ListSessionsRequest(domain="sts", status="live"),
            type=STS_SESSION_LIST,
            source="api",
        ),
        timeout=2,
    )
    await asyncio.wait_for(seen_list.wait(), timeout=2)
    await task

    listed = ListSessionsResult.model_validate(list_reply.payload)
    assert any(s.session_id == created.session_id for s in listed.sessions)

    await sts_manager.close_all()
