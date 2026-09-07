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
from mftik.liveness import owner_name
from mftik.protocol import (
    STS_LEASE_HEARTBEAT,
    TD_ERROR,
    TD_SESSION_ATTACH,
    Envelope,
    LeaseHeartbeat,
    RpcError,
    TdAttachRequest,
    TdAttachRequestEnvelope,
    TdAttachResult,
    Topics,
)
from mftik_td.rpc import dispatch
from mftik_td.session import PaperSessionFactory, SessionManager
from mftik_td.session.manager import AccountHeldElsewhere


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
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except TimeoutError:
            continue


@pytest.fixture
async def broker() -> Broker:
    async with a_broker() as client:
        yield client


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
async def test_lease_expiry_marks_done_and_destroys(
    broker: Broker, manager: SessionManager, store: FakeStore
) -> None:
    """Watchdog detach must not RecursionError / leave a live DB row."""
    stop = asyncio.Event()
    pub = asyncio.create_task(
        _lease_publisher(broker, "lease-die", stop, interval=0.05)
    )
    await manager.attach(
        TdAttachRequest(
            session_id="lease-die", api_id=9, timeout=2.0, created_by=1
        )
    )
    assert store.rows[("lease-die", 9)].status == "live"
    assert manager.get(9) is not None

    # Stop heartbeats; grace is 2.0s on the test manager.
    stop.set()
    await pub

    for _ in range(40):
        if manager.get(9) is None:
            break
        await asyncio.sleep(0.1)

    assert manager.get(9) is None
    assert store.rows[("lease-die", 9)].status == "done"


@pytest.mark.asyncio
async def test_detach_retry_cleans_orphan_account(
    broker: Broker, manager: SessionManager, store: FakeStore
) -> None:
    """A partial detach that already popped the link must still destroy."""
    stop = asyncio.Event()
    pub = asyncio.create_task(_lease_publisher(broker, "orphan", stop))
    await manager.attach(
        TdAttachRequest(
            session_id="orphan", api_id=11, timeout=2.0, created_by=1
        )
    )
    acct = manager._accounts[11]
    link = acct.links.pop("orphan")
    assert acct.refcount == 0
    assert manager.get(11) is not None

    await manager.detach(session_id="orphan", api_id=11, reason="sts_stop")
    assert manager.get(11) is None
    assert store.rows[("orphan", 11)].status == "done"

    link.stop.set()
    stop.set()
    await pub


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
async def test_rpc_attach_on_the_instance_subject(
    broker: Broker, manager: SessionManager
) -> None:
    """Attach reaches a TD addressed by name, not only by plane.

    The listing half of this test went with the RPC it exercised: reading
    ``td_sessions`` never needed a TD process, so the API runs that query
    itself now (``mftik_api.routes.td``). What is left is the part that does
    need one.
    """
    subject = Topics.td("td-jp-1")
    stop_lease = asyncio.Event()
    stop_serve = asyncio.Event()
    pub = asyncio.create_task(_lease_publisher(broker, "rpc-sts", stop_lease))

    async def server() -> None:
        async for req in broker.serve(subject, stop=stop_serve):
            await dispatch(req, sessions=manager)
            break
        stop_serve.set()

    serve_task = asyncio.create_task(server())
    await asyncio.sleep(0.05)

    create_reply = await broker.request(
        subject,
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
    await serve_task
    stop_lease.set()
    await pub

    created = TdAttachResult.model_validate(create_reply.payload)
    assert create_reply.type == TD_SESSION_ATTACH
    assert created.session_id == "rpc-sts"
    assert created.api_id == 3
    assert manager.refcount(3) == 1

    await manager.close_all()


@pytest.mark.asyncio
async def test_rpc_attach_timeout_error(
    broker: Broker, manager: SessionManager
) -> None:
    stop = asyncio.Event()

    async def server() -> None:
        async for req in broker.serve(Topics.td("td"), stop=stop):
            await dispatch(req, sessions=manager)
            break
        stop.set()

    task = asyncio.create_task(server())
    await asyncio.sleep(0.05)

    reply = await broker.request(
        Topics.td("td"),
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


@pytest.mark.asyncio
async def test_a_second_process_is_refused_the_same_account(
    broker: Broker, paper: PaperExchange, store: FakeStore
) -> None:
    """PI-7, against two real managers rather than the primitive alone.

    Two processes each holding one credential is two OMS views, two ledgers
    and two competing consumers on ``td.order.{api_id}`` — the invariant the
    lease, the OMS and the ``client_order_id`` slot all rest on. Before this it
    was only asserted: ``attach`` decided from process-local memory.
    """
    first = SessionManager(
        PaperSessionFactory(broker, paper),
        broker,
        persist_live=store.persist_live,
        mark_done=store.mark_done,
        list_db_sessions=store.list_sessions,
        lease_grace=2.0,
    )
    second = SessionManager(
        PaperSessionFactory(broker, paper),
        broker,
        persist_live=store.persist_live,
        mark_done=store.mark_done,
        list_db_sessions=store.list_sessions,
        lease_grace=2.0,
    )
    stop = asyncio.Event()
    pub = asyncio.create_task(_lease_publisher(broker, "own-sts", stop))
    try:
        await first.attach(
            TdAttachRequest(
                session_id="own-sts", api_id=3, timeout=2.0, created_by=1
            )
        )

        with pytest.raises(AccountHeldElsewhere) as refused:
            await second.attach(
                TdAttachRequest(
                    session_id="own-sts-2",
                    api_id=3,
                    timeout=2.0,
                    created_by=1,
                )
            )

        assert refused.value.api_id == 3
        assert refused.value.holder == first._owner  # noqa: SLF001
        assert "MFTIK_INSTANCE" in str(refused.value), (
            "the refusal has to point at the configuration that caused it"
        )
        assert second.active_api_ids == [], (
            "no venue session was opened for an account it may not hold"
        )
        assert first.active_api_ids == [3]
    finally:
        stop.set()
        await pub
        await first.close_all()
        await second.close_all()


@pytest.mark.asyncio
async def test_releasing_an_account_lets_another_process_take_it(
    broker: Broker, paper: PaperExchange, store: FakeStore
) -> None:
    """A redeploy must not have to wait out a TTL to get its accounts back."""
    first = SessionManager(
        PaperSessionFactory(broker, paper),
        broker,
        persist_live=store.persist_live,
        mark_done=store.mark_done,
        list_db_sessions=store.list_sessions,
        lease_grace=2.0,
    )
    second = SessionManager(
        PaperSessionFactory(broker, paper),
        broker,
        persist_live=store.persist_live,
        mark_done=store.mark_done,
        list_db_sessions=store.list_sessions,
        lease_grace=2.0,
    )
    stop = asyncio.Event()
    pub = asyncio.create_task(_lease_publisher(broker, "hand-over", stop))
    try:
        await first.attach(
            TdAttachRequest(
                session_id="hand-over", api_id=3, timeout=2.0, created_by=1
            )
        )
        await first.close_all()

        await second.attach(
            TdAttachRequest(
                session_id="hand-over", api_id=3, timeout=2.0, created_by=1
            )
        )
        assert second.active_api_ids == [3]
    finally:
        stop.set()
        await pub
        await second.close_all()


@pytest.mark.asyncio
async def test_losing_the_claim_tears_the_account_down(
    broker: Broker, paper: PaperExchange, store: FakeStore
) -> None:
    """The half of PI-7 that runs after attach, and had no coverage.

    A claim that lapses and is taken by a rival is not a missed refresh to
    shrug at: this process would go on serving ``td.order.{api_id}`` as a
    competing consumer against the account's new owner. The keepalive loop is
    where that is noticed, and noticing has to mean giving the account up.
    """
    manager = SessionManager(
        PaperSessionFactory(broker, paper),
        broker,
        persist_live=store.persist_live,
        mark_done=store.mark_done,
        list_db_sessions=store.list_sessions,
        lease_grace=2.0,
    )
    stop = asyncio.Event()
    pub = asyncio.create_task(_lease_publisher(broker, "yield-sts", stop))
    try:
        await manager.attach(
            TdAttachRequest(
                session_id="yield-sts", api_id=3, timeout=2.0, created_by=1
            )
        )
        assert manager.active_api_ids == [3]

        # A rival takes the account: the claim is gone, then theirs.
        name = owner_name("3", domain="td")
        await broker.lease_put(name, owner="some-other-process", ttl=30)

        deadline = asyncio.get_running_loop().time() + 5.0
        while (
            manager.active_api_ids
            and asyncio.get_running_loop().time() < deadline
        ):
            await asyncio.sleep(0.05)

        assert manager.active_api_ids == [], (
            "an account whose claim moved on must not still be served here"
        )
        assert await broker.lease_owner(name) == "some-other-process", (
            "and the rival's claim must not be stolen back on the way out"
        )
    finally:
        stop.set()
        await pub
        await manager.close_all()
