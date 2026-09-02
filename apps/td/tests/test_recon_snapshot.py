"""STS recon is an OMS snapshot — not a per-attach venue pass."""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.exchange import PaperExchange
from mftik.exchange.models import Order, OrderStatus, OrderType, Side
from mftik.protocol import (
    STS_LEASE_HEARTBEAT,
    STS_RECON,
    TD_RECON_DONE,
    Envelope,
    LeaseHeartbeat,
    Recon,
    ReconDone,
    TdAttachRequest,
    Topics,
)
from mftik_td.session import PaperSessionFactory, SessionManager
from mftik_td.session import manager as sessions_manager

API_ID = 42
SESSION = "sts-recon"
SESSION_B = "sts-recon-b"


@pytest.fixture
async def broker() -> Broker:
    async with a_broker() as client:
        yield client


@pytest.fixture
async def paper() -> PaperExchange:
    async with PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")}, tick_interval=0.05, seed=7
    ) as ex:
        yield ex


@pytest.fixture
def factory(broker: Broker, paper: PaperExchange) -> PaperSessionFactory:
    return PaperSessionFactory(broker, paper)


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
            await asyncio.wait_for(stop.wait(), timeout=0.1)
        except TimeoutError:
            continue


async def _wait_recon_done(
    broker: Broker, api_id: int, session_id: str, *, timeout: float = 2.0
) -> ReconDone:
    topic = Topics.td_session(api_id, session_id)
    stop = asyncio.Event()

    async def _once() -> ReconDone:
        async for env in broker.subscribe(topic, stop=stop):
            if env.type == TD_RECON_DONE:
                stop.set()
                return ReconDone.model_validate(env.payload)
        raise AssertionError("subscription ended without ReconDone")

    task = asyncio.create_task(_once())
    await asyncio.sleep(0.05)
    await broker.publish(
        Topics.sts_td_session(session_id),
        Envelope[Recon].wrap(
            Recon(session_id=session_id, api_id=api_id),
            type=STS_RECON,
            source="sts",
            session_id=session_id,
        ),
    )
    try:
        return await asyncio.wait_for(task, timeout=timeout)
    finally:
        stop.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_sts_recon_on_clean_book_does_not_hit_venue(
    broker: Broker, factory: PaperSessionFactory
) -> None:
    manager = SessionManager(factory, broker, lease_grace=2.0)
    stop = asyncio.Event()
    pub = asyncio.create_task(_lease_publisher(broker, SESSION, stop))
    await manager.attach(
        TdAttachRequest(
            session_id=SESSION, api_id=API_ID, timeout=2.0, created_by=1
        )
    )
    session = manager.get(API_ID)
    assert session is not None

    calls = {"n": 0}
    original = session.private.fetch_open_orders

    async def counted(*args, **kwargs):  # noqa: ANN001
        calls["n"] += 1
        return await original(*args, **kwargs)

    session.private.fetch_open_orders = counted  # type: ignore[method-assign]
    baseline = calls["n"]

    done = await _wait_recon_done(broker, API_ID, SESSION)
    assert done.api_id == API_ID
    assert done.session_id == SESSION
    assert calls["n"] == baseline
    # Balances come from the ledger, not a stale OMS recon copy.
    assert "USDT" in done.oms.balances or "BTC" in done.oms.balances

    stop.set()
    await asyncio.gather(pub, return_exceptions=True)
    await manager.close_all()


@pytest.mark.asyncio
async def test_second_sts_recon_also_skips_venue(
    broker: Broker, factory: PaperSessionFactory
) -> None:
    manager = SessionManager(factory, broker, lease_grace=2.0)
    stop_a = asyncio.Event()
    stop_b = asyncio.Event()
    pub_a = asyncio.create_task(_lease_publisher(broker, SESSION, stop_a))
    await manager.attach(
        TdAttachRequest(
            session_id=SESSION, api_id=API_ID, timeout=2.0, created_by=1
        )
    )
    pub_b = asyncio.create_task(_lease_publisher(broker, SESSION_B, stop_b))
    await manager.attach(
        TdAttachRequest(
            session_id=SESSION_B, api_id=API_ID, timeout=2.0, created_by=1
        )
    )
    session = manager.get(API_ID)
    assert session is not None

    calls = {"n": 0}
    original = session.private.fetch_open_orders

    async def counted(*args, **kwargs):  # noqa: ANN001
        calls["n"] += 1
        return await original(*args, **kwargs)

    session.private.fetch_open_orders = counted  # type: ignore[method-assign]

    await _wait_recon_done(broker, API_ID, SESSION)
    n_after_first = calls["n"]
    await _wait_recon_done(broker, API_ID, SESSION_B)
    assert calls["n"] == n_after_first

    stop_a.set()
    stop_b.set()
    await asyncio.gather(pub_a, pub_b, return_exceptions=True)
    await manager.close_all()


@pytest.mark.asyncio
async def test_sts_recon_waits_while_unknown_then_flushes_via_forced_recon(
    broker: Broker, factory: PaperSessionFactory
) -> None:
    """Stuck UNKNOWN must not park recon forever — chase forces venue recon."""
    manager = SessionManager(factory, broker, lease_grace=2.0)
    stop = asyncio.Event()
    pub = asyncio.create_task(_lease_publisher(broker, SESSION, stop))
    await manager.attach(
        TdAttachRequest(
            session_id=SESSION, api_id=API_ID, timeout=2.0, created_by=1
        )
    )
    session = manager.get(API_ID)
    assert session is not None

    unknown = Order(
        client_order_id="cid-unk",
        universal_ticker="Paper_Spot_BTCUSDT",
        side=Side.BUY,
        type=OrderType.LIMIT,
        status=OrderStatus.UNKNOWN,
        qty=Decimal("0.01"),
        price=Decimal("1000"),
    )
    session.oms.handle_order(unknown)
    await session.write_order(unknown)
    session._remember_unknown("cid-unk", if_missing=OrderStatus.REJECTED)
    # Aged past the force-recon deadline.
    session._unknown_since["cid-unk"] = (
        asyncio.get_running_loop().time() - session.unknown_force_recon - 1
    )
    assert session.has_unknown()

    async def never(_cid: str, *, ticker=None):  # noqa: ANN001
        raise RuntimeError("resolve deferred")

    session.private.fetch_order_by_client_order_id = never  # type: ignore[method-assign]

    topic = Topics.td_session(API_ID, SESSION)
    got: asyncio.Future[ReconDone] = asyncio.get_running_loop().create_future()
    sub_stop = asyncio.Event()

    async def _listen() -> None:
        async for env in broker.subscribe(topic, stop=sub_stop):
            if env.type == TD_RECON_DONE and not got.done():
                got.set_result(ReconDone.model_validate(env.payload))
                sub_stop.set()
                return

    listener = asyncio.create_task(_listen())
    await asyncio.sleep(0.05)
    await broker.publish(
        Topics.sts_td_session(SESSION),
        Envelope[Recon].wrap(
            Recon(session_id=SESSION, api_id=API_ID),
            type=STS_RECON,
            source="sts",
            session_id=SESSION,
        ),
    )
    await asyncio.sleep(0.2)
    assert not got.done()
    acct = manager._accounts[API_ID]
    assert len(acct.recon_waiters) == 1

    # Production path: sweeper chase forces venue recon and flushes waiters.
    await session.chase_unknown()

    done = await asyncio.wait_for(got, timeout=2.0)
    assert done.session_id == SESSION
    assert "cid-unk" not in done.oms.orders
    assert acct.recon_waiters == []
    assert not session.has_unknown()

    sub_stop.set()
    await asyncio.gather(listener, return_exceptions=True)
    stop.set()
    await asyncio.gather(pub, return_exceptions=True)
    await manager.close_all()


async def _park_on_unknown(manager: SessionManager, cid: str) -> None:
    """Leave an UNKNOWN in the book that nothing can resolve."""
    session = manager.get(API_ID)
    assert session is not None
    session.oms.handle_order(
        Order(
            client_order_id=cid,
            universal_ticker="Paper_Spot_BTCUSDT",
            side=Side.BUY,
            type=OrderType.LIMIT,
            status=OrderStatus.UNKNOWN,
            qty=Decimal("0.01"),
            price=Decimal("1000"),
        )
    )

    async def never(_cid: str, *, ticker=None):  # noqa: ANN001
        raise RuntimeError("resolve deferred")

    session.private.fetch_order_by_client_order_id = never  # type: ignore[method-assign]


async def _send_recon(broker: Broker) -> None:
    await broker.publish(
        Topics.sts_td_session(SESSION),
        Envelope[Recon].wrap(
            Recon(session_id=SESSION, api_id=API_ID),
            type=STS_RECON,
            source="sts",
            session_id=SESSION,
        ),
    )


async def test_resent_recon_while_parked_does_not_double_the_waiter(
    broker: Broker, factory: PaperSessionFactory
) -> None:
    """A resend must not earn a second ReconDone when the book settles."""
    manager = SessionManager(factory, broker, lease_grace=2.0)
    stop = asyncio.Event()
    pub = asyncio.create_task(_lease_publisher(broker, SESSION, stop))
    await manager.attach(
        TdAttachRequest(
            session_id=SESSION, api_id=API_ID, timeout=2.0, created_by=1
        )
    )
    await _park_on_unknown(manager, "cid-dup")

    for _ in range(3):
        await _send_recon(broker)
        await asyncio.sleep(0.1)

    acct = manager._accounts[API_ID]
    assert len(acct.recon_waiters) == 1

    stop.set()
    await asyncio.gather(pub, return_exceptions=True)
    await manager.close_all()


async def test_parked_recon_is_answered_once_the_wait_expires(
    broker: Broker, factory: PaperSessionFactory, monkeypatch
) -> None:
    """A venue that can never resolve must not park the strategy forever."""
    monkeypatch.setattr(sessions_manager, "RECON_WAIT_TIMEOUT_S", 0.3)
    manager = SessionManager(factory, broker, lease_grace=2.0)
    stop = asyncio.Event()
    pub = asyncio.create_task(_lease_publisher(broker, SESSION, stop))
    await manager.attach(
        TdAttachRequest(
            session_id=SESSION, api_id=API_ID, timeout=2.0, created_by=1
        )
    )
    await _park_on_unknown(manager, "cid-stuck")

    topic = Topics.td_session(API_ID, SESSION)
    got: asyncio.Future[ReconDone] = asyncio.get_running_loop().create_future()
    sub_stop = asyncio.Event()

    async def _listen() -> None:
        async for env in broker.subscribe(topic, stop=sub_stop):
            if env.type == TD_RECON_DONE and not got.done():
                got.set_result(ReconDone.model_validate(env.payload))
                sub_stop.set()
                return

    listener = asyncio.create_task(_listen())
    await asyncio.sleep(0.05)
    await _send_recon(broker)

    acct = manager._accounts[API_ID]
    await asyncio.sleep(0.15)
    assert not got.done()
    assert len(acct.recon_waiters) == 1

    # Late and honest: the snapshot still carries the UNKNOWN order.
    done = await asyncio.wait_for(got, timeout=2.0)
    assert done.session_id == SESSION
    assert "cid-stuck" in done.oms.orders
    assert acct.recon_waiters == []
    assert manager.get(API_ID).has_unknown()  # type: ignore[union-attr]

    sub_stop.set()
    await asyncio.gather(listener, return_exceptions=True)
    stop.set()
    await asyncio.gather(pub, return_exceptions=True)
    await manager.close_all()
