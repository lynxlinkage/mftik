"""History reaches Postgres from the paths that actually run, not just in unit.

The writer is exercised directly in ``test_history_writer``. What is checked
here is the wiring it depends on: that a submit arriving over
``td.order.{api_id}`` carries its STS session all the way into the order row,
and that the fills the venue reports afterwards find their way back to it.

That plumbing is easy to get wrong and impossible to notice from outside — TD
would trade exactly as it does now and simply record every order as somebody
else's, leaving the dashboard permanently empty of the sessions people care
about.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from decimal import Decimal

import fakeredis.aioredis
import pytest
from mft.broker import Broker, BrokerConfig
from mft.exchange import PaperExchange, Side
from mft.exchange.models import OrderStatus, OrderType, PlaceOrderRequest
from mft.protocol import (
    STS_LEASE_HEARTBEAT,
    STS_ORDER_SUBMIT,
    Envelope,
    LeaseHeartbeat,
    OrderAck,
    OrderSubmit,
    TdAttachRequest,
    Topics,
)
from mft_db.models import Base
from mft_db.models.history import Attribution
from mft_db.repositories import FillRepository, OrderRepository
from mft_td.history import HistoryWriter
from mft_td.session import PaperSessionFactory, SessionManager
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

API_ID = 42
SESSION = "sts-history"
CID = "281474976710656001"


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
        symbols={"BTCUSDT": Decimal("50000")}, tick_interval=0.05, seed=7
    ) as ex:
        yield ex


@pytest.fixture
async def scope():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)

    @asynccontextmanager
    async def open_scope():
        async with maker() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    yield open_scope
    await engine.dispose()


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


@pytest.fixture
async def attached(broker: Broker, paper: PaperExchange, scope):
    """A manager wired to a history writer, with SESSION attached."""
    writer = HistoryWriter(scope=scope, flush_interval=3600.0)
    manager = SessionManager(
        PaperSessionFactory(broker, paper),
        broker,
        history=writer,
        lease_grace=2.0,
    )
    stop = asyncio.Event()
    pub = asyncio.create_task(_lease_publisher(broker, SESSION, stop))
    await manager.attach(
        TdAttachRequest(
            session_id=SESSION, api_id=API_ID, timeout=2.0, created_by=1
        )
    )
    yield manager, writer
    stop.set()
    await asyncio.gather(pub, return_exceptions=True)
    await manager.close_all()


async def _submit(broker: Broker, **over) -> OrderAck:
    payload = {
        "session_id": SESSION,
        "api_id": API_ID,
        "universal_ticker": "Paper_Spot_BTCUSDT",
        "side": Side.BUY,
        "type": OrderType.LIMIT,
        "qty": Decimal("0.01"),
        "price": Decimal("1000"),
        "client_order_id": CID,
    }
    payload.update(over)
    reply = await broker.request(
        Topics.td_order(API_ID),
        Envelope[OrderSubmit].wrap(
            OrderSubmit.model_validate(payload),
            type=STS_ORDER_SUBMIT,
            source="sts",
            session_id=SESSION,
        ),
        timeout=2.0,
    )
    return OrderAck.model_validate(reply.payload)


async def test_a_submit_records_the_session_that_placed_it(
    attached, broker: Broker, scope
) -> None:
    manager, writer = attached
    ack = await _submit(broker)
    assert ack.accepted

    await writer.flush()

    async with scope() as db:
        row = await OrderRepository(db).get_by_key(API_ID, CID)
    assert row is not None, "the submit path must reach history"
    assert row.session_id == SESSION
    assert row.attribution == Attribution.DIRECT
    assert row.universal_ticker == "Paper_Spot_BTCUSDT"
    assert row.qty == Decimal("0.01")


async def test_a_fill_lands_in_the_session_that_placed_the_order(
    attached, broker: Broker, paper: PaperExchange, scope
) -> None:
    """The join has to hold through the venue, which knows no session at all.

    A real crossing rather than a synthesized fill: the paper engine matches
    against resting orders from other accounts, so a counterparty is what makes
    this the same path production takes.
    """
    manager, writer = attached
    maker = "maker-key"
    paper.register_api(
        maker,
        "maker-secret",
        balances={"BTC": Decimal("1"), "USDT": Decimal("100000")},
    )
    await paper.place_order(
        maker,
        PlaceOrderRequest(
            universal_ticker="Paper_Spot_BTCUSDT",
            side=Side.SELL,
            type=OrderType.LIMIT,
            qty=Decimal("0.01"),
            price=Decimal("50000"),
            client_order_id="maker-1",
        ),
    )

    ack = await _submit(broker, price=Decimal("60000"))
    assert ack.accepted

    fills = []
    for _ in range(40):
        await asyncio.sleep(0.05)
        await writer.flush()
        async with scope() as db:
            fills = await FillRepository(db).replay_for_session(SESSION)
        if fills:
            break

    assert fills, "the fill path must reach history"
    assert all(f.session_id == SESSION for f in fills)
    assert all(f.client_order_id == CID for f in fills)
    assert sum(f.qty for f in fills) == Decimal("0.01")


async def test_trading_survives_a_history_writer_that_cannot_write(
    broker: Broker, paper: PaperExchange
) -> None:
    """History is a bystander. A dead database must cost an order nothing."""

    @asynccontextmanager
    async def broken():
        raise RuntimeError("database is down")
        yield  # pragma: no cover

    writer = HistoryWriter(scope=broken, flush_interval=3600.0)
    manager = SessionManager(
        PaperSessionFactory(broker, paper),
        broker,
        history=writer,
        lease_grace=2.0,
    )
    stop = asyncio.Event()
    pub = asyncio.create_task(_lease_publisher(broker, SESSION, stop))
    await manager.attach(
        TdAttachRequest(
            session_id=SESSION, api_id=API_ID, timeout=2.0, created_by=1
        )
    )
    try:
        ack = await _submit(broker)
        assert ack.accepted, "an unwritable history must not fail a submit"
        await writer.flush()
        assert writer.written == 0
    finally:
        stop.set()
        await asyncio.gather(pub, return_exceptions=True)
        await manager.close_all()


async def test_a_rejected_order_does_not_stay_pending_in_the_record(
    attached, broker: Broker, scope
) -> None:
    """Both reject paths skip ``_publish_order_update``, deliberately.

    The submit publishes its own reject envelope and a second announcement
    would be noise — but history is not an announcement. An order left at the
    state it reached before the refusal sits in the record as pending forever,
    which reads as an order still working rather than one the venue killed.
    """
    manager, writer = attached
    ack = await _submit(broker)
    assert ack.accepted

    session = manager.get(API_ID)
    assert await session.record_rejected(CID) is not None
    await writer.flush()

    async with scope() as db:
        row = await OrderRepository(db).get_by_key(API_ID, CID)
    assert row.status == "rejected"
    assert row.session_id == SESSION, "and it is still the session's order"


async def test_a_reject_off_the_venue_stream_reaches_the_record(
    attached, broker: Broker, scope
) -> None:
    """The other path: accepted by the venue, refused afterwards."""
    manager, writer = attached
    ack = await _submit(broker)
    assert ack.accepted

    session = manager.get(API_ID)
    booked = session.oms.get_order(CID)
    await session._store_then_announce_order(
        booked.model_copy(
            update={"status": OrderStatus.REJECTED, "ts": booked.ts + 1}
        )
    )
    await writer.flush()

    async with scope() as db:
        row = await OrderRepository(db).get_by_key(API_ID, CID)
    assert row.status == "rejected"
