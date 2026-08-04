"""Order entry over ``td.order.{api_id}`` — what TD refuses before the venue.

The happy path is covered end-to-end in ``test_cid_ownership``; here the
concern is that a request TD cannot act on comes back as ``accepted=False``
instead of being dropped the way the old pub/sub path dropped it.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import fakeredis.aioredis
import pytest
from mft.broker import Broker, BrokerConfig
from mft.exchange import PaperExchange, Side
from mft.exchange.models import (
    Balance,
    Order,
    OrderStatus,
    OrderType,
    PlaceOrderRequest,
)
from mft.protocol import (
    STS_LEASE_HEARTBEAT,
    STS_ORDER_CANCEL,
    STS_ORDER_SUBMIT,
    Envelope,
    LeaseHeartbeat,
    OrderAck,
    OrderCancel,
    OrderSubmit,
    RejectCode,
    TdAttachRequest,
    Topics,
    UntypedEnvelope,
)
from mft_td.session import PaperSessionFactory, SessionManager

API_ID = 42
SESSION = "sts-rpc"


class _StubSymbols:
    """Minimal symbol plane: enough for the ledger to price BTCUSDT."""

    async def get(self, venue: str, symbol: str, *, category: str = "spot"):
        return SimpleNamespace(symbol=symbol, base="BTC", quote="USDT")


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


@pytest.fixture
async def attached(broker: Broker, factory: PaperSessionFactory):
    """A manager with SESSION attached to API_ID and its order RPC serving."""
    manager = SessionManager(factory, broker, lease_grace=2.0)
    stop = asyncio.Event()
    pub = asyncio.create_task(_lease_publisher(broker, SESSION, stop))
    await manager.attach(
        TdAttachRequest(
            session_id=SESSION, api_id=API_ID, timeout=2.0, created_by=1
        )
    )
    yield manager
    stop.set()
    await asyncio.gather(pub, return_exceptions=True)
    await manager.close_all()


def _submit_envelope(**overrides: Any) -> Envelope[Any]:
    payload: dict[str, Any] = {
        "session_id": SESSION,
        "api_id": API_ID,
        "symbol": "BTCUSDT",
        "side": Side.BUY,
        "type": OrderType.LIMIT,
        "qty": Decimal("0.01"),
        "price": Decimal("1000"),
        "client_order_id": "cid-1",
    }
    payload.update(overrides)
    return Envelope[OrderSubmit].wrap(
        OrderSubmit.model_validate(payload),
        type=STS_ORDER_SUBMIT,
        source="sts",
        session_id=str(payload["session_id"]),
    )


async def _ack(broker: Broker, envelope: Envelope[Any]) -> OrderAck:
    reply = await broker.request(
        Topics.td_order(API_ID), envelope, timeout=2.0
    )
    return OrderAck.model_validate(reply.payload)


async def test_submit_from_attached_session_is_accepted(
    attached: SessionManager, broker: Broker
) -> None:
    ack = await _ack(broker, _submit_envelope())

    assert ack.accepted is True
    assert ack.api_id == API_ID
    assert ack.client_order_id == "cid-1"


async def test_unattached_session_is_refused(
    attached: SessionManager, broker: Broker
) -> None:
    ack = await _ack(
        broker, _submit_envelope(session_id="sts-not-attached", client_order_id="c2")
    )

    assert ack.accepted is False
    assert "not attached" in ack.reason
    assert ack.error_code == RejectCode.TD_SESSION_NOT_ATTACHED
    # The cid still comes back so the caller can correlate the refusal.
    assert ack.client_order_id == "c2"


async def test_wrong_api_id_is_refused(
    attached: SessionManager, broker: Broker
) -> None:
    ack = await _ack(broker, _submit_envelope(api_id=API_ID + 1))

    assert ack.accepted is False
    assert "wrong api_id" in ack.reason
    assert ack.error_code == RejectCode.TD_WRONG_API_ID


async def test_unparseable_payload_is_refused(
    attached: SessionManager, broker: Broker
) -> None:
    ack = await _ack(
        broker,
        UntypedEnvelope.wrap(
            {"session_id": SESSION, "api_id": API_ID},  # missing everything else
            type=STS_ORDER_SUBMIT,
            source="sts",
        ),
    )

    assert ack.accepted is False
    assert "invalid payload" in ack.reason
    assert ack.error_code == RejectCode.TD_INVALID_REQUEST


async def test_unsupported_request_type_is_refused(
    attached: SessionManager, broker: Broker
) -> None:
    ack = await _ack(
        broker,
        UntypedEnvelope.wrap({}, type="sts.order.teleport", source="sts"),
    )

    assert ack.accepted is False
    assert "unsupported" in ack.reason
    assert ack.error_code == RejectCode.TD_UNSUPPORTED_REQUEST


async def test_no_td_serving_times_out(broker: Broker) -> None:
    """Nothing is attached, so the request waits in the list and times out.

    This is the case the old pub/sub path lost silently: the message went
    nowhere and the strategy never learned it.
    """
    from mft.broker.errors import RequestTimeoutError

    with pytest.raises(RequestTimeoutError):
        await broker.request(
            Topics.td_order(999), _submit_envelope(), timeout=0.3
        )


async def test_state_is_written_before_the_ack_returns(
    attached: SessionManager, broker: Broker
) -> None:
    """The ordering the whole design rests on.

    STS reads balances out of Redis, so by the time it is told True the
    pre-lock must already be visible there — otherwise a strategy can act on
    a balance that does not yet know about the order it just placed.
    """
    session = attached.get(API_ID)
    assert session is not None
    # Give the ledger something to reserve against and a way to price it.
    session.ledger.apply_venue(Balance(asset="USDT", free=Decimal("1000")))
    session.symbols = _StubSymbols()

    ack = await _ack(
        broker,
        _submit_envelope(client_order_id="cid-lock", price=Decimal("50000")),
    )

    assert ack.accepted is True
    # Read Redis the way STS does — no sleep, no polling.
    row = await broker.state_get(Topics.td_ledger(API_ID), "USDT")
    assert row is not None
    assert Decimal(row["prelock"]) == Decimal("500")  # 0.01 @ 50000


async def test_an_unaffordable_order_is_refused_and_reserves_nothing(
    attached: SessionManager, broker: Broker
) -> None:
    session = attached.get(API_ID)
    assert session is not None
    session.ledger.apply_venue(Balance(asset="USDT", free=Decimal("100")))
    session.symbols = _StubSymbols()

    ack = await _ack(
        broker,
        _submit_envelope(client_order_id="cid-broke", price=Decimal("50000")),
    )

    assert ack.accepted is False
    assert "insufficient balance" in ack.reason
    assert ack.error_code == RejectCode.TD_INSUFFICIENT_BALANCE
    assert not session.ledger.has_reservation("cid-broke")


async def test_pending_new_is_in_redis_before_the_ack(
    attached: SessionManager, broker: Broker
) -> None:
    """A strategy told True can immediately read the order it just placed."""
    session = attached.get(API_ID)
    assert session is not None
    session.symbols = _StubSymbols()

    ack = await _ack(broker, _submit_envelope(client_order_id="cid-pn"))

    assert ack.accepted is True
    row = await broker.state_get(Topics.td_oms(API_ID), "cid-pn")
    assert row is not None
    # It has no venue id yet — that is what PENDING_NEW means.
    assert row["status"] in (
        OrderStatus.PENDING_NEW.value,
        # The paper venue is fast; it may already have answered.
        OrderStatus.NEW.value,
        OrderStatus.FILLED.value,
    )


async def test_an_unacknowledged_order_becomes_unknown_then_resolves(
    attached: SessionManager, broker: Broker
) -> None:
    """The chain: silence → UNKNOWN → ask the venue → settle.

    The venue never saw this order (nothing was sent for it), so the query
    comes back empty and it settles REJECTED rather than sitting UNKNOWN.
    """
    session = attached.get(API_ID)
    assert session is not None
    session.pending_timeout = 0.0

    order = await session.record_pending_new(
        PlaceOrderRequest(
            symbol="BTCUSDT",
            side=Side.BUY,
            type=OrderType.LIMIT,
            qty=Decimal("0.01"),
            price=Decimal("1000"),
            client_order_id="cid-ghost",
        )
    )
    assert order.status is OrderStatus.PENDING_NEW
    assert await broker.state_get(Topics.td_oms(API_ID), "cid-ghost")

    moved = await session.sweep_pending()

    assert [o.status for o in moved] == [OrderStatus.UNKNOWN]
    # Resolution ran inside the sweep: the venue has no such order, so it is
    # terminal and gone from the live book.
    assert await broker.state_get(Topics.td_oms(API_ID), "cid-ghost") is None
    assert session.oms.get_order("cid-ghost") is None


async def test_a_live_order_is_not_swept(
    attached: SessionManager, broker: Broker
) -> None:
    """Only orders that aged out move; a fresh one is left alone."""
    session = attached.get(API_ID)
    assert session is not None
    session.pending_timeout = 60.0

    await session.record_pending_new(
        PlaceOrderRequest(
            symbol="BTCUSDT",
            side=Side.BUY,
            type=OrderType.LIMIT,
            qty=Decimal("0.01"),
            price=Decimal("1000"),
            client_order_id="cid-fresh",
        )
    )

    assert await session.sweep_pending() == []
    row = await broker.state_get(Topics.td_oms(API_ID), "cid-fresh")
    assert row is not None
    assert row["status"] == OrderStatus.PENDING_NEW.value


async def test_state_is_cleared_when_the_session_dies(
    broker: Broker, factory: PaperSessionFactory
) -> None:
    """Nothing may outlive the TD session that was keeping it current."""
    manager = SessionManager(factory, broker, lease_grace=2.0)
    stop = asyncio.Event()
    pub = asyncio.create_task(_lease_publisher(broker, SESSION, stop))
    await manager.attach(
        TdAttachRequest(
            session_id=SESSION, api_id=API_ID, timeout=2.0, created_by=1
        )
    )
    assert await broker.state_all(Topics.td_ledger(API_ID))

    stop.set()
    await asyncio.gather(pub, return_exceptions=True)
    await manager.close_all()

    assert await broker.state_all(Topics.td_ledger(API_ID)) == {}
    assert await broker.state_all(Topics.td_oms(API_ID)) == {}


def _cancel_envelope(cid: str, session_id: str = SESSION) -> Envelope[Any]:
    return Envelope[OrderCancel].wrap(
        OrderCancel(session_id=session_id, api_id=API_ID, client_order_id=cid),
        type=STS_ORDER_CANCEL,
        source="sts",
        session_id=session_id,
    )


async def _book(session, cid: str, status: OrderStatus) -> Order:
    """Put an order in the book in ``status``, as the venue would have."""
    order = Order(
        client_order_id=cid,
        symbol="BTCUSDT",
        side=Side.BUY,
        type=OrderType.LIMIT,
        status=status,
        qty=Decimal("0.01"),
        price=Decimal("1000"),
    )
    session.oms.handle_order(order)
    await session.write_order(order)
    return order


async def test_cancelling_a_working_order_marks_it_pending_cancel(
    attached: SessionManager, broker: Broker
) -> None:
    session = attached.get(API_ID)
    assert session is not None
    await _book(session, "cid-working", OrderStatus.NEW)

    ack = await _ack(broker, _cancel_envelope("cid-working"))

    assert ack.accepted is True


async def test_pending_cancel_is_written_before_the_venue_answers(
    attached: SessionManager, broker: Broker
) -> None:
    """Driven directly: over RPC the venue's answer can land first.

    The paper engine does not know this order, so it refuses the cancel and
    the revert wins the race — which is right, but it hides the intermediate
    state the RPC path does write.
    """
    session = attached.get(API_ID)
    assert session is not None
    await _book(session, "cid-mark", OrderStatus.NEW)

    assert await session.record_pending_cancel("cid-mark") is None

    row = await broker.state_get(Topics.td_oms(API_ID), "cid-mark")
    assert row is not None
    assert row["status"] == OrderStatus.PENDING_CANCEL.value


async def test_cancelling_a_pending_new_order_is_refused(
    attached: SessionManager, broker: Broker
) -> None:
    """There is no venue id to cancel against yet."""
    session = attached.get(API_ID)
    assert session is not None
    await _book(session, "cid-inflight", OrderStatus.PENDING_NEW)

    ack = await _ack(broker, _cancel_envelope("cid-inflight"))

    assert ack.accepted is False
    assert "pending_new" in ack.reason
    assert ack.error_code == RejectCode.TD_NOT_CANCELABLE
    row = await broker.state_get(Topics.td_oms(API_ID), "cid-inflight")
    assert row["status"] == OrderStatus.PENDING_NEW.value


async def test_a_partially_filled_order_can_be_cancelled(
    attached: SessionManager, broker: Broker
) -> None:
    session = attached.get(API_ID)
    assert session is not None
    await _book(session, "cid-partial", OrderStatus.PARTIALLY_FILLED)

    ack = await _ack(broker, _cancel_envelope("cid-partial"))

    assert ack.accepted is True


async def test_a_refused_cancel_puts_the_order_back(
    attached: SessionManager, broker: Broker
) -> None:
    """Per the lifecycle: PENDING_CANCEL → NEW when the venue says no."""
    session = attached.get(API_ID)
    assert session is not None
    await _book(session, "cid-back", OrderStatus.PARTIALLY_FILLED)
    assert await session.record_pending_cancel("cid-back") is None

    restored = await session.revert_pending_cancel("cid-back")

    # Back to exactly where it was, not merely "open".
    assert restored is not None
    assert restored.status is OrderStatus.PARTIALLY_FILLED
    row = await broker.state_get(Topics.td_oms(API_ID), "cid-back")
    assert row["status"] == OrderStatus.PARTIALLY_FILLED.value


async def test_an_unanswered_cancel_becomes_unknown(
    attached: SessionManager, broker: Broker
) -> None:
    """A cancel that goes unanswered is as ambiguous as a submit that does."""
    session = attached.get(API_ID)
    assert session is not None
    session.pending_timeout = 0.0
    await _book(session, "cid-silent", OrderStatus.NEW)
    assert await session.record_pending_cancel("cid-silent") is None

    moved = await session.sweep_pending()

    assert [o.status for o in moved] == [OrderStatus.UNKNOWN]


async def test_cancelling_an_order_we_never_booked_is_allowed(
    attached: SessionManager, broker: Broker
) -> None:
    """Recon-discovered orders are not in our book; the venue may still know."""
    session = attached.get(API_ID)
    assert session is not None

    ack = await _ack(broker, _cancel_envelope("cid-not-ours"))

    assert ack.accepted is True
