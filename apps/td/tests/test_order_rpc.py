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
from mftik.broker import Broker, BrokerConfig
from mftik.exchange import PaperExchange, Side
from mftik.exchange.models import (
    Balance,
    Order,
    OrderStatus,
    OrderType,
    PlaceOrderRequest,
)
from mftik.protocol import (
    STS_LEASE_HEARTBEAT,
    STS_ORDER_CANCEL,
    STS_ORDER_SUBMIT,
    TD_ORDER_UPDATE,
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
from mftik_td.session import PaperSessionFactory, SessionManager

API_ID = 42
SESSION = "sts-rpc"


class _StubSymbols:
    """Minimal symbol plane: enough for the ledger to price BTCUSDT."""

    async def get(self, ticker):  # noqa: ANN001
        return SimpleNamespace(symbol=ticker.symbol, base="BTC", quote="USDT")


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
        "universal_ticker": "Paper_Spot_BTCUSDT",
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
    from mftik.broker.errors import RequestTimeoutError

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
            universal_ticker="Paper_Spot_BTCUSDT",
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
            universal_ticker="Paper_Spot_BTCUSDT",
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
        universal_ticker="Paper_Spot_BTCUSDT",
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


# --- the instrument the order names -----------------------------------------


async def test_an_order_for_another_venue_is_refused_at_the_boundary(
    attached: SessionManager, broker: Broker
) -> None:
    """The check the order path could not make until it carried an instrument.

    ``api_id`` says which account; on a unified venue it does not say which
    book, and it never said which *venue* the strategy meant. MD has always
    made this check on its own feeds — TD could not, because a bare symbol is
    true of every venue at once.
    """
    ack = await _ack(
        broker,
        _submit_envelope(universal_ticker="Binance_Spot_BTCUSDT"),
    )

    assert ack.accepted is False
    assert ack.error_code == RejectCode.TD_WRONG_INSTRUMENT
    assert "Binance" in ack.reason and "Paper" in ack.reason


async def test_a_malformed_ticker_is_refused_rather_than_guessed_at(
    attached: SessionManager, broker: Broker
) -> None:
    """A bare symbol is exactly what this replaced, so it is not a shorthand
    for the session's own venue — it is a strategy that has not been updated,
    and guessing would send a real order somewhere plausible."""
    ack = await _ack(broker, _submit_envelope(universal_ticker="BTCUSDT"))

    assert ack.accepted is False
    assert ack.error_code == RejectCode.TD_WRONG_INSTRUMENT


async def test_reduce_only_on_spot_is_refused_not_dropped(
    attached: SessionManager, broker: Broker
) -> None:
    """Spot has no position to reduce, so the flag cannot mean anything.

    Refused rather than stripped: a caller sets this to be certain an order
    cannot open exposure, and an accepted order with the guarantee quietly
    removed is indistinguishable from a protected one until it flips a
    position that was never supposed to exist.
    """
    ack = await _ack(broker, _submit_envelope(reduce_only=True))

    assert ack.accepted is False
    assert ack.error_code == RejectCode.TD_REDUCE_ONLY_UNSUPPORTED
    assert "spot" in ack.reason


async def test_a_refused_reduce_only_reserves_nothing(
    attached: SessionManager, broker: Broker
) -> None:
    """Checked beside the instrument, so before anything is committed."""
    before = await broker.state_all(Topics.td_ledger(API_ID))

    ack = await _ack(broker, _submit_envelope(reduce_only=True))

    assert ack.accepted is False
    assert await broker.state_all(Topics.td_ledger(API_ID)) == before


async def test_an_ordinary_spot_order_is_unaffected(
    attached: SessionManager, broker: Broker
) -> None:
    """The flag defaults off, and off must change nothing about a spot order."""
    ack = await _ack(broker, _submit_envelope())

    assert ack.accepted is True


async def test_a_refused_instrument_reserves_nothing(
    attached: SessionManager, broker: Broker
) -> None:
    """Refused before the pre-lock: nothing about it can be made to work, so
    committing funds against it would strand them until recon."""
    before = await broker.state_all(Topics.td_ledger(API_ID))
    ack = await _ack(
        broker, _submit_envelope(universal_ticker="Gate_Spot_BTCUSDT")
    )

    assert ack.accepted is False
    assert await broker.state_all(Topics.td_ledger(API_ID)) == before


# --- transport-ambiguous send failures --------------------------------------


async def test_cancel_send_failure_resolves_missing_as_canceled(
    attached: SessionManager, broker: Broker
) -> None:
    """Cancel write fails and the venue has no row → treat as already gone."""
    session = attached.get(API_ID)
    assert session is not None
    await _book(session, "cid-sendfail", OrderStatus.NEW)

    async def boom(_cid: str) -> Order:
        raise RuntimeError("Error UNKNOWN while writing to socket. Connection lost.")

    session.private.cancel_by_client_order_id = boom  # type: ignore[method-assign]

    ack = await _ack(broker, _cancel_envelope("cid-sendfail"))
    assert ack.accepted is True
    await asyncio.sleep(0.2)

    assert session.oms.get_order("cid-sendfail") is None
    assert await broker.state_get(Topics.td_oms(API_ID), "cid-sendfail") is None


async def test_cancel_send_failure_keeps_order_when_venue_still_has_it(
    attached: SessionManager, broker: Broker
) -> None:
    """Cancel write fails but resolve finds the resting order → stay working."""
    session = attached.get(API_ID)
    assert session is not None

    ack = await _ack(
        broker,
        _submit_envelope(
            client_order_id="cid-keep",
            price=Decimal("1"),
        ),
    )
    assert ack.accepted is True
    await asyncio.sleep(0.2)
    assert session.oms.get_order("cid-keep") is not None

    async def boom(_cid: str) -> Order:
        raise RuntimeError("Connection lost")

    session.private.cancel_by_client_order_id = boom  # type: ignore[method-assign]

    ack = await _ack(broker, _cancel_envelope("cid-keep"))
    assert ack.accepted is True
    await asyncio.sleep(0.2)

    order = session.oms.get_order("cid-keep")
    assert order is not None
    assert order.status is OrderStatus.NEW


async def test_submit_send_failure_resolves_missing_as_rejected(
    attached: SessionManager, broker: Broker
) -> None:
    """Submit write fails and the venue never saw it → REJECTED."""
    session = attached.get(API_ID)
    assert session is not None

    async def boom(_req: PlaceOrderRequest) -> Order:
        raise RuntimeError("Connection lost")

    session.private.place_order = boom  # type: ignore[method-assign]

    ack = await _ack(
        broker, _submit_envelope(client_order_id="cid-ghost-send")
    )
    assert ack.accepted is True
    await asyncio.sleep(0.2)

    assert session.oms.get_order("cid-ghost-send") is None
    assert await broker.state_get(Topics.td_oms(API_ID), "cid-ghost-send") is None


async def test_submit_send_failure_stays_unknown_when_resolve_fails(
    attached: SessionManager, broker: Broker
) -> None:
    """Resolve failing on the same dead transport must not publish a reject."""
    session = attached.get(API_ID)
    assert session is not None
    manager = attached

    async def boom_place(_req: PlaceOrderRequest) -> Order:
        raise RuntimeError("Connection lost")

    async def boom_resolve(_cid: str, *, ticker=None) -> Order | None:  # noqa: ANN001
        raise RuntimeError("Connection lost")

    session.private.place_order = boom_place  # type: ignore[method-assign]
    session.private.fetch_order_by_client_order_id = boom_resolve  # type: ignore[method-assign]

    ack = await _ack(
        broker, _submit_envelope(client_order_id="cid-still-unk")
    )
    assert ack.accepted is True
    await asyncio.sleep(0.2)

    order = session.oms.get_order("cid-still-unk")
    assert order is not None
    assert order.status is OrderStatus.UNKNOWN
    assert manager._accounts[API_ID].cid_owner.get("cid-still-unk") == SESSION


async def test_unknown_order_accepts_cancel(
    attached: SessionManager, broker: Broker
) -> None:
    """A cancel retry after transport failure must not be TD_NOT_CANCELABLE."""
    session = attached.get(API_ID)
    assert session is not None
    await _book(session, "cid-unk-cancel", OrderStatus.UNKNOWN)

    ack = await _ack(broker, _cancel_envelope("cid-unk-cancel"))

    assert ack.accepted is True
    assert ack.error_code == RejectCode.NONE


async def test_cancel_ack_settles_without_waiting_on_stream(
    attached: SessionManager, broker: Broker
) -> None:
    """Venue cancel reply must clear PENDING_CANCEL even if the push is lost."""
    session = attached.get(API_ID)
    assert session is not None
    await _book(session, "cid-ack", OrderStatus.NEW)
    assert await session.record_pending_cancel("cid-ack") is None
    assert "cid-ack" in session._cancel_since

    await session.accept_venue_order(
        Order(
            client_order_id="cid-ack",
            universal_ticker="Paper_Spot_BTCUSDT",
            side=Side.BUY,
            type=OrderType.LIMIT,
            status=OrderStatus.CANCELED,
            qty=Decimal("0.01"),
            price=Decimal("1000"),
        )
    )

    assert session.oms.get_order("cid-ack") is None
    assert "cid-ack" not in session._cancel_since
    assert await broker.state_get(Topics.td_oms(API_ID), "cid-ack") is None


async def test_view_for_sts_uses_ledger_balances(
    attached: SessionManager, broker: Broker
) -> None:
    """ReconDone must not ship stale OMS balances from the last full recon."""
    session = attached.get(API_ID)
    assert session is not None
    session.oms._balances = {
        "USDT": Balance(asset="USDT", free=Decimal("1"), locked=Decimal("0")),
    }
    session.ledger.apply_venue(
        Balance(asset="USDT", free=Decimal("99"), locked=Decimal("0"))
    )

    view = session.view_for_sts()

    assert view.balances["USDT"].free == Decimal("99")


async def test_late_resolve_does_not_reinsert_after_reconcile(
    attached: SessionManager, broker: Broker
) -> None:
    """A resolve that finishes after reconnect recon must not resurrect the cid."""
    session = attached.get(API_ID)
    assert session is not None
    await _book(session, "cid-phantom", OrderStatus.UNKNOWN)
    session._remember_unknown("cid-phantom", if_missing=OrderStatus.REJECTED)

    released = asyncio.Event()
    hold = asyncio.Event()

    async def slow_resolve(_cid: str, *, ticker=None) -> Order | None:  # noqa: ANN001
        released.set()
        await hold.wait()
        return Order(
            client_order_id="cid-phantom",
            universal_ticker="Paper_Spot_BTCUSDT",
            side=Side.BUY,
            type=OrderType.LIMIT,
            status=OrderStatus.NEW,
            qty=Decimal("0.01"),
            price=Decimal("1000"),
        )

    session.private.fetch_order_by_client_order_id = slow_resolve  # type: ignore[method-assign]

    task = asyncio.create_task(
        session.resolve_unknown(session.oms.get_order("cid-phantom"))  # type: ignore[arg-type]
    )
    await released.wait()
    # Reconnect-style recon clears the book while resolve is still in flight.
    await session.reconcile()
    assert session.oms.get_order("cid-phantom") is None
    hold.set()
    result = await task

    assert result is None
    assert session.oms.get_order("cid-phantom") is None


async def test_kick_resolve_all_unknown_is_single_flight(
    attached: SessionManager, broker: Broker
) -> None:
    session = attached.get(API_ID)
    assert session is not None
    await _book(session, "cid-sf", OrderStatus.UNKNOWN)
    session._remember_unknown("cid-sf", if_missing=OrderStatus.REJECTED)

    calls = {"n": 0}
    gate = asyncio.Event()

    async def slow_resolve(_cid: str, *, ticker=None) -> Order | None:  # noqa: ANN001
        calls["n"] += 1
        await gate.wait()
        return None

    session.private.fetch_order_by_client_order_id = slow_resolve  # type: ignore[method-assign]

    first = session.kick_resolve_all_unknown()
    second = session.kick_resolve_all_unknown()
    assert first is not None and first is second

    gate.set()
    await first
    # One pass, one lookup — not two concurrent resolve_all runs.
    assert calls["n"] == 1


async def test_chase_unknown_backs_off_between_resolve_attempts(
    attached: SessionManager, broker: Broker
) -> None:
    """A venue that cannot answer must not be asked again every tick."""
    session = attached.get(API_ID)
    assert session is not None
    await _book(session, "cid-bo", OrderStatus.UNKNOWN)
    session._remember_unknown("cid-bo", if_missing=OrderStatus.REJECTED)

    calls = {"n": 0}

    async def dead(_cid: str, *, ticker=None) -> Order | None:  # noqa: ANN001
        calls["n"] += 1
        raise RuntimeError("venue unreachable")

    session.private.fetch_order_by_client_order_id = dead  # type: ignore[method-assign]

    for _ in range(5):
        await session.chase_unknown()
    # The first tick spends the one due attempt; the rest fall inside the
    # backoff it opened. Without one this would be five lookups, and on a
    # real venue five per second for as long as the link stays down.
    assert calls["n"] == 1
    still = session.oms.get_order("cid-bo")
    assert still is not None and still.status is OrderStatus.UNKNOWN

    # Once the backoff elapses the order is chased again — deferred, not dropped.
    session._unknown_next_try["cid-bo"] = asyncio.get_running_loop().time() - 1
    await session.chase_unknown()
    assert calls["n"] == 2


async def test_forced_recon_is_rate_limited_while_unknown_persists(
    attached: SessionManager, broker: Broker
) -> None:
    """``unknown_force_recon`` is an age — it must not mean "every tick"."""
    session = attached.get(API_ID)
    assert session is not None

    async def dead(_cid: str, *, ticker=None) -> Order | None:  # noqa: ANN001
        raise RuntimeError("venue unreachable")

    session.private.fetch_order_by_client_order_id = dead  # type: ignore[method-assign]

    recons = {"n": 0}
    real_fetch = session.private.fetch_open_orders

    async def counted(symbol: str | None = None) -> list[Order]:
        recons["n"] += 1
        return await real_fetch(symbol)

    session.private.fetch_open_orders = counted  # type: ignore[method-assign]

    async def _arm(cid: str) -> None:
        await _book(session, cid, OrderStatus.UNKNOWN)
        session._remember_unknown(cid, if_missing=OrderStatus.REJECTED)
        session._unknown_since[cid] = (
            asyncio.get_running_loop().time() - session.unknown_force_recon - 1
        )

    await _arm("cid-cd")
    await session.chase_unknown()
    assert recons["n"] == 1

    # Re-arm an equally stale UNKNOWN — the first recon cleared the book, so
    # this proves the cooldown is what stops the second pass, not an empty one.
    await _arm("cid-cd2")
    await session.chase_unknown()
    assert recons["n"] == 1

    session._last_force_recon = float("-inf")
    await session.chase_unknown()
    assert recons["n"] == 2


async def test_recon_announces_unknown_the_venue_no_longer_lists(
    attached: SessionManager, broker: Broker
) -> None:
    """A silently dropped order leaves STS holding a leg it can never close.

    ``apply_reconcile`` replaces the book wholesale, so the cid just vanishes.
    STS drops its leg on a terminal update and on nothing else.
    """
    session = attached.get(API_ID)
    assert session is not None
    await _book(session, "cid-drop", OrderStatus.UNKNOWN)
    session._remember_unknown("cid-drop", if_missing=OrderStatus.CANCELED)

    got: asyncio.Future[dict[str, Any]] = (
        asyncio.get_running_loop().create_future()
    )
    stop = asyncio.Event()

    async def _listen() -> None:
        async for env in broker.subscribe(Topics.td_global(API_ID), stop=stop):
            payload = env.payload
            if (
                env.type == TD_ORDER_UPDATE
                and payload.get("client_order_id") == "cid-drop"
                and not got.done()
            ):
                got.set_result(payload)
                stop.set()
                return

    listener = asyncio.create_task(_listen())
    await asyncio.sleep(0.05)

    # The paper venue has never heard of cid-drop, so recon settles it.
    await session.reconcile()

    payload = await asyncio.wait_for(got, timeout=2.0)
    assert payload["status"] == OrderStatus.CANCELED.value
    assert session.oms.get_order("cid-drop") is None

    stop.set()
    await asyncio.gather(listener, return_exceptions=True)


async def test_venue_ack_does_not_resurrect_a_finished_order(
    attached: SessionManager, broker: Broker
) -> None:
    """A cancel ack that lost the race must not put the order back live."""
    session = attached.get(API_ID)
    assert session is not None
    # The book already finished with it — a fill landed while the cancel
    # call was still in flight, so handle_order popped it.
    assert session.oms.get_order("cid-gone") is None

    ack = Order(
        client_order_id="cid-gone",
        universal_ticker="Paper_Spot_BTCUSDT",
        side=Side.BUY,
        type=OrderType.LIMIT,
        status=OrderStatus.PENDING_CANCEL,
        qty=Decimal("0.01"),
        price=Decimal("1000"),
    )
    await session.accept_venue_order(ack)

    # No stream will ever terminate a resurrected order again.
    assert session.oms.get_order("cid-gone") is None


async def test_venue_ack_does_not_regress_filled_qty(
    attached: SessionManager, broker: Broker
) -> None:
    """Bybit's cancel ack echoes a cached order, so it can predate a fill."""
    session = attached.get(API_ID)
    assert session is not None
    live = await _book(session, "cid-pf", OrderStatus.PARTIALLY_FILLED)
    session.oms.handle_order(
        live.model_copy(update={"filled_qty": Decimal("0.004")})
    )

    stale = live.model_copy(
        update={
            "status": OrderStatus.PENDING_CANCEL,
            "filled_qty": Decimal("0"),
        }
    )
    await session.accept_venue_order(stale)

    booked = session.oms.get_order("cid-pf")
    assert booked is not None
    # Outcome from the ack, fills from the stream.
    assert booked.status is OrderStatus.PENDING_CANCEL
    assert booked.filled_qty == Decimal("0.004")


# --- reduce_only, on the markets that have positions -------------------------


def test_reduce_only_passes_on_a_contract_ticker() -> None:
    """The spot refusal is about spot, not about the flag.

    Driven directly rather than through the RPC above, whose account is Paper —
    a spot-only venue, so it cannot answer the question this asks.
    """
    from mftik_td.session.manager import _reduce_only_unsupported

    for ticker in ("BinanceFuture_Perp_BTCUSDT", "Bybit_Perp_BTCUSDT"):
        payload = OrderSubmit.model_validate(
            {
                "session_id": SESSION,
                "api_id": API_ID,
                "universal_ticker": ticker,
                "side": Side.BUY,
                "type": OrderType.LIMIT,
                "qty": Decimal("1"),
                "price": Decimal("1000"),
                "client_order_id": "cid-1",
                "reduce_only": True,
            }
        )
        assert _reduce_only_unsupported(payload) is None


def test_a_malformed_ticker_is_left_to_the_instrument_check() -> None:
    """One refusal per fault. Answering here would report the wrong reason."""
    from mftik_td.session.manager import _reduce_only_unsupported

    payload = OrderSubmit.model_validate(
        {
            "session_id": SESSION,
            "api_id": API_ID,
            "universal_ticker": "BTCUSDT",
            "side": Side.BUY,
            "type": OrderType.LIMIT,
            "qty": Decimal("1"),
            "price": Decimal("1000"),
            "client_order_id": "cid-1",
            "reduce_only": True,
        }
    )
    assert _reduce_only_unsupported(payload) is None
