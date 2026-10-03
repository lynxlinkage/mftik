"""``StrategyOms.view(settled=True)``: the request, the timeout, the refusal.

No bus. The broker here records the call ``view`` makes and hands back
the reply the test chose. A real subject is ``test_settled_subject``.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest
from mftik.broker.errors import RequestTimeoutError
from mftik.exchange.models import Order, OrderStatus, OrderType, Side
from mftik.exchange.oms import OmsView
from mftik.protocol import TD_ERROR, TD_OMS_VIEW, Topics, UntypedEnvelope
from mftik.strategy.oms import ORDER_ACK_TIMEOUT_S, SETTLED_VIEW_TIMEOUT_S, StrategyOms

API = 7
CID = "101"


class _Broker:
    def __init__(
        self,
        reply: UntypedEnvelope | None = None,
        exc: BaseException | None = None,
    ) -> None:
        self.reply = reply
        self.exc = exc
        self.calls: list[tuple[str, UntypedEnvelope, float]] = []

    async def request(
        self, subject: str, envelope: UntypedEnvelope, *, timeout: float
    ) -> UntypedEnvelope:
        self.calls.append((subject, envelope, timeout))
        if self.exc is not None:
            raise self.exc
        assert self.reply is not None
        return self.reply


def _oms(broker: _Broker, *api_ids: int) -> StrategyOms:
    oms = StrategyOms()
    session = SimpleNamespace(
        broker=broker,
        td_api_ids=list(api_ids),
        session_id="abc123",
        strategy=SimpleNamespace(name="quiet", registry_key="quiet"),
    )
    oms.bind(SimpleNamespace(session=session))  # type: ignore[arg-type]
    return oms


def _order() -> Order:
    return Order(
        client_order_id=CID,
        universal_ticker="Paper_Spot_BTCUSDT",
        side=Side.BUY,
        type=OrderType.LIMIT,
        qty=Decimal("1"),
        price=Decimal("10"),
        status=OrderStatus.NEW,
    )


def test_the_settled_timeout_is_the_td_wait_plus_a_margin() -> None:
    """30 and 5 are literals. The SDK does not import the TD package."""
    assert SETTLED_VIEW_TIMEOUT_S == 35.0
    assert ORDER_ACK_TIMEOUT_S == 2.0


async def test_a_settled_view_sends_settled_and_waits_out_the_td_budget() -> None:
    order = _order()
    broker = _Broker(
        UntypedEnvelope.wrap(
            OmsView(orders={CID: order}).model_dump(mode="json"),
            type=TD_OMS_VIEW,
            source="td",
        )
    )
    oms = _oms(broker, API)

    view = await oms.view(API, settled=True)

    assert set(view.orders) == {CID}
    assert view.orders[CID].status is OrderStatus.NEW
    subject, envelope, timeout = broker.calls[0]
    assert subject == Topics.td_account(API)
    assert envelope.type == TD_OMS_VIEW
    payload = envelope.payload
    assert payload.settled is True  # type: ignore[attr-defined]
    assert payload.api_id == API  # type: ignore[attr-defined]
    assert timeout == SETTLED_VIEW_TIMEOUT_S


async def test_an_unsettled_view_keeps_the_ack_timeout() -> None:
    broker = _Broker(
        UntypedEnvelope.wrap(
            OmsView().model_dump(mode="json"),
            type=TD_OMS_VIEW,
            source="td",
        )
    )
    oms = _oms(broker, API)

    view = await oms.view(API)

    assert view.orders == {}
    _subject, envelope, timeout = broker.calls[0]
    assert envelope.payload.settled is False  # type: ignore[attr-defined]
    assert timeout == ORDER_ACK_TIMEOUT_S


async def test_a_td_error_is_not_an_empty_book() -> None:
    """``{code, message}`` validates as an empty ``OmsView``. That is a miss."""
    broker = _Broker(
        UntypedEnvelope.wrap(
            {"code": "107", "message": "venue is not connected"},
            type=TD_ERROR,
            source="td",
        )
    )
    oms = _oms(broker, API)

    with pytest.raises(RuntimeError, match="107: venue is not connected"):
        await oms.view(API, settled=True)
    with pytest.raises(RuntimeError, match="107: venue is not connected"):
        await oms.view(API, settled=False)

    assert [call[2] for call in broker.calls] == [
        SETTLED_VIEW_TIMEOUT_S,
        ORDER_ACK_TIMEOUT_S,
    ]


async def test_a_settled_view_timeout_propagates() -> None:
    """Same exception an unsettled read already lets out. It is not swallowed."""
    timeout = RequestTimeoutError(
        Topics.td_account(API), "req-1", SETTLED_VIEW_TIMEOUT_S
    )
    broker = _Broker(exc=timeout)
    oms = _oms(broker, API)

    with pytest.raises(RequestTimeoutError) as raised:
        await oms.view(API, settled=True)

    assert raised.value is timeout


async def test_an_unresolved_account_does_not_ask() -> None:
    """No account attached, and none named: there is no subject to wait on."""
    broker = _Broker()
    oms = _oms(broker)

    view = await oms.view(settled=True)

    assert view.orders == {}
    assert broker.calls == []
