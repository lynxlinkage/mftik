"""Paper ``td.order`` / unsettled views, called on the handler."""

from __future__ import annotations

from decimal import Decimal

import pytest
from mftik.exchange import PaperExchange, Side
from mftik.exchange.models import OrderType, limit_order
from mftik.protocol import (
    STS_ORDER_SUBMIT,
    TD_ORDER_ACK,
    Envelope,
    OrderAck,
    OrderCancel,
    OrderSubmit,
    RejectCode,
    TdLedgerViewRequest,
    TdOmsViewRequest,
    UntypedEnvelope,
)
from mftik_td.account import TICKET, AccountWorker
from mftik_td.account.session import Session

API = 7
SESSION = "sess"


class _Quiet:
    async def publish(self, subject: str, envelope: object) -> None:
        return None


def _submit(**overrides: object) -> OrderSubmit:
    payload: dict[str, object] = {
        "session_id": SESSION,
        "api_id": API,
        "universal_ticker": "Paper_Spot_BTCUSDT",
        "side": Side.BUY,
        "type": OrderType.LIMIT,
        "qty": Decimal("2"),
        "price": Decimal("49000"),
        "client_order_id": "cid-fill",
    }
    payload.update(overrides)
    return OrderSubmit(**payload)  # type: ignore[arg-type]


async def _started() -> tuple[AccountWorker, PaperExchange]:
    exchange = PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")},
        tick_interval=60,
    )
    exchange.register_api(
        "paper-key",
        "paper-secret",
        balances={"USDT": Decimal("1000000")},
    )
    await exchange.start()
    private = exchange.private(
        api_key="paper-key",
        api_secret="paper-secret",
        auto_register=False,
    )
    session = Session(
        api_id=API,
        broker=_Quiet(),  # type: ignore[arg-type]
        private=private,
    )
    worker = AccountWorker(
        API, venue="Paper", incarnation=1, private=private, session=session
    )
    await worker.resident.start()
    await worker.trading.activate()
    exchange.register_api(
        "maker-key",
        "maker-secret",
        balances={"BTC": Decimal("10"), "USDT": Decimal("1000000")},
    )
    maker = exchange.private(
        api_key="maker-key",
        api_secret="maker-secret",
        auto_register=False,
    )
    await maker.connect()
    await maker.place_order(
        limit_order(
            ticker="Paper_Spot_BTCUSDT",
            side=Side.SELL,
            qty=Decimal("1"),
            price=Decimal("49000"),
        )
    )
    return worker, exchange


async def _stop(worker: AccountWorker, exchange: PaperExchange) -> None:
    if worker.trading.active:
        await worker.trading.deactivate()
    await exchange.stop()


@pytest.mark.component
@pytest.mark.real_sleep(reason="Session.start arms a sweep that sleeps")
async def test_paper_submit_cancel_refusal_and_the_book_after_a_fill() -> None:
    worker, exchange = await _started()
    try:
        reply = await worker.orders(
            UntypedEnvelope.model_validate_json(
                Envelope[OrderSubmit]
                .wrap(
                    _submit(),
                    type=STS_ORDER_SUBMIT,
                    source="test",
                    session_id=SESSION,
                )
                .to_json()
            )
        )
        assert reply is not None
        assert reply.type == TD_ORDER_ACK
        ack = OrderAck.model_validate(reply.payload)
        assert ack.accepted is True
        assert ack.client_order_id == "cid-fill"

        view = await worker.oms.view(TdOmsViewRequest(api_id=API))
        booked = view.orders["cid-fill"]
        assert booked.filled_qty > 0
        assert booked.client_order_id == "cid-fill"

        cancelled = await worker.orders.cancel(
            OrderCancel(session_id=SESSION, api_id=API, client_order_id="cid-fill")
        )
        assert cancelled.accepted is True
        assert cancelled.client_order_id == "cid-fill"

        wrong = await worker.orders.submit(
            _submit(api_id=API + 1, client_order_id="cid-wrong")
        )
        assert wrong.accepted is False
        assert wrong.error_code == RejectCode.TD_WRONG_API_ID
        assert wrong.client_order_id == "cid-wrong"

        reduced = await worker.orders.submit(
            _submit(
                reduce_only=True,
                client_order_id="cid-reduce",
                qty=Decimal("0.01"),
                price=Decimal("1"),
            )
        )
        assert reduced.accepted is False
        assert reduced.error_code == RejectCode.TD_REDUCE_ONLY_UNSUPPORTED

        other_venue = await worker.orders.submit(
            _submit(
                universal_ticker="Binance_Spot_BTCUSDT",
                client_order_id="cid-venue",
                qty=Decimal("0.01"),
                price=Decimal("1"),
            )
        )
        assert other_venue.accepted is False
        assert other_venue.error_code == RejectCode.TD_WRONG_INSTRUMENT
        assert other_venue.client_order_id == "cid-venue"

        ledger = await worker.ledger.view(TdLedgerViewRequest(api_id=API))
        assert ledger.api_id == API
        with pytest.raises(NotImplementedError, match=TICKET):
            await worker.oms.view(TdOmsViewRequest(api_id=API, settled=True))
    finally:
        await _stop(worker, exchange)
