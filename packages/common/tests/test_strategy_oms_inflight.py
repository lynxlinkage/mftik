"""Strategy OMS refuses cancel while a submit or cancel is still inflight."""

from __future__ import annotations

from decimal import Decimal

import pytest
from mftik.exchange.models import Order, OrderStatus, OrderType, Side
from mftik.protocol import RejectCode
from mftik.strategy.oms import StrategyOms


def _order(cid: str, status: OrderStatus) -> Order:
    return Order(
        client_order_id=cid,
        universal_ticker="Paper_Spot_BTCUSDT",
        side=Side.BUY,
        type=OrderType.LIMIT,
        status=status,
        qty=Decimal("1"),
    )


def test_pending_is_what_blocks_cancel() -> None:
    assert OrderStatus.PENDING_NEW.is_pending()
    assert OrderStatus.PENDING_CANCEL.is_pending()
    assert not OrderStatus.NEW.is_pending()


@pytest.mark.asyncio
async def test_submit_mark_blocks_cancel_until_live() -> None:
    oms = StrategyOms()
    oms._inflight.add("cid-1")
    assert oms.is_inflight("cid-1")

    accepted = await oms.cancel_order(1, "cid-1")
    assert accepted is False
    assert oms.last_reject_code == RejectCode.TD_NOT_CANCELABLE
    assert "inflight" in oms.last_reject_reason

    oms.note_order(_order("cid-1", OrderStatus.NEW))
    assert not oms.is_inflight("cid-1")


def test_note_order_keeps_pending_statuses() -> None:
    oms = StrategyOms()
    oms.note_order(_order("cid-1", OrderStatus.PENDING_NEW))
    assert oms.is_inflight("cid-1")
    oms.note_order(_order("cid-1", OrderStatus.PENDING_CANCEL))
    assert oms.is_inflight("cid-1")
    oms.note_order(_order("cid-1", OrderStatus.UNKNOWN))
    assert not oms.is_inflight("cid-1")


def test_determined_reject_clears_inflight() -> None:
    oms = StrategyOms()
    oms._inflight.add("cid-1")
    oms.note_reject(RejectCode.VENUE_POST_ONLY_WOULD_CROSS, "cid-1")
    assert not oms.is_inflight("cid-1")


def test_transport_reject_keeps_inflight() -> None:
    oms = StrategyOms()
    oms._inflight.add("cid-1")
    oms.note_reject(RejectCode.TD_SEND_FAILED, "cid-1")
    assert oms.is_inflight("cid-1")


def test_late_pending_new_does_not_revive_a_rejected_cid() -> None:
    oms = StrategyOms()
    oms._inflight.add("cid-1")
    oms.note_reject(RejectCode.VENUE_INSUFFICIENT_BALANCE, "cid-1")
    assert not oms.is_inflight("cid-1")
    oms.note_order(_order("cid-1", OrderStatus.PENDING_NEW))
    assert not oms.is_inflight("cid-1")


def test_ack_mark_does_not_revive_a_settled_cid() -> None:
    """Reject can land before submit_order marks the cid inflight."""
    oms = StrategyOms()
    oms.note_gone("cid-1")
    oms._mark_inflight("cid-1")
    assert not oms.is_inflight("cid-1")
