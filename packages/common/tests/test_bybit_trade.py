"""Bybit's order-entry socket — framing, correlation, and refusals."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import pytest
from bybit_stub import API_KEY, API_SECRET, FakeBybit
from mftik.exchange.bybit.protocol import BybitWsError
from mftik.exchange.bybit.trade import BybitTradeSocket


def _trade(stub: FakeBybit, **kwargs: Any) -> BybitTradeSocket:
    return BybitTradeSocket(
        api_key=API_KEY,
        api_secret=API_SECRET,
        url=stub.url,
        ping_interval=0,
        **kwargs,
    )


async def test_an_order_carries_the_category_and_a_header(bybit: FakeBybit) -> None:
    """One connection trades every book, so each call says which."""
    async with _trade(bybit) as trade:
        ack = await trade.place_order(
            category="spot",
            symbol="BTCUSDT",
            side="buy",
            order_type="limit",
            qty=Decimal("0.00100000"),
            price=Decimal("60000"),
            time_in_force="GTC",
            order_link_id="c-42",
        )

    assert ack.order_id == "ord-1"
    frame = bybit.call("order.create")
    args = frame["args"][0]
    assert args["category"] == "spot"
    # Bybit's enums are title-cased and it means it.
    assert args["side"] == "Buy"
    assert args["orderType"] == "Limit"
    # A trailing-zero quantity is refused where the same number without them is
    # taken, because the written precision is checked against the qty step.
    assert args["qty"] == "0.001"
    assert args["orderLinkId"] == "c-42"
    assert frame["header"]["X-BAPI-TIMESTAMP"]
    # The connection's auth replaces the credential — but not the clock.
    assert "X-BAPI-SIGN" not in frame["header"]


async def test_a_spot_market_buy_can_be_told_to_size_in_base(
    bybit: FakeBybit,
) -> None:
    """Bybit reads ``qty`` on one as quote currency unless ``marketUnit`` says
    otherwise, which would spend 50 cents on an order for half a bitcoin."""
    async with _trade(bybit) as trade:
        await trade.place_order(
            category="spot",
            symbol="BTCUSDT",
            side="buy",
            order_type="market",
            qty=Decimal("0.5"),
            market_unit="baseCoin",
        )
    assert bybit.call("order.create")["args"][0]["marketUnit"] == "baseCoin"


async def test_cancel_goes_by_either_id(bybit: FakeBybit) -> None:
    async with _trade(bybit) as trade:
        await trade.cancel_order(
            category="spot", symbol="BTCUSDT", order_link_id="c-42"
        )
        await trade.cancel_order(category="spot", symbol="BTCUSDT", order_id="ord-1")

    frames = [f["args"][0] for f in bybit.frames_for("order.cancel")]
    assert frames[0]["orderLinkId"] == "c-42"
    assert "orderId" not in frames[0]
    assert frames[1]["orderId"] == "ord-1"
    # Bybit needs the symbol either way: an id alone does not identify an order.
    assert all(f["symbol"] == "BTCUSDT" for f in frames)


async def test_a_cancel_with_no_id_never_reaches_the_venue(
    bybit: FakeBybit,
) -> None:
    async with _trade(bybit) as trade:
        with pytest.raises(BybitWsError, match="orderId or orderLinkId"):
            await trade.cancel_order(category="spot", symbol="BTCUSDT")
    assert not bybit.frames_for("order.cancel")


async def test_a_venue_refusal_raises_with_its_code(bybit: FakeBybit) -> None:
    """TD normalizes on the code, so it has to survive the round trip."""
    bybit.errors["order.create"] = (110007, "ab not enough for new order")
    async with _trade(bybit) as trade:
        with pytest.raises(BybitWsError) as exc:
            await trade.place_order(
                category="spot",
                symbol="BTCUSDT",
                side="buy",
                order_type="market",
                qty=Decimal("1"),
            )
    assert exc.value.code == 110007
    assert "not enough" in str(exc.value)


async def test_replies_are_matched_by_id_not_by_arrival_order(
    bybit: FakeBybit,
) -> None:
    """Several orders in flight on one socket is the normal state here."""
    bybit.hold_replies = 3
    async with _trade(bybit) as trade:
        bybit.results["order.create"] = {"orderId": "ord-x", "orderLinkId": "c-x"}
        acks = await asyncio.gather(
            *(
                trade.place_order(
                    category="spot",
                    symbol="BTCUSDT",
                    side="buy",
                    order_type="market",
                    qty=Decimal("1"),
                    order_link_id=f"c-{n}",
                )
                for n in range(3)
            )
        )
    assert [ack.order_id for ack in acks] == ["ord-x"] * 3


async def test_an_order_is_refused_while_the_socket_is_unauthenticated(
    bybit: FakeBybit,
) -> None:
    """An order sent on a socket whose auth is gone is an order we would have
    to ask the venue about afterwards."""
    async with _trade(bybit, reconnect=False) as trade:
        trade._authenticated = False
        with pytest.raises(BybitWsError, match="not authenticated"):
            await trade.place_order(
                category="spot",
                symbol="BTCUSDT",
                side="buy",
                order_type="market",
                qty=Decimal("1"),
            )
    assert not bybit.frames_for("order.create")


async def test_the_socket_reauthenticates_after_a_drop(bybit: FakeBybit) -> None:
    async with _trade(bybit, retry_backoff=0.01) as trade:
        await bybit.drop()
        for _ in range(200):
            if bybit.auths >= 2:
                break
            await asyncio.sleep(0.01)
        assert bybit.auths >= 2
        assert trade.authenticated

        ack = await trade.place_order(
            category="spot",
            symbol="BTCUSDT",
            side="sell",
            order_type="market",
            qty=Decimal("1"),
        )
        assert ack.order_id == "ord-1"
