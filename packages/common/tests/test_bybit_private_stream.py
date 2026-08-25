"""Bybit's private stream — auth, subscriptions, pushes, reconnect.

TD's report path. Bybit's order acks carry no state, so everything the platform
knows about an order after it is sent comes down this socket.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import pytest
from bybit_stub import API_KEY, API_SECRET, FakeBybit
from mftik.exchange.bybit.account import BybitPrivateStream
from mftik.exchange.bybit.protocol import BybitAuthError, BybitWsError
from mftik.exchange.errors import ExchangeNotConnectedError
from mftik.exchange.models import OrderStatus, Side
from mftik.exchange.tickers import UniversalTicker

#: The instrument every payload in this module is stamped with.
TICKER = UniversalTicker.parse("Bybit_Spot_BTCUSDT")

ORDER_ROW = {
    "category": "spot",
    "symbol": "BTCUSDT",
    "orderId": "ord-1",
    "orderLinkId": "c-42",
    "side": "Buy",
    "orderType": "Limit",
    "orderStatus": "New",
    "price": "60000",
    "qty": "0.001",
    "cumExecQty": "0",
    "cumExecValue": "0",
    "cumExecFee": "0",
    "avgPrice": "",
    "leavesQty": "0.001",
    "timeInForce": "GTC",
    "createdTime": "1700000000000",
    "updatedTime": "1700000000100",
}

EXECUTION_ROW = {
    "category": "spot",
    "symbol": "BTCUSDT",
    "orderId": "ord-1",
    "orderLinkId": "c-42",
    "execId": "exec-1",
    "side": "Buy",
    "execPrice": "60000",
    "execQty": "0.0004",
    "execValue": "24",
    "execFee": "0.024",
    "execType": "Trade",
    "execTime": "1700000000200",
    "feeCurrency": "USDT",
    "isMaker": True,
    "leavesQty": "0.0006",
}

WALLET_ROW = {
    "accountType": "UNIFIED",
    "totalEquity": "1000",
    "coin": [
        {"coin": "USDT", "walletBalance": "900", "locked": "60",
         "availableToWithdraw": "840"},
        {"coin": "BTC", "walletBalance": "0.5", "locked": "0",
         "availableToWithdraw": "0.5"},
    ],
}


def _stream(stub: FakeBybit, **kwargs: Any) -> BybitPrivateStream:
    return BybitPrivateStream(
        api_key=API_KEY,
        api_secret=API_SECRET,
        url=stub.url,
        # The heartbeat is a wall-clock timer; a test that waited for one would
        # spend twenty seconds proving nothing. Its own test drives it.
        ping_interval=0,
        **kwargs,
    )


# --- authentication --------------------------------------------------------


async def test_connect_authenticates_before_anything_else(bybit: FakeBybit) -> None:
    stream = _stream(bybit)
    async with stream:
        assert stream.authenticated
    assert bybit.auths == 1
    # The auth is the first thing on the socket: a subscribe sent before it
    # would be refused by the venue.
    assert bybit.received[0]["op"] == "auth"


async def test_a_bad_secret_fails_the_connect(bybit: FakeBybit) -> None:
    """The stub verifies the signature, so this is the real refusal path."""
    stream = BybitPrivateStream(
        api_key=API_KEY, api_secret="wrong-secret", url=bybit.url, ping_interval=0
    )
    with pytest.raises(BybitWsError, match="sign"):
        await stream.connect()
    assert not stream.connected
    await stream.close()


def test_missing_credentials_fail_at_construction(bybit: FakeBybit) -> None:
    """A configuration mistake should read as one, not as a venue refusal."""
    with pytest.raises(BybitAuthError, match="api_key and api_secret"):
        BybitPrivateStream(api_key="", api_secret=API_SECRET, url=bybit.url)


async def test_subscribing_before_connect_is_refused(bybit: FakeBybit) -> None:
    stream = _stream(bybit)
    with pytest.raises(ExchangeNotConnectedError):
        await stream.subscribe_orders()


# --- subscriptions ---------------------------------------------------------


async def test_topics_are_scoped_to_the_category(bybit: FakeBybit) -> None:
    """An unscoped subscribe would deliver every perp update to a spot session."""
    async with _stream(bybit, product="spot") as stream:
        await stream.subscribe_orders()
        await stream.subscribe_executions()
        await stream.subscribe_wallets()
    assert bybit.subscribed == {"order.spot", "execution.spot", "wallet"}


async def test_wallet_is_never_scoped(bybit: FakeBybit) -> None:
    """A unified account has one balance sheet, not one per book."""
    async with _stream(bybit, product="linear") as stream:
        await stream.subscribe_wallets()
    assert bybit.subscribed == {"wallet"}


async def test_two_consumers_share_one_venue_subscription(
    bybit: FakeBybit,
) -> None:
    """Bybit refuses a duplicate subscribe, so a second reader must reuse it."""
    async with _stream(bybit) as stream:
        first = await stream.subscribe_orders()
        second = await stream.subscribe_orders()
        assert len(bybit.frames_for("subscribe")) == 1

        await bybit.push("order", [ORDER_ROW])
        assert (await asyncio.wait_for(first.__anext__(), 2)).order_id == "ord-1"
        assert (await asyncio.wait_for(second.__anext__(), 2)).order_id == "ord-1"


async def test_concurrent_consumers_share_one_venue_subscription(
    bybit: FakeBybit,
) -> None:
    """Reservation is the concurrent half of sharing: both callers race."""
    async with _stream(bybit) as stream:
        first, second = await asyncio.gather(
            stream.subscribe_orders(),
            stream.subscribe_orders(),
        )
        assert len(bybit.frames_for("subscribe")) == 1
        await bybit.push("order", [ORDER_ROW])
        assert (await asyncio.wait_for(first.__anext__(), 2)).order_id == "ord-1"
        assert (await asyncio.wait_for(second.__anext__(), 2)).order_id == "ord-1"


async def test_reconnect_resubscribes_a_shared_topic_once(
    bybit: FakeBybit,
) -> None:
    async with _stream(bybit, retry_backoff=0.01) as stream:
        first = await stream.subscribe_orders()
        second = await stream.subscribe_orders()
        await bybit.push("order", [ORDER_ROW])
        await asyncio.wait_for(first.__anext__(), 2)
        await asyncio.wait_for(second.__anext__(), 2)
        await bybit.drop()

        for _ in range(200):
            if bybit.auths >= 2 and len(bybit.frames_for("subscribe")) >= 2:
                break
            await asyncio.sleep(0.01)

        assert len(bybit.frames_for("subscribe")) == 2
        await bybit.push("order", [{**ORDER_ROW, "orderId": "ord-9"}])
        assert (await asyncio.wait_for(first.__anext__(), 2)).order_id == "ord-9"
        assert (await asyncio.wait_for(second.__anext__(), 2)).order_id == "ord-9"


# --- pushes ----------------------------------------------------------------


async def test_order_pushes_arrive_parsed(bybit: FakeBybit) -> None:
    async with _stream(bybit) as stream:
        orders = await stream.subscribe_orders()
        await bybit.push("order", [ORDER_ROW])
        row = await asyncio.wait_for(orders.__anext__(), 2)

    order = row.to_order(TICKER)
    assert order.order_id == "ord-1"
    assert order.client_order_id == "c-42"
    assert order.status is OrderStatus.NEW
    assert order.side is Side.BUY
    assert order.price == Decimal("60000")
    # ``avgPrice`` is the empty string until something fills, and None is what
    # that means — not a zero price.
    assert order.avg_price is None


async def test_one_push_carrying_several_rows_becomes_several_events(
    bybit: FakeBybit,
) -> None:
    async with _stream(bybit) as stream:
        fills = await stream.subscribe_executions()
        second = {**EXECUTION_ROW, "execId": "exec-2"}
        await bybit.push("execution", [EXECUTION_ROW, second])
        first = await asyncio.wait_for(fills.__anext__(), 2)
        again = await asyncio.wait_for(fills.__anext__(), 2)

    assert [row.exec_id for row in (first, again)] == ["exec-1", "exec-2"]
    fill = first.to_fill(TICKER)
    assert fill.qty == Decimal("0.0004")
    assert fill.fee == Decimal("0.024")
    assert fill.fee_asset == "USDT"


async def test_a_scoped_topic_pushes_back_under_its_bare_name(
    bybit: FakeBybit,
) -> None:
    """Bybit echoes ``order`` for a subscribe to ``order.spot``, and sometimes
    the suffixed form. Both have to route to the same stream."""
    async with _stream(bybit, product="spot") as stream:
        orders = await stream.subscribe_orders()
        await bybit.push("order.spot", [ORDER_ROW])
        first = await asyncio.wait_for(orders.__anext__(), 2)
        await bybit.push("order", [{**ORDER_ROW, "orderId": "ord-2"}])
        second = await asyncio.wait_for(orders.__anext__(), 2)

    assert [row.order_id for row in (first, second)] == ["ord-1", "ord-2"]


async def test_wallet_pushes_flatten_to_one_balance_per_coin(
    bybit: FakeBybit,
) -> None:
    async with _stream(bybit) as stream:
        wallets = await stream.subscribe_wallets()
        await bybit.push("wallet", [WALLET_ROW])
        wallet = await asyncio.wait_for(wallets.__anext__(), 2)

    balances = {b.asset: b for b in wallet.to_balances()}
    assert balances["USDT"].free == Decimal("840")
    assert balances["USDT"].locked == Decimal("60")
    assert balances["BTC"].free == Decimal("0.5")


async def test_a_row_that_will_not_parse_does_not_kill_the_stream(
    bybit: FakeBybit,
) -> None:
    async with _stream(bybit) as stream:
        orders = await stream.subscribe_orders()
        await bybit.push("order", [{"qty": "not-a-number"}])
        await bybit.push("order", [ORDER_ROW])
        row = await asyncio.wait_for(orders.__anext__(), 2)

    assert row.order_id == "ord-1"


# --- reconnect -------------------------------------------------------------


async def test_a_dropped_socket_reauthenticates_and_resubscribes(
    bybit: FakeBybit,
) -> None:
    """Both, in that order: a subscribe on an unauthenticated socket is refused.

    And what happened while the socket was down went unreported, which is why
    the reconnect callback fires — TD rebuilds rather than trusting a view that
    stopped updating at some unknown point.
    """
    async with _stream(bybit, retry_backoff=0.01) as stream:
        orders = await stream.subscribe_orders()
        seen: list[str] = []
        stream.on_reconnect(lambda: seen.append("reconnected"))

        await bybit.push("order", [ORDER_ROW])
        await asyncio.wait_for(orders.__anext__(), 2)
        await bybit.drop()

        for _ in range(200):
            if bybit.auths >= 2 and len(bybit.frames_for("subscribe")) >= 2:
                break
            await asyncio.sleep(0.01)

        assert bybit.auths >= 2
        assert bybit.connections >= 2
        assert "order" in bybit.subscribed
        assert seen == ["reconnected"]

        # And the stream is live again on the new socket.
        await bybit.push("order", [{**ORDER_ROW, "orderId": "ord-9"}])
        row = await asyncio.wait_for(orders.__anext__(), 2)
        assert row.order_id == "ord-9"


async def test_nothing_is_replayed_for_a_topic_no_one_reads(
    bybit: FakeBybit,
) -> None:
    """A socket carrying a firehose nobody drains is worse than a quiet one."""
    async with _stream(bybit, retry_backoff=0.01) as stream:
        orders = await stream.subscribe_orders()
        orders.close()

        await bybit.drop()
        for _ in range(200):
            if bybit.auths >= 2:
                break
            await asyncio.sleep(0.01)

        assert bybit.auths >= 2
        assert len(bybit.frames_for("subscribe")) == 1


# --- heartbeat -------------------------------------------------------------


async def test_the_heartbeat_goes_out_and_the_socket_survives_it(
    bybit: FakeBybit,
) -> None:
    """Bybit closes a connection that has been quiet, so this is not optional."""
    bybit.private_pong = True
    stream = BybitPrivateStream(
        api_key=API_KEY, api_secret=API_SECRET, url=bybit.url, ping_interval=0.08
    )
    async with stream:
        for _ in range(200):
            if bybit.pings >= 2:
                break
            await asyncio.sleep(0.01)
        assert bybit.pings >= 2
        # A pong that carries no req_id is expected, not an unmatched reply,
        # and must not have dropped the connection.
        assert stream.connected
        assert bybit.connections == 1
