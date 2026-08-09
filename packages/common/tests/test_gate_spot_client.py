"""Gate spot WebSocket client — driven against a local stand-in for Gate.

The stand-in speaks the real v4 envelope (ack on subscribe, ``spot.pong`` on
``spot.ping``, ``result`` pushes on ``update``) so the client is exercised
end-to-end over a real socket without touching the venue.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import pytest
from gate_stub import API_KEY, API_SECRET, FakeGate
from mft.exchange.errors import ExchangeError, ExchangeNotConnectedError
from mft.exchange.gate.spot import (
    GateApiError,
    GateSpotWebSocket,
    GateWsError,
    api_sign,
    sign,
)
from mft.exchange.gate.spot import channels as ch
from mft.exchange.models import OrderStatus, OrderType, Side
from mft.exchange.tickers import UniversalTicker

#: The instrument every payload in this module is stamped with.
TICKER = UniversalTicker.parse("Gate_Spot_BTCUSDT")


async def _client(gate: FakeGate, **kwargs: Any) -> GateSpotWebSocket:
    return GateSpotWebSocket(url=gate.url, ping_interval=0, **kwargs)  # type: ignore[attr-defined]


async def test_public_subscribe_streams_typed_trades(gate: FakeGate) -> None:
    async with await _client(gate) as ws:
        trades = await ws.subscribe_trades("BTC_USDT", "ETH_USDT")

        frame = gate.frames_for(ch.TRADES)[0]
        assert frame["payload"] == ["BTC_USDT", "ETH_USDT"]
        assert "auth" not in frame, "public channels must not be signed"

        await gate.push(
            ch.TRADES,
            [
                {
                    "id": 1,
                    "create_time": 1648725035,
                    "create_time_ms": "1648725035923.0",
                    "side": "sell",
                    "currency_pair": "BTC_USDT",
                    "amount": "0.5",
                    "price": "40000",
                }
            ],
        )
        trade = await asyncio.wait_for(anext(trades), timeout=2.0)

    assert trade.currency_pair == "BTC_USDT"
    assert trade.price == Decimal("40000")
    assert trade.to_trade(TICKER).qty == Decimal("0.5")


async def test_connect_with_credentials_logs_in(gate: FakeGate) -> None:
    async with await _client(gate, api_key=API_KEY, api_secret=API_SECRET) as ws:
        assert ws.authenticated
        assert ws.logged_in

    login = gate.api_call(ch.LOGIN)
    payload = login["payload"]
    assert "req_param" not in payload
    assert payload["api_key"] == API_KEY
    assert payload["signature"] == api_sign(
        API_SECRET, ch.LOGIN, "", int(payload["timestamp"])
    )


async def test_private_subscribe_is_signed(gate: FakeGate) -> None:
    async with await _client(gate, api_key=API_KEY, api_secret=API_SECRET) as ws:
        assert ws.authenticated
        assert ws.logged_in
        orders = await ws.subscribe_orders()

        frame = gate.frames_for(ch.ORDERS)[0]
        assert frame["payload"] == ["!all"]
        auth = frame["auth"]
        assert auth["method"] == "api_key"
        assert auth["KEY"] == API_KEY
        assert auth["SIGN"] == sign(
            API_SECRET, ch.ORDERS, ch.SUBSCRIBE, frame["time"]
        )

        await gate.push(
            ch.ORDERS,
            [
                {
                    "id": "1036717689726",
                    "text": "t-42",
                    "currency_pair": "BTC_USDT",
                    "type": "limit",
                    "side": "buy",
                    "amount": "0.1",
                    "price": "200",
                    "left": "0",
                    "event": "finish",
                    "finish_as": "filled",
                    "update_time_ms": "1774613210391",
                }
            ],
        )
        update = await asyncio.wait_for(anext(orders), timeout=2.0)

    assert update.client_order_id == "42"
    assert update.to_order(TICKER).status.value == "filled"


async def test_private_channel_without_credentials_raises(gate: FakeGate) -> None:
    async with await _client(gate) as ws:
        assert not ws.authenticated
        with pytest.raises(ExchangeError, match="private channel"):
            await ws.subscribe_balances()
    assert gate.frames_for(ch.BALANCES) == []


async def test_balances_subscribes_with_empty_payload(gate: FakeGate) -> None:
    async with await _client(gate, api_key=API_KEY, api_secret=API_SECRET) as ws:
        balances = await ws.subscribe_balances()
        assert gate.frames_for(ch.BALANCES)[0]["payload"] == []

        await gate.push(
            ch.BALANCES,
            [
                {
                    "timestamp_ms": "1667556323730",
                    "currency": "USDT",
                    "total": "100",
                    "available": "95",
                    "freeze": "5",
                    "change_type": "order-create",
                }
            ],
        )
        bal = await asyncio.wait_for(anext(balances), timeout=2.0)

    assert bal.to_balance().free == Decimal("95")
    assert bal.change_type == "order-create"


async def test_candlestick_and_book_ticker_payload_arity(gate: FakeGate) -> None:
    async with await _client(gate) as ws:
        await ws.subscribe_candlesticks("1m", "BTC_USDT")
        await ws.subscribe_book_ticker("BTC_USDT")
        await ws.subscribe_order_book("BTC_USDT", level="5", interval="1000ms")
        await ws.subscribe_order_book_update("BTC_USDT", interval="100ms")

    # Interval leads for candlesticks; the book channels take their own tail args.
    assert gate.frames_for(ch.CANDLESTICKS)[0]["payload"] == ["1m", "BTC_USDT"]
    assert gate.frames_for(ch.BOOK_TICKER)[0]["payload"] == ["BTC_USDT"]
    assert gate.frames_for(ch.ORDER_BOOK)[0]["payload"] == [
        "BTC_USDT",
        "5",
        "1000ms",
    ]
    assert gate.frames_for(ch.ORDER_BOOK_UPDATE)[0]["payload"] == [
        "BTC_USDT",
        "100ms",
    ]


async def test_subscribe_error_is_raised(gate: FakeGate) -> None:
    gate.errors[ch.TICKERS] = {"code": 2, "message": "invalid currency pair"}
    async with await _client(gate) as ws:
        with pytest.raises(GateWsError, match="invalid currency pair") as exc:
            await ws.subscribe_tickers("NOPE_USDT")
    assert exc.value.code == 2
    assert exc.value.channel == ch.TICKERS


async def test_concurrent_subscribes_to_one_channel_all_get_acked(
    gate: FakeGate,
) -> None:
    """A subscribe ack has nothing to correlate on but channel+event.

    MD opens one feed per symbol, so two subscribes to ``spot.trades`` can be
    in flight at once. Both have to come back — matching the second ack against
    the first, already-resolved waiter would strand it until its ack timeout.
    """
    async with await _client(gate, ack_timeout=1.0) as ws:
        btc, eth = await asyncio.gather(
            ws.subscribe_trades("BTC_USDT"),
            ws.subscribe_trades("ETH_USDT"),
        )

        assert btc is not eth
        assert len(gate.frames_for(ch.TRADES)) == 2

        # Both are live: the channel fans out to every stream on it.
        await gate.push(
            ch.TRADES,
            [
                {
                    "id": 1,
                    "create_time": 1648725035,
                    "create_time_ms": "1648725035923.0",
                    "side": "buy",
                    "currency_pair": "ETH_USDT",
                    "amount": "1",
                    "price": "3000",
                }
            ],
        )
        for stream in (btc, eth):
            row = await asyncio.wait_for(anext(stream), timeout=2.0)
            assert row.currency_pair == "ETH_USDT"


async def test_subscribe_before_connect_raises(gate: FakeGate) -> None:
    ws = await _client(gate)
    with pytest.raises(ExchangeNotConnectedError):
        await ws.subscribe_trades("BTC_USDT")


async def test_ping_gets_pong(gate: FakeGate) -> None:
    ws = GateSpotWebSocket(url=gate.url, ping_interval=0.05)  # type: ignore[attr-defined]
    async with ws:
        await asyncio.sleep(0.25)
        assert ws.stats.last_pong_at > 0


async def test_unsubscribe_closes_the_stream(gate: FakeGate) -> None:
    async with await _client(gate) as ws:
        trades = await ws.subscribe_trades("BTC_USDT")
        await ws.unsubscribe(ch.TRADES, ch.trades("BTC_USDT"))

        assert gate.frames_for(ch.TRADES, ch.UNSUBSCRIBE)
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(anext(trades), timeout=2.0)


async def test_reconnect_replays_subscriptions(gate: FakeGate) -> None:
    ws = GateSpotWebSocket(  # type: ignore[attr-defined]
        url=gate.url,
        api_key=API_KEY,
        api_secret=API_SECRET,
        ping_interval=0,
        retry_backoff=0.05,
    )
    async with ws:
        trades = await ws.subscribe_trades("BTC_USDT")
        await ws.subscribe_orders("BTC_USDT")

        # Next frame the server handles, it hangs up on us.
        gate.drop_next = True
        await ws.subscribe_tickers("BTC_USDT")

        for _ in range(40):
            await asyncio.sleep(0.05)
            if ws.stats.reconnects:
                break
        assert ws.stats.reconnects == 1, "expected exactly one reconnect"
        assert gate.connections == 2

        # Both original subscriptions were replayed, the private one re-signed.
        assert len(gate.frames_for(ch.TRADES)) == 2
        replayed = gate.frames_for(ch.ORDERS)[-1]
        assert replayed["auth"]["SIGN"] == sign(
            API_SECRET, ch.ORDERS, ch.SUBSCRIBE, replayed["time"]
        )

        # And the stream opened before the drop still delivers.
        await gate.push(
            ch.TRADES,
            [
                {
                    "id": 2,
                    "create_time": 1,
                    "create_time_ms": "1000",
                    "side": "buy",
                    "currency_pair": "BTC_USDT",
                    "amount": "1",
                    "price": "50000",
                }
            ],
        )
        trade = await asyncio.wait_for(anext(trades), timeout=2.0)
        assert trade.price == Decimal("50000")


# --- trading calls ---------------------------------------------------------


ORDER_RESULT = {
    "id": "1852454420",
    "text": "t-42",
    "currency_pair": "BTC_USDT",
    "type": "limit",
    "account": "spot",
    "side": "buy",
    "amount": "0.001",
    "price": "60000",
    "left": "0.001",
    "status": "open",
    "finish_as": "open",
    "time_in_force": "gtc",
    "create_time_ms": 1774613210391,
    "update_time_ms": 1774613210391,
}


async def test_place_order_skips_ack_echo_and_returns_the_order(
    gate: FakeGate,
) -> None:
    """Gate's first frame is ``ack: true`` with a req_param echo (no pair).

    We must wait for the real order reply — otherwise ``currency_pair`` is
    empty and symbol resolution blows up with ``spelled ''``.
    """
    gate.api_data[ch.ORDER_PLACE] = {"result": ORDER_RESULT}
    async with await _client(gate, api_key=API_KEY, api_secret=API_SECRET) as ws:
        assert ws.logged_in
        ack = await ws.place_order(
            currency_pair="BTC_USDT",
            side="buy",
            amount="0.001",
            price="60000",
            time_in_force="gtc",
            text="42",
        )

    # Login must precede trading; order entry rides the session, not a
    # per-call signature.
    assert gate.api_call(ch.LOGIN)["channel"] == ch.LOGIN
    call = gate.api_call(ch.ORDER_PLACE)
    payload = call["payload"]
    param = payload["req_param"]

    # Client order id is wrapped into Gate's t- form.
    assert param["text"] == "t-42"
    assert param["currency_pair"] == "BTC_USDT"
    assert param["side"] == "buy"
    assert param["amount"] == "0.001"
    assert param["price"] == "60000"
    assert param["account"] == "spot"
    assert "signature" not in payload
    assert "api_key" not in payload

    assert ack.id == "1852454420"
    assert ack.client_order_id == "42"
    assert ack.to_order(TICKER).status is OrderStatus.NEW
    assert ack.to_order(TICKER).universal_ticker == str(TICKER)


async def test_place_market_order_omits_price(gate: FakeGate) -> None:
    gate.api_data[ch.ORDER_PLACE] = {"result": ORDER_RESULT}
    async with await _client(gate, api_key=API_KEY, api_secret=API_SECRET) as ws:
        await ws.place_order(
            currency_pair="BTC_USDT",
            side="sell",
            amount="0.5",
            type="market",
            time_in_force="ioc",
        )

    param = gate.api_call(ch.ORDER_PLACE)["payload"]["req_param"]
    assert param["type"] == "market"
    assert "price" not in param
    assert "text" not in param


async def test_place_order_accepts_enums_and_decimals(gate: FakeGate) -> None:
    gate.api_data[ch.ORDER_PLACE] = {"result": ORDER_RESULT}
    async with await _client(gate, api_key=API_KEY, api_secret=API_SECRET) as ws:
        await ws.place_order(
            currency_pair="BTC_USDT",
            side=Side.BUY,
            type=OrderType.LIMIT,
            amount=Decimal("0.001"),
            price=Decimal("60000.5"),
        )

    param = gate.api_call(ch.ORDER_PLACE)["payload"]["req_param"]
    assert param["side"] == "buy"
    assert param["type"] == "limit"
    assert param["amount"] == "0.001"
    assert param["price"] == "60000.5"


async def test_cancel_order(gate: FakeGate) -> None:
    cancelled = dict(ORDER_RESULT, status="cancelled", finish_as="cancelled")
    gate.api_data[ch.ORDER_CANCEL] = {"result": cancelled}
    async with await _client(gate, api_key=API_KEY, api_secret=API_SECRET) as ws:
        ack = await ws.cancel_order("1852454420", currency_pair="BTC_USDT")

    param = gate.api_call(ch.ORDER_CANCEL)["payload"]["req_param"]
    assert param == {"order_id": "1852454420", "currency_pair": "BTC_USDT"}
    assert ack.to_order(TICKER).status is OrderStatus.CANCELED


async def test_batch_cancel_reports_each_leg(gate: FakeGate) -> None:
    gate.api_data[ch.ORDER_CANCEL_IDS] = {
        "result": [
            {"id": "1", "currency_pair": "BTC_USDT", "succeeded": True},
            {
                "id": "2",
                "currency_pair": "BTC_USDT",
                "succeeded": False,
                "label": "ORDER_NOT_FOUND",
            },
        ]
    }
    async with await _client(gate, api_key=API_KEY, api_secret=API_SECRET) as ws:
        acks = await ws.cancel_orders(
            [
                {"id": "1", "currency_pair": "BTC_USDT"},
                {"id": "2", "currency_pair": "BTC_USDT"},
            ]
        )

    # A batch cancel resolves without raising even when legs fail.
    assert [a.succeeded for a in acks] == [True, False]
    assert acks[1].label == "ORDER_NOT_FOUND"


async def test_api_error_raises(gate: FakeGate) -> None:
    gate.api_data[ch.ORDER_PLACE] = {
        "errs": {"label": "BALANCE_NOT_ENOUGH", "message": "not enough balance"}
    }
    async with await _client(gate, api_key=API_KEY, api_secret=API_SECRET) as ws:
        with pytest.raises(GateApiError, match="BALANCE_NOT_ENOUGH") as exc:
            await ws.place_order(
                currency_pair="BTC_USDT", side="buy", amount="99999", price="1"
            )

    assert exc.value.label == "BALANCE_NOT_ENOUGH"
    assert exc.value.channel == ch.ORDER_PLACE
    # TD catches ExchangeError to turn a failed submit into an order reject.
    assert isinstance(exc.value, ExchangeError)


async def test_trading_call_without_credentials_raises(gate: FakeGate) -> None:
    async with await _client(gate) as ws:
        with pytest.raises(ExchangeError, match="trading call"):
            await ws.place_order(
                currency_pair="BTC_USDT", side="buy", amount="1", price="1"
            )
    assert gate.api_calls == []


async def test_concurrent_calls_correlate_by_req_id(gate: FakeGate) -> None:
    """Replies come back out of order; req_id has to sort them out."""
    gate.api_data[ch.ORDER_CANCEL] = {"result": ORDER_RESULT}
    gate.hold_api_replies = 2

    async with await _client(gate, api_key=API_KEY, api_secret=API_SECRET) as ws:
        first, second = await asyncio.gather(
            ws.cancel_order("1", currency_pair="BTC_USDT"),
            ws.cancel_order("2", currency_pair="BTC_USDT"),
        )

    assert first.id == second.id == "1852454420"
    req_ids = {
        c["payload"]["req_id"]
        for c in gate.api_calls
        if c["channel"] == ch.ORDER_CANCEL
    }
    assert len(req_ids) == 2, "each call must carry its own req_id"


async def test_api_request_is_a_generic_escape_hatch(gate: FakeGate) -> None:
    gate.api_data[ch.ORDER_STATUS] = {"result": ORDER_RESULT}
    async with await _client(gate, api_key=API_KEY, api_secret=API_SECRET) as ws:
        result = await ws.api_request(
            ch.ORDER_STATUS,
            {"order_id": "1852454420", "currency_pair": "BTC_USDT"},
        )

    assert result["id"] == "1852454420"
    assert gate.api_call(ch.ORDER_STATUS)["payload"]["req_param"]["order_id"] == (
        "1852454420"
    )
