"""Gate futures WebSocket client — login uid, signed size, private payloads."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

from gate_future_stub import API_KEY, API_SECRET, UID, FakeGateFutures
from mftik.exchange.gate.future import channels as ch
from mftik.exchange.gate.future.client import GateFuturesWebSocket
from mftik.exchange.gate.future.protocol import api_sign


async def _client(gate: FakeGateFutures, **kwargs: Any) -> GateFuturesWebSocket:
    return GateFuturesWebSocket(url=gate.url, ping_interval=0, **kwargs)  # type: ignore[attr-defined]


async def test_connect_with_credentials_logs_in_and_keeps_uid(
    gate_futures: FakeGateFutures,
) -> None:
    async with await _client(
        gate_futures, api_key=API_KEY, api_secret=API_SECRET
    ) as ws:
        assert ws.logged_in
        assert ws.uid == UID

    login = gate_futures.api_call(ch.LOGIN)
    payload = login["payload"]
    assert payload["signature"] == api_sign(
        API_SECRET, ch.LOGIN, "", int(payload["timestamp"])
    )
    assert login["channel"] == "futures.login"


async def test_private_subscribe_includes_uid(
    gate_futures: FakeGateFutures,
) -> None:
    async with await _client(
        gate_futures, api_key=API_KEY, api_secret=API_SECRET
    ) as ws:
        await ws.subscribe_orders()
        await ws.subscribe_positions()
        await ws.subscribe_balances()

    assert gate_futures.frames_for(ch.ORDERS)[0]["payload"] == [UID, "!all"]
    assert gate_futures.frames_for(ch.POSITIONS)[0]["payload"] == [UID, "!all"]
    assert gate_futures.frames_for(ch.BALANCES)[0]["payload"] == [UID]


async def test_two_consumers_share_one_venue_subscription(
    gate_futures: FakeGateFutures,
) -> None:
    async with await _client(gate_futures) as ws:
        first, second = await asyncio.gather(
            ws.subscribe_trades("BTC_USDT"),
            ws.subscribe_trades("BTC_USDT"),
        )
        assert len(gate_futures.frames_for(ch.TRADES)) == 1
        await gate_futures.push(
            ch.TRADES,
            [
                {
                    "id": 1,
                    "contract": "BTC_USDT",
                    "size": "-10",
                    "price": "60000",
                    "create_time": 1_700_000_000,
                }
            ],
        )
        for stream in (first, second):
            row = await asyncio.wait_for(anext(stream), timeout=2.0)
            assert row.contract == "BTC_USDT"


async def test_place_order_sends_a_negative_sell_size(
    gate_futures: FakeGateFutures,
) -> None:
    gate_futures.api_data[ch.ORDER_PLACE] = {
        "result": {
            "id": "1",
            "contract": "BTC_USDT",
            "size": "-10",
            "left": "-10",
            "price": "60000",
            "status": "open",
            "text": "t-42",
        }
    }
    async with await _client(
        gate_futures, api_key=API_KEY, api_secret=API_SECRET
    ) as ws:
        ack = await ws.place_order(
            contract="BTC_USDT",
            size=Decimal("-10"),
            price="60000",
            tif="gtc",
            text="42",
        )

    assert ack.id == "1"
    param = gate_futures.api_call(ch.ORDER_PLACE)["payload"]["req_param"]
    assert param["size"] == "-10"
    assert param["text"] == "t-42"
    assert param["tif"] == "gtc"
