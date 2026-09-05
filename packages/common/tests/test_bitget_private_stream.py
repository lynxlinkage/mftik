"""Bitget private stream — one UTA subscribe arg, two consumers."""

from __future__ import annotations

import asyncio
from typing import Any

from mftik.exchange.bitget.account import BitgetPrivateStream
from mftik.exchange.bitget.channels import orders
from bitget_stub import API_KEY, API_SECRET, PASSPHRASE, FakeBitget


def _feed(stub: FakeBitget, **kwargs: Any) -> BitgetPrivateStream:
    return BitgetPrivateStream(
        api_key=API_KEY,
        api_secret=API_SECRET,
        passphrase=PASSPHRASE,
        url=stub.url,
        ping_interval=0,
        **kwargs,
    )


async def test_two_consumers_share_one_uta_orders_subscription(
    bitget: FakeBitget,
) -> None:
    async with _feed(bitget) as feed:
        first, second = await asyncio.gather(
            feed.subscribe_orders(),
            feed.subscribe_orders(),
        )
        assert bitget.logins == 1
        frames = bitget.frames_for("subscribe")
        assert len(frames) == 1
        assert frames[0]["args"] == [orders()]
        await bitget.push(
            orders(),
            [{"category": "USDC-FUTURES", "symbol": "BTCPERP", "orderId": "ord-1"}],
        )
        assert (await asyncio.wait_for(first.__anext__(), 2)).order_id == "ord-1"
        assert (await asyncio.wait_for(second.__anext__(), 2)).symbol == "BTCPERP"


async def test_reconnect_re_logs_in_before_any_subscribe(bitget: FakeBitget) -> None:
    async with _feed(bitget) as feed:
        await feed.subscribe_orders()
        assert bitget.logins == 1
        await bitget.drop()
        await asyncio.sleep(0.2)
        assert bitget.logins >= 2
        frames = bitget.frames_for("login")
        assert frames
        ts = frames[-1]["args"][0]["timestamp"]
        assert len(ts) <= 11
