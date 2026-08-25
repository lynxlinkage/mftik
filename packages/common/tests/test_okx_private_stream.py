"""OKX private stream — one subscribe arg, two consumers."""

from __future__ import annotations

import asyncio
from typing import Any

from mftik.exchange.okx.account import OkxPrivateStream
from mftik.exchange.okx.channels import orders
from okx_stub import API_KEY, API_SECRET, PASSPHRASE, FakeOkx


def _feed(stub: FakeOkx, **kwargs: Any) -> OkxPrivateStream:
    return OkxPrivateStream(
        api_key=API_KEY,
        api_secret=API_SECRET,
        passphrase=PASSPHRASE,
        url=stub.url,
        ping_interval=0,
        **kwargs,
    )


async def test_two_consumers_share_one_orders_subscription(okx: FakeOkx) -> None:
    async with _feed(okx) as feed:
        first, second = await asyncio.gather(
            feed.subscribe_orders(),
            feed.subscribe_orders(),
        )
        assert okx.logins == 1
        frames = okx.frames_for("subscribe")
        assert len(frames) == 1
        assert frames[0]["args"] == [orders()]
        await okx.push(orders(), [{"instId": "BTC-USDT", "ordId": "ord-1"}])
        assert (await asyncio.wait_for(first.__anext__(), 2)).ord_id == "ord-1"
        assert (await asyncio.wait_for(second.__anext__(), 2)).ord_id == "ord-1"
