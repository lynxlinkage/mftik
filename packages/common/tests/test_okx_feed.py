"""OKX public stream — sharing, restore, and the folded book."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

from mftik.exchange.okx import channels as ch
from mftik.exchange.okx.feed import OkxPublicStream
from okx_stub import FakeOkx

NATIVE = "BTC-USDT"
TICKER_ARG = ch.tickers(NATIVE)
BOOK_ARG = ch.books(NATIVE)


def _feed(stub: FakeOkx, **kwargs: Any) -> OkxPublicStream:
    return OkxPublicStream(stub.url, ping_interval=0, **kwargs)


def _book(
    seq: int,
    bids: list[list[str]],
    asks: list[list[str]],
    *,
    prev: int = -1,
) -> dict[str, Any]:
    return {
        "instId": NATIVE,
        "bids": bids,
        "asks": asks,
        "seqId": seq,
        "prevSeqId": prev,
    }


async def test_two_consumers_share_one_venue_subscription(okx_public: FakeOkx) -> None:
    async with _feed(okx_public) as feed:
        first, second = await asyncio.gather(
            feed.subscribe_tickers(NATIVE),
            feed.subscribe_tickers(NATIVE),
        )
        frames = okx_public.frames_for("subscribe")
        assert len(frames) == 1
        assert frames[0]["args"] == [TICKER_ARG]
        await okx_public.push(TICKER_ARG, [{"instId": NATIVE, "last": "60000"}])
        assert (await asyncio.wait_for(first.__anext__(), 2)).last == Decimal("60000")
        assert (await asyncio.wait_for(second.__anext__(), 2)).last == Decimal("60000")


async def test_reconnect_resubscribes_a_shared_channel_once(
    okx_public: FakeOkx,
) -> None:
    async with _feed(okx_public, retry_backoff=0.01) as feed:
        first = await feed.subscribe_tickers(NATIVE)
        second = await feed.subscribe_tickers(NATIVE)
        await okx_public.drop()
        for _ in range(200):
            if len(okx_public.frames_for("subscribe")) >= 2:
                break
            await asyncio.sleep(0.01)
        assert len(okx_public.frames_for("subscribe")) == 2
        replay = okx_public.frames_for("subscribe")[-1]
        assert replay["args"] == [TICKER_ARG]
        await okx_public.push(TICKER_ARG, [{"instId": NATIVE, "last": "61000"}])
        assert (await asyncio.wait_for(first.__anext__(), 2)).last == Decimal("61000")
        assert (await asyncio.wait_for(second.__anext__(), 2)).last == Decimal("61000")


async def test_a_second_folder_is_replayed_the_live_book(okx_public: FakeOkx) -> None:
    async with _feed(okx_public) as feed:
        first = await feed.subscribe_order_book(NATIVE)
        await okx_public.push(
            BOOK_ARG,
            [_book(1, [["59999", "1"]], [["60001", "2"]])],
            action="snapshot",
        )
        await asyncio.wait_for(first.__anext__(), 2)

        second = await feed.subscribe_order_book(NATIVE)
        replay = await asyncio.wait_for(second.__anext__(), 2)

    assert [level.price for level in replay.bids] == [Decimal("59999")]
    assert len(okx_public.frames_for("subscribe")) == 1


async def test_a_gap_on_a_shared_fold_resyncs_exactly_once(okx_public: FakeOkx) -> None:
    async with _feed(okx_public) as feed:
        first = await feed.subscribe_order_book(NATIVE)
        await okx_public.push(BOOK_ARG, [_book(1, [["1", "1"]], [])], action="snapshot")
        await asyncio.wait_for(first.__anext__(), 2)
        second = await feed.subscribe_order_book(NATIVE)
        await asyncio.wait_for(second.__anext__(), 2)
        held = feed._ledger.held()

        await okx_public.push(
            BOOK_ARG, [_book(99, [["2", "1"]], [], prev=50)], action="update"
        )
        for _ in range(200):
            if okx_public.frames_for("unsubscribe"):
                break
            await asyncio.sleep(0.01)

        assert feed._ledger.held() == held
        assert len(okx_public.frames_for("unsubscribe")) == 1
        assert len(okx_public.frames_for("subscribe")) == 2

        await okx_public.push(BOOK_ARG, [_book(1, [["3", "1"]], [])], action="snapshot")
        recovered_first = await asyncio.wait_for(first.__anext__(), 2)
        recovered_second = await asyncio.wait_for(second.__anext__(), 2)
        assert [level.price for level in recovered_first.bids] == [Decimal("3")]
        assert [level.price for level in recovered_second.bids] == [Decimal("3")]
