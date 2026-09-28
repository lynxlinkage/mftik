"""The setup-only socket read must not leak into tasks the read loop starts.

``_SETUP`` lets connect and reconnect call ``recv`` while nothing else is
reading. ``create_task`` copies that context, and resetting the flag in the
caller does not clear the copy. A book resync spawned from the read loop
would then read the socket beside the reconnect.
"""

from __future__ import annotations

import asyncio

from deribit_stub import FakeDeribit
from mftik.exchange.deribit import channels as ch
from mftik.exchange.deribit.feed import DeribitPublicStream
from mftik.exchange.deribit.socket import _SETUP


def _flag(task: asyncio.Task[object] | None) -> bool:
    assert task is not None
    return bool(task.get_context().run(_SETUP.get))


async def test_read_loop_tasks_do_not_inherit_the_setup_flag(
    deribit_public: FakeDeribit,
) -> None:
    feed = DeribitPublicStream(
        deribit_public.url, ping_interval=60, heartbeat=0
    )
    callback_flags: list[bool] = []

    def on_reconnect() -> object:
        async def callback() -> None:
            callback_flags.append(_SETUP.get())

        return callback()

    async with feed:
        feed.on_reconnect(on_reconnect)
        assert _flag(feed._task) is False
        assert _flag(feed._watch_task) is False
        stream = await feed.subscribe_order_book("BTC_USDC")
        await deribit_public.push(
            ch.book("BTC_USDC"),
            {
                "instrument_name": "BTC_USDC",
                "change_id": 1,
                "timestamp": 1700000000000,
                "bids": [["new", "100", "5"]],
                "asks": [["new", "101", "5"]],
            },
        )
        await asyncio.wait_for(stream.__anext__(), 2)
        resync_flags: list[bool] = []
        resync_done = asyncio.Event()
        real_resync = feed._resync_book

        async def resync(channel: str, book: object) -> None:
            resync_flags.append(_SETUP.get())
            try:
                await real_resync(channel, book)
            finally:
                resync_done.set()

        feed._resync_book = resync  # type: ignore[method-assign]
        await deribit_public.push(
            ch.book("BTC_USDC"),
            {
                "instrument_name": "BTC_USDC",
                "change_id": 9,
                "prev_change_id": 8,
                "timestamp": 1700000001000,
                "bids": [["delete", "100", "0"]],
            },
        )
        await asyncio.wait_for(resync_done.wait(), 2)
        feed.retry_backoff = 0.01
        await deribit_public.drop()
        for _ in range(100):
            if feed.stats.reconnects >= 1 and callback_flags:
                break
            await asyncio.sleep(0.02)
        assert _flag(feed._task) is False
    assert resync_flags == [False]
    assert callback_flags == [False]
    assert feed.stats.reconnects == 1


async def test_a_resync_during_reconnect_waits_for_the_read_loop(
    deribit_public: FakeDeribit,
) -> None:
    """A resync already in flight must not ``recv`` beside ``_restore``."""
    feed = DeribitPublicStream(
        deribit_public.url, ping_interval=0, heartbeat=0, ack_timeout=2
    )
    feed.retry_backoff = 0.01
    reconnecting = asyncio.Event()
    resync_waiting = asyncio.Event()
    resync_done = asyncio.Event()
    setup_during_reconnect: list[bool] = []
    resync_flags: list[bool] = []
    direct_reads: list[str] = []
    errors: list[BaseException] = []
    real_open = feed._open
    real_handshake = feed.handshake
    real_resync = feed._resync_book

    async def open_then_mark() -> None:
        await real_open()
        setup_during_reconnect.append(_SETUP.get())
        reconnecting.set()

    async def handshake(frame: dict, req_id: str, *, op: str = "") -> object:
        task = asyncio.current_task()
        if task is not None and task.get_name().endswith("book-resync"):
            direct_reads.append(op)
        return await real_handshake(frame, req_id, op=op)

    async def resync(channel: str, book: object) -> None:
        try:
            resync_flags.append(_SETUP.get())
            resync_waiting.set()
            await reconnecting.wait()
            await real_resync(channel, book)
        except Exception as exc:
            errors.append(exc)
        finally:
            resync_done.set()

    async with feed:
        stream = await feed.subscribe_order_book("BTC_USDC")
        await deribit_public.push(
            ch.book("BTC_USDC"),
            {
                "instrument_name": "BTC_USDC",
                "change_id": 1,
                "timestamp": 1700000000000,
                "bids": [["new", "100", "5"]],
                "asks": [["new", "101", "5"]],
            },
        )
        await asyncio.wait_for(stream.__anext__(), 2)
        feed._open = open_then_mark  # type: ignore[method-assign]
        feed.handshake = handshake  # type: ignore[method-assign]
        feed._resync_book = resync  # type: ignore[method-assign]
        await deribit_public.push(
            ch.book("BTC_USDC"),
            {
                "instrument_name": "BTC_USDC",
                "change_id": 9,
                "prev_change_id": 8,
                "timestamp": 1700000001000,
                "bids": [["delete", "100", "0"]],
            },
        )
        await asyncio.wait_for(resync_waiting.wait(), 2)
        resync_task = next(
            task
            for task in asyncio.all_tasks()
            if task.get_name().endswith("book-resync")
        )
        assert _flag(resync_task) is False
        await deribit_public.drop()
        await asyncio.wait_for(resync_done.wait(), 5)
    assert errors == []
    assert resync_flags == [False]
    assert setup_during_reconnect == [True]
    assert direct_reads == []
    assert feed.stats.reconnects == 1
