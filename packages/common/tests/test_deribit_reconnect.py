"""A failed reconnect setup must not leave the new socket half-open.

``_open`` replaces ``_conn`` before ``_on_open`` and ``_restore``. If
either of those raises, the read loop has to close that socket and try
again. A request whose reply died with the old socket fails at once, and
a book resync may close only the connection it started on.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest
from deribit_stub import API_KEY, API_SECRET, FakeDeribit
from mftik.exchange.deribit import channels as ch
from mftik.exchange.deribit.account import DeribitPrivateStream
from mftik.exchange.deribit.feed import DeribitPublicStream
from websockets.exceptions import ConnectionClosed

# §9.1 component (loopback venue stub). Slow cases miss the 50 ms unit call cap;
# the 500 ms component cap still applies.
pytestmark = pytest.mark.component


def _fast(url: str, **kwargs: float) -> DeribitPublicStream:
    return DeribitPublicStream(
        url,
        ping_interval=0,
        heartbeat=0,
        retry_backoff=0.01,
        max_retry_backoff=0.05,
        **kwargs,
    )


async def _until(ready: Callable[[], bool], detail: str) -> None:
    for _ in range(100):
        if ready():
            return
        await asyncio.sleep(0.02)
    raise AssertionError(detail)


def _snapshot() -> dict[str, object]:
    return {
        "instrument_name": "BTC_USDC",
        "change_id": 1,
        "timestamp": 1700000000000,
        "bids": [["new", "100", "5"]],
        "asks": [["new", "101", "5"]],
    }


def _gap() -> dict[str, object]:
    return {
        "instrument_name": "BTC_USDC",
        "change_id": 9,
        "prev_change_id": 8,
        "timestamp": 1700000001000,
        "bids": [["delete", "100", "0"]],
    }


@pytest.mark.real_sleep(
    reason="the venue socket still sleeps on the wall clock"
)
@pytest.mark.parametrize("hook", ["_on_open", "_restore"])
async def test_a_failed_setup_closes_the_socket_and_retries(
    deribit_public: FakeDeribit, hook: str
) -> None:
    """One raised ``_on_open`` or ``_restore`` is followed by a full retry."""
    feed = _fast(deribit_public.url)
    calls = 0
    real = getattr(feed, hook)
    # ``_on_open`` also runs for the first connect. Fail the reconnect.
    fail_on = 2 if hook == "_on_open" else 1

    async def flaky() -> None:
        nonlocal calls
        calls += 1
        if calls == fail_on:
            raise OSError(f"{hook} failed")
        await real()

    setattr(feed, hook, flaky)
    async with feed:
        await feed.subscribe_order_book("BTC_USDC")
        await deribit_public.drop()
        await _until(
            lambda: feed.stats.reconnects >= 1 and deribit_public.connections >= 3,
            f"reconnects={feed.stats.reconnects} "
            f"connections={deribit_public.connections} calls={calls}",
        )
        assert calls == fail_on + 1
        assert len(deribit_public.frames_for(ch.PUBLIC_SUBSCRIBE)) == 2
        assert ch.book("BTC_USDC") in deribit_public.subscribed
        seen = deribit_public.connections
        await asyncio.sleep(0.25)
        assert deribit_public.connections == seen
        assert feed.stats.reconnects == 1
        assert feed.connected


@pytest.mark.real_sleep(
    reason="the venue socket still sleeps on the wall clock"
)
async def test_a_failed_private_auth_is_retried(deribit: FakeDeribit) -> None:
    """An auth error after the socket is open must not leave it unauthenticated."""
    stream = DeribitPrivateStream(
        api_key=API_KEY,
        api_secret=API_SECRET,
        url=deribit.url,
        ping_interval=0,
        heartbeat=0,
        retry_backoff=0.01,
        max_retry_backoff=0.05,
    )
    attempts = 0
    real = stream.handshake

    async def handshake(frame: dict, req_id: str, *, op: str = "") -> object:
        nonlocal attempts
        if op == ch.PUBLIC_AUTH:
            attempts += 1
            if attempts == 2:
                raise OSError("auth failed")
        return await real(frame, req_id, op=op)

    stream.handshake = handshake  # type: ignore[method-assign]
    async with stream:
        assert attempts == 1
        assert stream.authenticated
        await deribit.drop()
        await _until(
            lambda: stream.stats.reconnects >= 1 and attempts >= 3,
            f"reconnects={stream.stats.reconnects} attempts={attempts} "
            f"connections={deribit.connections} authed={stream.authenticated}",
        )
        assert attempts == 3
        assert deribit.auths == 2
        assert deribit.connections == 3
        seen = deribit.connections
        await asyncio.sleep(0.25)
        assert deribit.connections == seen
        assert stream.stats.reconnects == 1
        assert stream.authenticated


@pytest.mark.real_sleep(
    reason="the venue socket still sleeps on the wall clock"
)
async def test_a_dropped_socket_fails_the_pending_request(
    deribit: FakeDeribit,
) -> None:
    """A reply that died with the socket does not wait out ``ack_timeout``."""
    stream = DeribitPrivateStream(
        api_key=API_KEY,
        api_secret=API_SECRET,
        url=deribit.url,
        ping_interval=0,
        heartbeat=0,
        ack_timeout=5,
        retry_backoff=0.01,
        max_retry_backoff=0.05,
    )
    sent = asyncio.Event()
    real = deribit._answer

    async def answer(websocket: object, msg: dict) -> None:
        if msg.get("method") == ch.PRIVATE_GET_OPEN_ORDERS:
            sent.set()
            return
        await real(websocket, msg)

    deribit._answer = answer  # type: ignore[method-assign]
    async with stream:
        task = asyncio.create_task(stream.rpc(ch.PRIVATE_GET_OPEN_ORDERS))
        await asyncio.wait_for(sent.wait(), 2)
        await deribit.drop()
        with pytest.raises(ConnectionClosed):
            await asyncio.wait_for(task, 1)
        await _until(
            lambda: stream.stats.reconnects >= 1 and stream.authenticated,
            f"reconnects={stream.stats.reconnects} authed={stream.authenticated}",
        )
        assert stream.stats.reconnects == 1
        assert deribit.connections == 2


# over the 500 ms component cap
@pytest.mark.integration
@pytest.mark.real_sleep(
    reason="the venue socket still sleeps on the wall clock"
)
async def test_a_resync_reply_lost_on_drop_does_not_drop_the_restored_socket(
    deribit_public: FakeDeribit,
) -> None:
    """The resync's lost unsubscribe must not close the socket that replaced it."""
    feed = _fast(deribit_public.url, ack_timeout=0.4)
    swallowed = asyncio.Event()
    real = deribit_public._answer

    async def answer(websocket: object, msg: dict) -> None:
        if msg.get("method") == ch.PUBLIC_UNSUBSCRIBE and not swallowed.is_set():
            swallowed.set()
            return
        await real(websocket, msg)

    deribit_public._answer = answer  # type: ignore[method-assign]
    async with feed:
        stream = await feed.subscribe_order_book("BTC_USDC")
        channel = ch.book("BTC_USDC")
        await deribit_public.push(channel, _snapshot())
        await asyncio.wait_for(stream.__anext__(), 2)
        await deribit_public.push(channel, _gap())
        await asyncio.wait_for(swallowed.wait(), 2)
        await deribit_public.drop()
        await _until(
            lambda: feed.stats.reconnects >= 1 and feed.connected,
            f"reconnects={feed.stats.reconnects} "
            f"connections={deribit_public.connections}",
        )
        # Longer than ``ack_timeout``: the old bug drops the restored
        # socket when the swallowed reply finally times out.
        await asyncio.sleep(0.7)
        assert feed.stats.reconnects == 1
        assert deribit_public.connections == 2
        assert channel in feed._ledger.held()


# over the 500 ms component cap
@pytest.mark.integration
@pytest.mark.real_sleep(
    reason="the venue socket still sleeps on the wall clock"
)
async def test_a_resync_from_the_old_socket_does_not_drop_the_new_one(
    deribit_public: FakeDeribit,
) -> None:
    """A resync that times out while setup runs leaves the new socket up."""
    feed = _fast(deribit_public.url, ack_timeout=0.25)
    restore_entered = asyncio.Event()
    release_restore = asyncio.Event()
    unsub_entered = asyncio.Event()
    real_restore = feed._restore
    real_request = feed.request

    async def held_restore() -> None:
        if not restore_entered.is_set():
            restore_entered.set()
            await release_restore.wait()
        await real_restore()

    async def request(
        frame: dict, req_id: str, *, op: str = "", timeout: float | None = None
    ) -> object:
        if op == ch.PUBLIC_UNSUBSCRIBE and not unsub_entered.is_set():
            unsub_entered.set()
            await restore_entered.wait()
        return await real_request(frame, req_id, op=op, timeout=timeout)

    async with feed:
        stream = await feed.subscribe_order_book("BTC_USDC")
        channel = ch.book("BTC_USDC")
        await deribit_public.push(channel, _snapshot())
        await asyncio.wait_for(stream.__anext__(), 2)
        feed._restore = held_restore  # type: ignore[method-assign]
        feed.request = request  # type: ignore[method-assign]
        await deribit_public.push(channel, _gap())
        await asyncio.wait_for(unsub_entered.wait(), 2)
        await deribit_public.drop()
        await asyncio.wait_for(restore_entered.wait(), 2)
        # The unsubscribe is now waiting for the read loop, and that wait
        # is shorter than the held restore. Timing out must not close it.
        await asyncio.sleep(0.6)
        release_restore.set()
        await _until(
            lambda: feed.stats.reconnects >= 1 and feed.connected,
            f"reconnects={feed.stats.reconnects} "
            f"connections={deribit_public.connections}",
        )
        await asyncio.sleep(0.3)
        assert feed.stats.reconnects == 1
        assert deribit_public.connections == 2
        assert channel in feed._ledger.held()
