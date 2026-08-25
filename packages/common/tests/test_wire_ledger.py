"""The per-socket wire ledger — reservation before the venue ack.

I2 is the concurrent case: two ``acquire`` calls for the same identity
must send one frame. A failed send rolls the reservation back so the
next caller retries. Restore clears the set first so a failed replay
does not stick a name as subscribed with nothing on the wire.
"""

from __future__ import annotations

import asyncio

import pytest
from mftik.exchange.wire import WireLedger, first_seen


def test_first_seen_keeps_order_and_drops_duplicates() -> None:
    assert first_seen(["tickers.BTC", "order", "tickers.BTC", "wallet"]) == [
        "tickers.BTC",
        "order",
        "wallet",
    ]


async def test_a_second_acquire_of_a_held_key_does_not_send() -> None:
    ledger: WireLedger[str] = WireLedger()
    sent: list[list[str]] = []

    async def send(keys: list[str]) -> None:
        sent.append(list(keys))

    await ledger.acquire(["tickers.BTC", "tickers.BTC"], send)
    await ledger.acquire(["tickers.BTC"], send)

    assert sent == [["tickers.BTC"]]
    assert ledger.held() == frozenset({"tickers.BTC"})


async def test_concurrent_acquire_of_one_key_sends_once() -> None:
    ledger: WireLedger[str] = WireLedger()
    started = asyncio.Event()
    release = asyncio.Event()
    sent: list[list[str]] = []

    async def send(keys: list[str]) -> None:
        sent.append(list(keys))
        started.set()
        await release.wait()

    first = asyncio.create_task(ledger.acquire(["tickers.BTC"], send))
    await started.wait()
    second = asyncio.create_task(ledger.acquire(["tickers.BTC"], send))
    for _ in range(20):
        if not second.done():
            await asyncio.sleep(0)
    release.set()
    await asyncio.gather(first, second)

    assert sent == [["tickers.BTC"]]
    assert ledger.held() == frozenset({"tickers.BTC"})


async def test_a_failed_send_rolls_back_so_the_next_acquire_retries() -> None:
    ledger: WireLedger[str] = WireLedger()
    sent: list[list[str]] = []

    async def fail(keys: list[str]) -> None:
        sent.append(list(keys))
        raise RuntimeError("ack failed")

    async def ok(keys: list[str]) -> None:
        sent.append(list(keys))

    with pytest.raises(RuntimeError, match="ack failed"):
        await ledger.acquire(["tickers.BTC"], fail)
    assert not ledger.held()

    await ledger.acquire(["tickers.BTC"], ok)
    assert sent == [["tickers.BTC"], ["tickers.BTC"]]
    assert ledger.held() == frozenset({"tickers.BTC"})


async def test_a_waiter_fails_when_the_leader_fails() -> None:
    ledger: WireLedger[str] = WireLedger()
    started = asyncio.Event()
    release = asyncio.Event()

    async def fail(keys: list[str]) -> None:
        started.set()
        await release.wait()
        raise RuntimeError("ack failed")

    first = asyncio.create_task(ledger.acquire(["tickers.BTC"], fail))
    await started.wait()
    second = asyncio.create_task(ledger.acquire(["tickers.BTC"], fail))
    for _ in range(20):
        if not second.done():
            await asyncio.sleep(0)
    release.set()
    results = await asyncio.gather(first, second, return_exceptions=True)

    assert all(isinstance(exc, RuntimeError) for exc in results)
    assert not ledger.held()


async def test_clear_before_restore_sends_again_and_a_failed_restore_stays_empty() -> (
    None
):
    ledger: WireLedger[str] = WireLedger()
    sent: list[list[str]] = []

    async def ok(keys: list[str]) -> None:
        sent.append(list(keys))

    async def fail(keys: list[str]) -> None:
        sent.append(list(keys))
        raise RuntimeError("restore failed")

    await ledger.acquire(["a", "b"], ok)
    ledger.clear()
    assert not ledger.held()

    with pytest.raises(RuntimeError, match="restore failed"):
        await ledger.acquire(["a", "b"], fail)
    assert not ledger.held()

    await ledger.acquire(["a", "b"], ok)
    assert sent == [["a", "b"], ["a", "b"], ["a", "b"]]


async def test_clear_fails_inflight_waiters_at_once() -> None:
    ledger: WireLedger[str] = WireLedger()
    started = asyncio.Event()
    release = asyncio.Event()

    async def hang(keys: list[str]) -> None:
        started.set()
        await release.wait()

    leader = asyncio.create_task(ledger.acquire(["tickers.BTC"], hang))
    await started.wait()
    waiter = asyncio.create_task(ledger.acquire(["tickers.BTC"], hang))
    for _ in range(20):
        if not waiter.done():
            await asyncio.sleep(0)

    ledger.clear()
    assert not ledger.held()
    with pytest.raises(ConnectionError, match="cleared"):
        await waiter
    release.set()
    await leader
    assert not ledger.held()

    sent: list[list[str]] = []

    async def send(keys: list[str]) -> None:
        sent.append(list(keys))

    await ledger.acquire(["tickers.BTC"], send)
    assert sent == [["tickers.BTC"]]


async def test_a_leader_that_acks_after_clear_does_not_mark_held() -> None:
    ledger: WireLedger[str] = WireLedger()
    started = asyncio.Event()
    release = asyncio.Event()

    async def hang(keys: list[str]) -> None:
        started.set()
        await release.wait()

    leader = asyncio.create_task(ledger.acquire(["tickers.BTC"], hang))
    await started.wait()
    ledger.clear()
    release.set()
    await leader
    assert not ledger.held()

    sent: list[list[str]] = []

    async def send(keys: list[str]) -> None:
        sent.append(list(keys))

    await ledger.acquire(["tickers.BTC"], send)
    assert sent == [["tickers.BTC"]]
    assert ledger.held() == frozenset({"tickers.BTC"})


async def test_discard_forgets_an_explicit_unsubscribe() -> None:
    ledger: WireLedger[str] = WireLedger()
    sent: list[list[str]] = []

    async def send(keys: list[str]) -> None:
        sent.append(list(keys))

    await ledger.acquire(["a", "b"], send)
    ledger.discard(["a"])
    assert ledger.held() == frozenset({"b"})

    await ledger.acquire(["a"], send)
    assert sent == [["a", "b"], ["a"]]
