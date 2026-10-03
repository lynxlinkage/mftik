"""The per-socket wire ledger — reservation before the venue ack.

I2 is the concurrent case: two ``acquire`` calls for the same identity
must send one frame. A failed send rolls the reservation back so the
next caller retries. Restore clears the set first so a failed replay
does not stick a name as subscribed with nothing on the wire.
"""

from __future__ import annotations

import asyncio
import threading

import pytest
from mftik.exchange.errors import ExchangeNotConnectedError
from mftik.exchange.wire import (
    IdleReleaser,
    ReleaseOutcome,
    WireLedger,
    classify_release,
    first_seen,
)

# §9.1 component (loopback venue stub). Slow cases miss the 50 ms unit call cap;
# the 500 ms component cap still applies.
pytestmark = pytest.mark.component


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


async def test_a_leader_that_fails_after_clear_does_not_kill_a_fresh_reservation() -> (
    None
):
    ledger: WireLedger[str] = WireLedger()
    started = asyncio.Event()
    release = asyncio.Event()

    async def hang_then_fail(keys: list[str]) -> None:
        started.set()
        await release.wait()
        raise ConnectionError("old socket")

    leader_a = asyncio.create_task(ledger.acquire(["k"], hang_then_fail))
    await started.wait()
    ledger.clear()

    sent: list[list[str]] = []
    restored = asyncio.Event()

    async def restore(keys: list[str]) -> None:
        sent.append(list(keys))
        restored.set()

    leader_b = asyncio.create_task(ledger.acquire(["k"], restore))
    waiter = asyncio.create_task(ledger.acquire(["k"], restore))
    await restored.wait()

    release.set()
    with pytest.raises(ConnectionError, match="old socket"):
        await leader_a
    await leader_b
    await waiter
    assert sent == [["k"]]
    assert ledger.held() == frozenset({"k"})


def test_a_timeout_message_is_unknown_and_a_venue_error_is_rejected() -> None:
    assert classify_release(TimeoutError()) is ReleaseOutcome.UNKNOWN
    assert classify_release(ConnectionError("reset")) is ReleaseOutcome.UNKNOWN
    assert (
        classify_release(RuntimeError("no reply within 10s")) is ReleaseOutcome.UNKNOWN
    )
    assert classify_release(RuntimeError("unknown stream")) is ReleaseOutcome.REJECTED
    assert (
        classify_release(ExchangeNotConnectedError("bybit is not connected"))
        is ReleaseOutcome.UNKNOWN
    )
    assert (
        classify_release(RuntimeError("socket not ready within 10s"))
        is ReleaseOutcome.UNKNOWN
    )


async def test_release_of_the_last_reader_drops_the_key() -> None:
    ledger: WireLedger[str] = WireLedger()
    sent: list[list[str]] = []

    async def subscribe(keys: list[str]) -> None:
        sent.append(list(keys))

    async def unsubscribe(keys: list[str]) -> dict[str, ReleaseOutcome]:
        sent.append(list(keys))
        return {key: ReleaseOutcome.ACKED for key in keys}

    await ledger.acquire(["a", "b"], subscribe)
    outcomes = await ledger.release(["a"], unsubscribe, lambda _key: False)
    assert outcomes == {"a": ReleaseOutcome.ACKED}
    assert ledger.held() == frozenset({"b"})
    assert sent == [["a", "b"], ["a"]]


async def test_release_leaves_a_key_a_reader_still_wants() -> None:
    ledger: WireLedger[str] = WireLedger()
    sent: list[list[str]] = []

    async def subscribe(keys: list[str]) -> None:
        pass

    async def unsubscribe(keys: list[str]) -> dict[str, ReleaseOutcome]:
        sent.append(list(keys))
        return {key: ReleaseOutcome.ACKED for key in keys}

    await ledger.acquire(["a"], subscribe)
    outcomes = await ledger.release(["a"], unsubscribe, lambda _key: True)
    assert outcomes == {}
    assert sent == []
    assert ledger.held() == frozenset({"a"})


async def test_an_explicit_rejection_puts_the_key_back() -> None:
    ledger: WireLedger[str] = WireLedger()
    subscribed: list[list[str]] = []

    async def subscribe(keys: list[str]) -> None:
        subscribed.append(list(keys))

    async def unsubscribe(keys: list[str]) -> dict[str, ReleaseOutcome]:
        return {key: ReleaseOutcome.REJECTED for key in keys}

    await ledger.acquire(["a"], subscribe)
    await ledger.release(["a"], unsubscribe, lambda _key: False)
    assert ledger.held() == frozenset({"a"})

    await ledger.acquire(["a"], subscribe)
    assert subscribed == [["a"]]


async def test_an_unknown_unsubscribe_lets_the_next_acquire_subscribe() -> None:
    ledger: WireLedger[str] = WireLedger()
    subscribed: list[list[str]] = []

    async def subscribe(keys: list[str]) -> None:
        subscribed.append(list(keys))

    async def unsubscribe(keys: list[str]) -> dict[str, ReleaseOutcome]:
        return {key: ReleaseOutcome.UNKNOWN for key in keys}

    await ledger.acquire(["a"], subscribe)
    await ledger.release(["a"], unsubscribe, lambda _key: False)
    assert not ledger.held()

    await ledger.acquire(["a"], subscribe)
    assert subscribed == [["a"], ["a"]]


async def test_a_raised_unsubscribe_is_unknown_and_propagates() -> None:
    ledger: WireLedger[str] = WireLedger()

    async def subscribe(keys: list[str]) -> None:
        pass

    async def unsubscribe(keys: list[str]) -> dict[str, ReleaseOutcome]:
        raise ConnectionError("socket lost")

    await ledger.acquire(["a"], subscribe)
    with pytest.raises(ConnectionError, match="socket lost"):
        await ledger.release(["a"], unsubscribe, lambda _key: False)
    assert not ledger.held()


async def test_acquire_during_release_resubscribes_without_reserving_a_sibling() -> (
    None
):
    ledger: WireLedger[str] = WireLedger()

    async def subscribe(keys: list[str]) -> None:
        pass

    await ledger.acquire(["a", "b"], subscribe)

    started = asyncio.Event()
    finish = asyncio.Event()
    sent: list[list[str]] = []

    async def unsubscribe(keys: list[str]) -> dict[str, ReleaseOutcome]:
        started.set()
        await finish.wait()
        return {key: ReleaseOutcome.ACKED for key in keys}

    releasing = asyncio.create_task(
        ledger.release(["a"], unsubscribe, lambda _key: False)
    )
    await started.wait()

    async def resubscribe(keys: list[str]) -> None:
        sent.append(list(keys))

    acquiring = asyncio.create_task(ledger.acquire(["a", "b"], resubscribe))
    for _ in range(20):
        if sent:
            break
        await asyncio.sleep(0)
    assert sent == []
    finish.set()
    await releasing
    await acquiring
    assert sent == [["a"]]
    assert ledger.held() == frozenset({"a", "b"})


async def test_clear_during_release_does_not_mark_the_new_generation_held() -> None:
    ledger: WireLedger[str] = WireLedger()

    async def subscribe(keys: list[str]) -> None:
        pass

    await ledger.acquire(["a"], subscribe)
    started = asyncio.Event()
    finish = asyncio.Event()

    async def unsubscribe(keys: list[str]) -> dict[str, ReleaseOutcome]:
        started.set()
        await finish.wait()
        return {key: ReleaseOutcome.ACKED for key in keys}

    releasing = asyncio.create_task(
        ledger.release(["a"], unsubscribe, lambda _key: False)
    )
    await started.wait()
    ledger.clear()
    finish.set()
    await releasing
    assert not ledger.held()

    sent: list[list[str]] = []

    async def send(keys: list[str]) -> None:
        sent.append(list(keys))

    await ledger.acquire(["a"], send)
    assert sent == [["a"]]
    assert ledger.held() == frozenset({"a"})


async def test_one_release_can_ack_one_key_and_reject_another() -> None:
    ledger: WireLedger[str] = WireLedger()

    async def subscribe(keys: list[str]) -> None:
        pass

    async def unsubscribe(keys: list[str]) -> dict[str, ReleaseOutcome]:
        return {"a": ReleaseOutcome.ACKED, "b": ReleaseOutcome.REJECTED}

    await ledger.acquire(["a", "b"], subscribe)
    outcomes = await ledger.release(["a", "b"], unsubscribe, lambda _key: False)
    assert outcomes["a"] is ReleaseOutcome.ACKED
    assert outcomes["b"] is ReleaseOutcome.REJECTED
    assert ledger.held() == frozenset({"b"})


async def test_a_second_release_waits_instead_of_sending_again() -> None:
    ledger: WireLedger[str] = WireLedger()

    async def subscribe(keys: list[str]) -> None:
        pass

    await ledger.acquire(["a"], subscribe)
    started = asyncio.Event()
    finish = asyncio.Event()
    sent: list[list[str]] = []

    async def unsubscribe(keys: list[str]) -> dict[str, ReleaseOutcome]:
        sent.append(list(keys))
        started.set()
        await finish.wait()
        return {key: ReleaseOutcome.ACKED for key in keys}

    first = asyncio.create_task(ledger.release(["a"], unsubscribe, lambda _key: False))
    await started.wait()
    second = asyncio.create_task(ledger.release(["a"], unsubscribe, lambda _key: False))
    for _ in range(20):
        if second.done():
            break
        await asyncio.sleep(0)
    assert not second.done()
    finish.set()
    assert await first == {"a": ReleaseOutcome.ACKED}
    assert await second == {"a": ReleaseOutcome.ACKED}
    assert sent == [["a"]]
    assert not ledger.held()


async def test_release_waits_out_a_resync_cycle() -> None:
    ledger: WireLedger[str] = WireLedger()

    async def subscribe(keys: list[str]) -> None:
        pass

    await ledger.acquire(["a"], subscribe)
    sent: list[list[str]] = []
    entered = asyncio.Event()
    finish_cycle = asyncio.Event()

    async def unsubscribe(keys: list[str]) -> dict[str, ReleaseOutcome]:
        sent.append(list(keys))
        return {key: ReleaseOutcome.ACKED for key in keys}

    async def cycle() -> None:
        async with ledger.cycling("a"):
            entered.set()
            await finish_cycle.wait()

    cycling = asyncio.create_task(cycle())
    await entered.wait()
    releasing = asyncio.create_task(
        ledger.release(["a"], unsubscribe, lambda _key: False)
    )
    for _ in range(20):
        await asyncio.sleep(0)
    assert sent == []
    finish_cycle.set()
    await cycling
    await releasing
    assert sent == [["a"]]
    assert not ledger.held()


async def _second_cycle_waits() -> None:
    ledger: WireLedger[str] = WireLedger()

    async def subscribe(keys: list[str]) -> None:
        del keys

    await ledger.acquire(["a"], subscribe)
    started = asyncio.Event()
    release = asyncio.Event()
    second_in = asyncio.Event()
    other_ran = asyncio.Event()

    async def first() -> None:
        async with ledger.cycling("a"):
            started.set()
            await release.wait()

    async def second() -> None:
        async with ledger.cycling("a"):
            second_in.set()

    async def other() -> None:
        await asyncio.sleep(0)
        other_ran.set()

    first_task = asyncio.create_task(first())
    await started.wait()
    second_task = asyncio.create_task(second())
    other_task = asyncio.create_task(other())
    await asyncio.wait_for(other_ran.wait(), 0.5)
    assert not second_in.is_set()
    release.set()
    await asyncio.wait_for(second_task, 0.5)
    await first_task
    await other_task
    assert second_in.is_set()


def test_a_second_cycle_waits_on_the_one_already_running() -> None:
    """Another resync for the same key awaits the cycle, and the loop still runs."""
    errors: list[BaseException] = []

    def run() -> None:
        try:
            asyncio.run(_second_cycle_waits())
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(1)
    assert not thread.is_alive()
    assert errors == []


@pytest.mark.real_sleep(
    reason="WireLedger still sleeps on the wall clock"
)
async def test_an_acquire_in_flight_keeps_a_held_key_and_retries_it() -> None:
    """A batch subscribe attaches its reader only after the ack.

    The key that was already held is not in ``_inflight``, but the new
    reader is not in ``_subs`` yet either. Releasing it now drops data
    the reader is about to need. Deferring and retrying unsubscribes it
    if that subscribe then fails to attach anyone.
    """
    ledger: WireLedger[str] = WireLedger()
    unsubscribed: list[list[str]] = []
    gate = asyncio.Event()
    started = asyncio.Event()

    async def subscribe_ok(keys: list[str]) -> None:
        del keys

    async def subscribe_blocked(keys: list[str]) -> None:
        del keys
        started.set()
        await gate.wait()

    async def unsubscribe(keys: list[str]) -> dict[str, ReleaseOutcome]:
        unsubscribed.append(list(keys))
        return {key: ReleaseOutcome.ACKED for key in keys}

    await ledger.acquire(["a"], subscribe_ok)
    acquiring = asyncio.create_task(ledger.acquire(["a", "b"], subscribe_blocked))
    await started.wait()
    releaser = IdleReleaser(
        ledger,
        unsubscribe,
        lambda _key: False,
        linger=0.05,
        reconcile_interval=60,
    )
    try:
        releaser.enqueue(["a"])
        await asyncio.sleep(0.12)
        assert unsubscribed == []
        assert "a" in ledger.held()
        gate.set()
        await acquiring
        await releaser.drained()
        assert unsubscribed == [["a"]]
        assert "a" not in ledger.held()
    finally:
        releaser.cancel()


# linger is wall-clock; over the 500 ms component cap
@pytest.mark.integration
@pytest.mark.real_sleep(
    reason="WireLedger still sleeps on the wall clock"
)
async def test_each_idle_key_waits_out_its_own_linger() -> None:
    ledger: WireLedger[str] = WireLedger()
    sent: list[list[str]] = []

    async def subscribe(keys: list[str]) -> None:
        del keys

    async def unsubscribe(keys: list[str]) -> dict[str, ReleaseOutcome]:
        sent.append(list(keys))
        return {key: ReleaseOutcome.ACKED for key in keys}

    await ledger.acquire(["a", "b"], subscribe)
    releaser = IdleReleaser(
        ledger,
        unsubscribe,
        lambda _key: False,
        linger=0.3,
        reconcile_interval=60,
    )
    try:
        releaser.enqueue(["a"])
        await asyncio.sleep(0.15)
        releaser.enqueue(["b"])
        await asyncio.sleep(0.2)
        assert sent == [["a"]]
        assert "b" in ledger.held()
        await releaser.drained()
        assert sent == [["a"], ["b"]]
    finally:
        releaser.cancel()


@pytest.mark.real_sleep(
    reason="WireLedger still sleeps on the wall clock"
)
async def test_a_release_of_an_unacked_subscribe_is_deferred() -> None:
    """A subscribe that has not acked is not held, and must not look done.

    Reporting it acked drops the retry. The venue then keeps pushing a
    key nobody will release until the socket reconnects.
    """
    ledger: WireLedger[str] = WireLedger()
    unsubscribed: list[list[str]] = []
    gate = asyncio.Event()
    started = asyncio.Event()

    async def subscribe(keys: list[str]) -> None:
        del keys
        started.set()
        await gate.wait()

    async def unsubscribe(keys: list[str]) -> dict[str, ReleaseOutcome]:
        unsubscribed.append(list(keys))
        return {key: ReleaseOutcome.ACKED for key in keys}

    acquiring = asyncio.create_task(ledger.acquire(["a"], subscribe))
    await started.wait()
    releaser = IdleReleaser(
        ledger,
        unsubscribe,
        lambda _key: False,
        linger=0.05,
        reconcile_interval=60,
    )
    try:
        releaser.enqueue(["a"])
        await asyncio.sleep(0.12)
        assert unsubscribed == []
        assert "a" not in ledger.held()
        gate.set()
        await acquiring
        await releaser.drained()
        assert unsubscribed == [["a"]]
        assert "a" not in ledger.held()
    finally:
        releaser.cancel()


# linger is wall-clock; over the 500 ms component cap
@pytest.mark.integration
@pytest.mark.real_sleep(
    reason="WireLedger still sleeps on the wall clock"
)
async def test_closing_again_restarts_the_linger() -> None:
    ledger: WireLedger[str] = WireLedger()
    sent: list[list[str]] = []

    async def subscribe(keys: list[str]) -> None:
        del keys

    async def unsubscribe(keys: list[str]) -> dict[str, ReleaseOutcome]:
        sent.append(list(keys))
        return {key: ReleaseOutcome.ACKED for key in keys}

    await ledger.acquire(["a"], subscribe)
    releaser = IdleReleaser(
        ledger,
        unsubscribe,
        lambda _key: False,
        linger=0.3,
        reconcile_interval=60,
    )
    try:
        releaser.enqueue(["a"])
        await asyncio.sleep(0.2)
        releaser.enqueue(["a"])
        await asyncio.sleep(0.2)
        assert sent == []
        await releaser.drained()
        assert sent == [["a"]]
    finally:
        releaser.cancel()


@pytest.mark.real_sleep(
    reason="WireLedger still sleeps on the wall clock"
)
async def test_reconcile_retries_a_rejected_unsubscribe() -> None:
    ledger: WireLedger[str] = WireLedger()
    sent: list[list[str]] = []

    async def subscribe(keys: list[str]) -> None:
        del keys

    async def unsubscribe(keys: list[str]) -> dict[str, ReleaseOutcome]:
        sent.append(list(keys))
        if len(sent) == 1:
            return {key: ReleaseOutcome.REJECTED for key in keys}
        return {key: ReleaseOutcome.ACKED for key in keys}

    await ledger.acquire(["a"], subscribe)
    releaser = IdleReleaser(
        ledger,
        unsubscribe,
        lambda _key: False,
        linger=0.05,
        reconcile_interval=0.05,
    )
    try:
        releaser.enqueue(["a"])
        for _ in range(40):
            if len(sent) >= 2:
                break
            await asyncio.sleep(0.02)
        assert sent == [["a"], ["a"]]
        assert "a" not in ledger.held()
    finally:
        releaser.cancel()


@pytest.mark.real_sleep(
    reason="WireLedger still sleeps on the wall clock"
)
async def test_reconcile_leaves_a_key_that_was_never_unsubscribed() -> None:
    """A held key the flusher never attempted is not a failed unsubscribe."""
    ledger: WireLedger[str] = WireLedger()
    sent: list[list[str]] = []

    async def subscribe(keys: list[str]) -> None:
        del keys

    async def unsubscribe(keys: list[str]) -> dict[str, ReleaseOutcome]:
        sent.append(list(keys))
        return {key: ReleaseOutcome.ACKED for key in keys}

    await ledger.acquire(["public", "private"], subscribe)
    releaser = IdleReleaser(
        ledger,
        unsubscribe,
        lambda _key: False,
        linger=0.05,
        reconcile_interval=0.05,
    )
    try:
        releaser.enqueue(["public"])
        await releaser.drained()
        await asyncio.sleep(0.15)
        assert sent == [["public"]]
        assert ledger.held() == frozenset({"private"})
    finally:
        releaser.cancel()


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
