"""Clock, FakeClock, and the ban on wall-clock sleep in unit tests.

§9.2 rule 1. ``advance`` is what moves a ``FakeClock``: it completes
``sleep`` and it fires timers. A unit test that calls ``asyncio.sleep``
with a positive delay fails, and the failure names the caller.
"""

from __future__ import annotations

import asyncio
import inspect

import pytest
from mftik.clock import Clock, FakeClock, SystemClock
from sleep_guard import RealSleepForbidden


def test_a_library_path_is_not_this_repository() -> None:
    """nats-py and websockets sleep on their own; that is not the test."""
    import sleep_guard

    root = sleep_guard._ROOT
    assert sleep_guard._in_repo(str(root / "packages/common/src/mftik/clock.py"))
    assert not sleep_guard._in_repo(
        str(root / ".venv/lib/python3.12/site-packages/nats/aio/client.py")
    )
    assert not sleep_guard._in_repo("/usr/lib/python3.12/asyncio/tasks.py")


def test_both_clocks_satisfy_the_protocol() -> None:
    assert isinstance(SystemClock(), Clock)
    assert isinstance(FakeClock(), Clock)


async def test_system_clock_reads_the_process_clock() -> None:
    import time

    clock = SystemClock()
    before = time.time()
    assert before <= clock.now() <= time.time()
    mono = time.monotonic()
    assert mono <= clock.monotonic() <= time.monotonic()
    # Zero is a yield, not a wait, so the guard allows it.
    await clock.sleep(0)


async def test_advance_wakes_sleep() -> None:
    clock = FakeClock()
    woke = False

    async def sleeper() -> None:
        nonlocal woke
        await clock.sleep(5)
        woke = True

    task = asyncio.create_task(sleeper())
    await asyncio.sleep(0)
    assert woke is False
    assert clock.monotonic() == 0
    clock.advance(4)
    await asyncio.sleep(0)
    assert woke is False
    clock.advance(1)
    await asyncio.sleep(0)
    assert woke is True
    assert clock.monotonic() == 5
    assert clock.now() == 5
    await task


async def test_advance_wakes_sleepers_in_deadline_order() -> None:
    clock = FakeClock()
    order: list[str] = []

    async def nap(name: str, seconds: float) -> None:
        await clock.sleep(seconds)
        order.append(name)

    early = asyncio.create_task(nap("early", 1))
    late = asyncio.create_task(nap("late", 2))
    await asyncio.sleep(0)
    clock.advance(2)
    await asyncio.sleep(0)
    assert order == ["early", "late"]
    assert clock.now() == 2
    await early
    await late


async def test_one_advance_fires_a_timer_and_wakes_a_sleep() -> None:
    """Timers run inside advance; the sleeper resumes on the next turn."""
    clock = FakeClock()
    seen: list[str] = []
    clock.call_later(3, lambda: seen.append(f"timer@{clock.monotonic()}"))

    async def sleeper() -> None:
        await clock.sleep(3)
        seen.append(f"sleep@{clock.monotonic()}")

    task = asyncio.create_task(sleeper())
    await asyncio.sleep(0)
    clock.advance(3)
    assert seen == ["timer@3.0"]
    await asyncio.sleep(0)
    assert seen == ["timer@3.0", "sleep@3.0"]
    await task


def test_advance_fires_timers_at_their_deadline() -> None:
    clock = FakeClock()
    seen: list[float] = []
    clock.call_later(2, lambda: seen.append(clock.monotonic()))
    clock.call_later(1, lambda: seen.append(clock.monotonic()))
    clock.advance(1)
    assert seen == [1.0]
    assert clock.monotonic() == 1
    clock.advance(1)
    assert seen == [1.0, 2.0]
    assert clock.now() == 2


def test_advance_zero_fires_a_timer_that_is_already_due() -> None:
    clock = FakeClock()
    seen: list[float] = []
    clock.call_later(0, lambda: seen.append(clock.monotonic()))
    assert seen == []
    clock.advance(0)
    assert seen == [0.0]
    assert clock.monotonic() == 0


def test_a_timer_scheduled_by_a_timer_at_the_same_instant_fires_too() -> None:
    clock = FakeClock()
    seen: list[str] = []

    def first() -> None:
        seen.append("first")
        clock.call_later(0, lambda: seen.append("second"))

    clock.call_later(1, first)
    clock.advance(1)
    assert seen == ["first", "second"]
    assert clock.monotonic() == 1


def test_cancelling_a_timer_keeps_advance_from_firing_it() -> None:
    clock = FakeClock()
    seen: list[int] = []
    handle = clock.call_later(1, lambda: seen.append(1))
    handle.cancel()
    assert handle.cancelled()
    clock.advance(1)
    assert seen == []


async def test_cancelling_a_sleep_then_advancing_does_not_wake_it() -> None:
    clock = FakeClock()
    woke = False

    async def sleeper() -> None:
        nonlocal woke
        await clock.sleep(5)
        woke = True

    task = asyncio.create_task(sleeper())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    clock.advance(5)
    assert woke is False


def test_a_past_deadline_fires_on_the_next_advance() -> None:
    clock = FakeClock()
    seen: list[float] = []
    clock.advance(10)
    clock.call_at(3, lambda: seen.append(clock.monotonic()))
    clock.advance(0)
    assert seen == [10.0]


def test_negative_advance_and_delay_are_rejected() -> None:
    clock = FakeClock()
    with pytest.raises(ValueError):
        clock.advance(-1)
    with pytest.raises(ValueError):
        clock.call_later(-1, lambda: None)


async def test_negative_sleep_is_rejected_on_the_running_loop() -> None:
    clock = FakeClock()
    with pytest.raises(ValueError):
        await clock.sleep(-1)


def test_a_timer_callback_cannot_reenter_advance() -> None:
    clock = FakeClock()

    def reenter() -> None:
        clock.advance(1)

    clock.call_later(1, reenter)
    with pytest.raises(RuntimeError):
        clock.advance(1)


def test_a_timer_callback_must_be_synchronous() -> None:
    clock = FakeClock()

    async def callback() -> None:
        return None

    clock.call_later(1, callback)
    with pytest.raises(TypeError):
        clock.advance(1)


async def test_sleep_zero_does_not_wait_for_advance() -> None:
    clock = FakeClock()
    await clock.sleep(0)
    assert clock.monotonic() == 0


async def _sleep_for_real() -> None:
    await asyncio.sleep(1)


async def test_a_unit_test_that_sleeps_for_real_fails_at_the_call_site() -> None:
    """The failure names the caller, not the guard."""
    with pytest.raises(RealSleepForbidden) as caught:
        await _sleep_for_real()
    err = caught.value
    assert err.func == "_sleep_for_real"
    assert err.filename.endswith("test_clock.py")
    source_line = inspect.getsourcelines(_sleep_for_real)[1]
    # The await is the second line of the function body.
    assert err.lineno == source_line + 1
    assert "test_clock.py" in str(err)
    assert "_sleep_for_real" in str(err)
    assert err.nodeid.endswith(
        "test_a_unit_test_that_sleeps_for_real_fails_at_the_call_site"
    )


async def test_a_zero_sleep_is_not_a_real_sleep() -> None:
    await asyncio.sleep(0)


@pytest.mark.component
async def test_component_tier_still_forbids_real_sleep() -> None:
    with pytest.raises(RealSleepForbidden) as caught:
        await asyncio.sleep(1)
    assert caught.value.func == "test_component_tier_still_forbids_real_sleep"


@pytest.mark.real_sleep(reason="proves the opt-out reaches asyncio.sleep")
async def test_real_sleep_marker_allows_a_positive_sleep() -> None:
    await asyncio.sleep(0.001)


@pytest.mark.integration
async def test_integration_tier_may_sleep_for_real() -> None:
    await asyncio.sleep(0.001)


@pytest.mark.e2e
async def test_e2e_tier_may_sleep_for_real() -> None:
    await asyncio.sleep(0.001)
