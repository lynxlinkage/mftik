from __future__ import annotations

import asyncio

import pytest
from mftik.strategy.timer import Timer, now_ms


@pytest.mark.asyncio
async def test_one_shot_register_and_fire() -> None:
    timer = Timer()
    hits: list[int] = []
    token = timer.token()
    first = now_ms() + 50
    token.register(first, 0, lambda: hits.append(timer.now_ms()))

    await asyncio.sleep(0.15)
    assert len(hits) == 1
    assert hits[0] >= first
    assert not token.active


@pytest.mark.asyncio
async def test_interval_and_cancel() -> None:
    timer = Timer()
    hits: list[int] = []
    token = timer.token()
    first = now_ms() + 30
    token.register(first, 40, lambda: hits.append(1))

    await asyncio.sleep(0.14)
    assert len(hits) >= 2
    token.cancel()
    n = len(hits)
    await asyncio.sleep(0.1)
    assert len(hits) == n
    assert not token.active


@pytest.mark.asyncio
async def test_async_callback() -> None:
    timer = Timer()
    done = asyncio.Event()

    async def cb() -> None:
        done.set()

    token = timer.token()
    token.register(now_ms(), 0, cb)
    await asyncio.wait_for(done.wait(), timeout=1.0)
    token.cancel()


@pytest.mark.asyncio
async def test_cancel_before_fire() -> None:
    timer = Timer()
    hits: list[int] = []
    token = timer.token()
    token.register(now_ms() + 500, 0, lambda: hits.append(1))
    token.cancel()
    await asyncio.sleep(0.05)
    assert hits == []


@pytest.mark.asyncio
async def test_close_cancels_all() -> None:
    timer = Timer()
    hits: list[int] = []
    t1 = timer.token()
    t2 = timer.token()
    t1.register(now_ms() + 200, 0, lambda: hits.append(1))
    t2.register(now_ms() + 200, 50, lambda: hits.append(2))
    timer.close()
    await asyncio.sleep(0.05)
    assert hits == []
    with pytest.raises(RuntimeError, match="closed"):
        timer.token()


@pytest.mark.asyncio
async def test_skips_while_paused() -> None:
    class _Paused:
        paused = True

    timer = Timer()
    timer.bind(_Paused())  # type: ignore[arg-type]
    hits: list[int] = []
    token = timer.token()
    token.register(now_ms(), 30, lambda: hits.append(1))
    await asyncio.sleep(0.1)
    assert hits == []
    timer._strategy.paused = False  # type: ignore[union-attr]
    await asyncio.sleep(0.08)
    assert len(hits) >= 1
    token.cancel()


@pytest.mark.asyncio
async def test_a_callback_can_cancel_its_own_token_and_keep_going() -> None:
    """Self-cancel must not kill the coroutine that asked for it.

    A strategy ending itself cancels the timer and then still has work to do —
    cancelling the resting order, sending a closing order, exiting the session.
    Cancelling the token's task from inside the callback would throw
    CancelledError into that very coroutine at its next await, and all of it
    would be skipped in silence.
    """
    timer = Timer()
    token = timer.token()
    done: list[str] = []

    async def _tick() -> None:
        token.cancel()
        # An await that actually suspends is where the cancellation would land.
        await asyncio.sleep(0.01)
        done.append("finished")

    token.register(now_ms(), 30, _tick)
    await asyncio.sleep(0.15)

    assert done == ["finished"]
    # And it really did stop: no second tick.
    assert not token.active
