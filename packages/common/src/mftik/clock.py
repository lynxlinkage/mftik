"""Injected time (§9.2 rule 1, §3.4).

Controllers, workers and reconcilers are meant to read time from a
:class:`Clock` — :meth:`~Clock.now`, :meth:`~Clock.monotonic`,
:meth:`~Clock.sleep` — instead of ``time.time()`` and ``asyncio.sleep``.
A test passes a :class:`FakeClock` and moves it with
:meth:`FakeClock.advance`. :class:`SystemClock` is the process clock, for
when a caller is wired up for real.

Nothing in the planes is switched over yet. That conversion belongs to the
tickets that build each plane; this module only gives them the type to
switch to.

**State authority (§3.3):** a clock is not the authority for any row in
that table. It supplies the time those authorities observe.
:class:`SystemClock` reads the process clock and stores nothing.
:class:`FakeClock` owns the simulated instant for the test that built it:
it does not move unless that test calls :meth:`~FakeClock.advance`, and it
is not shared between tests.
"""

from __future__ import annotations

import asyncio
import heapq
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    """Where a controller, worker or reconciler reads the time.

    ``now`` is wall time, ``monotonic`` is the clock sleep is measured on.
    The two are not the same number on a real machine; code that needs an
    interval uses :meth:`monotonic`, code that needs a timestamp uses
    :meth:`now`.
    """

    def now(self) -> float:
        """Wall time in seconds since the Unix epoch, as ``time.time()``."""
        ...

    def monotonic(self) -> float:
        """Monotonic time in seconds, as ``time.monotonic()``."""
        ...

    async def sleep(self, seconds: float) -> None:
        """Wait until ``seconds`` have passed on :meth:`monotonic`.

        ``seconds == 0`` yields to the event loop and does not wait for
        time to move. A negative length raises ``ValueError``, as
        ``asyncio.sleep`` does.
        """
        ...


class SystemClock:
    """The process clock. ``sleep`` is ``asyncio.sleep``."""

    def now(self) -> float:
        return time.time()

    def monotonic(self) -> float:
        return time.monotonic()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


@dataclass(order=True)
class _Waiter:
    when: float
    seq: int
    future: asyncio.Future[None] | None = field(compare=False, default=None)
    callback: Callable[..., Any] | None = field(compare=False, default=None)
    args: tuple[Any, ...] = field(compare=False, default=())
    cancelled: bool = field(compare=False, default=False)


class TimerHandle:
    """One timer scheduled on a :class:`FakeClock`.

    ``cancel`` is the same shape as ``asyncio.TimerHandle.cancel``: calling
    it means :meth:`FakeClock.advance` will not run the callback.
    """

    def __init__(self, waiter: _Waiter) -> None:
        self._waiter = waiter

    def cancel(self) -> None:
        self._waiter.cancelled = True

    def cancelled(self) -> bool:
        return self._waiter.cancelled


class FakeClock:
    """A clock that moves only when :meth:`advance` says so.

    :meth:`sleep` and timers (:meth:`call_later`, :meth:`call_at`) share one
    queue, ordered by the monotonic deadline and then by the order they were
    scheduled. :meth:`advance` moves :meth:`now` and :meth:`monotonic` by the
    same amount — they start equal — and along the way fires every timer
    whose deadline it reaches and completes every :meth:`sleep` whose
    deadline it reaches.

    Timer callbacks run *inside* :meth:`advance`, synchronously, at the
    deadline. A sleeping coroutine is resumed by the event loop on its next
    turn, after :meth:`advance` has returned; the test yields (``await
    asyncio.sleep(0)``, which is not a wait) to see that. A callback may
    schedule another timer, including one due at the same instant, and this
    ``advance`` fires that too. A callback may not call :meth:`advance`.

    ``sleep(0)`` does not wait for an advance. It yields, matching
    ``asyncio.sleep(0)``, because a zero delay is how a coroutine lets
    another task run rather than how it waits for time.
    """

    def __init__(self, *, start: float = 0.0) -> None:
        self._wall = start
        self._mono = start
        self._heap: list[_Waiter] = []
        self._seq = 0
        self._advancing = False

    def now(self) -> float:
        return self._wall

    def monotonic(self) -> float:
        return self._mono

    async def sleep(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("sleep length must be non-negative")
        if seconds == 0:
            await asyncio.sleep(0)
            return
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        waiter = self._push(self._mono + seconds, future=future)
        try:
            await future
        except asyncio.CancelledError:
            # Leave the future pending. Cancelling it here stores a
            # CancelledError nobody retrieves. The flag is what keeps a
            # later advance from completing a sleep whose task is gone.
            waiter.cancelled = True
            raise

    def call_later(
        self,
        delay: float,
        callback: Callable[..., Any],
        *args: Any,
    ) -> TimerHandle:
        """Run ``callback(*args)`` after ``delay`` seconds of this clock.

        The callback is synchronous. It does not run until :meth:`advance`
        reaches the deadline, so ``call_later(0, ...)`` fires on the next
        ``advance``, including ``advance(0)``. A negative delay raises
        ``ValueError``.
        """
        if delay < 0:
            raise ValueError("delay must be non-negative")
        return self.call_at(self._mono + delay, callback, *args)

    def call_at(
        self,
        when: float,
        callback: Callable[..., Any],
        *args: Any,
    ) -> TimerHandle:
        """Run ``callback(*args)`` when :meth:`monotonic` reaches ``when``.

        ``when`` is a monotonic instant, the same unit :meth:`monotonic`
        returns. A deadline already in the past fires on the next
        :meth:`advance`, including ``advance(0)``.
        """
        waiter = self._push(when, callback=callback, args=args)
        return TimerHandle(waiter)

    def advance(self, seconds: float) -> None:
        """Move the clock forward by ``seconds`` and run what that makes due.

        Sleepers whose deadline is now in the past are completed, and timer
        callbacks whose deadline is now in the past are called, in deadline
        order. ``seconds == 0`` fires anything already due and does not move
        the clock. A negative length raises ``ValueError``.
        """
        if seconds < 0:
            raise ValueError("advance length must be non-negative")
        if self._advancing:
            raise RuntimeError(
                "FakeClock.advance cannot be called from a timer callback"
            )
        self._advancing = True
        try:
            target = self._mono + seconds
            while True:
                when = self._next_deadline(target)
                if when is None:
                    break
                self._move_to(when)
                self._fire_due()
            self._move_to(target)
        finally:
            self._advancing = False

    def _next_seq(self) -> int:
        seq = self._seq
        self._seq += 1
        return seq

    def _push(
        self,
        when: float,
        *,
        future: asyncio.Future[None] | None = None,
        callback: Callable[..., Any] | None = None,
        args: tuple[Any, ...] = (),
    ) -> _Waiter:
        waiter = _Waiter(
            when=when,
            seq=self._next_seq(),
            future=future,
            callback=callback,
            args=args,
        )
        heapq.heappush(self._heap, waiter)
        return waiter

    def _next_deadline(self, target: float) -> float | None:
        while self._heap and self._heap[0].cancelled:
            heapq.heappop(self._heap)
        if not self._heap or self._heap[0].when > target:
            return None
        return self._heap[0].when

    def _move_to(self, mono: float) -> None:
        if mono < self._mono:
            return
        self._wall += mono - self._mono
        self._mono = mono

    def _fire_due(self) -> None:
        while self._heap:
            if self._heap[0].cancelled:
                heapq.heappop(self._heap)
                continue
            if self._heap[0].when > self._mono:
                break
            waiter = heapq.heappop(self._heap)
            self._dispatch(waiter)

    def _dispatch(self, waiter: _Waiter) -> None:
        if waiter.cancelled:
            return
        future = waiter.future
        if future is not None:
            if not future.done():
                future.set_result(None)
            return
        callback = waiter.callback
        if callback is None:
            return
        result = callback(*waiter.args)
        if asyncio.iscoroutine(result):
            result.close()
            raise TypeError("FakeClock timer callbacks must be synchronous")


__all__ = [
    "Clock",
    "FakeClock",
    "SystemClock",
    "TimerHandle",
]
