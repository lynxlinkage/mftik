"""Strategy timer tokens — schedule / cancel time-based callbacks (unix ms)."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from mft_sts.strategy import Strategy

logger = logging.getLogger(__name__)

TimerFunc = Callable[[], Any]


def now_ms() -> int:
    """Current unix time in milliseconds."""
    return int(time.time() * 1000)


class TimerToken:
    """Handle for one scheduled callback.

    Usage::

        token = self.timer.token()
        token.register(first_ms, interval_ms, self.on_tick)
        ...
        token.cancel()
    """

    def __init__(self, timer: Timer) -> None:
        self._timer = timer
        self._task: asyncio.Task[None] | None = None
        self._cancelled = asyncio.Event()
        self._func: TimerFunc | None = None
        self._first_ms: int | None = None
        self._interval_ms: int = 0
        self._label: str = "timer"

    @property
    def active(self) -> bool:
        return self._task is not None and not self._task.done()

    @property
    def label(self) -> str:
        """What this token is called in the event log."""
        return self._label

    def register(
        self,
        first_ms: int,
        interval_ms: int,
        func: TimerFunc,
        *,
        label: str | None = None,
    ) -> TimerToken:
        """Schedule ``func`` at ``first_ms`` (unix ms), then every ``interval_ms``.

        ``interval_ms <= 0`` means one-shot. Re-registering replaces any prior
        schedule on this token.

        ``label`` names the token in the session event log, defaulting to the
        callback's qualified name. Worth setting when one method backs several
        tokens — two schedules that log under one name cannot be told apart by
        anyone reading back what the session did.
        """
        if not callable(func):
            raise TypeError("func must be callable")
        self.cancel()
        self._cancelled = asyncio.Event()
        self._func = func
        self._first_ms = int(first_ms)
        self._interval_ms = int(interval_ms)
        self._label = label or getattr(func, "__qualname__", None) or repr(func)
        self._timer._track(self)
        self._timer._record(
            "registered",
            self._label,
            first_ms=self._first_ms,
            interval_ms=self._interval_ms,
        )
        self._task = asyncio.create_task(
            self._run(),
            name=f"timer-token-{id(self):x}",
        )
        return self

    def cancel(self) -> None:
        """Cancel this token's scheduled function (no-op if idle).

        Safe to call from inside the callback: cancelling the task there would
        throw ``CancelledError`` into the coroutine doing the cancelling, at
        its next await, and the rest of that caller's work would silently never
        run — a strategy that cancels its timer and then places a closing order
        would cancel itself instead. The ``_cancelled`` event alone stops the
        loop once the callback returns, so self-cancellation only sets it.
        """
        self._cancelled.set()
        task = self._task
        self._task = None
        self._func = None
        if (
            task is not None
            and not task.done()
            and task is not asyncio.current_task()
        ):
            task.cancel()
        self._timer._untrack(self)

    async def _run(self) -> None:
        assert self._first_ms is not None and self._func is not None
        func = self._func
        next_ms = self._first_ms
        interval = self._interval_ms
        try:
            while not self._cancelled.is_set():
                delay_s = (next_ms - self._timer.now_ms()) / 1000.0
                if delay_s > 0:
                    try:
                        await asyncio.wait_for(
                            self._cancelled.wait(), timeout=delay_s
                        )
                        return
                    except TimeoutError:
                        pass
                if self._cancelled.is_set():
                    return
                if not self._timer._should_fire():
                    # Paused / closed: hold the due time until resumed or cancel.
                    try:
                        await asyncio.wait_for(
                            self._cancelled.wait(), timeout=0.05
                        )
                        return
                    except TimeoutError:
                        continue
                # ``due_ms`` alongside the wall clock: a tick that fires late
                # is the timer's own version of wire latency, and a strategy
                # that acted on a stale schedule shows it here.
                self._timer._record(
                    "fired", self._label, due_ms=next_ms
                )
                await self._invoke(func)
                if interval <= 0 or self._cancelled.is_set():
                    return
                next_ms += interval
                # Skip missed ticks if the callback ran long.
                now = self._timer.now_ms()
                if next_ms <= now:
                    missed = (now - next_ms) // interval + 1
                    next_ms += missed * interval
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._timer._record(
                "failed", self._label, error=repr(exc)
            )
            logger.exception("timer token callback failed")
        finally:
            self._task = None
            self._timer._untrack(self)

    async def _invoke(self, func: TimerFunc) -> None:
        result = func()
        if isinstance(result, Awaitable):
            await result


class Timer:
    """Session-scoped factory for :class:`TimerToken` (unix ms clock)."""

    def __init__(
        self,
        *,
        time_fn: Callable[[], int] | None = None,
    ) -> None:
        self._time_fn = time_fn or now_ms
        self._strategy: Strategy | None = None
        self._tokens: set[TimerToken] = set()
        self._closed = False

    def bind(self, strategy: Strategy) -> None:
        self._strategy = strategy

    def now_ms(self) -> int:
        return int(self._time_fn())

    def token(self) -> TimerToken:
        """Create a new idle timer token."""
        if self._closed:
            raise RuntimeError("timer is closed")
        return TimerToken(self)

    def close(self) -> None:
        """Cancel all tokens (called when the STS session stops)."""
        self._closed = True
        for token in list(self._tokens):
            token.cancel()
        self._tokens.clear()

    def _record(self, event: str, label: str, **fields: Any) -> None:
        """Log one token event, if this timer's session keeps an event log."""
        strategy = self._strategy
        session = getattr(strategy, "session", None) if strategy else None
        log = getattr(session, "event_log", None)
        if log is not None:
            log.record("timer", event, dir="self", token=label, **fields)

    def _track(self, token: TimerToken) -> None:
        if self._closed:
            raise RuntimeError("timer is closed")
        self._tokens.add(token)

    def _untrack(self, token: TimerToken) -> None:
        self._tokens.discard(token)

    def _should_fire(self) -> bool:
        if self._closed:
            return False
        if self._strategy is not None and self._strategy.paused:
            return False
        return True
