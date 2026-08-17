"""In-process async stream helpers for venue push feeds."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from typing import TypeVar

T = TypeVar("T")
_STOP = object()


class EventStream(AsyncIterator[T]):
    """Fan-out subscription backed by an ``asyncio.Queue``."""

    def __init__(
        self,
        *,
        on_close: Callable[[EventStream[T]], None] | None = None,
        maxsize: int = 256,
    ) -> None:
        self._queue: asyncio.Queue[T | object] = asyncio.Queue(maxsize=maxsize)
        self._on_close = on_close
        self._closed = False

    def push(self, item: T) -> None:
        if self._closed:
            return
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull:
            # Drop oldest to keep the stream live under backpressure.
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            try:
                self._queue.put_nowait(item)
            except asyncio.QueueFull:
                pass

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._queue.put_nowait(_STOP)
        except asyncio.QueueFull:
            pass
        if self._on_close is not None:
            self._on_close(self)

    def __aiter__(self) -> EventStream[T]:
        return self

    async def __anext__(self) -> T:
        item = await self._queue.get()
        if item is _STOP:
            raise StopAsyncIteration
        return item  # type: ignore[return-value]

    async def aclose(self) -> None:
        self.close()
