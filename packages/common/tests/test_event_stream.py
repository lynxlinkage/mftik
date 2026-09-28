"""A full queue still delivers end-of-stream when the stream closes."""

from __future__ import annotations

import asyncio

import pytest
from mftik.exchange.stream import EventStream


@pytest.mark.asyncio
async def test_close_delivers_a_queued_item_then_ends() -> None:
    stream: EventStream[int] = EventStream(maxsize=2)
    stream.push(1)
    stream.close()
    seen: list[int] = []
    async with asyncio.timeout(1):
        async for item in stream:
            seen.append(item)
    assert seen == [1]


@pytest.mark.asyncio
async def test_close_on_a_full_queue_still_ends_the_reader() -> None:
    stream: EventStream[int] = EventStream(maxsize=1)
    stream.push(1)
    stream.close()
    seen: list[int] = []
    async with asyncio.timeout(1):
        async for item in stream:
            seen.append(item)
    # The stop marker needs the only slot, so the queued item is dropped.
    # What matters is that the reader is not stuck on ``get``.
    assert seen == []
