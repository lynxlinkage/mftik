"""The connection worker tells its shim it is alive through the shared helper.

``ready`` stays false until the paper client connects, and a shim that is
already gone stops the process instead of raising out of the beat.
"""

from __future__ import annotations

import asyncio
import os

from mftik.clock import FakeClock
from mftik.procman import WorkerHeartbeat, decode_heartbeat
from mftik_md.conn_worker import _BEAT_INTERVAL_S, _heartbeat, _Ready


def _frames(fd: int) -> list[WorkerHeartbeat]:
    chunks: list[bytes] = []
    while True:
        try:
            chunk = os.read(fd, 65536)
        except BlockingIOError:
            break
        if not chunk:
            break
        chunks.append(chunk)
    lines = [line for line in b"".join(chunks).split(b"\n") if line]
    return [decode_heartbeat(line + b"\n") for line in lines]


async def test_ready_is_false_until_connect_and_then_stays_true() -> None:
    read_fd, write_fd = os.pipe()
    os.set_blocking(read_fd, False)
    stop = asyncio.Event()
    clock = FakeClock()
    ready = _Ready()
    task = asyncio.create_task(_heartbeat(clock, stop, ready, write_fd))
    try:
        await asyncio.sleep(0)
        assert _frames(read_fd) == [WorkerHeartbeat(ready=False)]
        assert ready() is False

        ready.connected(stop, write_fd)
        assert ready() is True
        assert not stop.is_set()
        assert _frames(read_fd) == [WorkerHeartbeat(ready=True)]

        # A later drop does not clear ready. The next period carries it.
        clock.advance(_BEAT_INTERVAL_S)
        await asyncio.sleep(0)
        assert _frames(read_fd) == [WorkerHeartbeat(ready=True)]
        assert _BEAT_INTERVAL_S == 0.2

        stop.set()
        clock.advance(_BEAT_INTERVAL_S)
        await asyncio.sleep(0)
        await task
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        os.close(read_fd)
        os.close(write_fd)


async def test_a_closed_shim_stops_the_heartbeat() -> None:
    read_fd, write_fd = os.pipe()
    os.set_blocking(read_fd, False)
    stop = asyncio.Event()
    clock = FakeClock()
    task = asyncio.create_task(
        _heartbeat(clock, stop, _Ready(), write_fd)
    )
    try:
        await asyncio.sleep(0)
        os.close(read_fd)
        read_fd = -1
        clock.advance(_BEAT_INTERVAL_S)
        await asyncio.sleep(0)
        await task
        assert stop.is_set()
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if read_fd >= 0:
            os.close(read_fd)
        os.close(write_fd)


def test_connecting_after_the_shim_is_gone_stops() -> None:
    read_fd, write_fd = os.pipe()
    os.close(read_fd)
    stop = asyncio.Event()
    ready = _Ready()
    try:
        ready.connected(stop, write_fd)
    finally:
        os.close(write_fd)
    assert ready() is True
    assert stop.is_set()


async def test_no_shim_connects_and_still_stops_when_asked() -> None:
    stop = asyncio.Event()
    clock = FakeClock()
    ready = _Ready()
    task = asyncio.create_task(_heartbeat(clock, stop, ready, None))
    await asyncio.sleep(0)
    ready.connected(stop, None)
    assert ready() is True
    assert not stop.is_set()
    stop.set()
    clock.advance(_BEAT_INTERVAL_S)
    await asyncio.sleep(0)
    await task
