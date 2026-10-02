"""A worker's status pipe: one snapshot per beat, never a blocked write."""

from __future__ import annotations

import asyncio
import os
import time

import pytest
from mftik.clock import FakeClock
from mftik.procman import (
    STATUS_FD_ENV,
    WorkerHeartbeat,
    decode_heartbeat,
    heartbeat_loop,
    status_fd,
    write_heartbeat,
)


def _drain(fd: int) -> bytes:
    chunks: list[bytes] = []
    while True:
        try:
            chunk = os.read(fd, 65536)
        except BlockingIOError:
            break
        if not chunk:
            break
        chunks.append(chunk)
    return b"".join(chunks)


def _frames(fd: int) -> list[WorkerHeartbeat]:
    lines = [line for line in _drain(fd).split(b"\n") if line]
    return [decode_heartbeat(line + b"\n") for line in lines]


def test_status_fd_is_absent_without_a_usable_descriptor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(STATUS_FD_ENV, raising=False)
    assert status_fd() is None
    monkeypatch.setenv(STATUS_FD_ENV, "nope")
    assert status_fd() is None
    monkeypatch.setenv(STATUS_FD_ENV, "-1")
    assert status_fd() is None


def test_status_fd_is_the_pipe_the_shim_named(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    read_fd, write_fd = os.pipe()
    try:
        monkeypatch.setenv(STATUS_FD_ENV, str(write_fd))
        assert status_fd() == write_fd
        assert os.get_blocking(write_fd) is False
        assert write_heartbeat(write_fd, WorkerHeartbeat(ready=True)) is True
        os.set_blocking(read_fd, False)
        assert _frames(read_fd) == [WorkerHeartbeat(ready=True)]
    finally:
        os.close(read_fd)
        os.close(write_fd)


def test_a_closed_descriptor_is_not_a_status_fd(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    read_fd, write_fd = os.pipe()
    os.close(write_fd)
    os.close(read_fd)
    monkeypatch.setenv(STATUS_FD_ENV, str(write_fd))
    assert status_fd() is None


def test_a_full_pipe_drops_the_beat_without_blocking() -> None:
    read_fd, write_fd = os.pipe()
    os.set_blocking(write_fd, False)
    blob = b"x" * 4096
    try:
        while True:
            os.write(write_fd, blob)
    except BlockingIOError:
        pass
    started = time.perf_counter()
    try:
        accepted = write_heartbeat(write_fd, WorkerHeartbeat(ready=True))
    finally:
        os.close(read_fd)
        os.close(write_fd)
    assert accepted is False
    assert time.perf_counter() - started < 0.05


def test_a_missing_reader_is_a_broken_pipe() -> None:
    read_fd, write_fd = os.pipe()
    os.close(read_fd)
    try:
        with pytest.raises(BrokenPipeError):
            write_heartbeat(write_fd, WorkerHeartbeat(ready=False))
    finally:
        os.close(write_fd)


async def test_the_loop_sends_a_full_snapshot_each_period() -> None:
    read_fd, write_fd = os.pipe()
    os.set_blocking(read_fd, False)
    os.set_blocking(write_fd, False)
    ready = False
    stop = asyncio.Event()
    clock = FakeClock()

    def _ready() -> bool:
        return ready

    task = asyncio.create_task(
        heartbeat_loop(
            clock,
            ready=_ready,
            period_s=1.0,
            stop=stop,
            fd=write_fd,
            extra=lambda: {"step": 1 if ready else 0},
        )
    )
    try:
        await asyncio.sleep(0)
        assert _frames(read_fd) == [
            WorkerHeartbeat(ready=False, extra={"step": 0})
        ]
        ready = True
        clock.advance(1.0)
        await asyncio.sleep(0)
        assert _frames(read_fd) == [
            WorkerHeartbeat(ready=True, extra={"step": 1})
        ]
        stop.set()
        clock.advance(1.0)
        await asyncio.sleep(0)
        await task
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        os.close(read_fd)
        os.close(write_fd)


async def test_a_full_pipe_drops_one_beat_and_the_next_is_the_whole_state() -> None:
    read_fd, write_fd = os.pipe()
    os.set_blocking(read_fd, False)
    os.set_blocking(write_fd, False)
    blob = b"x" * 4096
    try:
        while True:
            os.write(write_fd, blob)
    except BlockingIOError:
        pass
    ready = False
    stop = asyncio.Event()
    clock = FakeClock()
    task = asyncio.create_task(
        heartbeat_loop(
            clock,
            ready=lambda: ready,
            period_s=1.0,
            stop=stop,
            fd=write_fd,
        )
    )
    try:
        await asyncio.sleep(0)
        _drain(read_fd)
        ready = True
        clock.advance(1.0)
        await asyncio.sleep(0)
        assert _frames(read_fd) == [WorkerHeartbeat(ready=True)]
        stop.set()
        clock.advance(1.0)
        await asyncio.sleep(0)
        await task
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        os.close(read_fd)
        os.close(write_fd)


async def test_a_broken_pipe_stops_the_loop() -> None:
    read_fd, write_fd = os.pipe()
    os.close(read_fd)
    stop = asyncio.Event()
    task = asyncio.create_task(
        heartbeat_loop(
            FakeClock(),
            ready=lambda: True,
            period_s=30.0,
            stop=stop,
            fd=write_fd,
        )
    )
    try:
        await asyncio.sleep(0)
        assert stop.is_set()
        assert task.done()
        await task
    finally:
        os.close(write_fd)


async def test_no_fd_still_waits_out_the_period() -> None:
    stop = asyncio.Event()
    clock = FakeClock()
    task = asyncio.create_task(
        heartbeat_loop(
            clock, ready=lambda: True, period_s=5.0, stop=stop, fd=None
        )
    )
    await asyncio.sleep(0)
    assert not task.done()
    stop.set()
    clock.advance(5.0)
    await asyncio.sleep(0)
    await task


@pytest.mark.parametrize("period", [-1, True])
async def test_a_bad_period_is_refused(period: object) -> None:
    with pytest.raises(ValueError):
        await heartbeat_loop(
            FakeClock(),
            ready=lambda: True,
            period_s=period,  # type: ignore[arg-type]
            stop=asyncio.Event(),
        )
