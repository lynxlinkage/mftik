"""How a worker tells its shim it is alive (S6).

The shim owns the status pipe. It puts the write end's number in
:data:`~mftik.procman.messages.STATUS_FD_ENV` before exec. This module is
the worker's side of that pipe: one full snapshot per beat, written so a
full pipe never stalls the process. The next beat carries the whole
snapshot again. A missing shim (``EPIPE``) is the worker's signal to
stop (S2); a full pipe is not.

Nothing here starts a process, reads a socket, or changes a
:class:`~mftik.procman.Supervisor` slot. Planes call it. The supervisor
keeps polling the shim.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable, Mapping
from typing import Any

from mftik.clock import Clock
from mftik.procman.messages import STATUS_FD_ENV, WorkerHeartbeat, encode_heartbeat

#: ``extra`` a beat may carry. The frame is still one snapshot (S6).
Extra = Callable[[], Mapping[str, Any]]
Ready = Callable[[], bool]


def status_fd() -> int | None:
    """The status-pipe fd from the environment, or ``None`` when there is none.

    The fd is made non-blocking. An unset variable, a value that is not a
    fd, or a fd this process cannot use is ``None``: a worker started
    without a shim still runs, and simply has no pipe to write.
    """
    raw = os.environ.get(STATUS_FD_ENV, "").strip()
    if not raw:
        return None
    try:
        fd = int(raw)
    except ValueError:
        return None
    if fd < 0:
        return None
    try:
        os.set_blocking(fd, False)
    except OSError:
        return None
    return fd


def write_heartbeat(fd: int, beat: WorkerHeartbeat) -> bool:
    """Write one heartbeat frame. ``False`` means the pipe could not take it.

    The write does not block. A full pipe drops this beat (S6); the caller
    sends a whole snapshot next time. ``BrokenPipeError`` means the read
    end is gone — the shim has disappeared (S2). The fd is not closed.
    """
    frame = encode_heartbeat(beat)
    try:
        os.set_blocking(fd, False)
        written = os.write(fd, frame)
    except BlockingIOError:
        return False
    return written == len(frame)


async def heartbeat_loop(
    clock: Clock,
    *,
    ready: Ready,
    period_s: float,
    stop: asyncio.Event,
    fd: int | None = None,
    extra: Extra | None = None,
) -> None:
    """Write a full snapshot every ``period_s`` until ``stop`` or ``EPIPE``.

    ``ready`` and ``extra`` are read on each beat, so a change shows up on
    the next write rather than being edited into a frame already sent.
    ``fd`` of ``None`` still waits out the period: the loop is the pace,
    and a process with no shim has nothing to write. ``period_s`` is the
    caller's. This module does not pick a heartbeat timeout.

    ``BrokenPipeError`` sets ``stop`` and returns. The worker treats that
    as the shim going away (S2).
    """
    if type(period_s) is bool or not isinstance(period_s, int | float) or period_s < 0:
        raise ValueError("period_s must be >= 0")
    while not stop.is_set():
        beat = WorkerHeartbeat(
            ready=bool(ready()),
            extra=dict(extra()) if extra is not None else {},
        )
        if fd is not None:
            try:
                write_heartbeat(fd, beat)
            except BrokenPipeError:
                stop.set()
                return
        if stop.is_set():
            return
        await clock.sleep(float(period_s))


__all__ = [
    "heartbeat_loop",
    "status_fd",
    "write_heartbeat",
]
