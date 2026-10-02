"""Status-pipe heartbeat for one account worker.

The write is :func:`mftik.procman.heartbeat_loop` (B7-05). This module
only names how often this worker beats. Procman reads ``ready`` and
nothing else. A missing fd — the process was not started under a shim —
writes nothing.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

from mftik.clock import SystemClock
from mftik.procman import WorkerHeartbeat, heartbeat_loop, status_fd
from mftik.procman import write_heartbeat as write_frame

# provisional, pending Yi Te (#286)
#: Shorter than the controller's heartbeat timeout, so a live worker is
#: not declared lost between beats. Passed to ``heartbeat_loop``; the
#: helper does not choose it.
BEAT_PERIOD_S = 1.0


def write_heartbeat(ready: bool) -> None:
    """One beat, so a ready flip is not waiting out :data:`BEAT_PERIOD_S`.

    A full pipe drops the beat. ``EPIPE`` means the shim is gone; the
    periodic loop is what sets the worker's stop.
    """
    fd = status_fd()
    if fd is None:
        return
    try:
        write_frame(fd, WorkerHeartbeat(ready=ready))
    except BrokenPipeError:
        return


async def beat_until(stop: asyncio.Event, ready: Callable[[], bool]) -> None:
    """Beat immediately, then every :data:`BEAT_PERIOD_S` until ``stop``."""
    await heartbeat_loop(
        SystemClock(),
        ready=ready,
        period_s=BEAT_PERIOD_S,
        stop=stop,
        fd=status_fd(),
    )
