"""Status-pipe heartbeat for one account worker.

B7-05 has not added a helper. This module writes
:class:`~mftik.procman.WorkerHeartbeat` to the fd the shim put in
:data:`~mftik.procman.STATUS_FD_ENV`. Procman reads ``ready`` and nothing
else. A missing fd — the process was not started under a shim — writes
nothing.
"""

from __future__ import annotations

import asyncio
import errno
import logging
import os
from collections.abc import Callable

from mftik.procman import STATUS_FD_ENV, WorkerHeartbeat, encode_heartbeat

logger = logging.getLogger(__name__)

# provisional, pending Yi Te (#286)
#: Shorter than the controller's heartbeat timeout, so a live worker is
#: not declared lost between beats.
BEAT_PERIOD_S = 1.0


def write_heartbeat(ready: bool) -> None:
    """One non-blocking beat. A full pipe or a closed fd is dropped."""
    fd = _status_fd()
    if fd is None:
        return
    frame = encode_heartbeat(WorkerHeartbeat(ready=ready))
    try:
        os.write(fd, frame)
    except BlockingIOError:
        return
    except OSError as exc:
        if exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
            return
        logger.warning("account worker heartbeat write failed: %s", exc)


async def beat_until(stop: asyncio.Event, ready: Callable[[], bool]) -> None:
    """Beat immediately, then every :data:`BEAT_PERIOD_S` until ``stop``."""
    write_heartbeat(ready())
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=BEAT_PERIOD_S)
        except TimeoutError:
            write_heartbeat(ready())


def _status_fd() -> int | None:
    raw = os.environ.get(STATUS_FD_ENV)
    if raw is None or raw == "":
        return None
    try:
        fd = int(raw)
    except ValueError:
        logger.warning("status fd %r is not an int", raw)
        return None
    try:
        os.set_blocking(fd, False)
    except OSError:
        logger.warning("status fd %s cannot be set nonblocking", fd)
        return None
    return fd
