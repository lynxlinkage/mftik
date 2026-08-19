from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
from typing import Any


def configure_logging(name: str) -> logging.Logger:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    return logging.getLogger(name)


async def run_until_stopped(
    stop: asyncio.Event,
    *tasks: asyncio.Task[Any],
    logger: logging.Logger,
) -> bool:
    """Block until ``stop`` is set, or until one of ``tasks`` ends before it.

    What a service's bootstrap used to do here was ``await stop.wait()``, and
    the tasks beside it were never awaited at all. A long-lived task that ended
    on its own therefore ended in silence: the process stayed up, every other
    task kept running, and because nothing ever retrieves the exception from a
    task still referenced by a local, not even "Task exception was never
    retrieved" reached the log. On 2026-08-18 STS lost its RPC loop that way
    and went on trading for seven hours with nothing able to list, pause or
    stop a session, while ``docker ps`` showed it up and healthy.

    So a task that finishes first is treated as shutdown: it is reported with
    whatever ended it, and ``stop`` is set so the caller runs its ordinary
    teardown. Returning False lets the caller exit non-zero, which is the
    difference between a container the restart policy brings back and one that
    sits there looking fine.

    Tasks that are *meant* to finish — a boot-time rebuild, a one-shot scan —
    do not belong in ``tasks``.
    """
    stopper = asyncio.create_task(stop.wait(), name="run-until-stopped")
    try:
        done, _ = await asyncio.wait(
            [stopper, *tasks], return_when=asyncio.FIRST_COMPLETED
        )
    finally:
        stopper.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await stopper

    clean = True
    for task in tasks:
        if task not in done:
            continue
        clean = False
        # ``cancelled()`` first: asking a cancelled task for its exception
        # raises the CancelledError rather than returning it, which would
        # replace the report with the failure it is trying to report.
        error = None if task.cancelled() else task.exception()
        logger.error(
            "%s ended before shutdown — stopping the process",
            task.get_name(),
            exc_info=error,
        )
        stop.set()
    return clean


async def run_heartbeat_service(source: str) -> None:
    from mftik.broker import Broker

    logger = logging.getLogger(source)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    async with Broker() as broker:
        logger.info("%s started; heartbeat → Redis", source)
        await broker.heartbeat_loop(
            source,
            interval=5.0,
            stop=stop,
            on_tick=lambda: logger.debug("heartbeat"),
        )
    logger.info("%s stopped", source)
