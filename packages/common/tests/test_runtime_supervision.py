"""A long-lived task that ends must end the process, not go unnoticed.

The bootstrap this replaces was ``await stop.wait()`` with the service's tasks
sitting beside it, awaited by nobody. A task that raised was therefore never
collected, never logged — not even by asyncio's own "Task exception was never
retrieved", which only fires once a task is garbage collected and these are
held by a local for the life of the process. The service stayed up, one of its
loops silently gone.
"""

from __future__ import annotations

import asyncio
import logging

import pytest
from mftik import run_until_stopped


async def _forever() -> None:
    await asyncio.Event().wait()


@pytest.mark.asyncio
async def test_a_task_that_raises_stops_the_service_and_says_why(
    caplog: pytest.LogCaptureFixture,
) -> None:
    stop = asyncio.Event()
    doomed = asyncio.create_task(_boom(), name="demo-rpc")
    healthy = asyncio.create_task(_forever(), name="demo-heartbeat")
    logger = logging.getLogger("demo")

    with caplog.at_level(logging.ERROR, logger="demo"):
        clean = await asyncio.wait_for(
            run_until_stopped(stop, doomed, healthy, logger=logger), timeout=5
        )

    assert clean is False
    # Set, so the caller's ordinary teardown runs — the sessions still get
    # their terminal rows written before the process goes.
    assert stop.is_set()
    assert "demo-rpc ended before shutdown" in caplog.text
    # The exception that ended it, not just the fact that something did.
    assert "RuntimeError" in caplog.text
    assert "poll failed" in caplog.text

    healthy.cancel()


async def _boom() -> None:
    raise RuntimeError("poll failed")


@pytest.mark.asyncio
async def test_an_ordinary_shutdown_reports_nothing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """SIGTERM must not look like a failure, or the real one stops standing out."""
    stop = asyncio.Event()
    tasks = [
        asyncio.create_task(_forever(), name="demo-rpc"),
        asyncio.create_task(_forever(), name="demo-reaper"),
    ]
    logger = logging.getLogger("demo")

    waiter = asyncio.create_task(
        run_until_stopped(stop, *tasks, logger=logger)
    )
    await asyncio.sleep(0.05)
    stop.set()

    with caplog.at_level(logging.ERROR, logger="demo"):
        assert await asyncio.wait_for(waiter, timeout=5) is True

    assert caplog.text == ""
    assert all(not task.done() for task in tasks)
    for task in tasks:
        task.cancel()


@pytest.mark.asyncio
async def test_a_cancelled_task_is_reported_without_being_re_raised() -> None:
    """Asking a cancelled task for its exception raises it back at you.

    Reporting a dead loop must not itself blow up on the way, or the report
    that mattered is replaced by the failure of the reporter.
    """
    stop = asyncio.Event()
    cancelled = asyncio.create_task(_forever(), name="demo-rpc")
    cancelled.cancel()

    clean = await asyncio.wait_for(
        run_until_stopped(stop, cancelled, logger=logging.getLogger("demo")),
        timeout=5,
    )

    assert clean is False
    assert stop.is_set()
