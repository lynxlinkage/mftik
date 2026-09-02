"""How long a serving loop takes to notice it should stop.

``serve`` parks in a blocking ``BLPOP``. It cannot be cancelled out of one —
that leaves the unread reply on the pooled connection and corrupts whatever
borrows it next — so a domain shutting down waits for the pop to lapse and
the loop to recheck its stop event. ``BrokerConfig.serve_poll_seconds`` is
that lapse, and this is what makes it real rather than a field nobody reads.
"""

from __future__ import annotations

import asyncio
import time

from broker_harness import TEST_POLL_SECONDS, a_broker
from mftik.broker import BrokerConfig

SUBJECT = "poll-demo"


def test_production_polls_in_one_second_laps() -> None:
    """The default is the production value, not whatever tests wanted."""
    assert BrokerConfig().serve_poll_seconds == 1.0
    assert TEST_POLL_SECONDS < BrokerConfig().serve_poll_seconds


async def test_a_stopped_loop_retires_within_one_poll() -> None:
    """Nothing arrives; only the stop event ends this, one lapse later."""
    async with a_broker("test-poll") as broker:
        stop = asyncio.Event()

        async def serve() -> None:
            async for _req in broker.serve(SUBJECT, stop=stop):
                raise AssertionError("nothing was ever sent")

        task = asyncio.create_task(serve())
        # Long enough to be parked in the pop rather than still starting up.
        await asyncio.sleep(TEST_POLL_SECONDS * 2)

        began = time.monotonic()
        stop.set()
        await asyncio.wait_for(task, timeout=5)
        waited = time.monotonic() - began

    # The bound that matters is "one poll, not one second": generous enough
    # for a loaded runner, far under the default this would take unwired.
    assert waited < 0.5
