from __future__ import annotations

import asyncio
import logging
import os
import signal

from mft import configure_logging
from mft.broker import BrokerClient
from mft.strategy import Strategy

SOURCE = "sts"


async def amain(strategy: Strategy | None = None) -> None:
    from mft_sts.impl.noop import NoopStrategy

    logger = logging.getLogger(SOURCE)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    async with BrokerClient() as broker:
        strat = strategy or NoopStrategy(
            broker, session_id=os.getenv("SESSION_ID")
        )
        await strat.on_start()
        logger.info("Strategy domain started name=%s", strat.name)
        try:
            await broker.heartbeat_loop(
                SOURCE,
                interval=5.0,
                stop=stop,
                on_tick=lambda: logger.debug("heartbeat"),
            )
        finally:
            await strat.on_stop()
    logger.info("Strategy domain stopped")


def main() -> None:
    configure_logging(SOURCE)
    asyncio.run(amain())
