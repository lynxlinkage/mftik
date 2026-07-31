from __future__ import annotations

import asyncio
import logging
import os
import signal


def configure_logging(name: str) -> logging.Logger:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    return logging.getLogger(name)


async def run_heartbeat_service(source: str) -> None:
    from mft.broker import BrokerClient

    logger = logging.getLogger(source)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    async with BrokerClient() as broker:
        logger.info("%s started; heartbeat → Redis", source)
        await broker.heartbeat_loop(
            source,
            interval=5.0,
            stop=stop,
            on_tick=lambda: logger.debug("heartbeat"),
        )
    logger.info("%s stopped", source)
