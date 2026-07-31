from __future__ import annotations

import asyncio
import logging
import os
import signal

from mft_broker import BrokerClient

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("mft_trading")

SOURCE = "trading"


async def amain() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    async with BrokerClient() as broker:
        logger.info("Trading domain started; heartbeat → Redis")
        await broker.heartbeat_loop(
            SOURCE,
            interval=5.0,
            stop=stop,
            on_tick=lambda: logger.debug("heartbeat"),
        )
    logger.info("Trading domain stopped")


def run() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    run()
