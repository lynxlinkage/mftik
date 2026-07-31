from __future__ import annotations

import asyncio

from mft import configure_logging, run_heartbeat_service

SOURCE = "md"


def main() -> None:
    configure_logging(SOURCE)
    asyncio.run(run_heartbeat_service(SOURCE))
