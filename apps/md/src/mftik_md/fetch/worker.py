"""Fetch worker process: today's readers, serving ``md.fetch``.

One process per MD instance (§3.1). The MD controller spawns it and
detaches on SIGTERM, so a controller roll does not stop the reads.
Heartbeats go to the shim on the status pipe. The process does not
restart itself.

``python -m mftik_md.fetch`` is the entry. An optional
``module:callable`` argument builds the reader factory; production
passes none and uses :class:`~mftik_md.fetch.readers.VenueReaderFactory`.
The argument exists so a test can answer without contacting a venue.
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import signal
import sys

import uvloop
from mftik.broker import Broker
from mftik.broker.handler import serve
from mftik.clock import SystemClock
from mftik.procman import heartbeat_loop, status_fd
from mftik.protocol import Topics
from mftik.runtime import configure_logging

from mftik_md.defaults import FETCH_HEARTBEAT_PERIOD_S
from mftik_md.fetch.readers import ReaderFactory
from mftik_md.fetch.session import FetchHandler

logger = logging.getLogger("md.fetch")


def _production_factory(broker: Broker) -> ReaderFactory:
    from mftik.symbols import SymbolClient

    from mftik_md.fetch.readers import VenueReaderFactory

    return VenueReaderFactory(SymbolClient(broker))


def _import_factory(ref: str) -> ReaderFactory:
    module_name, sep, attr = ref.partition(":")
    if not sep or not module_name or not attr:
        raise SystemExit(
            f"mftik_md.fetch: factory ref {ref!r} must be 'module:callable'"
        )
    module = importlib.import_module(module_name)
    builder = getattr(module, attr)
    made = builder()
    if not hasattr(made, "create"):
        raise SystemExit(f"mftik_md.fetch: {ref} did not build a reader factory")
    return made


def _factory_ref(argv: list[str]) -> str | None:
    if not argv:
        return None
    if len(argv) != 1:
        raise SystemExit("mftik_md.fetch: expected at most one factory ref")
    return argv[0]


async def amain(factory_ref: str | None = None) -> bool:
    """Serve ``md.fetch`` until SIGTERM, then close the readers.

    Ready is reported on the first heartbeat. The subscription is opened
    by :func:`serve` on that same loop; a caller who arrives in the gap
    retries, the way any request-reply client already does.
    """
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    clock = SystemClock()
    ready = False

    def _ready() -> bool:
        return ready

    async with Broker() as broker:
        factory = (
            _import_factory(factory_ref)
            if factory_ref is not None
            else _production_factory(broker)
        )
        handler = FetchHandler(broker.publish, factory)
        serve_task = asyncio.create_task(
            serve(broker, Topics.md_fetch(), handler, stop=stop),
            name="md-fetch-serve",
        )
        # Serving is up as far as this process can see: the broker is
        # connected and the serve task is scheduled. The shim learns
        # that from the next beat.
        ready = True
        heartbeat = asyncio.create_task(
            heartbeat_loop(
                clock,
                ready=_ready,
                period_s=FETCH_HEARTBEAT_PERIOD_S,
                stop=stop,
                fd=status_fd(),
            ),
            name="md-fetch-heartbeat",
        )
        logger.info("MD fetch worker serving subject=%s", Topics.md_fetch())
        stopper = asyncio.create_task(stop.wait(), name="md-fetch-stop")
        clean = True
        try:
            done, _pending = await asyncio.wait(
                {serve_task, heartbeat, stopper},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if stopper not in done:
                clean = False
                stop.set()
                for task in (serve_task, heartbeat):
                    if task in done and not task.cancelled():
                        error = task.exception()
                        if error is not None:
                            logger.error(
                                "%s ended before shutdown",
                                task.get_name(),
                                exc_info=error,
                            )
        finally:
            stop.set()
            stopper.cancel()
            serve_task.cancel()
            heartbeat.cancel()
            await asyncio.gather(
                stopper, serve_task, heartbeat, return_exceptions=True
            )
            await handler.aclose()
    logger.info("MD fetch worker stopped")
    return clean


def main(argv: list[str] | None = None) -> None:
    configure_logging("md.fetch")
    args = list(sys.argv[1:] if argv is None else argv)
    if not uvloop.run(amain(_factory_ref(args))):
        raise SystemExit(1)


__all__ = ["amain", "main"]
