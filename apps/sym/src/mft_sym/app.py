"""Sym process bootstrap — start, keep the plane fresh, close.

There is no session lifecycle here. The plane is up for as long as the process
is, independent of any TD/MD/STS deployment, so a strategy can ask what an
instrument looks like before anything is trading.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal

from mft import configure_logging
from mft.broker import Broker
from mft.protocol import Topics

from mft_sym import db as sym_db
from mft_sym.plane import SymbolPlane
from mft_sym.rpc import dispatch
from mft_sym.sources import default_sources

SOURCE = "sym"
logger = logging.getLogger(SOURCE)

DEFAULT_REFRESH_INTERVAL = 3600.0


def refresh_interval() -> float:
    try:
        return float(os.getenv("SYM_REFRESH_INTERVAL", DEFAULT_REFRESH_INTERVAL))
    except ValueError:
        return DEFAULT_REFRESH_INTERVAL


def build_plane(broker: Broker) -> SymbolPlane:
    return SymbolPlane(
        default_sources(broker),
        upsert=sym_db.upsert_instrument,
        deactivate_missing=sym_db.deactivate_missing,
        list_tickers=sym_db.list_tickers,
        list_filters_for=sym_db.list_filters_for,
        refresh_interval=refresh_interval(),
    )


async def run_rpc(
    broker: Broker, plane: SymbolPlane, stop: asyncio.Event
) -> None:
    logger.info("SYM RPC listening on subject=%s", Topics.SYM)
    async for req in broker.serve(Topics.SYM, stop=stop):
        try:
            await dispatch(req, plane=plane)
        except Exception:
            logger.exception(
                "SYM RPC handler failed type=%s id=%s",
                req.envelope.type,
                req.envelope.id,
            )


async def amain() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    async with Broker() as broker:
        # Built inside the broker context: the paper source talks over IPC.
        plane = build_plane(broker)
        # Serve before the first refresh so a slow venue endpoint cannot hold
        # the whole process hostage at startup; queries just see fewer rows.
        rpc_task = asyncio.create_task(
            run_rpc(broker, plane, stop), name="sym-rpc"
        )
        hb_task = asyncio.create_task(
            broker.heartbeat_loop(
                SOURCE,
                interval=5.0,
                stop=stop,
                on_tick=lambda: logger.debug("heartbeat"),
            ),
            name="sym-heartbeat",
        )
        refresh_task = asyncio.create_task(
            _initial_then_periodic(plane, stop), name="sym-refresh"
        )
        logger.info(
            "SYM started venues=%s refresh_interval=%ss",
            plane.venues,
            plane.refresh_interval,
        )
        try:
            await stop.wait()
        finally:
            stop.set()
            for task in (rpc_task, hb_task, refresh_task):
                task.cancel()
            await asyncio.gather(
                rpc_task, hb_task, refresh_task, return_exceptions=True
            )
            await plane.close()
    logger.info("SYM stopped")


async def _initial_then_periodic(
    plane: SymbolPlane, stop: asyncio.Event
) -> None:
    result = await plane.refresh()
    if result["failed"]:
        logger.warning("SYM initial refresh partial: %s", result["failed"])
    await plane.refresh_loop(stop)


def main() -> None:
    configure_logging(SOURCE)
    asyncio.run(amain())
