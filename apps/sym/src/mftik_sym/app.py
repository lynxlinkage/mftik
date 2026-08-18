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

from mftik import configure_logging, run_until_stopped
from mftik.broker import Broker
from mftik.protocol import Topics

from mftik_sym import db as sym_db
from mftik_sym.plane import SymbolPlane
from mftik_sym.rpc import dispatch
from mftik_sym.sources import default_sources

SOURCE = "sym"
logger = logging.getLogger(SOURCE)

#: How long a serve loop waits before rebuilding itself after an exception it
#: did not expect. ``Broker.serve`` already survives what it knows how to
#: survive, so this only paces the failures nothing has a name for yet.
RPC_RESTART_DELAY_SECONDS = 1.0


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
        count_tickers=sym_db.count_tickers,
        refresh_interval=refresh_interval(),
    )


async def run_rpc(
    broker: Broker, plane: SymbolPlane, stop: asyncio.Event
) -> None:
    logger.info("SYM RPC listening on subject=%s", Topics.SYM)
    while not stop.is_set():
        try:
            async for req in broker.serve(Topics.SYM, stop=stop):
                try:
                    await dispatch(req, plane=plane)
                except Exception:
                    logger.exception(
                        "SYM RPC handler failed type=%s id=%s",
                        req.envelope.type,
                        req.envelope.id,
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            # Reaching here means something ``serve`` does not already handle,
            # and the answer is still to serve: this coroutine returning is how
            # SYM answers nobody while every venue's symbols go on refreshing.
            logger.exception("SYM RPC serve loop failed — restarting")
            try:
                await asyncio.wait_for(
                    stop.wait(), timeout=RPC_RESTART_DELAY_SECONDS
                )
            except TimeoutError:
                continue


async def amain() -> bool:
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
            clean = await run_until_stopped(
                stop, rpc_task, hb_task, refresh_task, logger=logger
            )
        finally:
            stop.set()
            for task in (rpc_task, hb_task, refresh_task):
                task.cancel()
            await asyncio.gather(
                rpc_task, hb_task, refresh_task, return_exceptions=True
            )
            await plane.close()
    logger.info("SYM stopped")
    return clean


async def _initial_then_periodic(
    plane: SymbolPlane, stop: asyncio.Event
) -> None:
    result = await plane.refresh()
    if result["failed"]:
        logger.warning("SYM initial refresh partial: %s", result["failed"])
    await plane.refresh_loop(stop)


def main() -> None:
    configure_logging(SOURCE)
    # Non-zero when a long-lived task ended on its own: the restart
    # policy is what puts the process back, and an exit code is what
    # tells anyone reading ``docker ps`` that SYM did not just stop.
    if not asyncio.run(amain()):
        raise SystemExit(1)
