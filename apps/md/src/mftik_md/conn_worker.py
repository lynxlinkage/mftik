"""Paper connection-worker process (B4-06).

One process, one :class:`~mftik_md.conn.ConnId`, one incarnation. The
atom list is the process's argv (``--atom``, repeated). That list is the
only desired this process will ever apply: it is subscribed on connect
and again after the paper stream drops. There is no desired-set push
(:meth:`~mftik_md.conn.ConnWorker.accept_desired` is B8-03) and this
module is not started by the MD process (``app.py`` stays with B4-07
and B7-05).

The shim is told this process is up with
:func:`mftik.procman.heartbeat_loop` on ``MFTIK_STATUS_FD``. ``ready``
flips true after the paper client connects, and stays true. A missing
read end (``EPIPE``) sets the process stop, so a shim that is already
gone ends this process cleanly (S2). No status fd means this process
was not launched by a shim: the beats are a no-op and a signal still
stops it. A process that dies still trying to connect is an init
failure (``FAILED``, not restarted). A later drop of the paper stream
is a reconnect inside this process, not a return to "not ready".

``seq`` is :class:`~mftik_md.conn.SeqClock` on the worker. A reconnect
inside this process does not replace the clock (C6). A new incarnation
is a new process and starts again at :data:`~mftik_md.conn.SEQ_ORIGIN`.

The incarnation is logged. It is not a field on the envelope. The
envelope ``source`` is the worker id (C8).
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal
import sys
from collections.abc import AsyncIterator, Callable, Sequence

import uvloop
from mftik.broker import Broker
from mftik.clock import Clock, SystemClock
from mftik.exchange.atoms import TOPIC_ORDERBOOK, Atom, UnsupportedTopicError
from mftik.exchange.paper.atoms import PUBLIC, VENUE, parse_channel
from mftik.exchange.paper.atoms import decode as paper_decode
from mftik.exchange.paper.remote_public import PaperRemotePublicClient
from mftik.exchange.tickers import UniversalTicker
from mftik.procman import (
    WorkerHeartbeat,
    heartbeat_loop,
    status_fd,
    write_heartbeat,
)
from mftik.runtime import configure_logging

from mftik_md.conn import ConnError, ConnId, ConnWorker

logger = logging.getLogger("md.conn")

#: How often the status pipe is refreshed. Not a product restart timer
#: and not the B8-02 heartbeat budget: the shim counts beats, and a
#: stuck loop is what a missed beat means (S6). This worker's own pace.
#: Default; adjust from measurement (Appendix D).
_BEAT_INTERVAL_S = 0.2

#: Pause before subscribing the same fixed set again, so a dead stream
#: does not spin. Not the B8-03 token bucket and not a B8-02 restart
#: delay. Paper only; B8-03 does not carry this to a real venue.
#: Default; adjust from measurement (Appendix D).
_RESUBSCRIBE_PAUSE_S = 0.05


def argv_for(
    *,
    python: str,
    instance: str,
    incarnation: int,
    conn: ConnId,
    atoms: Sequence[Atom],
) -> tuple[str, ...]:
    """argv a supervisor execs for this entry.

    The atom list is the whole desired set. Nothing else is read later.
    """
    if not atoms:
        raise ConnError("a connection worker needs at least one atom")
    argv = [
        python,
        "-m",
        "mftik_md.conn_worker",
        "--instance",
        instance,
        "--incarnation",
        str(incarnation),
        "--venue",
        conn.venue,
        "--endpoint",
        conn.endpoint,
        "--index",
        str(conn.n),
    ]
    for atom in atoms:
        argv.extend(("--atom", atom.atom_id))
    return tuple(argv)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mftik_md.conn_worker")
    parser.add_argument("--instance", required=True)
    parser.add_argument("--incarnation", type=int, required=True)
    parser.add_argument("--venue", required=True)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--index", type=int, required=True)
    parser.add_argument("--atom", action="append", required=True, dest="atoms")
    return parser


def _subscriptions(atoms: Sequence[Atom]) -> tuple[tuple[Atom, UniversalTicker], ...]:
    """The fixed set, checked before the process says it is ready."""
    if len(atoms) != len(set(atoms)):
        raise ConnError("the fixed atom list names an atom twice")
    subscribed: list[tuple[Atom, UniversalTicker]] = []
    for atom in atoms:
        if atom.venue != VENUE or atom.endpoint != PUBLIC:
            raise ConnError(f"atom {atom.atom_id} is not on {VENUE} {PUBLIC}")
        topic, symbol = parse_channel(atom.channel)
        if topic != TOPIC_ORDERBOOK:
            raise UnsupportedTopicError(
                f"paper connection worker does not stream {topic!r}"
            )
        subscribed.append((atom, UniversalTicker.of(VENUE, "Spot", symbol)))
    return tuple(subscribed)


class _Ready:
    """False until the paper client connects, then true for this process."""

    def __init__(self) -> None:
        self._ready = False

    def __call__(self) -> bool:
        return self._ready

    def connected(self, stop: asyncio.Event, fd: int | None) -> None:
        """The client is up. A later drop does not clear this.

        One beat goes out now, so the shim does not wait out the period.
        ``EPIPE`` means that shim is already gone (S2): ``stop`` ends the
        process. No fd means there is no shim to tell, and this process
        keeps running until something else stops it.
        """
        self._ready = True
        if fd is None:
            return
        try:
            write_heartbeat(fd, WorkerHeartbeat(ready=True))
        except BrokenPipeError:
            stop.set()


async def _heartbeat(
    clock: Clock,
    stop: asyncio.Event,
    ready: Callable[[], bool],
    fd: int | None,
) -> None:
    """Periodic beats on the shim's status pipe.

    :func:`mftik.procman.heartbeat_loop` drops a beat the pipe cannot
    take and, on ``EPIPE``, sets ``stop``. That is the shim leaving.
    """
    await heartbeat_loop(
        clock,
        ready=ready,
        period_s=_BEAT_INTERVAL_S,
        stop=stop,
        fd=fd,
    )


async def _wait(clock: Clock, stop: asyncio.Event, seconds: float) -> None:
    """Sleep on ``clock``, or return early when ``stop`` is set."""
    sleep = asyncio.create_task(clock.sleep(seconds))
    stopped = asyncio.create_task(stop.wait())
    _done, pending = await asyncio.wait(
        {sleep, stopped},
        return_when=asyncio.FIRST_COMPLETED,
    )
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)


async def _close_when_stopped(
    stop: asyncio.Event, client: PaperRemotePublicClient
) -> None:
    try:
        await stop.wait()
        await client.close()
    except asyncio.CancelledError:
        raise


async def _pump(
    client: PaperRemotePublicClient,
    atom: Atom,
    ticker: UniversalTicker,
    queue: asyncio.Queue[tuple[Atom, dict[str, object]] | BaseException | None],
    stop: asyncio.Event,
) -> None:
    try:
        async for book in client.stream_order_book(ticker):
            if stop.is_set():
                break
            await queue.put((atom, book.model_dump(mode="json")))
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        await queue.put(exc)
        return
    await queue.put(None)


async def _one_connection(
    client: PaperRemotePublicClient,
    subscriptions: Sequence[tuple[Atom, UniversalTicker]],
    stop: asyncio.Event,
) -> AsyncIterator[tuple[Atom, dict[str, object]]]:
    """Every atom of the fixed set, on this connect. Ends when one stream does."""
    queue: asyncio.Queue[tuple[Atom, dict[str, object]] | BaseException | None] = (
        asyncio.Queue()
    )
    tasks = [
        asyncio.create_task(
            _pump(client, atom, ticker, queue, stop),
            name=atom.atom_id,
        )
        for atom, ticker in subscriptions
    ]
    watch = asyncio.create_task(_close_when_stopped(stop, client))
    try:
        while True:
            item = await queue.get()
            if stop.is_set():
                return
            # One stream ended or failed: the caller subscribes the whole
            # fixed set again. A stopped process returns above instead.
            if item is None:
                return
            if isinstance(item, BaseException):
                raise item
            yield item
    finally:
        watch.cancel()
        for task in tasks:
            task.cancel()
        await asyncio.gather(watch, *tasks, return_exceptions=True)


async def _frames(
    client: PaperRemotePublicClient,
    atoms: Sequence[Atom],
    stop: asyncio.Event,
    *,
    clock: Clock,
    on_up: Callable[[], None],
    worker_id: str,
    incarnation: int,
) -> AsyncIterator[tuple[Atom, dict[str, object]]]:
    """Subscribe the fixed set, and subscribe it again after a drop.

    The same process keeps its :class:`~mftik_md.conn.SeqClock`. Only a
    new incarnation starts ``seq`` over. ``on_up`` runs after a connect
    and is how the process reports ready; it is not cleared on a drop.
    """
    subscriptions = _subscriptions(atoms)
    while not stop.is_set():
        try:
            await client.connect()
            on_up()
            async for item in _one_connection(client, subscriptions, stop):
                if stop.is_set():
                    return
                yield item
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "paper connection dropped; subscribing the fixed set again "
                "worker_id=%s incarnation=%s",
                worker_id,
                incarnation,
            )
        finally:
            with contextlib.suppress(Exception):
                await client.close()
        if stop.is_set():
            return
        await _wait(clock, stop, _RESUBSCRIBE_PAUSE_S)


async def _amain(argv: Sequence[str]) -> None:
    args = _parser().parse_args(list(argv))
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    conn = ConnId(args.venue, args.endpoint, args.index)
    if conn.venue != VENUE or conn.endpoint != PUBLIC:
        raise ConnError(
            f"this entry serves {VENUE} {PUBLIC}, not {conn.venue} {conn.endpoint}"
        )
    atoms = tuple(Atom.parse(item) for item in args.atoms)
    for atom in atoms:
        if (atom.venue, atom.endpoint) != (conn.venue, conn.endpoint):
            raise ConnError(
                f"atom {atom.atom_id} is not on connection {conn.worker_id}"
            )
    # Reject a bad channel before the process reports ready.
    _subscriptions(atoms)

    clock = SystemClock()
    worker = ConnWorker(
        conn,
        instance=args.instance,
        incarnation=args.incarnation,
        clock=clock,
    )
    logger.info(
        "md conn worker started worker_id=%s incarnation=%s atoms=%s",
        worker.worker_id,
        worker.incarnation,
        ",".join(atom.atom_id for atom in atoms),
    )
    fd = status_fd()
    ready = _Ready()
    beat_task = asyncio.create_task(
        _heartbeat(clock, stop, ready, fd),
        name="md-conn-heartbeat",
    )
    try:
        async with Broker() as broker:
            client = PaperRemotePublicClient(broker)
            frames = _frames(
                client,
                atoms,
                stop,
                clock=clock,
                on_up=lambda: ready.connected(stop, fd),
                worker_id=worker.worker_id,
                incarnation=worker.incarnation,
            )
            await worker.run(frames, decode=paper_decode, publisher=broker)
    finally:
        stop.set()
        beat_task.cancel()
        await asyncio.gather(beat_task, return_exceptions=True)


def main(argv: list[str] | None = None) -> None:
    configure_logging("md.conn")
    try:
        uvloop.run(_amain(sys.argv[1:] if argv is None else argv))
    except Exception:
        logger.exception("md conn worker failed")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
