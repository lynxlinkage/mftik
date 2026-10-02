"""A fetch worker keeps answering ``md.fetch`` across an MD controller roll.

The wiring is the MD process's: one supervisor, :class:`FetchController`,
the fetch worker entry. The reader factory is a stub, so no venue is
contacted. ``close(detach)`` leaves the worker up; the next supervisor
on the same work directory adopts that pid.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import signal
import socket
import struct
import subprocess
import tempfile
import time
from decimal import Decimal
from pathlib import Path

import pytest
from broker_harness import test_config as broker_config
from broker_harness import unique_key_prefix
from mftik.broker import Broker, NoRespondersError, RequestTimeoutError
from mftik.clock import SystemClock
from mftik.exchange.tickers import UniversalTicker
from mftik.procman import (
    CloseMode,
    DesiredSlot,
    ObservedWorker,
    ReattachAction,
    Supervisor,
    WorkerPhase,
    log_path,
    reattach_action,
    socket_path,
)
from mftik.protocol import (
    MD_FETCH_KLINES,
    Envelope,
    MdFetchKlines,
    MdKlinesResult,
    MdQueryAck,
    Topics,
)
from mftik_md.fetch_ctl import (
    FETCH_WORKER_ID,
    FetchController,
    fetch_worker_argv,
)

pytestmark = pytest.mark.integration

_ROOT = Path(__file__).resolve().parents[3]
_CLOSE = Decimal("1.5")
_TICKER = UniversalTicker.of("Gate", "Spot", "BTCUSDT")


def _pythonpath() -> str:
    entries = (
        _ROOT / "apps" / "md" / "tests",
        _ROOT / "apps" / "md" / "src",
        _ROOT / "packages" / "common" / "src",
    )
    return os.pathsep.join(str(path) for path in entries)


def _pid_running(pid: int | None) -> bool:
    if pid is None or pid <= 1:
        return False
    try:
        text = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return False
    try:
        state = text.rsplit(")", 1)[1].split()[0]
    except (IndexError, ValueError):
        return True
    return state != "Z"


def _children(pid: int) -> list[int]:
    path = Path(f"/proc/{pid}/task/{pid}/children")
    try:
        text = path.read_text()
    except OSError:
        return []
    found: list[int] = []
    for part in text.split():
        try:
            found.append(int(part))
        except ValueError:
            continue
    return found


def _kill(pid: int) -> None:
    if pid <= 1 or pid == os.getpid():
        return
    try:
        os.kill(pid, signal.SIGKILL)
    except OSError:
        pass


def _kill_tree(pid: int) -> None:
    if pid <= 1 or pid == os.getpid():
        return
    for child in _children(pid):
        _kill_tree(child)
    _kill(pid)


def _ps_family(work_dir: Path) -> tuple[set[int], set[int]]:
    listing = subprocess.check_output(
        ["ps", "-ww", "-eo", "pid=,ppid=,args="], text=True
    )
    rows: list[tuple[int, int, str]] = []
    for line in listing.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) < 2:
            continue
        args = parts[2] if len(parts) == 3 else ""
        rows.append((int(parts[0]), int(parts[1]), args))
    marker = str(work_dir)
    shims = {pid for pid, _ppid, args in rows if marker in args and _pid_running(pid)}
    workers = {
        pid
        for pid, ppid, _args in rows
        if ppid in shims and _pid_running(pid)
    }
    return shims, workers


def _reap(work_dir: Path) -> None:
    shims, _workers = _ps_family(work_dir)
    for pid in shims:
        _kill_tree(pid)


def _peer(path: Path) -> int | None:
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.2)
            sock.connect(os.fspath(path))
            size = struct.calcsize("iii")
            pid, _uid, _gid = struct.unpack(
                "iii",
                sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, size),
            )
    except OSError:
        return None
    return pid if pid > 1 else None


def _detail(work_dir: Path) -> str:
    parts: list[str] = []
    for stream in ("stderr", "stdout"):
        path = log_path(work_dir, FETCH_WORKER_ID, stream)
        try:
            text = path.read_text(errors="replace")
        except OSError:
            continue
        if text.strip():
            parts.append(f"{stream}:\n{text[-2000:]}")
    return "\n".join(parts)


async def _until_running(supervisor: Supervisor, work_dir: Path):
    deadline = time.monotonic() + 6
    last = None
    while time.monotonic() < deadline:
        last = await supervisor.status(FETCH_WORKER_ID)
        if (
            last is not None
            and last.phase is WorkerPhase.RUNNING
            and last.pid
            and _pid_running(last.pid)
        ):
            return last
        await asyncio.sleep(0.02)
    raise AssertionError(f"{last}\n{_detail(work_dir)}")


async def _until_gone(work_dir: Path) -> None:
    deadline = time.monotonic() + 2
    last: tuple[set[int], set[int]] | None = None
    while time.monotonic() < deadline:
        last = _ps_family(work_dir)
        if last == (set(), set()):
            return
        await asyncio.sleep(0.02)
    raise AssertionError(last)


async def _ask(broker: Broker, topic: str, query_id: str) -> MdQueryAck:
    deadline = time.monotonic() + 3
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            reply = await broker.request(
                Topics.md_fetch(),
                Envelope[MdFetchKlines].wrap(
                    MdFetchKlines(
                        reply_channel=topic,
                        query_id=query_id,
                        ticker=str(_TICKER),
                        interval="1h",
                        limit=2,
                    ),
                    type=MD_FETCH_KLINES,
                    source="test",
                ),
                timeout=0.5,
            )
        except (NoRespondersError, RequestTimeoutError) as exc:
            last = exc
            continue
        ack = MdQueryAck.model_validate(reply.payload)
        if ack.accepted:
            return ack
        last = AssertionError(ack)
    raise AssertionError(last)


async def test_a_controller_roll_does_not_interrupt_fetch() -> None:
    work = Path(tempfile.mkdtemp(prefix="mf"))
    prefix = unique_key_prefix("fetch-roll")
    config = broker_config(prefix)
    env = {
        "BROKER_KEY_PREFIX": prefix,
        "LOG_LEVEL": "INFO",
        "NATS_URL": config.nats_url,
        "PYTHONPATH": _pythonpath(),
    }
    argv = fetch_worker_argv("fetch_stub:build")
    first: Supervisor | None = None
    second: Supervisor | None = None
    watch: asyncio.Task[None] | None = None
    stop = asyncio.Event()
    listen_stop = asyncio.Event()
    pump: asyncio.Task[None] | None = None
    try:
        clock = SystemClock()
        first = Supervisor(work, plane="md", instance="md", clock=clock)
        observations = await first.start()
        assert observations == ()
        controller = FetchController(
            first, clock=clock, code_ref="test", env=env, argv=argv
        )
        await controller.reconcile(observations)
        watch = asyncio.create_task(controller.watch(stop), name="fetch-watch")
        running = await _until_running(first, work)
        worker_pid = running.pid
        shim_pid = _peer(socket_path(work, FETCH_WORKER_ID))
        assert shim_pid is not None
        assert running.spec.incarnation == 1

        reply = Topics.md_fetch_reply("roll")
        queue: asyncio.Queue[object] = asyncio.Queue()
        ready = asyncio.Event()

        async with Broker(config) as broker:

            async def _pump() -> None:
                async for envelope in broker.subscribe(
                    reply, stop=listen_stop, ready=ready
                ):
                    await queue.put(envelope)

            pump = asyncio.create_task(_pump(), name="fetch-roll-replies")
            await asyncio.wait_for(ready.wait(), timeout=2)

            async def answered(query_id: str) -> None:
                ack = await _ask(broker, reply, query_id)
                assert ack.query_id == query_id
                envelope = await asyncio.wait_for(queue.get(), timeout=2)
                result = MdKlinesResult.model_validate(envelope.payload)
                assert result.query_id == query_id
                assert result.ok is True
                assert result.klines[0].close == _CLOSE

            await answered("while-up")

            stop.set()
            watch.cancel()
            await asyncio.gather(watch, return_exceptions=True)
            watch = None
            await first.close(CloseMode.DETACH)

            await answered("while-detached")

            second_clock = SystemClock()
            second = Supervisor(
                work, plane="md", instance="md", clock=second_clock
            )
            adopted = await second.start()
            found = next(item for item in adopted if item.id == FETCH_WORKER_ID)
            assert found.observed is ObservedWorker.RUNNING
            assert (
                reattach_action(
                    plane="md",
                    desired=DesiredSlot.PRESENT,
                    observed=found.observed,
                )
                is ReattachAction.ADOPT
            )
            nxt = FetchController(
                second,
                clock=second_clock,
                code_ref="test",
                env=env,
                argv=argv,
            )
            await nxt.reconcile(adopted)
            again = await second.status(FETCH_WORKER_ID)
            assert again is not None
            assert again.pid == worker_pid
            assert again.spec.incarnation == 1
            assert again.phase is WorkerPhase.RUNNING
            assert _peer(socket_path(work, FETCH_WORKER_ID)) == shim_pid
            socks = list((work / "run").rglob("*.sock"))
            assert socks == [socket_path(work, FETCH_WORKER_ID)]

            await second.close(CloseMode.STOP)
        await _until_gone(work)
    finally:
        listen_stop.set()
        if pump is not None:
            pump.cancel()
            await asyncio.gather(pump, return_exceptions=True)
        if watch is not None:
            watch.cancel()
            await asyncio.gather(watch, return_exceptions=True)
        try:
            if second is not None and not second._closed:  # noqa: SLF001
                await second.close(CloseMode.STOP)
            elif first is not None and not first._closed:  # noqa: SLF001
                await first.close(CloseMode.STOP)
        finally:
            _reap(work)
            shutil.rmtree(work, ignore_errors=True)
