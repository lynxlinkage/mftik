"""A subscriber receives paper books on ``md.a.*`` (B4-06).

The worker is a real process under a supervisor this test builds. The
MD process does not spawn it (that wiring is B8-02). There is no session
worker yet (B4-03); the subscriber is a plain broker client.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import sys
import time
from decimal import Decimal
from pathlib import Path

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.exchange import PaperExchange
from mftik.exchange.atoms import TOPIC_ORDERBOOK, Atom, AtomOptions
from mftik.exchange.models import OrderBook
from mftik.exchange.paper.atoms import atoms_for
from mftik.exchange.tickers import UniversalTicker
from mftik.procman import (
    OOM_SCORE_ADJ,
    CloseMode,
    Supervisor,
    WorkerPhase,
    WorkerSpec,
    log_path,
)
from mftik.protocol import (
    MD_ORDERBOOK,
    PAPER_ORDER_BOOK,
    Topics,
    UntypedEnvelope,
)
from mftik_md.conn import SEQ_ORIGIN, ConnId
from mftik_md.conn_worker import argv_for

# Real subprocesses (§9.1). The paper tick below is short so the whole
# call, including a second incarnation, stays inside the 10s cap.
pytestmark = pytest.mark.integration

WORKER_ID = ConnId("Paper", "public", 0).worker_id
INSTANCE = "md-paper"
BTC = UniversalTicker.parse("Paper_Spot_BTCUSDT")
ATOM = atoms_for(TOPIC_ORDERBOOK, BTC, AtomOptions()).atoms[0]


def _spec(
    argv: tuple[str, ...], *, incarnation: int, env: dict[str, str]
) -> WorkerSpec:
    """Test values. Product restart numbers belong to B8-02 (#239, F42)."""
    return WorkerSpec(
        id=WORKER_ID,
        plane="md",
        kind="conn",
        incarnation=incarnation,
        argv=argv,
        env=env,
        code_ref="b4-06-test",
        restart="never",
        start_timeout_s=5,
        hb_timeout_s=None,
        oom_score_adj=OOM_SCORE_ADJ[("md", "conn")],
        rlimit_data_bytes=None,
        stop_grace_s=0.5,
        labels={},
    )


def _alive(pid: int | None) -> bool:
    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _conn_worker_pids() -> list[int]:
    found: list[int] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            command = (entry / "cmdline").read_bytes()
        except OSError:
            continue
        if b"mftik_md.conn_worker" in command:
            found.append(int(entry.name))
    return found


def _stderr(work_dir: Path) -> str:
    path = log_path(work_dir, WORKER_ID, "stderr")
    if not path.exists():
        return ""
    return path.read_text(errors="replace")


async def _take(queue: asyncio.Queue[UntypedEnvelope], count: int, timeout: float):
    got: list[UntypedEnvelope] = []
    deadline = time.monotonic() + timeout
    for _ in range(count):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        got.append(await asyncio.wait_for(queue.get(), remaining))
    return got


async def _until_ready(supervisor: Supervisor) -> None:
    """The shim has seen ``ready`` after the paper client connected."""
    deadline = time.monotonic() + 2
    status = None
    while time.monotonic() < deadline:
        status = await supervisor.status(WORKER_ID)
        if (
            status is not None
            and status.ready
            and status.phase is WorkerPhase.RUNNING
        ):
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"connection worker did not report ready: {status}")


def _assert_books(messages: list[UntypedEnvelope], seqs: list[int]) -> None:
    assert [message.seq for message in messages] == seqs
    for message in messages:
        assert message.type == MD_ORDERBOOK
        assert message.source == WORKER_ID
        book = OrderBook.model_validate(message.payload)
        assert book.symbol == "BTCUSDT"
        assert book.venue == "Paper"
        assert book.bids and book.asks


async def _publish_books(
    broker: Broker, exchange: PaperExchange, stop: asyncio.Event
) -> None:
    topic = Topics.paper_order_book("BTCUSDT")
    while not stop.is_set():
        book = exchange.get_order_book("BTCUSDT")
        await broker.publish(
            topic,
            UntypedEnvelope.wrap(
                book.model_dump(mode="json"),
                type=PAPER_ORDER_BOOK,
                source="paper",
            ),
        )
        try:
            await asyncio.wait_for(stop.wait(), 0.05)
        except TimeoutError:
            continue


async def _serve_paper(
    broker: Broker, exchange: PaperExchange, stop: asyncio.Event
) -> None:
    from mftik_paper.rpc import dispatch

    async for request in broker.serve(Topics.PAPER, stop=stop):
        await dispatch(request, exchange=exchange)


async def test_a_subscriber_receives_paper_atoms_and_seq_restarts(
    tmp_path: Path,
) -> None:
    """``md.a.*`` carries decoded books, seq 1, 2, 3, then 1 again.

    Incarnation 2 is a new process. The clock does not survive the kill.
    """
    assert SEQ_ORIGIN == 1
    assert isinstance(ATOM, Atom)
    subject = Topics.atom_subject(ATOM.atom_id)
    paper_stop = asyncio.Event()
    sub_stop = asyncio.Event()
    ready = asyncio.Event()
    queue: asyncio.Queue[UntypedEnvelope] = asyncio.Queue()
    pids: list[int] = []
    supervisor = Supervisor(tmp_path, plane="md", instance=INSTANCE)

    async with a_broker("md-conn") as broker:
        env = {
            "NATS_URL": broker.config.nats_url,
            "BROKER_KEY_PREFIX": broker.config.key_prefix,
            "BROKER_REQUEST_TIMEOUT": "2",
            "PYTHONUNBUFFERED": "1",
        }
        async with PaperExchange(
            symbols={"BTCUSDT": Decimal("50000")},
            tick_interval=10.0,
            volatility_bps=0,
        ) as exchange:
            rpc = asyncio.create_task(_serve_paper(broker, exchange, paper_stop))
            tick = asyncio.create_task(_publish_books(broker, exchange, paper_stop))

            async def listen() -> None:
                async for message in broker.subscribe(
                    subject, stop=sub_stop, ready=ready
                ):
                    await queue.put(message)

            listener = asyncio.create_task(listen())
            try:
                await asyncio.wait_for(ready.wait(), 2)
                # The RPC subscription has to be up before the worker connects.
                await asyncio.sleep(0.05)
                await supervisor.start()

                def spawn(incarnation: int) -> WorkerSpec:
                    spec = _spec(
                        argv_for(
                            python=sys.executable,
                            instance=INSTANCE,
                            incarnation=incarnation,
                            conn=ConnId("Paper", "public", 0),
                            atoms=(ATOM,),
                        ),
                        incarnation=incarnation,
                        env=env,
                    )
                    return spec

                first = spawn(1)
                await supervisor.spawn(first)
                status = await supervisor.status(WORKER_ID)
                assert status is not None and status.pid is not None
                pids.append(status.pid)
                try:
                    first_books = await _take(queue, 3, 4)
                except TimeoutError as exc:
                    raise AssertionError(_stderr(tmp_path)) from exc
                _assert_books(first_books, [1, 2, 3])
                await _until_ready(supervisor)
                assert "incarnation=1" in _stderr(tmp_path)

                await supervisor.stop(WORKER_ID)
                assert not _alive(pids[-1])
                while not queue.empty():
                    queue.get_nowait()

                second = spawn(2)
                await supervisor.spawn(second)
                status = await supervisor.status(WORKER_ID)
                assert status is not None and status.pid is not None
                pids.append(status.pid)
                try:
                    second_books = await _take(queue, 3, 4)
                except TimeoutError as exc:
                    raise AssertionError(_stderr(tmp_path)) from exc
                _assert_books(second_books, [SEQ_ORIGIN, 2, 3])
                await _until_ready(supervisor)
                assert "incarnation=2" in _stderr(tmp_path)
            finally:
                paper_stop.set()
                sub_stop.set()
                for task in (rpc, tick, listener):
                    task.cancel()
                await asyncio.gather(rpc, tick, listener, return_exceptions=True)
                with contextlib.suppress(Exception):
                    await supervisor.close(CloseMode.STOP)
                deadline = time.monotonic() + 2
                while time.monotonic() < deadline and any(_alive(pid) for pid in pids):
                    await asyncio.sleep(0.02)
                for pid in _conn_worker_pids():
                    with contextlib.suppress(OSError):
                        os.kill(pid, 9)
                for pid in pids:
                    if _alive(pid):
                        with contextlib.suppress(OSError):
                            os.kill(pid, 9)

    assert _conn_worker_pids() == []
    assert all(not _alive(pid) for pid in pids)
