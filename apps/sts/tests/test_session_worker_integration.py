"""A real session worker under a real shim and NATS.

The quiet test is the controller path: spawn ``python -m
mftik_sts.session_worker``, reach ``running``, ``sts.session.end`` runs
``on_stop``, the row ends ``done``. The hold tests are the order path
while a hook occupies the strategy loop. TD is a stand-in. MD is a
publisher on the atom subject. B4-05 and the MD connection worker are
not involved.

The 3.5s hold is ``integration`` and stays under that tier's cap. The
30s hold is ``e2e`` and has no per-test cap.
"""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from pathlib import Path

import pytest
from broker_harness import unique_key_prefix
from db_harness import a_database, an_owner
from mftik.broker import Broker, BrokerConfig
from mftik.broker.errors import RequestTimeoutError
from mftik.broker.handler import serve
from mftik.exchange.atoms import AtomOptions
from mftik.exchange.models import BookLevel, OrderBook
from mftik.exchange.oms import LedgerView, OmsView
from mftik.exchange.paper.atoms import atoms_for
from mftik.exchange.tickers import UniversalTicker
from mftik.procman import CloseMode, Supervisor, WorkerPhase
from mftik.procman.messages import log_path
from mftik.protocol import (
    MD_ORDERBOOK,
    STS_ORDER_SUBMIT,
    STS_REASON_OPERATOR_STOP,
    STS_SESSION_END,
    STS_SESSION_FAIL,
    STS_SESSION_START,
    STS_SESSION_STATUS,
    TD_LEDGER_VIEW,
    TD_OMS_VIEW,
    TD_ORDER_ACK,
    Envelope,
    OrderAck,
    StsCreateSessionRequest,
    StsSessionEndRequest,
    StsSessionStatus,
    Topics,
    UntypedEnvelope,
)
from mftik.registry.qualify import qualify
from mftik.registry.store import RegistryStore
from mftik_db.repositories.session import StsSessionRepository
from mftik_sts.controller import (
    StsOrchestrator,
    end_handler,
    session_worker_id,
    start_handler,
)
from mftik_sts.controller.status import DbStatusStore

_QUIET = """\
from pathlib import Path

from mftik.strategy import Strategy


class Quiet(Strategy):
    async def on_stop(self) -> None:
        path = self.paras.get("marker")
        if path:
            Path(path).write_text("stopped", encoding="utf-8")
"""

_HOLD = """\
import asyncio
import time
from decimal import Decimal
from pathlib import Path

from mftik.exchange.models import OrderType, Side
from mftik.strategy import Strategy


class Hold(Strategy):
    def __init__(self) -> None:
        super().__init__()
        self._spun = False

    async def on_order_book(self, book: object) -> None:
        if self._spun:
            path = Path(self.paras["count"])
            raw = path.read_text(encoding="utf-8").strip() if path.is_file() else ""
            current = int(raw) if raw.isdigit() else 0
            tmp = path.with_name(path.name + ".tmp")
            tmp.write_text(str(current + 1), encoding="utf-8")
            tmp.replace(path)
            return
        self._spun = True
        spin = float(self.paras["spin_s"])
        ack = Path(self.paras["ack"])

        async def _send() -> None:
            accepted = await self.oms.submit_order(
                7,
                ticker="Paper_Spot_BTCUSDT",
                side=Side.BUY,
                type=OrderType.LIMIT,
                qty=Decimal("0.01"),
                price=Decimal("1"),
            )
            ack.write_text("ack" if accepted else "nack", encoding="utf-8")

        task = asyncio.create_task(_send())
        # Publish and flush, then park on the ack, before the hold. The
        # stand-in waits longer than this yield, so the reply lands while
        # the loop is inside the spin.
        for _ in range(16):
            await asyncio.sleep(0)
        await asyncio.sleep(0.15)
        deadline = time.monotonic() + spin
        while time.monotonic() < deadline:
            pass
        await task
"""

#: Long enough that the reply is still in flight when the spin starts,
#: and short enough that it is inside the 2s ack budget.
_ACK_DELAY_S = 0.4
_API_ID = 7
_FEED = "orderbook.Paper_Spot_BTCUSDT"


def _plant(data: Path, source: str) -> str:
    added = RegistryStore(data).add({"strategy.py": source})
    return qualify("private", added.type)


def _paper_subject() -> str:
    ticker = UniversalTicker.parse("Paper_Spot_BTCUSDT")
    plan = atoms_for("orderbook", ticker, AtomOptions())
    return Topics.atom_subject(plan.atoms[0].atom_id)


def _stderr(work: Path, session_id: str) -> str:
    path = log_path(work, session_worker_id(session_id), "stderr")
    if not path.is_file():
        return ""
    text = path.read_text(encoding="utf-8", errors="replace")
    return text[-1500:]


async def _until(check, *, seconds: float) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if await check():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("timed out")


def _start(request: StsCreateSessionRequest) -> Envelope[dict]:
    return Envelope[dict].wrap(
        request.model_dump(mode="json"),
        type=STS_SESSION_START,
        source="api",
    )


def _end(session_id: str) -> Envelope[dict]:
    body = StsSessionEndRequest(
        session_id=session_id, reason=STS_REASON_OPERATOR_STOP
    )
    return Envelope[dict].wrap(
        body.model_dump(), type=STS_SESSION_END, source="api"
    )


async def _views(message: UntypedEnvelope) -> Envelope[object] | None:
    if message.type == TD_OMS_VIEW:
        return Envelope[OmsView].wrap(OmsView(), type=TD_OMS_VIEW, source="td")
    if message.type == TD_LEDGER_VIEW:
        return Envelope[LedgerView].wrap(
            LedgerView(), type=TD_LEDGER_VIEW, source="td"
        )
    return None


async def _orders(message: UntypedEnvelope) -> Envelope[OrderAck] | None:
    if message.type != STS_ORDER_SUBMIT:
        return None
    await asyncio.sleep(_ACK_DELAY_S)
    body = message.payload if isinstance(message.payload, dict) else {}
    ack = OrderAck(
        api_id=int(body.get("api_id", _API_ID)),
        client_order_id=str(body.get("client_order_id", "")),
        accepted=True,
    )
    return Envelope[OrderAck].wrap(ack, type=TD_ORDER_ACK, source="td")


async def _books(broker: Broker, subject: str, stop: asyncio.Event) -> None:
    seq = 1
    book = OrderBook(
        universal_ticker="Paper_Spot_BTCUSDT",
        bids=[BookLevel(price=Decimal("1"), qty=Decimal("1"))],
        asks=[BookLevel(price=Decimal("2"), qty=Decimal("1"))],
    )
    while not stop.is_set():
        envelope = Envelope[OrderBook].wrap(
            book, type=MD_ORDERBOOK, source="md", seq=seq
        )
        seq += 1
        await broker.publish(subject, envelope)
        try:
            await asyncio.wait_for(stop.wait(), timeout=0.05)
        except TimeoutError:
            pass


@pytest.mark.integration
async def test_the_controller_spawns_the_worker_and_end_is_done(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data = tmp_path / "data"
    key = _plant(data, _QUIET)
    marker = tmp_path / "stopped"
    session_id = "a10001"
    request = StsCreateSessionRequest(
        session_id=session_id,
        created_by=1,
        strategy=key,
        type=key,
        st_paras={"marker": str(marker)},
    )
    work = tmp_path / "supervisor"
    prefix = unique_key_prefix("b403")
    monkeypatch.setenv("BROKER_KEY_PREFIX", prefix)
    monkeypatch.setenv("MFTIK_DATA", str(data))
    published: list[tuple[str, Envelope[object]]] = []

    async def _publish(subject: str, envelope: Envelope[object]) -> None:
        published.append((subject, envelope))

    broker = Broker(BrokerConfig.from_env())
    await broker.connect()
    async with a_database() as database:
        async with database.scope() as session:
            await an_owner(session)
            await StsSessionRepository(session).create_live(
                session_id=session_id,
                created_by=1,
                type=key,
                restart="never",
            )
        supervisor = Supervisor(work, plane="sts", instance="sts")
        orch = StsOrchestrator(
            supervisor,
            store=DbStatusStore(database.scope),
            publish=_publish,
            code_ref="test",
        )
        try:
            await orch.boot()
            assert await start_handler(orch)(_start(request)) is not None
            await orch.converge(session_id)

            async def _running() -> bool:
                await orch.observe_all()
                view = await supervisor.status(session_worker_id(session_id))
                return (
                    view is not None
                    and view.phase is WorkerPhase.RUNNING
                    and view.ready
                )

            try:
                await _until(_running, seconds=6)
            except AssertionError:
                raise AssertionError(_stderr(work, session_id)) from None

            status = await broker.request(
                Topics.sts_control(session_id),
                Envelope[dict].wrap(
                    {"session_id": session_id},
                    type=STS_SESSION_STATUS,
                    source="test",
                ),
                timeout=2.0,
            )
            snapshot = StsSessionStatus.model_validate(status.payload)
            assert snapshot.status == "running"
            assert snapshot.session_id == session_id

            with pytest.raises(RequestTimeoutError):
                await broker.request(
                    Topics.sts_control(session_id),
                    Envelope[dict].wrap(
                        {"session_id": session_id},
                        type=STS_SESSION_FAIL,
                        source="test",
                    ),
                    timeout=0.4,
                )
            view = await supervisor.status(session_worker_id(session_id))
            assert view is not None
            assert view.phase is WorkerPhase.RUNNING
            assert view.ready

            reply = await end_handler(orch)(_end(session_id))
            assert reply is not None
            assert marker.read_text(encoding="utf-8") == "stopped"
            async with database.scope() as session:
                done = await StsSessionRepository(session).get_by_session_id(
                    session_id
                )
            assert done is not None
            assert done.status == "done"
            terminal = StsSessionStatus.model_validate(published[-1][1].payload)
            assert terminal.status == "done"
        finally:
            await supervisor.close(CloseMode.STOP)
            await broker.close()


@pytest.mark.integration
async def test_sts_ctl_end_exits_zero_and_the_row_is_done(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``STS_SESSION_END`` on ``sts.ctl`` is phase 5, then exit 0.

    Desired stays ``running``. The controller records ``done`` with
    ``worker_exited:0``. ``sts.session.fail`` is not answered here.
    """
    data = tmp_path / "data"
    key = _plant(data, _QUIET)
    marker = tmp_path / "stopped"
    session_id = "a10002"
    request = StsCreateSessionRequest(
        session_id=session_id,
        created_by=1,
        strategy=key,
        type=key,
        st_paras={"marker": str(marker)},
    )
    work = tmp_path / "supervisor"
    prefix = unique_key_prefix("b403")
    monkeypatch.setenv("BROKER_KEY_PREFIX", prefix)
    monkeypatch.setenv("MFTIK_DATA", str(data))
    published: list[tuple[str, Envelope[object]]] = []

    async def _publish(subject: str, envelope: Envelope[object]) -> None:
        published.append((subject, envelope))

    broker = Broker(BrokerConfig.from_env())
    await broker.connect()
    async with a_database() as database:
        async with database.scope() as session:
            await an_owner(session)
            await StsSessionRepository(session).create_live(
                session_id=session_id,
                created_by=1,
                type=key,
                restart="never",
            )
        supervisor = Supervisor(work, plane="sts", instance="sts")
        orch = StsOrchestrator(
            supervisor,
            store=DbStatusStore(database.scope),
            publish=_publish,
            code_ref="test",
        )
        try:
            await orch.boot()
            assert await start_handler(orch)(_start(request)) is not None
            await orch.converge(session_id)

            async def _running() -> bool:
                await orch.observe_all()
                view = await supervisor.status(session_worker_id(session_id))
                return (
                    view is not None
                    and view.phase is WorkerPhase.RUNNING
                    and view.ready
                )

            try:
                await _until(_running, seconds=6)
            except AssertionError:
                raise AssertionError(_stderr(work, session_id)) from None

            reply = await broker.request(
                Topics.sts_control(session_id),
                Envelope[StsSessionEndRequest].wrap(
                    StsSessionEndRequest(
                        session_id=session_id,
                        reason=STS_REASON_OPERATOR_STOP,
                    ),
                    type=STS_SESSION_END,
                    source="test",
                ),
                timeout=2.0,
            )
            stopping = StsSessionStatus.model_validate(reply.payload)
            assert stopping.status == "stopping"

            async def _done() -> bool:
                await orch.observe_all()
                async with database.scope() as session:
                    row = await StsSessionRepository(session).get_by_session_id(
                        session_id
                    )
                return row is not None and row.status == "done"

            await _until(_done, seconds=5)
            assert marker.read_text(encoding="utf-8") == "stopped"
            terminal = StsSessionStatus.model_validate(published[-1][1].payload)
            assert terminal.status == "done"
            assert terminal.reason == "worker_exited:0"
        finally:
            await supervisor.close(CloseMode.STOP)
            await broker.close()


async def _hold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    spin_s: float,
    wait_s: float,
    session_id: str,
) -> None:
    data = tmp_path / "data"
    key = _plant(data, _HOLD)
    ack = tmp_path / "ack"
    count = tmp_path / "count"
    request = StsCreateSessionRequest(
        session_id=session_id,
        created_by=1,
        strategy=key,
        type=key,
        td={"main": {"api_id": _API_ID}},  # type: ignore[dict-item]
        md=[_FEED],  # type: ignore[arg-type]
        st_paras={
            "spin_s": spin_s,
            "ack": str(ack),
            "count": str(count),
        },
    )
    work = tmp_path / "supervisor"
    prefix = unique_key_prefix("b403")
    monkeypatch.setenv("BROKER_KEY_PREFIX", prefix)
    monkeypatch.setenv("MFTIK_DATA", str(data))
    broker = Broker(BrokerConfig.from_env())
    await broker.connect()
    stop = asyncio.Event()
    tasks = [
        asyncio.create_task(
            serve(broker, Topics.td_account(_API_ID), _views, stop=stop),
            name="td-account",
        ),
        asyncio.create_task(
            serve(broker, Topics.td_order(_API_ID), _orders, stop=stop),
            name="td-order",
        ),
        asyncio.create_task(
            _books(broker, _paper_subject(), stop), name="md-books"
        ),
    ]
    await asyncio.sleep(0.05)
    async with a_database() as database:
        async with database.scope() as session:
            await an_owner(session)
            await StsSessionRepository(session).create_live(
                session_id=session_id,
                created_by=1,
                type=key,
                restart="never",
            )
        supervisor = Supervisor(work, plane="sts", instance="sts")
        orch = StsOrchestrator(
            supervisor,
            store=DbStatusStore(database.scope),
            code_ref="test",
        )
        try:
            await orch.boot()
            assert await start_handler(orch)(_start(request)) is not None
            await orch.converge(session_id)
            saw = False
            left = False
            last = None
            deadline = time.monotonic() + wait_s
            while time.monotonic() < deadline:
                await orch.observe_all()
                last = await supervisor.status(session_worker_id(session_id))
                running = (
                    last is not None
                    and last.phase is WorkerPhase.RUNNING
                    and last.ready
                )
                if running:
                    saw = True
                elif saw:
                    left = True
                    break
                if ack.is_file():
                    break
                await asyncio.sleep(0.1)
            detail = f"view={last} stderr={_stderr(work, session_id)}"
            assert saw and not left, detail
            assert ack.read_text(encoding="utf-8") == "ack", detail

            async def _counted() -> bool:
                if not count.is_file():
                    return False
                text = count.read_text(encoding="utf-8").strip()
                return text.isdigit() and int(text) >= 1

            await _until(_counted, seconds=2)
            reply = await end_handler(orch)(_end(session_id))
            assert reply is not None
        finally:
            stop.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await supervisor.close(CloseMode.STOP)
            await broker.close()


@pytest.mark.integration
async def test_a_long_hook_keeps_the_session_and_the_ack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The hook holds the strategy loop for 3.5s, past the ack and the beat.

    The session stays ``running``, the shim does not miss a beat, and the
    ack is not ``TD_NO_ACK``. Books keep arriving and are dispatched after
    the hold.
    """
    await _hold(
        tmp_path,
        monkeypatch,
        spin_s=3.5,
        wait_s=8.0,
        session_id="a10035",
    )


@pytest.mark.e2e
async def test_a_thirty_second_hook_keeps_the_session_and_the_ack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The same hold, for the 30s run the plan names. No per-test cap."""
    started = time.monotonic()
    await _hold(
        tmp_path,
        monkeypatch,
        spin_s=30.0,
        wait_s=45.0,
        session_id="a10030",
    )
    elapsed = time.monotonic() - started
    assert elapsed >= 30.0
