"""B4-09: paper path, controller rolls, and a start over budget.

Real subprocesses (§9.1). Startup is the fixture. The call is the
deploy, the roll, or the refusal. Compose is ``test_b4_09_e2e.py``.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
import uuid
from decimal import Decimal
from pathlib import Path

import httpx
import mftik_db.session as db_session
import pytest
from fastapi import FastAPI
from mftik.broker import Broker, BrokerConfig
from mftik.broker.handler import serve
from mftik.clock import SystemClock
from mftik.exchange import PaperExchange
from mftik.exchange.atoms import TOPIC_ORDERBOOK, AtomOptions
from mftik.exchange.paper.atoms import atoms_for
from mftik.exchange.tickers import UniversalTicker
from mftik.health import serve_health
from mftik.intent_gc import on_sts_report
from mftik.procman import (
    OOM_SCORE_ADJ,
    CloseMode,
    DesiredSlot,
    ObservedWorker,
    ReattachAction,
    Supervisor,
    WorkerPhase,
    WorkerSpec,
    admission_budget_from_environ,
    log_path,
    publish_reports,
    reattach_action,
)
from mftik.protocol import (
    PAPER_ORDER_BOOK,
    TD_OMS_VIEW,
    Envelope,
    ProcmanReport,
    ProcmanWorker,
    TdOmsViewRequest,
    Topics,
    UntypedEnvelope,
)
from mftik.registry.qualify import qualify
from mftik.registry.store import RegistryStore
from mftik_api.auth import AuthMiddleware
from mftik_api.deps import get_registry_store
from mftik_api.routes.sts import router as sts_router
from mftik_db.models import Account, Api, Base, Instance, User
from mftik_db.models.intent import MdIntent, TdIntent
from mftik_db.models.session import StsSessionRow
from mftik_db.repositories.session import StsSessionRepository
from mftik_db.session import session_scope
from mftik_md.conn import ConnId
from mftik_md.conn_worker import argv_for
from mftik_md.fetch_ctl import FETCH_WORKER_ID, FetchController
from mftik_md.intents import MdIntentBook
from mftik_md.rpc.router import control_handler as md_control_handler
from mftik_paper.accounts import LIQUIDITY_ORDERS, SEEDED_ACCOUNTS
from mftik_paper.app import BrokerEventBridge
from mftik_paper.rpc import dispatch
from mftik_sts.app import bind_orchestrator
from mftik_sts.controller import StsOrchestrator, session_worker_id
from mftik_sts.controller.status import DbStatusStore
from mftik_sts.rpc.router import control_handler as sts_control_handler
from mftik_td.app import _reconcile_once
from mftik_td.controller.handlers import intent_book, intent_handler
from mftik_td.controller.orchestrator import TdOrchestrator
from mftik_td.controller.types import account_worker_id
from mftik_td.supervise import account_restart_intensity
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

pytestmark = pytest.mark.integration

_REGION = "lab"
_TRADER = "paper-key-1"
_PATH = """\
import asyncio
import time
from decimal import Decimal
from pathlib import Path

from mftik.exchange.models import OrderType, Side
from mftik.strategy import Strategy


class PathRun(Strategy):
    def __init__(self) -> None:
        super().__init__()
        self._t0 = 0.0

    async def on_start(self) -> None:
        Path(self.paras["start"]).write_text("start", encoding="utf-8")

    async def on_ready(self, ready: object) -> None:
        del ready
        self._t0 = time.perf_counter()
        api_id = self.oms.api_ids[0]
        ok = False
        while time.perf_counter() - self._t0 < 8:
            ok = await self.oms.submit_order(
                api_id,
                ticker="Paper_Spot_BTCUSDT",
                side=Side.BUY,
                type=OrderType.MARKET,
                qty=Decimal("0.01"),
            )
            if ok:
                break
            await asyncio.sleep(0.05)
        Path(self.paras["ready"]).write_text(
            f"ok={ok} submit_s={time.perf_counter() - self._t0:.4f} "
            f"code={self.oms.last_reject_code}\\n",
            encoding="utf-8",
        )

    async def on_fill(self, api_id: int, fill: object) -> None:
        Path(self.paras["fill"]).write_text(
            f"api_id={api_id} qty={getattr(fill, 'qty', '')} "
            f"since_ready_s={time.perf_counter() - self._t0:.4f}\\n",
            encoding="utf-8",
        )

    async def on_order_book(self, book: object) -> None:
        del book
        path = Path(self.paras["books"])
        n = int(path.read_text() or "0") if path.is_file() else 0
        path.write_text(str(n + 1), encoding="utf-8")
"""

_ROLL = """\
import time
from decimal import Decimal
from pathlib import Path

from mftik.exchange.models import OrderType, Side
from mftik.strategy import Strategy
from mftik.strategy.timer import now_ms


class RollRun(Strategy):
    def __init__(self) -> None:
        super().__init__()
        self._traded = False

    async def on_start(self) -> None:
        Path(self.paras["start"]).write_text("start", encoding="utf-8")

    async def on_ready(self, ready: object) -> None:
        del ready
        Path(self.paras["ready"]).write_text("ready", encoding="utf-8")
        self.timer.token().register(
            now_ms() + 50, 100, self._poke, label="b409-poke"
        )

    async def _poke(self) -> None:
        flag = Path(self.paras["trade"])
        if not flag.is_file() or self._traded:
            return
        self._traded = True
        api_id = self.oms.api_ids[0]
        t0 = time.perf_counter()
        ok = await self.oms.submit_order(
            api_id,
            ticker="Paper_Spot_BTCUSDT",
            side=Side.BUY,
            type=OrderType.LIMIT,
            qty=Decimal("0.01"),
            price=Decimal("1"),
        )
        cid = self.oms.last_client_order_id
        cancelled = False
        if ok and cid:
            cancelled = await self.oms.cancel_order(api_id, cid)
        Path(self.paras["traded"]).write_text(
            f"ok={ok} cancel={cancelled} cid={cid} "
            f"s={time.perf_counter() - t0:.4f} "
            f"code={self.oms.last_reject_code}\\n",
            encoding="utf-8",
        )

    async def on_order_book(self, book: object) -> None:
        del book
        path = Path(self.paras["books"])
        n = int(path.read_text() or "0") if path.is_file() else 0
        path.write_text(str(n + 1), encoding="utf-8")
"""

_OVER = """\
from mftik.strategy import Strategy


class OverBudget(Strategy):
    async def on_start(self) -> None:
        return None
"""


def _pids(needle: bytes) -> list[int]:
    found: list[int] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            command = (entry / "cmdline").read_bytes()
        except OSError:
            continue
        if needle in command:
            found.append(int(entry.name))
    return found


def _tail(path: Path) -> str:
    if not path.is_file():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")[-1500:]


def _socks(work: Path) -> list[str]:
    run = work / "run"
    if not run.is_dir():
        return []
    return sorted(path.name for path in run.glob("*.sock"))


async def _until(check, *, seconds: float, detail: str | object) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if await check():
            return
        await asyncio.sleep(0.05)
    text = detail() if callable(detail) else detail
    raise AssertionError(text)


def _yaml(root: Path, *, books: bool) -> str:
    lines = [
        "td:",
        '  "paper trader": {}',
    ]
    if books:
        lines.extend(
            [
                "md:",
                "  md:",
                "    - orderbook.Paper_Spot_BTCUSDT",
            ]
        )
    lines.extend(
        [
            "sts:",
            f'  start: "{root / "start"}"',
            f'  ready: "{root / "ready"}"',
            f'  books: "{root / "books"}"',
            f'  fill: "{root / "fill"}"',
            f'  trade: "{root / "trade"}"',
            f'  traded: "{root / "traded"}"',
        ]
    )
    return "\n".join(lines) + "\n"


def _spec(
    *,
    worker_id: str,
    plane: str,
    kind: str,
    argv: tuple[str, ...],
    env: dict[str, str],
) -> WorkerSpec:
    return WorkerSpec(
        id=worker_id,
        plane=plane,  # type: ignore[arg-type]
        kind=kind,
        incarnation=1,
        argv=argv,
        env=env,
        code_ref="b4-09",
        restart="never",
        start_timeout_s=8,
        hb_timeout_s=None,
        oom_score_adj=OOM_SCORE_ADJ[(plane, kind)],
        rlimit_data_bytes=None,
        stop_grace_s=1,
        labels={},
    )


async def _reset_engine() -> None:
    engine = db_session._engine
    db_session._engine = None
    db_session._session_factory = None
    if engine is not None:
        await engine.dispose()


async def _scratch(path: Path) -> tuple[str, str, int]:
    url = f"sqlite+aiosqlite:///{path}"
    sync = f"sqlite:///{path}"
    engine = create_async_engine(url)
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await conn.exec_driver_sql("PRAGMA journal_mode=WAL")
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with maker() as session:
            session.add(User(id=1, email="owner-1@test.invalid"))
            await session.flush()
            td = Instance(name="td", domain="td", region=_REGION, enabled=True)
            md = Instance(name="md", domain="md", region=_REGION, enabled=True)
            sts = Instance(name="sts", domain="sts", region=_REGION, enabled=True)
            session.add_all([td, md, sts])
            await session.flush()
            secret = next(
                item[1] for item in SEEDED_ACCOUNTS if item[0] == _TRADER
            )
            row = Api(
                owner_id=1,
                venue="Paper",
                api_key=_TRADER,
                api_secret=secret,
                instance_id=td.id,
                cancel_on_disconnect=False,
            )
            session.add(row)
            await session.flush()
            session.add(Account(name="paper trader", api_id=row.id, created_by=1))
            await session.commit()
            api_id = int(row.id)
    finally:
        await engine.dispose()
    return url, sync, api_id


class Stack:
    """One paper node: API, STS, MD, TD, and the paper engine."""

    def __init__(self) -> None:
        self.tasks: list[asyncio.Task[None]] = []
        self.stop = asyncio.Event()
        self.broker: Broker
        self.client: httpx.AsyncClient
        self.exchange: PaperExchange
        self.sts: Supervisor
        self.md: Supervisor
        self.td: Supervisor
        self.sts_orch: StsOrchestrator
        self.td_orch: TdOrchestrator
        self.work: Path
        self.api_id = 0
        self.path_key = ""
        self.roll_key = ""
        self._reconcile_stop = asyncio.Event()
        self._reconcile: asyncio.Task[None] | None = None

    def _track(self, coro: object, name: str) -> None:
        self.tasks.append(asyncio.create_task(coro, name=name))  # type: ignore[arg-type]

    async def oms(self) -> dict[str, object]:
        reply = await self.broker.request(
            Topics.td_account(self.api_id),
            Envelope[TdOmsViewRequest].wrap(
                TdOmsViewRequest(api_id=self.api_id),
                type=TD_OMS_VIEW,
                source="b4-09",
            ),
            timeout=2,
        )
        payload = reply.payload if isinstance(reply.payload, dict) else {}
        orders = payload.get("orders") or {}
        return dict(orders)

    async def deploy(self, key: str, yaml: str) -> str:
        response = await self.client.post(
            f"/sts/deploy/{key}",
            json={"yaml": yaml, "created_by": 1, "instance": "sts"},
        )
        assert response.status_code == 202, response.text
        body = response.json()
        return str(body["session_id"])

    async def phase(self, session_id: str) -> str | None:
        response = await self.client.get(f"/sts/sessions/{session_id}")
        if response.status_code != 200:
            return None
        phase = response.json().get("phase")
        return phase if isinstance(phase, str) else None

    def _sts_log(self, session_id: str) -> str:
        path = log_path(self.work / "sts", session_worker_id(session_id), "stderr")
        return _tail(path)

    async def roll_sts(self, session_id: str) -> int:
        worker = session_worker_id(session_id)
        before = await self.sts.status(worker)
        assert before is not None and before.pid, self._sts_log(session_id)
        pid = before.pid
        await self._drop("sts-")
        await self.sts.close(CloseMode.DETACH)
        self.sts = Supervisor(self.work / "sts", plane="sts", instance="sts")
        self.sts_orch = StsOrchestrator(
            self.sts,
            store=DbStatusStore(session_scope),
            publish=self.broker.publish,
            broker=self.broker,
            code_ref="b4-09",
        )
        bind_orchestrator(self.sts_orch)
        await self.sts_orch.boot()
        self._serve_sts()
        await _wait_adopted(
            self.sts,
            worker,
            pid,
            log_path(self.work / "sts", worker, "stderr"),
        )
        async with session_scope() as db:
            row = await StsSessionRepository(db).get_by_session_id(session_id)
        assert row is not None and row.status == "live", row.reason if row else None
        assert (
            reattach_action(
                plane="sts",
                desired=DesiredSlot.PRESENT,
                observed=ObservedWorker.RUNNING,
            )
            is ReattachAction.ADOPT
        )
        report = await self.sts.report()
        assert any(item.id == worker for item in report.workers), report
        return pid

    async def roll_td(self) -> int:
        worker = account_worker_id(self.api_id)
        before = await self.td.status(worker)
        assert before is not None and before.pid
        pid = before.pid
        self._reconcile_stop.set()
        if self._reconcile is not None:
            await self._reconcile
        await self._drop("td-report")
        await self.td.close(CloseMode.DETACH)
        self.td = Supervisor(self.work / "td", plane="td", instance="td")
        observations = await self.td.start()
        self.td_orch = TdOrchestrator(
            self.td,
            intensity=account_restart_intensity(),
            code_ref="b4-09",
        )
        intent_book().clear()
        seeded = await _seed()
        await _reconcile_once(
            self.td,
            self.td_orch,
            observations,
            self.broker,
            publish=seeded,
        )
        self._serve_td_report()
        self._start_reconcile()
        await _wait_adopted(
            self.td,
            worker,
            pid,
            log_path(self.work / "td", worker, "stderr"),
        )
        assert len(_pids(b"mftik_td.account")) == 1
        return pid

    async def roll_md(self) -> tuple[int, int]:
        conn = ConnId("Paper", "public", 0).worker_id
        fetch_before = await self.md.status(FETCH_WORKER_ID)
        conn_before = await self.md.status(conn)
        assert fetch_before is not None and fetch_before.pid
        assert conn_before is not None and conn_before.pid
        await self._drop("md-report")
        await self.md.close(CloseMode.DETACH)
        self.md = Supervisor(self.work / "md", plane="md", instance="md")
        observations = await self.md.start()
        fetch = FetchController(
            self.md, clock=SystemClock(), code_ref="b4-09"
        )
        await fetch.reconcile(observations)
        self._serve_md_report()
        await _wait_adopted(
            self.md,
            FETCH_WORKER_ID,
            fetch_before.pid,
            log_path(self.work / "md", FETCH_WORKER_ID, "stderr"),
        )
        await _wait_adopted(
            self.md,
            conn,
            conn_before.pid,
            log_path(self.work / "md", conn, "stderr"),
        )
        assert len(_pids(b"mftik_md.conn_worker")) == 1
        assert len(_pids(b"mftik_md.fetch")) == 1
        return fetch_before.pid, conn_before.pid

    async def _drop(self, prefix: str) -> None:
        kept: list[asyncio.Task[None]] = []
        dropping: list[asyncio.Task[None]] = []
        for task in self.tasks:
            if (task.get_name() or "").startswith(prefix):
                task.cancel()
                dropping.append(task)
            else:
                kept.append(task)
        if dropping:
            await asyncio.gather(*dropping, return_exceptions=True)
        self.tasks = kept

    def _serve_sts(self) -> None:
        self._track(
            serve(
                self.broker,
                Topics.sts("sts"),
                sts_control_handler(self.broker, self.sts_orch),
                stop=self.stop,
            ),
            "sts-rpc",
        )
        self._track(
            publish_reports(
                self.sts,
                plane="sts",
                instance="sts",
                publish=self.broker.publish,
                clock=SystemClock(),
                extra_workers=self.sts_orch.extra_workers,
            ),
            "sts-report",
        )
        self._track(self.sts_orch.watch(self.stop), "sts-watch")

    def _serve_td_report(self) -> None:
        self._track(
            publish_reports(
                self.td,
                plane="td",
                instance="td",
                publish=self.broker.publish,
                clock=SystemClock(),
            ),
            "td-report",
        )

    def _serve_md_report(self) -> None:
        self._track(
            publish_reports(
                self.md,
                plane="md",
                instance="md",
                publish=self.broker.publish,
                clock=SystemClock(),
            ),
            "md-report",
        )

    def _start_reconcile(self) -> None:
        self._reconcile_stop = asyncio.Event()
        stop = self._reconcile_stop

        async def loop() -> None:
            while not stop.is_set():
                try:
                    await asyncio.wait_for(stop.wait(), 0.2)
                    return
                except TimeoutError:
                    pass
                try:
                    await _reconcile_once(
                        self.td,
                        self.td_orch,
                        (),
                        self.broker,
                        publish=True,
                    )
                except Exception:
                    pass

        self._reconcile = asyncio.create_task(loop(), name="td-reconcile")


async def _seed() -> bool:
    from mftik_td.db import seed_intent_book

    return await seed_intent_book(intent_book(), instance="td")


async def _serve_paper(
    broker: Broker, exchange: PaperExchange, stop: asyncio.Event
) -> None:
    async for request in broker.serve(Topics.PAPER, stop=stop):
        await dispatch(request, exchange=exchange)


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
            await asyncio.wait_for(stop.wait(), 0.2)
        except TimeoutError:
            continue


async def _wait_adopted(
    supervisor: Supervisor, worker_id: str, pid: int, log: Path
) -> None:
    """The reattached worker is the same pid and has reached running."""
    deadline = time.monotonic() + 3
    status = None
    while time.monotonic() < deadline:
        status = await supervisor.status(worker_id)
        if (
            status is not None
            and status.pid == pid
            and status.phase is WorkerPhase.RUNNING
        ):
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"{worker_id} not adopted: {status}\n{_tail(log)}")


async def _wait_ready(supervisor: Supervisor, worker_id: str, log: Path) -> None:
    deadline = time.monotonic() + 8
    status = None
    while time.monotonic() < deadline:
        status = await supervisor.status(worker_id)
        if status is not None and status.ready and status.phase is WorkerPhase.RUNNING:
            return
        if status is not None and status.phase in (
            WorkerPhase.FAILED,
            WorkerPhase.CRASHED,
            WorkerPhase.FATAL,
        ):
            break
        await asyncio.sleep(0.05)
    raise AssertionError(f"{worker_id} not ready: {status}\n{_tail(log)}")


@pytest.fixture
async def stack(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Stack:
    url, sync, api_id = await _scratch(tmp_path / "node.db")
    prefix = f"b409-{uuid.uuid4().hex[:8]}"
    nats = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")
    data = tmp_path / "data"
    monkeypatch.setenv("DATABASE_URL", url)
    monkeypatch.setenv("DATABASE_URL_SYNC", sync)
    monkeypatch.setenv("NATS_URL", nats)
    monkeypatch.setenv("BROKER_KEY_PREFIX", prefix)
    monkeypatch.setenv("BROKER_REQUEST_TIMEOUT", "2")
    monkeypatch.setenv("MFTIK_DATA", str(data))
    monkeypatch.setenv("MFTIK_AUTH_ENABLED", "0")
    monkeypatch.setenv("PYTHONUNBUFFERED", "1")
    monkeypatch.delenv("MFTIK_STATUS_FD", raising=False)
    monkeypatch.delenv("PROCMAN_MAX_WORKERS", raising=False)
    monkeypatch.delenv("PROCMAN_MEMORY_BUDGET_MB", raising=False)
    await _reset_engine()
    store = RegistryStore(data)
    path_key = qualify("private", store.add({"strategy.py": _PATH}).type)
    roll_key = qualify("private", store.add({"strategy.py": _ROLL}).type)

    node = Stack()
    node.api_id = api_id
    node.path_key = path_key
    node.roll_key = roll_key
    node.work = tmp_path / "work"
    node.broker = Broker(BrokerConfig(nats_url=nats, key_prefix=prefix))
    await node.broker.connect()
    bridge = BrokerEventBridge(node.broker)
    node.exchange = PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")},
        tick_interval=60,
        volatility_bps=Decimal("0"),
        on_order=bridge.on_order,
        on_fill=bridge.on_fill,
        on_balance=bridge.on_balance,
    )
    for key, secret, balances in SEEDED_ACCOUNTS:
        node.exchange.register_api(key, secret, balances=balances)
    try:
        await node.exchange.start()
        for key, request in LIQUIDITY_ORDERS:
            await node.exchange.place_order(key, request)
        node._track(_serve_paper(node.broker, node.exchange, node.stop), "paper-rpc")
        node._track(
            _publish_books(node.broker, node.exchange, node.stop), "paper-books"
        )
        await asyncio.sleep(0.05)

        child = {
            "NATS_URL": nats,
            "BROKER_KEY_PREFIX": prefix,
            "BROKER_REQUEST_TIMEOUT": "2",
            "PYTHONUNBUFFERED": "1",
            "DATABASE_URL": url,
            "DATABASE_URL_SYNC": sync,
        }
        node.td = Supervisor(node.work / "td", plane="td", instance="td")
        observations = await node.td.start()
        node.td_orch = TdOrchestrator(
            node.td,
            intensity=account_restart_intensity(),
            code_ref="b4-09",
        )
        intent_book().clear()
        seeded = await _seed()
        await _reconcile_once(
            node.td, node.td_orch, observations, node.broker, publish=seeded
        )
        await _wait_ready(
            node.td,
            account_worker_id(api_id),
            log_path(node.work / "td", account_worker_id(api_id), "stderr"),
        )
        node._track(
            serve(
                node.broker,
                Topics.td("td"),
                intent_handler(intent_book(), broker=node.broker, instance="td"),
                stop=node.stop,
            ),
            "td-rpc",
        )
        node._track(
            serve_health(node.broker, domain="td", instance="td", stop=node.stop),
            "td-health",
        )
        node._serve_td_report()
        node._start_reconcile()

        node.md = Supervisor(node.work / "md", plane="md", instance="md")
        md_obs = await node.md.start()
        fetch = FetchController(node.md, clock=SystemClock(), code_ref="b4-09")
        await fetch.reconcile(md_obs)
        await _wait_ready(
            node.md,
            FETCH_WORKER_ID,
            log_path(node.work / "md", FETCH_WORKER_ID, "stderr"),
        )
        ticker = UniversalTicker.parse("Paper_Spot_BTCUSDT")
        atom = atoms_for(TOPIC_ORDERBOOK, ticker, AtomOptions()).atoms[0]
        conn = ConnId("Paper", "public", 0)
        await node.md.spawn(
            _spec(
                worker_id=conn.worker_id,
                plane="md",
                kind="conn",
                argv=argv_for(
                    python=sys.executable,
                    instance="md",
                    incarnation=1,
                    conn=conn,
                    atoms=(atom,),
                ),
                env=child,
            )
        )
        await _wait_ready(
            node.md,
            conn.worker_id,
            log_path(node.work / "md", conn.worker_id, "stderr"),
        )
        node._track(
            serve(
                node.broker,
                Topics.md("md"),
                md_control_handler(None, MdIntentBook()),
                stop=node.stop,
            ),
            "md-rpc",
        )
        node._track(
            serve_health(node.broker, domain="md", instance="md", stop=node.stop),
            "md-health",
        )
        node._serve_md_report()

        node.sts = Supervisor(node.work / "sts", plane="sts", instance="sts")
        node.sts_orch = StsOrchestrator(
            node.sts,
            store=DbStatusStore(session_scope),
            publish=node.broker.publish,
            broker=node.broker,
            code_ref="b4-09",
        )
        bind_orchestrator(node.sts_orch)
        await node.sts_orch.boot()
        node._serve_sts()
        node._track(
            serve_health(
                node.broker, domain="sts", instance="sts", stop=node.stop
            ),
            "sts-health",
        )
        app = FastAPI()
        app.add_middleware(AuthMiddleware)
        app.include_router(sts_router)
        app.state.broker = node.broker
        app.dependency_overrides[get_registry_store] = lambda: store
        node.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        )
        yield node
    finally:
        node.stop.set()
        node._reconcile_stop.set()
        if node._reconcile is not None:
            node._reconcile.cancel()
        for task in node.tasks:
            task.cancel()
        await asyncio.gather(*node.tasks, return_exceptions=True)
        if node._reconcile is not None:
            await asyncio.gather(node._reconcile, return_exceptions=True)
        for supervisor in (
            getattr(node, "sts", None),
            getattr(node, "md", None),
            getattr(node, "td", None),
        ):
            if supervisor is None:
                continue
            try:
                await supervisor.close(CloseMode.STOP)
            except Exception:
                pass
        bind_orchestrator(None)
        intent_book().clear()
        if getattr(node, "client", None) is not None:
            await node.client.aclose()
        if getattr(node, "exchange", None) is not None:
            await node.exchange.stop()
        if getattr(node, "broker", None) is not None:
            await node.broker.close()
        await _reset_engine()


def _books(root: Path) -> int:
    path = root / "books"
    if not path.is_file():
        return 0
    return int(path.read_text(encoding="utf-8") or "0")


async def _gc_one_miss_keeps_the_owner(session_id: str) -> None:
    """One report that omits the session does not drop the intent."""
    owner = next(iter(intent_book().owners()))
    assert owner.session_id == session_id
    held = intent_book().owners()
    states: dict = {}
    present = ProcmanReport(
        generation=1,
        workers=(
            ProcmanWorker(
                id=session_worker_id(session_id),
                code_ref="b4-09",
                rss_bytes=1,
                phase="running",
                ready=True,
                incarnation=1,
            ),
        ),
    )
    assert (
        on_sts_report(states, sts_instance="sts", held=held, report=present)
        == frozenset()
    )
    missed = ProcmanReport(generation=2, workers=())
    assert (
        on_sts_report(states, sts_instance="sts", held=held, report=missed)
        == frozenset()
    )
    assert owner in intent_book().owners()


async def test_paper_deploy_reaches_on_fill_and_end(
    stack: Stack, tmp_path: Path
) -> None:
    """API deploy, trading from the held intent, fill, then intents released."""
    root = tmp_path / "markers"
    root.mkdir()
    session_id = await stack.deploy(stack.path_key, _yaml(root, books=True))

    async def filled() -> bool:
        return (root / "fill").is_file() and (root / "start").is_file()

    def _why_not_filled() -> str:
        ready_text = ""
        if (root / "ready").is_file():
            ready_text = (root / "ready").read_text(encoding="utf-8")
        return (
            f"markers={list(root.iterdir())} ready={ready_text}\n"
            f"{stack._sts_log(session_id)}"
        )

    await _until(filled, seconds=8, detail=_why_not_filled)
    ready = (root / "ready").read_text(encoding="utf-8")
    assert ready.startswith("ok=True"), ready
    print(f"b4-09 path {ready.strip()} {(root / 'fill').read_text().strip()}")

    async def running() -> bool:
        return await stack.phase(session_id) == "running"

    await _until(running, seconds=3, detail=f"phase={await stack.phase(session_id)}")
    assert _books(root) >= 1
    assert len(_pids(b"mftik_sts.session_worker")) == 1
    assert len(_pids(b"mftik_td.account")) == 1

    stopped = await stack.client.post(f"/sts/sessions/{session_id}/stop")
    assert stopped.status_code == 200, stopped.text
    async with session_scope() as db:
        td = (
            await db.execute(select(TdIntent).where(TdIntent.session_id == session_id))
        ).scalars().all()
        md = (
            await db.execute(select(MdIntent).where(MdIntent.session_id == session_id))
        ).scalars().all()
    assert td and all(row.released_at is not None for row in td)
    assert md and all(row.released_at is not None for row in md)


async def test_controller_rolls_keep_workers_md_and_orders(
    stack: Stack, tmp_path: Path
) -> None:
    """Detach and reattach each plane. Workers, books, and the book stay."""
    root = tmp_path / "markers"
    root.mkdir()
    session_id = await stack.deploy(stack.roll_key, _yaml(root, books=True))

    async def ready() -> bool:
        return (root / "ready").is_file() and _books(root) >= 1

    await _until(
        ready,
        seconds=8,
        detail=f"ready missing\n{stack._sts_log(session_id)}",
    )
    before = await stack.oms()
    session_pid = await stack.roll_sts(session_id)
    await _gc_one_miss_keeps_the_owner(session_id)
    books = _books(root)

    async def more_books() -> bool:
        return _books(root) > books

    await _until(more_books, seconds=3, detail=f"books stuck at {books}")
    assert await stack.sts.status(session_worker_id(session_id))
    assert (await stack.sts.status(session_worker_id(session_id))).pid == session_pid
    assert await stack.oms() == before

    account_pid = await stack.roll_td()
    assert (await stack.td.status(account_worker_id(stack.api_id))).pid == account_pid
    assert await stack.oms() == before
    books = _books(root)
    await _until(more_books, seconds=3, detail=f"books stuck at {books} after td")

    fetch_pid, conn_pid = await stack.roll_md()
    conn = ConnId("Paper", "public", 0).worker_id
    assert (await stack.md.status(FETCH_WORKER_ID)).pid == fetch_pid
    assert (await stack.md.status(conn)).pid == conn_pid
    books = _books(root)
    await _until(more_books, seconds=3, detail=f"books stuck at {books} after md")
    (root / "trade").write_text("1", encoding="utf-8")

    async def traded() -> bool:
        return (root / "traded").is_file()

    await _until(traded, seconds=3, detail="place/cancel did not run")
    text = (root / "traded").read_text(encoding="utf-8")
    assert text.startswith("ok=True"), text
    assert "cancel=True" in text, text
    print(f"b4-09 roll {text.strip()}")
    assert len(_pids(b"mftik_sts.session_worker")) == 1


_OVER_YAML = """\
td:
  "paper trader": {}
sts: {}
"""


@pytest.fixture
async def budget(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """STS with a 1 MiB budget. The session estimate does not fit."""
    url, sync, api_id = await _scratch(tmp_path / "cap.db")
    prefix = f"b409c-{uuid.uuid4().hex[:8]}"
    nats = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")
    data = tmp_path / "data"
    monkeypatch.setenv("DATABASE_URL", url)
    monkeypatch.setenv("DATABASE_URL_SYNC", sync)
    monkeypatch.setenv("NATS_URL", nats)
    monkeypatch.setenv("BROKER_KEY_PREFIX", prefix)
    monkeypatch.setenv("MFTIK_DATA", str(data))
    monkeypatch.setenv("MFTIK_AUTH_ENABLED", "0")
    monkeypatch.setenv("PROCMAN_MEMORY_BUDGET_MB", "1")
    monkeypatch.delenv("PROCMAN_MAX_WORKERS", raising=False)
    await _reset_engine()
    store = RegistryStore(data)
    key = qualify("private", store.add({"strategy.py": _OVER}).type)
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "strategy.py").write_text(_OVER, encoding="utf-8")
    (tree / "strategy.yml").write_text(_OVER_YAML, encoding="utf-8")
    broker = Broker(BrokerConfig(nats_url=nats, key_prefix=prefix))
    await broker.connect()
    stop = asyncio.Event()
    work = tmp_path / "sts"
    tasks: list[asyncio.Task[None]] = []
    supervisor: Supervisor | None = None
    try:
        supervisor = Supervisor(
            work,
            plane="sts",
            instance="sts",
            budget=admission_budget_from_environ("sts"),
        )
        orchestrator = StsOrchestrator(
            supervisor,
            store=DbStatusStore(session_scope),
            publish=broker.publish,
            broker=broker,
            code_ref="b4-09",
        )
        bind_orchestrator(orchestrator)
        await orchestrator.boot()
        tasks.append(
            asyncio.create_task(
                serve(
                    broker,
                    Topics.sts("sts"),
                    sts_control_handler(broker, orchestrator),
                    stop=stop,
                )
            )
        )
        tasks.append(
            asyncio.create_task(
                serve_health(broker, domain="sts", instance="sts", stop=stop)
            )
        )
        tasks.append(
            asyncio.create_task(
                serve(
                    broker,
                    Topics.td("td"),
                    intent_handler(intent_book(), broker=broker, instance="td"),
                    stop=stop,
                )
            )
        )
        app = FastAPI()
        app.add_middleware(AuthMiddleware)
        app.include_router(sts_router)
        app.state.broker = broker
        app.dependency_overrides[get_registry_store] = lambda: store
        client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        )
        yield {
            "client": client,
            "key": key,
            "work": work,
            "tree": tree,
            "api_id": api_id,
            "broker": broker,
            "app": app,
        }
    finally:
        stop.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if supervisor is not None:
            try:
                await supervisor.close(CloseMode.STOP)
            except Exception:
                pass
        bind_orchestrator(None)
        intent_book().clear()
        await broker.close()
        await _reset_engine()


async def test_a_start_over_budget_is_refused(
    budget: dict, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """HTTP 503 and ``mftik run`` both refuse. No worker is spawned."""
    client = budget["client"]
    response = await client.post(
        f"/sts/deploy/{budget['key']}",
        json={"yaml": _OVER_YAML, "created_by": 1},
    )
    assert response.status_code == 503, response.text
    # The route puts ``exc.message`` in ``detail``. The code word is on
    # the failed row, which is what abandon records.
    assert "memory_budget_mb" in response.text
    async with session_scope() as db:
        stored = (await db.execute(select(StsSessionRow))).scalars().all()
    assert stored
    assert all("capacity_exceeded" in (row.reason or "") for row in stored)
    assert _socks(budget["work"]) == []
    assert _pids(b"mftik_sts.session_worker") == []

    import socket

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    import uvicorn

    server = uvicorn.Server(
        uvicorn.Config(
            budget["app"], host="127.0.0.1", port=port, log_level="warning"
        )
    )
    task = asyncio.create_task(server.serve())
    try:
        deadline = time.monotonic() + 5
        while not server.started and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        assert server.started
        config = tmp_path / "mftik.toml"
        config.write_text(
            f'default = "local"\n\n[profiles.local]\nurl = "http://127.0.0.1:{port}"\n',
            encoding="utf-8",
        )
        env = os.environ.copy()
        env["MFTIK_CONFIG"] = str(config)
        env["MFTIK_AUTH_ENABLED"] = "0"
        binary = Path(sys.executable).parent / "mftik"
        # The API is this loop's uvicorn. A blocking subprocess would stall
        # the refusal, and ``--no-wait`` would look like a hang.
        proc = await asyncio.create_subprocess_exec(
            str(binary),
            "run",
            "--no-push",
            "--no-wait",
            str(budget["tree"]),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        try:
            stdout_b, stderr_b = await asyncio.wait_for(
                proc.communicate(), timeout=20
            )
        except TimeoutError:
            proc.kill()
            await proc.wait()
            raise
        stdout = stdout_b.decode()
        stderr = stderr_b.decode()
        assert proc.returncode != 0, stdout
        assert "memory_budget_mb" in stderr + stdout
    finally:
        server.should_exit = True
        await task
    assert _socks(budget["work"]) == []
    assert _pids(b"mftik_sts.session_worker") == []
