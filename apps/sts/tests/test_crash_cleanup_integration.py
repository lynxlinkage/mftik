"""Crash classes A, B, and C against a paper account and a real worker.

Each test stays inside the integration call cap. The paper worker is
started in fixture setup, which is not the call phase. Cleanup is the
account worker's ``cancel_session``. A rehang is a new process: the
start count lives in a file, not in strategy memory (F10).
"""

from __future__ import annotations

import asyncio
import os
import signal
import time
import uuid
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from broker_harness import unique_key_prefix
from db_harness import a_database, an_instance, an_owner
from mftik.broker import Broker, BrokerConfig
from mftik.exchange import PaperExchange
from mftik.intent_gc import gc_owners, owners_in_report
from mftik.procman import CloseMode, Supervisor, WorkerPhase, log_path
from mftik.protocol import (
    STS_REASON_OPERATOR_STOP,
    STS_SESSION_START,
    TD_OMS_VIEW,
    Envelope,
    StsCreateSessionRequest,
    StsSessionEndRequest,
    TdOmsViewRequest,
    Topics,
)
from mftik.registry.qualify import qualify
from mftik.registry.store import RegistryStore
from mftik_db.models import Base
from mftik_db.models.api import Api
from mftik_db.repositories.session import StsSessionRepository
from mftik_paper.rpc import dispatch
from mftik_sts.controller import (
    SESSION_STOP_GRACE_S,
    StsOrchestrator,
    end_handler,
    session_worker_id,
    start_handler,
)
from mftik_sts.controller.status import DbStatusStore
from mftik_td.controller.defaults import (
    ACCOUNT_HB_TIMEOUT_S,
    ACCOUNT_START_TIMEOUT_S,
    ACCOUNT_STOP_GRACE_S,
)
from mftik_td.controller.types import BoundAccount, account_worker_id
from mftik_td.controller.worker import account_worker_spec
from mftik_td.supervise import account_worker_argv
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

pytestmark = pytest.mark.integration

_API_KEY = "paper-key"
_API_SECRET = "paper-secret"

_BOOM = """\
import asyncio
from decimal import Decimal
from pathlib import Path

from mftik.exchange.models import OrderType, Side
from mftik.strategy import Strategy


class Boom(Strategy):
    def __init__(self) -> None:
        super().__init__()
        self._armed = False

    async def on_start(self) -> None:
        path = Path(self.paras["starts"])
        text = path.read_text(encoding="utf-8") if path.is_file() else ""
        count = text.count("start") + 1
        path.write_text("start\\n" * count, encoding="utf-8")

    async def on_ready(self, ready: object) -> None:
        del ready
        await self.oms.submit_order(
            int(self.paras["api_id"]),
            ticker="Paper_Spot_BTCUSDT",
            side=Side.BUY,
            type=OrderType.LIMIT,
            qty=Decimal("0.01"),
            price=Decimal("1"),
        )

    async def on_order_update(self, api_id: int, order: object) -> None:
        del api_id, order
        if self.paras.get("raise_once") != "1":
            return
        path = Path(self.paras["starts"])
        text = path.read_text(encoding="utf-8") if path.is_file() else ""
        if text.count("start") != 1 or self._armed:
            return
        self._armed = True
        # The ready bit is the next heartbeat after on_ready returns.
        # Raising in this same turn would die before that beat, and F11
        # would call it an init failure. Later updates, including the
        # ones dispatched during on_stop, must not sleep again.
        await asyncio.sleep(1.3)
        raise RuntimeError("boom")
"""

_STUCK = """\
import asyncio
from decimal import Decimal
from pathlib import Path

from mftik.exchange.models import OrderType, Side
from mftik.strategy import Strategy


class Stuck(Strategy):
    async def on_ready(self, ready: object) -> None:
        del ready
        await self.oms.submit_order(
            int(self.paras["api_id"]),
            ticker="Paper_Spot_BTCUSDT",
            side=Side.BUY,
            type=OrderType.LIMIT,
            qty=Decimal("0.01"),
            price=Decimal("1"),
        )
        Path(self.paras["ready"]).write_text("ready", encoding="utf-8")

    async def on_stop(self) -> None:
        await asyncio.sleep(30)
"""

_SIT = """\
from decimal import Decimal
from pathlib import Path

from mftik.exchange.models import OrderType, Side
from mftik.strategy import Strategy


class Sit(Strategy):
    async def on_start(self) -> None:
        path = Path(self.paras["starts"])
        text = path.read_text(encoding="utf-8") if path.is_file() else ""
        path.write_text("start\\n" * (text.count("start") + 1), encoding="utf-8")

    async def on_ready(self, ready: object) -> None:
        del ready
        await self.oms.submit_order(
            int(self.paras["api_id"]),
            ticker="Paper_Spot_BTCUSDT",
            side=Side.BUY,
            type=OrderType.LIMIT,
            qty=Decimal("0.01"),
            price=Decimal("1"),
        )
        Path(self.paras["ready"]).write_text("ready", encoding="utf-8")
"""


def _plant(data: Path, source: str) -> str:
    added = RegistryStore(data).add({"strategy.py": source})
    return qualify("private", added.type)


def _tail(path: Path) -> str:
    if not path.is_file():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")[-2000:]


def _stderr(work: Path, session_id: str) -> str:
    return _tail(log_path(work, session_worker_id(session_id), "stderr"))


async def _until(check, *, seconds: float) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if await check():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("timed out")


async def _scratch_api(path: Path) -> tuple[str, str, int]:
    url = f"sqlite+aiosqlite:///{path}"
    sync = f"sqlite:///{path}"
    engine = create_async_engine(url)
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with maker() as session:
            await an_owner(session)
            instance = await an_instance(session, name="td")
            row = Api(
                owner_id=1,
                venue="Paper",
                api_key=_API_KEY,
                api_secret=_API_SECRET,
                instance_id=instance.id,
                cancel_on_disconnect=False,
            )
            session.add(row)
            await session.commit()
            api_id = row.id
    finally:
        await engine.dispose()
    return url, sync, api_id


async def _serve_paper(
    broker: Broker, exchange: PaperExchange, stop: asyncio.Event
) -> None:
    async for req in broker.serve(Topics.PAPER, stop=stop):
        await dispatch(req, exchange=exchange)


@pytest.fixture
async def paper(tmp_path: Path):
    url, sync, api_id = await _scratch_api(tmp_path / "td.db")
    exchange = PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")},
        tick_interval=60,
    )
    exchange.register_api(
        _API_KEY, _API_SECRET, balances={"USDT": Decimal("1000000")}
    )
    await exchange.start()
    stop = asyncio.Event()
    work = tmp_path / "td-work"
    supervisor = Supervisor(work, plane="td", instance="td", budget=None)
    prefix = unique_key_prefix("b506")
    nats_url = os.getenv("NATS_URL", "nats://localhost:4222")
    paper_task: asyncio.Task[None] | None = None
    broker = Broker(BrokerConfig(nats_url=nats_url, key_prefix=prefix))
    await broker.connect()
    try:
        paper_task = asyncio.create_task(
            _serve_paper(broker, exchange, stop), name="paper-rpc"
        )
        await asyncio.sleep(0.05)
        await supervisor.start()
        env = dict(os.environ)
        env.pop("MFTIK_STATUS_FD", None)
        env["DATABASE_URL"] = url
        env["DATABASE_URL_SYNC"] = sync
        env["NATS_URL"] = nats_url
        env["BROKER_KEY_PREFIX"] = prefix
        env["BROKER_REQUEST_TIMEOUT"] = "0.5"
        spec = account_worker_spec(
            BoundAccount(api_id=api_id, venue="Paper", instance="td"),
            incarnation=1,
            argv=account_worker_argv(api_id, 1, False),
            code_ref="test",
            start_timeout_s=ACCOUNT_START_TIMEOUT_S,
            hb_timeout_s=ACCOUNT_HB_TIMEOUT_S,
            stop_grace_s=ACCOUNT_STOP_GRACE_S,
            env=env,
        )
        await supervisor.spawn(spec)
        deadline = time.monotonic() + 6.0
        ready = False
        phase = None
        while time.monotonic() < deadline:
            status = await supervisor.status(account_worker_id(api_id))
            if status is not None:
                phase = status.phase
                if status.ready:
                    ready = True
                    break
                if status.phase in (
                    WorkerPhase.FAILED,
                    WorkerPhase.CRASHED,
                    WorkerPhase.FATAL,
                ):
                    break
            await asyncio.sleep(0.05)
        assert ready, f"paper worker phase={phase}"
        yield SimpleNamespace(
            broker=broker,
            api_id=api_id,
            prefix=prefix,
            nats_url=nats_url,
        )
    finally:
        stop.set()
        if paper_task is not None:
            paper_task.cancel()
            await asyncio.gather(paper_task, return_exceptions=True)
        try:
            await supervisor.close(CloseMode.STOP)
        except Exception:
            pass
        await broker.close()
        await exchange.stop()


def _start_message(request: StsCreateSessionRequest) -> Envelope[dict]:
    return Envelope[dict].wrap(
        request.model_dump(mode="json"),
        type=STS_SESSION_START,
        source="api",
    )


async def _open_orders(broker: Broker, api_id: int) -> dict[str, object]:
    reply = await broker.request(
        Topics.td_account(api_id),
        Envelope[TdOmsViewRequest].wrap(
            TdOmsViewRequest(api_id=api_id),
            type=TD_OMS_VIEW,
            source="test",
        ),
        timeout=2,
    )
    payload = reply.payload if isinstance(reply.payload, dict) else {}
    orders = payload.get("orders") or {}
    return dict(orders)


async def _session(
    tmp_path: Path,
    paper: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
    *,
    source: str,
    session_id: str,
    restart: str,
    paras: dict[str, str],
):
    data = tmp_path / "data"
    key = _plant(data, source)
    monkeypatch.setenv("BROKER_KEY_PREFIX", paper.prefix)
    monkeypatch.setenv("NATS_URL", paper.nats_url)
    monkeypatch.setenv("MFTIK_DATA", str(data))
    monkeypatch.setenv("BROKER_REQUEST_TIMEOUT", "0.5")
    request = StsCreateSessionRequest(
        session_id=session_id,
        created_by=1,
        strategy=key,
        type=key,
        restart=restart,
        td={"main": {"api_id": paper.api_id}},  # type: ignore[dict-item]
        st_paras=paras,
    )
    work = tmp_path / "sts-work"
    database_cm = a_database()
    database = await database_cm.__aenter__()
    async with database.scope() as session:
        await an_owner(session)
        await StsSessionRepository(session).create_live(
            session_id=session_id,
            created_by=1,
            type=key,
            restart=restart,
            td={"main": {"api_id": paper.api_id}},
        )
    supervisor = Supervisor(work, plane="sts", instance="sts")
    published: list[tuple[str, object]] = []

    async def _publish(subject: str, envelope: object) -> None:
        published.append((subject, envelope))

    orch = StsOrchestrator(
        supervisor,
        store=DbStatusStore(database.scope),
        publish=_publish,
        broker=paper.broker,
        code_ref="test",
    )
    await orch.boot()
    return SimpleNamespace(
        orch=orch,
        supervisor=supervisor,
        request=request,
        work=work,
        database=database,
        database_cm=database_cm,
        published=published,
        session_id=session_id,
    )


async def _close(run: SimpleNamespace) -> None:
    try:
        await run.supervisor.close(CloseMode.STOP)
    except Exception:
        pass
    await run.database_cm.__aexit__(None, None, None)


async def test_class_a_on_failure_cancels_rehanges_and_keeps_intents(
    tmp_path: Path, paper: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    starts = tmp_path / "starts"
    session_id = f"a{uuid.uuid4().hex[:5]}"
    run = await _session(
        tmp_path,
        paper,
        monkeypatch,
        source=_BOOM,
        session_id=session_id,
        restart="on_failure",
        paras={"starts": str(starts), "api_id": str(paper.api_id), "raise_once": "1"},
    )
    try:
        assert await start_handler(run.orch)(_start_message(run.request)) is not None
        await run.orch.converge(session_id)

        async def _worker_ready() -> bool:
            view = await run.supervisor.status(session_worker_id(session_id))
            return (
                view is not None
                and view.phase is WorkerPhase.RUNNING
                and view.ready
            )

        try:
            await _until(_worker_ready, seconds=5)
        except AssertionError:
            raise AssertionError(_stderr(run.work, session_id)) from None
        cid = ""

        async def _resting() -> bool:
            nonlocal cid
            orders = await _open_orders(paper.broker, paper.api_id)
            if orders:
                cid = next(iter(orders))
                return True
            return False

        await _until(_resting, seconds=2)
        assert cid
        seen_restarting = False
        worker_id = session_worker_id(session_id)

        async def _sample_report() -> None:
            nonlocal seen_restarting
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline and not seen_restarting:
                extras = run.orch.extra_workers()
                if any(worker.id == worker_id for worker in extras):
                    seen_restarting = True
                    owners = owners_in_report("sts", extras)
                    first = gc_owners(
                        owners, (), None, report=owners, report_generation=1
                    )
                    second = gc_owners(
                        owners,
                        first.absent,
                        first.generation,
                        report=owners,
                        report_generation=2,
                    )
                    assert second.release == frozenset()
                    return
                await asyncio.sleep(0.02)

        async def _drive() -> None:
            # Yield so the supervisor driver can ingest the exit. A tight
            # status loop keeps the cached phase at RUNNING after the
            # process has already logged the crash.
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline:
                await run.orch.observe_all()
                view = await run.supervisor.status(worker_id)
                starts_n = (
                    starts.read_text(encoding="utf-8").count("start")
                    if starts.is_file()
                    else 0
                )
                if (
                    view is not None
                    and view.spec.incarnation >= 2
                    and starts_n >= 2
                ):
                    return
                await asyncio.sleep(0.05)
            held = run.orch._sessions[session_id]  # noqa: SLF001
            view = await run.supervisor.status(worker_id)
            raise AssertionError(
                f"phase={held.phase.value} reason={held.reason} "
                f"ready={held.ready} class={held.crash_class} "
                f"cleanup={held.cleanup} incarnation={held.worker_incarnation} "
                f"view={view}\n{_stderr(run.work, session_id)}"
            )

        await asyncio.gather(_drive(), _sample_report())
        assert seen_restarting
        assert starts.read_text(encoding="utf-8").count("start") >= 2
        orders = await _open_orders(paper.broker, paper.api_id)
        assert cid not in orders, _stderr(run.work, session_id)
        text = " ".join(
            str(getattr(envelope.payload, "message", ""))
            for _topic, envelope in run.published
            if str(getattr(envelope, "type", "")) == "log"
        )
        assert "class=A" in text
        assert "reason=on_failure" in text
    finally:
        await _close(run)


async def test_class_a_on_never_fails_and_cancels(
    tmp_path: Path, paper: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    starts = tmp_path / "starts"
    session_id = f"b{uuid.uuid4().hex[:5]}"
    run = await _session(
        tmp_path,
        paper,
        monkeypatch,
        source=_BOOM,
        session_id=session_id,
        restart="never",
        paras={"starts": str(starts), "api_id": str(paper.api_id), "raise_once": "1"},
    )
    try:
        assert await start_handler(run.orch)(_start_message(run.request)) is not None
        await run.orch.converge(session_id)

        async def _worker_ready() -> bool:
            view = await run.supervisor.status(session_worker_id(session_id))
            return (
                view is not None
                and view.phase is WorkerPhase.RUNNING
                and view.ready
            )

        try:
            await _until(_worker_ready, seconds=5)
        except AssertionError:
            raise AssertionError(_stderr(run.work, session_id)) from None
        cid = ""

        async def _resting() -> bool:
            nonlocal cid
            orders = await _open_orders(paper.broker, paper.api_id)
            if orders:
                cid = next(iter(orders))
                return True
            return False

        await _until(_resting, seconds=2)

        async def _failed() -> bool:
            await run.orch.observe_all()
            held = run.orch._sessions[session_id]  # noqa: SLF001
            return held.phase.value == "failed"

        await _until(_failed, seconds=3)
        held = run.orch._sessions[session_id]  # noqa: SLF001
        assert held.reason == "restart_never"
        assert starts.read_text(encoding="utf-8").count("start") == 1
        orders = await _open_orders(paper.broker, paper.api_id)
        assert cid not in orders
        text = " ".join(
            str(getattr(envelope.payload, "message", ""))
            for _topic, envelope in run.published
            if str(getattr(envelope, "type", "")) == "log"
        )
        assert "class=A" in text
        assert "reason=restart_never" in text
    finally:
        await _close(run)


async def test_a_stop_that_kills_runs_cleanup(
    tmp_path: Path, paper: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    ready = tmp_path / "ready"
    session_id = f"c{uuid.uuid4().hex[:5]}"
    monkeypatch.setattr(
        "mftik_sts.controller.orchestrator.SESSION_STOP_GRACE_S", 0.3
    )
    assert SESSION_STOP_GRACE_S == 10.0
    run = await _session(
        tmp_path,
        paper,
        monkeypatch,
        source=_STUCK,
        session_id=session_id,
        restart="on_failure",
        paras={"api_id": str(paper.api_id), "ready": str(ready)},
    )
    try:
        assert await start_handler(run.orch)(_start_message(run.request)) is not None
        await run.orch.converge(session_id)

        async def _marked() -> bool:
            await run.orch.observe_all()
            return ready.is_file()

        await _until(_marked, seconds=4)
        cid = ""

        async def _resting() -> bool:
            nonlocal cid
            orders = await _open_orders(paper.broker, paper.api_id)
            if orders:
                cid = next(iter(orders))
                return True
            return False

        await _until(_resting, seconds=2)
        reply = await end_handler(run.orch)(
            Envelope[dict].wrap(
                StsSessionEndRequest(
                    session_id=session_id, reason=STS_REASON_OPERATOR_STOP
                ).model_dump(),
                type="sts.session.end",
                source="api",
            )
        )
        assert reply is not None
        held = run.orch._sessions[session_id]  # noqa: SLF001
        assert held.phase.value == "failed", _stderr(run.work, session_id)
        assert held.reason == "crash_class_b"
        orders = await _open_orders(paper.broker, paper.api_id)
        assert cid not in orders
    finally:
        await _close(run)


async def test_sigkill_is_class_c_and_does_not_rehang(
    tmp_path: Path, paper: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    ready = tmp_path / "ready"
    starts = tmp_path / "starts"
    session_id = f"d{uuid.uuid4().hex[:5]}"
    run = await _session(
        tmp_path,
        paper,
        monkeypatch,
        source=_SIT,
        session_id=session_id,
        restart="on_failure",
        paras={
            "api_id": str(paper.api_id),
            "ready": str(ready),
            "starts": str(starts),
        },
    )
    try:
        assert await start_handler(run.orch)(_start_message(run.request)) is not None
        await run.orch.converge(session_id)

        async def _marked() -> bool:
            await run.orch.observe_all()
            return ready.is_file()

        await _until(_marked, seconds=4)
        view = await run.supervisor.status(session_worker_id(session_id))
        assert view is not None and view.pid
        os.kill(view.pid, signal.SIGKILL)
        cid = ""

        async def _resting() -> bool:
            nonlocal cid
            orders = await _open_orders(paper.broker, paper.api_id)
            if orders:
                cid = next(iter(orders))
                return True
            return False

        await _until(_resting, seconds=2)

        async def _failed() -> bool:
            await run.orch.observe_all()
            held = run.orch._sessions[session_id]  # noqa: SLF001
            return held.phase.value == "failed"

        await _until(_failed, seconds=3)
        held = run.orch._sessions[session_id]  # noqa: SLF001
        assert held.reason == "crash_class_c", _stderr(run.work, session_id)
        assert starts.read_text(encoding="utf-8").count("start") == 1
        orders = await _open_orders(paper.broker, paper.api_id)
        assert cid not in orders
        text = " ".join(
            str(getattr(envelope.payload, "message", ""))
            for _topic, envelope in run.published
            if str(getattr(envelope, "type", "")) == "log"
        )
        assert "class=C" in text
    finally:
        await _close(run)
