"""A session worker process holds no database connection (F10, B5-09).

The worker's entry must not import the controller. ``mftik_sts.app``
imports :mod:`mftik_db`, and importing the package used to import the
app, so every session worker loaded SQLAlchemy. The controller still
imports the app explicitly (``python -m mftik_sts``, the ``sts``
script). Strategies are not guarded: a built-in that imported
:mod:`mftik_db` would show up in the running worker's module list.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import textwrap
import time
import uuid
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from broker_harness import unique_key_prefix
from db_harness import POSTGRES_URL_ENV, a_database, an_instance, an_owner
from mftik.broker import Broker, BrokerConfig
from mftik.exchange import PaperExchange
from mftik.procman import CloseMode, Supervisor, WorkerPhase, log_path
from mftik.protocol import (
    STS_SESSION_START,
    TD_ACCOUNT_TRADING,
    TD_OMS_VIEW,
    Envelope,
    StsCreateSessionRequest,
    TdAccountTrading,
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
    StsOrchestrator,
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
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

pytestmark = pytest.mark.integration

_API_KEY = "paper-key"
_API_SECRET = "paper-secret"
_BANNED_ROOTS = ("mftik_db", "sqlalchemy", "asyncpg", "psycopg", "aiosqlite")
_LISTEN = 0x0A

_ROUND = textwrap.dedent(
    """\
    import sys
    from decimal import Decimal
    from pathlib import Path

    from mftik.exchange.models import OrderStatus, OrderType, Side
    from mftik.strategy import Strategy

    _BANNED = ("mftik_db", "sqlalchemy", "asyncpg", "psycopg", "aiosqlite")


    def _banned() -> list[str]:
        found = []
        for name in sys.modules:
            head = name.split(".", 1)[0]
            if any(head == root or head.startswith(root) for root in _BANNED):
                found.append(name)
        return sorted(found)


    class Round(Strategy):
        def __init__(self) -> None:
            super().__init__()
            self._cid = None
            self._sent = False

        async def on_ready(self, ready: object) -> None:
            del ready
            found = _banned()
            Path(self.paras["modules"]).write_text(
                "none" if not found else "\\n".join(found),
                encoding="utf-8",
            )
            api_id = int(self.paras["api_id"])
            accepted = await self.oms.submit_order(
                api_id,
                ticker="Paper_Spot_BTCUSDT",
                side=Side.BUY,
                type=OrderType.LIMIT,
                qty=Decimal("0.01"),
                price=Decimal("1"),
            )
            self._cid = self.oms.last_client_order_id
            detail = "ok" if accepted else "no:" + self.oms.last_reject_reason
            Path(self.paras["placed"]).write_text(detail, encoding="utf-8")

        async def on_order_update(self, api_id: int, order: object) -> None:
            if self._sent or self._cid is None:
                return
            if str(getattr(order, "client_order_id", "")) != self._cid:
                return
            status = getattr(order, "status", None)
            if status is None or status.is_pending() or status.is_terminal():
                return
            if status is not OrderStatus.NEW and not status.is_working():
                return
            self._sent = True
            accepted = await self.oms.cancel_order(api_id, self._cid)
            detail = "ok" if accepted else "no:" + self.oms.last_reject_reason
            Path(self.paras["cancelled"]).write_text(detail, encoding="utf-8")
    """
)


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
    db_path = tmp_path / "td.db"
    url, sync, api_id = await _scratch_api(db_path)
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
    prefix = unique_key_prefix("b509")
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
        await _enable_trading(broker, api_id, work)
        yield SimpleNamespace(
            broker=broker,
            api_id=api_id,
            prefix=prefix,
            nats_url=nats_url,
            db_path=db_path,
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


async def _enable_trading(broker: Broker, api_id: int, work: Path) -> None:
    reply = await broker.request(
        Topics.td_account(api_id),
        Envelope[TdAccountTrading].wrap(
            TdAccountTrading(api_id=api_id, active=True),
            type=TD_ACCOUNT_TRADING,
            source="test",
        ),
        timeout=8,
    )
    body = TdAccountTrading.model_validate(reply.payload)
    log = log_path(work, account_worker_id(api_id), "stderr")
    detail = ""
    if log.is_file():
        detail = log.read_text(encoding="utf-8", errors="replace")[-2000:]
    assert reply.type == TD_ACCOUNT_TRADING, detail
    assert body.api_id == api_id
    assert body.active is True, detail


def _start_message(request: StsCreateSessionRequest) -> Envelope[dict]:
    return Envelope[dict].wrap(
        request.model_dump(mode="json"),
        type=STS_SESSION_START,
        source="api",
    )


def _banned_in(names: list[str]) -> list[str]:
    found = []
    for name in names:
        head = name.split(".", 1)[0]
        if any(head == root or head.startswith(root) for root in _BANNED_ROOTS):
            found.append(name)
    return sorted(found)


def test_worker_entry_does_not_import_a_database(tmp_path: Path) -> None:
    """The module ``python -m mftik_sts.session_worker`` loads.

    A child interpreter, because the check is the worker's own
    ``sys.modules`` and this process already imported the controller.
    """
    dest = tmp_path / "modules.json"
    script = textwrap.dedent(
        f"""\
        import json
        import runpy
        import sys
        from pathlib import Path

        runpy.run_module("mftik_sts.session_worker", run_name="mftik_entry")
        Path({str(dest)!r}).write_text(
            json.dumps(sorted(sys.modules)), encoding="utf-8"
        )
        """
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert completed.returncode == 0, completed.stderr
    present = _banned_in(json.loads(dest.read_text(encoding="utf-8")))
    assert present == []


def _postgres_port(url: str) -> int | None:
    if not url or url.startswith("sqlite"):
        return None
    parsed = make_url(url)
    if not parsed.drivername.startswith("postgres"):
        return None
    return parsed.port or 5432


def _sqlite_file(url: str) -> Path | None:
    if not url.startswith("sqlite"):
        return None
    parsed = make_url(url)
    name = parsed.database
    if not name or name == ":memory:":
        return None
    return Path(name)


def _database_ports() -> set[int]:
    ports: set[int] = set()
    for key in (POSTGRES_URL_ENV, "DATABASE_URL", "DATABASE_URL_SYNC"):
        port = _postgres_port(os.environ.get(key, ""))
        if port is not None:
            ports.add(port)
    return ports


def _socket_inodes(pid: int) -> set[int]:
    inodes: set[int] = set()
    for entry in Path(f"/proc/{pid}/fd").iterdir():
        try:
            target = os.readlink(entry)
        except OSError:
            continue
        if target.startswith("socket:[") and target.endswith("]"):
            inodes.add(int(target.removeprefix("socket:[").removesuffix("]")))
    return inodes


def _tcp_hits(pid: int, inodes: set[int], ports: set[int]) -> list[str]:
    """Lines whose socket is this process's and whose port is a database."""
    if not ports or not inodes:
        return []
    hits: list[str] = []
    for name in ("tcp", "tcp6"):
        table = Path(f"/proc/{pid}/net/{name}")
        if not table.is_file():
            continue
        for line in table.read_text(encoding="utf-8").splitlines()[1:]:
            parts = line.split()
            if len(parts) < 10:
                continue
            try:
                inode = int(parts[9])
                state = int(parts[3], 16)
            except ValueError:
                continue
            if inode not in inodes:
                continue
            local_port = int(parts[1].rsplit(":", 1)[-1], 16)
            remote_port = int(parts[2].rsplit(":", 1)[-1], 16)
            if remote_port in ports or (state == _LISTEN and local_port in ports):
                hits.append(line.strip())
    return hits


def _open_db_files(pid: int, files: list[Path]) -> list[str]:
    wanted = {str(path.resolve()) for path in files}
    hits: list[str] = []
    for entry in Path(f"/proc/{pid}/fd").iterdir():
        try:
            target = os.readlink(entry)
        except OSError:
            continue
        cleaned = target.removesuffix(" (deleted)")
        if cleaned.startswith("socket:") or cleaned.startswith("pipe:"):
            continue
        try:
            resolved = str(Path(cleaned).resolve())
        except OSError:
            continue
        if resolved in wanted:
            hits.append(target)
    return hits


def _environ(pid: int) -> dict[str, str]:
    raw = Path(f"/proc/{pid}/environ").read_bytes()
    found: dict[str, str] = {}
    for item in raw.split(b"\0"):
        if b"=" not in item:
            continue
        key, value = item.split(b"=", 1)
        found[key.decode()] = value.decode(errors="replace")
    return found


@pytest.mark.skipif(sys.platform != "linux", reason="B5-09 reads /proc/<pid>/net")
async def test_session_worker_has_no_database_connection(
    tmp_path: Path, paper: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Spawn through the controller, place and cancel, then read /proc.

    The parent has ``DATABASE_URL`` and ``DATABASE_URL_SYNC``. The
    worker's environ must not. With ``TEST_POSTGRES_URL`` set, no socket
    of the worker is a TCP connection to that port. The sqlite files
    the parent names are not open in the worker.
    """
    scratch = tmp_path / "scratch.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{scratch}")
    monkeypatch.setenv("DATABASE_URL_SYNC", f"sqlite:///{scratch}")
    monkeypatch.setenv("BROKER_KEY_PREFIX", paper.prefix)
    monkeypatch.setenv("NATS_URL", paper.nats_url)
    monkeypatch.setenv("MFTIK_DATA", str(tmp_path / "data"))
    monkeypatch.setenv("BROKER_REQUEST_TIMEOUT", "0.5")
    assert os.environ["DATABASE_URL"].startswith("sqlite")
    assert "DATABASE_URL" in os.environ

    data = tmp_path / "data"
    key = _plant(data, _ROUND)
    placed = tmp_path / "placed"
    cancelled = tmp_path / "cancelled"
    modules = tmp_path / "modules"
    session_id = f"d{uuid.uuid4().hex[:5]}"
    request = StsCreateSessionRequest(
        session_id=session_id,
        created_by=1,
        strategy=key,
        type=key,
        restart="never",
        td={"main": {"api_id": paper.api_id}},  # type: ignore[dict-item]
        st_paras={
            "api_id": str(paper.api_id),
            "placed": str(placed),
            "cancelled": str(cancelled),
            "modules": str(modules),
        },
    )
    work = tmp_path / "sts-work"
    database_cm = a_database()
    database = await database_cm.__aenter__()
    supervisor = Supervisor(work, plane="sts", instance="sts")
    try:
        async with database.scope() as session:
            await an_owner(session)
            await StsSessionRepository(session).create_live(
                session_id=session_id,
                created_by=1,
                type=key,
                restart="never",
                td={"main": {"api_id": paper.api_id}},
            )
        orch = StsOrchestrator(
            supervisor,
            store=DbStatusStore(database.scope),
            publish=None,
            broker=paper.broker,
            code_ref="test",
        )
        await orch.boot()
        assert await start_handler(orch)(_start_message(request)) is not None
        await orch.converge(session_id)

        async def _finished() -> bool:
            if cancelled.is_file():
                return True
            if placed.is_file() and not placed.read_text(encoding="utf-8").startswith(
                "ok"
            ):
                raise AssertionError(
                    placed.read_text(encoding="utf-8")
                    + "\n"
                    + _stderr(work, session_id)
                )
            return False

        try:
            await _until(_finished, seconds=8)
        except AssertionError:
            raise AssertionError(_stderr(work, session_id)) from None
        assert placed.read_text(encoding="utf-8") == "ok", _stderr(work, session_id)
        assert cancelled.read_text(encoding="utf-8") == "ok", _stderr(
            work, session_id
        )
        assert modules.read_text(encoding="utf-8") == "none", _stderr(
            work, session_id
        )

        view = await supervisor.status(session_worker_id(session_id))
        assert view is not None and view.pid is not None, _stderr(work, session_id)
        pid = view.pid
        cmdline = (
            Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode()
        )
        assert "mftik_sts.session_worker" in cmdline, cmdline
        env = _environ(pid)
        leaked = sorted(key for key in env if key.startswith("DATABASE_URL"))
        assert leaked == [], env
        db_files = [scratch.resolve(), Path(paper.db_path).resolve()]
        assert _open_db_files(pid, db_files) == []
        inodes = _socket_inodes(pid)
        assert inodes, cmdline
        assert _tcp_hits(pid, inodes, _database_ports()) == []

        reply = await paper.broker.request(
            Topics.td_account(paper.api_id),
            Envelope[TdOmsViewRequest].wrap(
                TdOmsViewRequest(api_id=paper.api_id),
                type=TD_OMS_VIEW,
                source="test",
            ),
            timeout=2,
        )
        orders = (reply.payload or {}).get("orders") or {}
        resting = [
            cid
            for cid, order in orders.items()
            if isinstance(order, dict) and order.get("status") in ("new", "pending_new")
        ]
        assert resting == [], orders
    finally:
        try:
            await supervisor.close(CloseMode.STOP)
        except Exception:
            pass
        await database_cm.__aexit__(None, None, None)
