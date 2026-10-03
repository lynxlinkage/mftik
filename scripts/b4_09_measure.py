"""B4-09 RSS samples for §4.7.

Spawns one real worker of each kind under a supervisor and prints
``procman.report`` ``rss_bytes`` (worker-tree Pss, shim excluded) next
to ``VmRSS`` from ``/proc/<pid>/status``. Nothing here is a product
default. Estimates in ``KIND_RSS_ESTIMATE_MIB`` are chosen from a run
of this script.

    uv run --all-packages python scripts/b4_09_measure.py

Needs a NATS server on ``NATS_URL`` (default ``nats://127.0.0.1:4222``).
The scratch sqlite is under the system temp dir. No venue key is used.
"""

from __future__ import annotations

import asyncio
import os
import platform
import statistics
import sys
import tempfile
import time
import uuid
from decimal import Decimal
from importlib.metadata import version
from pathlib import Path

from mftik.broker import Broker, BrokerConfig
from mftik.exchange import PaperExchange
from mftik.exchange.atoms import TOPIC_ORDERBOOK, AtomOptions
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
    PAPER_ORDER_BOOK,
    TD_ACCOUNT_TRADING,
    Envelope,
    StsCreateSessionRequest,
    TdAccountTrading,
    Topics,
    UntypedEnvelope,
)
from mftik.registry.qualify import qualify
from mftik.registry.store import RegistryStore
from mftik_db.models import Api, Base, Instance, User
from mftik_md.conn import ConnId
from mftik_md.conn_worker import argv_for
from mftik_md.defaults import FETCH_HB_TIMEOUT_S, FETCH_START_TIMEOUT_S
from mftik_md.fetch_ctl import FETCH_WORKER_ID, fetch_worker_argv
from mftik_paper.rpc import dispatch
from mftik_sts.controller.spawn import session_worker_argv, write_session_request
from mftik_sts.controller.types import session_worker_id
from mftik_td.controller.defaults import (
    ACCOUNT_HB_TIMEOUT_S,
    ACCOUNT_START_TIMEOUT_S,
    ACCOUNT_STOP_GRACE_S,
)
from mftik_td.controller.types import BoundAccount
from mftik_td.controller.worker import account_worker_spec
from mftik_td.supervise import account_worker_argv
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

_API_KEY = "paper-key-1"
_API_SECRET = "paper-secret-1"
_DEAD = (
    WorkerPhase.FAILED,
    WorkerPhase.CRASHED,
    WorkerPhase.FATAL,
    WorkerPhase.LOST,
)

_IDLE = """\
from pathlib import Path

from mftik.strategy import Strategy


class Idle(Strategy):
    async def on_ready(self, ready: object) -> None:
        del ready
        Path(self.paras["marker"]).write_text("idle", encoding="utf-8")
"""

_LOADED = """\
from decimal import Decimal
from pathlib import Path

from mftik.exchange.models import OrderType, Side
from mftik.strategy import Strategy


class Loaded(Strategy):
    async def on_ready(self, ready: object) -> None:
        del ready
        held = (Decimal("0"), OrderType.MARKET, Side.BUY)
        Path(self.paras["marker"]).write_text(
            f"loaded {len(held)}", encoding="utf-8"
        )
"""


def _vm_kib(pid: int, field: str) -> int | None:
    try:
        text = Path(f"/proc/{pid}/status").read_text(encoding="utf-8")
    except OSError:
        return None
    prefix = f"{field}:"
    for line in text.splitlines():
        if line.startswith(prefix):
            return int(line.split()[1])
    return None


def _ppid(pid: int) -> int | None:
    try:
        text = Path(f"/proc/{pid}/status").read_text(encoding="utf-8")
    except OSError:
        return None
    for line in text.splitlines():
        if line.startswith("PPid:"):
            return int(line.split()[1])
    return None


def _tail(path: Path) -> str:
    if not path.is_file():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")[-1500:]


def _spec(
    *,
    worker_id: str,
    plane: str,
    kind: str,
    argv: tuple[str, ...],
    env: dict[str, str],
    hb_timeout_s: float | None,
    start_timeout_s: float,
) -> WorkerSpec:
    return WorkerSpec(
        id=worker_id,
        plane=plane,  # type: ignore[arg-type]
        kind=kind,
        incarnation=1,
        argv=argv,
        env=env,
        code_ref="b4-09-measure",
        restart="never",
        start_timeout_s=start_timeout_s,
        hb_timeout_s=hb_timeout_s,
        oom_score_adj=OOM_SCORE_ADJ[(plane, kind)],
        rlimit_data_bytes=None,
        stop_grace_s=1.0,
        labels={},
    )


async def _wait_ready(
    supervisor: Supervisor, worker_id: str, *, seconds: float
) -> object:
    deadline = time.monotonic() + seconds
    status = None
    while time.monotonic() < deadline:
        status = await supervisor.status(worker_id)
        if status is not None and status.ready and status.phase is WorkerPhase.RUNNING:
            return status
        if status is not None and status.phase in _DEAD:
            return status
        await asyncio.sleep(0.05)
    return status


async def _sample(supervisor: Supervisor, worker_id: str, *, n: int = 3) -> dict:
    """Median of ``n`` reports. Tree Pss is ``rss_bytes``; VmRSS is /proc."""
    pss: list[int] = []
    worker_vm: list[int] = []
    shim_vm: list[int] = []
    worker_pid = None
    shim_pid = None
    for index in range(n):
        report = await supervisor.report()
        status = await supervisor.status(worker_id)
        worker = next((item for item in report.workers if item.id == worker_id), None)
        if worker is not None and worker.rss_bytes is not None:
            pss.append(worker.rss_bytes)
        if status is not None and status.pid:
            worker_pid = status.pid
            vm = _vm_kib(status.pid, "VmRSS")
            if vm is not None:
                worker_vm.append(vm)
            parent = _ppid(status.pid)
            if parent:
                shim_pid = parent
                shim = _vm_kib(parent, "VmRSS")
                if shim is not None:
                    shim_vm.append(shim)
        if index + 1 < n:
            await asyncio.sleep(0.25)
    return {
        "rss_bytes_pss": statistics.median(pss) if pss else None,
        "worker_vmrss_kib": statistics.median(worker_vm) if worker_vm else None,
        "shim_vmrss_kib": statistics.median(shim_vm) if shim_vm else None,
        "worker_pid": worker_pid,
        "shim_pid": shim_pid,
        "samples": n,
    }


def _print_row(name: str, row: dict) -> None:
    pss = row.get("rss_bytes_pss")
    mib = None if pss is None else pss / (1024 * 1024)
    print(
        f"{name}\t"
        f"pss_bytes={pss}\t"
        f"pss_mib={None if mib is None else round(mib, 2)}\t"
        f"worker_vmrss_kib={row.get('worker_vmrss_kib')}\t"
        f"shim_vmrss_kib={row.get('shim_vmrss_kib')}\t"
        f"worker_pid={row.get('worker_pid')}\t"
        f"shim_pid={row.get('shim_pid')}",
        flush=True,
    )


def _machine() -> None:
    model = ""
    mhz = ""
    bogomips = ""
    cores = 0
    try:
        text = Path("/proc/cpuinfo").read_text(encoding="utf-8")
    except OSError:
        text = ""
    for line in text.splitlines():
        if line.startswith("model name") and not model:
            model = line.split(":", 1)[1].strip()
        elif line.startswith("cpu MHz") and not mhz:
            mhz = line.split(":", 1)[1].strip()
        elif line.startswith("bogomips") and not bogomips:
            bogomips = line.split(":", 1)[1].strip()
        elif line.startswith("processor"):
            cores += 1
    print(
        "machine\t"
        f"system={platform.system()} {platform.release()}\t"
        f"python={platform.python_version()} ({platform.python_compiler()})\t"
        f"cpu={model}\t"
        f"cores={cores}\t"
        f"mhz={mhz}\t"
        f"bogomips={bogomips}\t"
        f"uvloop={version('uvloop')}\t"
        f"nats-py={version('nats-py')}",
        flush=True,
    )


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


async def _scratch(path: Path) -> tuple[str, str, int]:
    url = f"sqlite+aiosqlite:///{path}"
    sync = f"sqlite:///{path}"
    engine = create_async_engine(url)
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with maker() as session:
            session.add(User(id=1, email="owner-1@test.invalid"))
            await session.flush()
            instance = Instance(name="td", domain="td", region="paper", enabled=True)
            session.add(instance)
            await session.flush()
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
            api_id = int(row.id)
    finally:
        await engine.dispose()
    return url, sync, api_id


def _bus_env(broker: Broker) -> dict[str, str]:
    return {
        "NATS_URL": broker.config.nats_url,
        "BROKER_KEY_PREFIX": broker.config.key_prefix,
        "BROKER_REQUEST_TIMEOUT": "2",
        "PYTHONUNBUFFERED": "1",
    }


async def _measure_sleep(root: Path) -> None:
    work = root / "shim"
    supervisor = Supervisor(work, plane="td", instance="measure")
    await supervisor.start()
    spec = _spec(
        worker_id="td/account/sleep",
        plane="td",
        kind="account",
        argv=(sys.executable, "-c", "import time; time.sleep(30)"),
        env={"PYTHONUNBUFFERED": "1"},
        hb_timeout_s=None,
        start_timeout_s=5,
    )
    try:
        await supervisor.spawn(spec)
        deadline = time.monotonic() + 3
        status = None
        while time.monotonic() < deadline:
            status = await supervisor.status(spec.id)
            if status is not None and status.pid:
                break
            await asyncio.sleep(0.05)
        await asyncio.sleep(0.3)
        row = await _sample(supervisor, spec.id)
        _print_row("shim_sleep_worker", row)
    finally:
        await supervisor.close(CloseMode.STOP)


async def _measure_fetch(root: Path, env: dict[str, str]) -> None:
    work = root / "fetch"
    supervisor = Supervisor(work, plane="md", instance="measure")
    await supervisor.start()
    spec = _spec(
        worker_id=FETCH_WORKER_ID,
        plane="md",
        kind="fetch",
        argv=fetch_worker_argv(),
        env=env,
        hb_timeout_s=FETCH_HB_TIMEOUT_S,
        start_timeout_s=FETCH_START_TIMEOUT_S,
    )
    try:
        await supervisor.spawn(spec)
        status = await _wait_ready(supervisor, spec.id, seconds=8)
        if status is None or not getattr(status, "ready", False):
            print(
                "md_fetch FAILED",
                _tail(log_path(work, spec.id, "stderr")),
                flush=True,
            )
            return
        await asyncio.sleep(0.3)
        _print_row("md_fetch", await _sample(supervisor, spec.id))
    finally:
        await supervisor.close(CloseMode.STOP)


async def _measure_conn(
    root: Path, env: dict[str, str], supervisor_holder: list
) -> Supervisor:
    work = root / "conn"
    supervisor = Supervisor(work, plane="md", instance="measure")
    await supervisor.start()
    ticker = UniversalTicker.parse("Paper_Spot_BTCUSDT")
    atom = atoms_for(TOPIC_ORDERBOOK, ticker, AtomOptions()).atoms[0]
    conn = ConnId("Paper", "public", 0)
    spec = _spec(
        worker_id=conn.worker_id,
        plane="md",
        kind="conn",
        argv=argv_for(
            python=sys.executable,
            instance="measure",
            incarnation=1,
            conn=conn,
            atoms=(atom,),
        ),
        env=env,
        hb_timeout_s=None,
        start_timeout_s=8,
    )
    await supervisor.spawn(spec)
    status = await _wait_ready(supervisor, spec.id, seconds=8)
    if status is None or not getattr(status, "ready", False):
        print(
            "md_conn FAILED",
            _tail(log_path(work, spec.id, "stderr")),
            flush=True,
        )
    else:
        await asyncio.sleep(0.4)
        _print_row("md_conn_paper", await _sample(supervisor, spec.id))
    supervisor_holder.append(supervisor)
    return supervisor


async def _measure_account(
    root: Path,
    env: dict[str, str],
    broker: Broker,
    api_id: int,
) -> Supervisor:
    work = root / "td"
    supervisor = Supervisor(work, plane="td", instance="td")
    await supervisor.start()
    worker_env = dict(env)
    spec = account_worker_spec(
        BoundAccount(api_id=api_id, venue="Paper", instance="td"),
        incarnation=1,
        argv=account_worker_argv(api_id, 1, False),
        code_ref="b4-09-measure",
        start_timeout_s=ACCOUNT_START_TIMEOUT_S,
        hb_timeout_s=ACCOUNT_HB_TIMEOUT_S,
        stop_grace_s=ACCOUNT_STOP_GRACE_S,
        env=worker_env,
    )
    await supervisor.spawn(spec)
    status = await _wait_ready(supervisor, spec.id, seconds=8)
    if status is None or not getattr(status, "ready", False):
        print(
            "td_account FAILED",
            _tail(log_path(work, spec.id, "stderr")),
            flush=True,
        )
        return supervisor
    await asyncio.sleep(0.3)
    _print_row("td_account_trading_off", await _sample(supervisor, spec.id))
    reply = await broker.request(
        Topics.td_account(api_id),
        Envelope[TdAccountTrading].wrap(
            TdAccountTrading(api_id=api_id, active=True),
            type=TD_ACCOUNT_TRADING,
            source="measure",
        ),
        timeout=8,
    )
    body = TdAccountTrading.model_validate(reply.payload)
    if body.active is not True:
        print("td trading push did not stick", body, flush=True)
        return supervisor
    await asyncio.sleep(0.5)
    _print_row("td_account_trading_on", await _sample(supervisor, spec.id))
    return supervisor


def _plant(data: Path, source: str) -> str:
    added = RegistryStore(data).add({"strategy.py": source})
    return qualify("private", added.type)


async def _measure_session(
    root: Path,
    env: dict[str, str],
    *,
    name: str,
    source: str,
    session_id: str,
    td: dict | None,
    api_id: int | None,
) -> None:
    data = root / f"data-{name}"
    key = _plant(data, source)
    marker = root / f"{name}.marker"
    work = root / name
    # Product session env does not forward database URLs (F10).
    child_env = {
        key: value
        for key, value in env.items()
        if key not in {"DATABASE_URL", "DATABASE_URL_SYNC"}
    }
    child_env["MFTIK_DATA"] = str(data)
    request = StsCreateSessionRequest(
        session_id=session_id,
        created_by=1,
        strategy=key,
        type=key,
        td=td or {},
        st_paras={
            "marker": str(marker),
            "api_id": "" if api_id is None else str(api_id),
        },
    )
    path = write_session_request(work, request)
    supervisor = Supervisor(work, plane="sts", instance="sts")
    await supervisor.start()
    worker_id = session_worker_id(session_id)
    spec = _spec(
        worker_id=worker_id,
        plane="sts",
        kind="session",
        argv=session_worker_argv(path),
        env=child_env,
        hb_timeout_s=None,
        start_timeout_s=20,
    )
    try:
        await supervisor.spawn(spec)
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline and not marker.is_file():
            status = await supervisor.status(worker_id)
            if status is not None and status.phase in _DEAD:
                break
            await asyncio.sleep(0.05)
        status = await _wait_ready(supervisor, worker_id, seconds=3)
        ready = status is not None and getattr(status, "ready", False)
        if not marker.is_file() or not ready:
            print(
                f"{name} FAILED marker={marker.is_file()} status={status}",
                _tail(log_path(work, worker_id, "stderr")),
                flush=True,
            )
            return
        await asyncio.sleep(0.3)
        _print_row(name, await _sample(supervisor, worker_id))
    finally:
        await supervisor.close(CloseMode.STOP)


async def amain() -> None:
    _machine()
    nats = os.getenv("NATS_URL", "nats://127.0.0.1:4222")
    prefix = f"b409-{uuid.uuid4().hex[:10]}"
    broker = Broker(BrokerConfig(nats_url=nats, key_prefix=prefix))
    await broker.connect()
    stop = asyncio.Event()
    paper_tasks: list[asyncio.Task[None]] = []
    supervisors: list[Supervisor] = []
    try:
        with tempfile.TemporaryDirectory(prefix="b409-") as tmp:
            root = Path(tmp)
            await _measure_sleep(root)
            env = _bus_env(broker)
            await _measure_fetch(root, env)
            url, sync, api_id = await _scratch(root / "td.db")
            env = dict(env)
            env["DATABASE_URL"] = url
            env["DATABASE_URL_SYNC"] = sync
            exchange = PaperExchange(
                symbols={"BTCUSDT": Decimal("50000")},
                tick_interval=60,
            )
            exchange.register_api(
                _API_KEY,
                _API_SECRET,
                balances={"USDT": Decimal("1000000"), "BTC": Decimal("1")},
            )
            await exchange.start()
            try:
                paper_tasks.append(
                    asyncio.create_task(_serve_paper(broker, exchange, stop))
                )
                paper_tasks.append(
                    asyncio.create_task(_publish_books(broker, exchange, stop))
                )
                await asyncio.sleep(0.1)
                held: list[Supervisor] = []
                await _measure_conn(root, env, held)
                supervisors.extend(held)
                account = await _measure_account(root, env, broker, api_id)
                supervisors.append(account)
                await _measure_session(
                    root,
                    env,
                    name="sts_session_idle",
                    source=_IDLE,
                    session_id="idle1",
                    td=None,
                    api_id=None,
                )
                await _measure_session(
                    root,
                    env,
                    name="sts_session_loaded",
                    source=_LOADED,
                    session_id="load1",
                    td={"main": {"api_id": api_id}},
                    api_id=api_id,
                )
            finally:
                await exchange.stop()
    finally:
        stop.set()
        for task in paper_tasks:
            task.cancel()
        if paper_tasks:
            await asyncio.gather(*paper_tasks, return_exceptions=True)
        for supervisor in supervisors:
            try:
                await supervisor.close(CloseMode.STOP)
            except Exception:
                pass
        await broker.close()


def main() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    main()
