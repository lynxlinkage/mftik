"""Pinned digest and env generation against a real session worker (B5-10).

Paper and the scratch database are fixture setup. Each test's call phase
is one of the acceptance bullets, split so none of them holds the worker
open longer than the integration cap.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
import uuid
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from broker_harness import unique_key_prefix
from db_harness import a_database, an_owner
from mftik.broker import Broker, BrokerConfig
from mftik.cli import operator
from mftik.environment import NodeEnv, PackageRecord
from mftik.exchange import PaperExchange
from mftik.procman import CloseMode, Supervisor, WorkerPhase, load_supervisor_state
from mftik.protocol import (
    STS_ENV_SYNC,
    STS_REGISTRY_SYNC,
    Envelope,
    StsCreateSessionRequest,
    StsEnvSyncRequest,
    StsRegistrySyncRequest,
    StsRegistryTreeOp,
)
from mftik.registry.qualify import qualify
from mftik.registry.store import RegistryStore
from mftik_sts.controller import (
    SessionSpec,
    StsOrchestrator,
    env_sync_handler,
    registry_sync_handler,
    session_worker_id,
    start_handler,
)
from mftik_sts.controller.status import DbStatusStore
from mftik_sts.hostdisk import TreeReplica, deployable, rehang_code
from mftik_sts.hostdisk.checks import MFTIK_DEV_RELEASE
from mftik_sts.hostdisk.sync import apply_registry_sync
from test_crash_cleanup_integration import (
    _API_KEY,
    _API_SECRET,
    _enable_trading,
    _scratch_api,
    _serve_paper,
    _start_message,
    _stderr,
    _until,
)

pytestmark = pytest.mark.integration

_V1 = "v1"
_V2 = "v2"


def _pinned(token: str) -> str:
    return (
        "import asyncio\n"
        "from decimal import Decimal\n"
        "from pathlib import Path\n"
        "\n"
        "from mftik.exchange.models import OrderType, Side\n"
        "from mftik.strategy import Strategy\n"
        "\n"
        f"TOKEN = {token!r}\n"
        "\n"
        "\n"
        "class Pinned(Strategy):\n"
        "    def __init__(self) -> None:\n"
        "        super().__init__()\n"
        "        self._armed = False\n"
        "\n"
        "    async def on_start(self) -> None:\n"
        "        path = Path(self.paras['token'])\n"
        "        previous = ''\n"
        "        if path.is_file():\n"
        "            previous = path.read_text(encoding='utf-8')\n"
        "        path.write_text(previous + TOKEN + '\\n', encoding='utf-8')\n"
        "\n"
        "    async def on_ready(self, ready: object) -> None:\n"
        "        del ready\n"
        "        await self.oms.submit_order(\n"
        "            int(self.paras['api_id']),\n"
        "            ticker='Paper_Spot_BTCUSDT',\n"
        "            side=Side.BUY,\n"
        "            type=OrderType.LIMIT,\n"
        "            qty=Decimal('0.01'),\n"
        "            price=Decimal('1'),\n"
        "        )\n"
        "\n"
        "    async def on_order_update(self, api_id: int, order: object) -> None:\n"
        "        del api_id, order\n"
        "        if self.paras.get('raise_once') != '1':\n"
        "            return\n"
        "        path = Path(self.paras['token'])\n"
        "        text = path.read_text(encoding='utf-8') if path.is_file() else ''\n"
        "        if text.count(TOKEN) != 1 or self._armed:\n"
        "            return\n"
        "        self._armed = True\n"
        "        await asyncio.sleep(1.3)\n"
        "        raise RuntimeError('boom')\n"
    )


_LAZY = (
    "import asyncio\n"
    "from pathlib import Path\n"
    "\n"
    "from mftik.strategy import Strategy\n"
    "\n"
    "\n"
    "class LazyPin(Strategy):\n"
    "    requires = ('extra_mod',)\n"
    "\n"
    "    async def on_start(self) -> None:\n"
    "        Path(self.paras['state']).write_text('waiting', encoding='utf-8')\n"
    "\n"
    "        async def _watch() -> None:\n"
    "            trigger = Path(self.paras['trigger'])\n"
    "            while not trigger.is_file():\n"
    "                await asyncio.sleep(0.05)\n"
    "            import extra_mod\n"
    "\n"
    "            Path(self.paras['state']).write_text(\n"
    "                extra_mod.TOKEN, encoding='utf-8'\n"
    "            )\n"
    "\n"
    "        asyncio.get_running_loop().create_task(_watch())\n"
)


def _publish_tree(
    data: Path,
    source: str,
    *,
    replace: bool = False,
    keep_digests: frozenset[str] = frozenset(),
):
    added = RegistryStore(data).add({"strategy.py": source}, replace=replace)
    key = qualify(added.origin, added.type)
    apply_registry_sync(
        StsRegistrySyncRequest(
            trees=[
                StsRegistryTreeOp(
                    op="upsert",
                    origin=added.origin,
                    name=added.type,
                    digest=added.digest,
                    files={"strategy.py": source},
                )
            ],
            reload=False,
        ),
        keep_digests=keep_digests,
        data_dir=data,
    )
    return added, key


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
    prefix = unique_key_prefix("b510")
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
        from mftik_td.controller.defaults import (
            ACCOUNT_HB_TIMEOUT_S,
            ACCOUNT_START_TIMEOUT_S,
            ACCOUNT_STOP_GRACE_S,
        )
        from mftik_td.controller.types import BoundAccount, account_worker_id
        from mftik_td.controller.worker import account_worker_spec
        from mftik_td.supervise import account_worker_argv

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


async def _open(
    tmp_path: Path,
    paper: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
    *,
    source: str,
    session_id: str,
    restart: str,
    paras: dict[str, str],
    digest: str | None = None,
    env_generation: int | None = None,
    replace: bool = False,
):
    data = tmp_path / "data"
    monkeypatch.setenv("MFTIK_DATA", str(data))
    monkeypatch.setenv(MFTIK_DEV_RELEASE, "1")
    monkeypatch.setenv("BROKER_KEY_PREFIX", paper.prefix)
    monkeypatch.setenv("NATS_URL", paper.nats_url)
    monkeypatch.setenv("BROKER_REQUEST_TIMEOUT", "0.5")
    added, key = _publish_tree(data, source, replace=replace)
    pinned = added.digest if digest is None else digest
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
        from mftik_db.repositories.session import StsSessionRepository

        await StsSessionRepository(session).create_live(
            session_id=session_id,
            created_by=1,
            type=key,
            restart=restart,
            td={"main": {"api_id": paper.api_id}},
            strategy_digest=pinned,
            env_generation=env_generation,
        )
    supervisor = Supervisor(work, plane="sts", instance="sts")
    orch = StsOrchestrator(
        supervisor,
        store=DbStatusStore(database.scope),
        publish=None,
        broker=paper.broker,
        code_ref="test",
    )
    await orch.boot()
    return SimpleNamespace(
        orch=orch,
        supervisor=supervisor,
        request=request,
        work=work,
        data=data,
        database_cm=database_cm,
        session_id=session_id,
        digest=pinned,
        key=key,
        added=added,
    )


async def _close(run: SimpleNamespace) -> None:
    try:
        await run.supervisor.close(CloseMode.STOP)
    except Exception:
        pass
    await run.database_cm.__aexit__(None, None, None)


async def _ready(run: SimpleNamespace) -> None:
    session_id = run.session_id

    async def _worker_ready() -> bool:
        view = await run.supervisor.status(session_worker_id(session_id))
        return view is not None and view.phase is WorkerPhase.RUNNING and view.ready

    try:
        await _until(_worker_ready, seconds=5)
    except AssertionError:
        raise AssertionError(_stderr(run.work, session_id)) from None


async def test_rehang_keeps_the_pinned_digest_and_stale_lists_it(
    tmp_path: Path, paper: SimpleNamespace, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    token = tmp_path / "token"
    session_id = uuid.uuid4().hex[:6]
    run = await _open(
        tmp_path,
        paper,
        monkeypatch,
        source=_pinned(_V1),
        session_id=session_id,
        restart="on_failure",
        paras={
            "token": str(token),
            "api_id": str(paper.api_id),
            "raise_once": "1",
        },
    )
    try:
        reply = await start_handler(run.orch)(_start_message(run.request))
        assert reply is not None and reply.type != "error"
        await run.orch.converge(session_id)
        await _ready(run)
        added, _key = _publish_tree(
            run.data,
            _pinned(_V2),
            replace=True,
            keep_digests=frozenset({run.digest}),
        )
        assert added.digest != run.digest
        replica = TreeReplica(run.data)
        assert replica.current(run.key) == added.digest
        assert replica.path_of(run.digest) is not None
        worker_id = session_worker_id(session_id)

        async def _rehanged() -> bool:
            await run.orch.observe_all()
            view = await run.supervisor.status(worker_id)
            text = token.read_text(encoding="utf-8") if token.is_file() else ""
            return (
                view is not None
                and view.spec.incarnation >= 2
                and text.count(_V1) >= 2
                and _V2 not in text
            )

        try:
            await _until(_rehanged, seconds=8)
        except AssertionError:
            held = run.orch._sessions[session_id]  # noqa: SLF001
            raise AssertionError(
                f"phase={held.phase.value} reason={held.reason} "
                f"incarnation={held.worker_incarnation}\n"
                f"{_stderr(run.work, session_id)}"
            ) from None
        old_tree = replica.path_of(run.digest)
        assert old_tree is not None
        assert f"TOKEN = {_V1!r}" in (old_tree / "strategy.py").read_text(
            encoding="utf-8"
        )
        records = load_supervisor_state(run.work)
        rows = []
        current = replica.current(run.key)
        for record in records:
            labels = record.spec.labels
            rows.append(
                {
                    "plane": "sts",
                    "instance": "sts",
                    "id": record.spec.id,
                    "incarnation": record.spec.incarnation,
                    "phase": record.phase.value,
                    "ready": True,
                    "code_ref": record.spec.code_ref,
                    "rss_bytes": None,
                    "age_s": 1.0,
                    "strategy_digest": labels.get("strategy_digest"),
                    "current_digest": current,
                }
            )
        assert any(row["strategy_digest"] == run.digest for row in rows)
        assert any(row["strategy_digest"] != current for row in rows)

        def connected(profile: str):
            del profile

            class _Client:
                def __enter__(self) -> _Client:
                    return self

                def __exit__(self, *args: object) -> None:
                    return None

                def get(self, path: str) -> dict[str, object]:
                    assert path == "/workers"
                    return {"workers": rows}

            return None, _Client()

        monkeypatch.setattr(operator, "connected", connected)
        assert operator.workers(SimpleNamespace(stale=True, profile="local")) == 0
        out = capsys.readouterr().out
        assert worker_id in out
    finally:
        await _close(run)


async def test_a_new_session_runs_the_current_digest(
    tmp_path: Path, paper: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    token = tmp_path / "token-new"
    session_id = uuid.uuid4().hex[:6]
    run = await _open(
        tmp_path,
        paper,
        monkeypatch,
        source=_pinned(_V2),
        session_id=session_id,
        restart="never",
        paras={"token": str(token), "api_id": str(paper.api_id)},
    )
    try:
        reply = await start_handler(run.orch)(_start_message(run.request))
        assert reply is not None and reply.type != "error"
        await run.orch.converge(session_id)

        async def _wrote() -> bool:
            return token.is_file() and token.read_text(encoding="utf-8").startswith(_V2)

        try:
            await _until(_wrote, seconds=5)
        except AssertionError:
            raise AssertionError(_stderr(run.work, session_id)) from None
        tree = TreeReplica(run.data).path_of(run.digest)
        assert tree is not None
        assert f"TOKEN = {_V2!r}" in (tree / "strategy.py").read_text(
            encoding="utf-8"
        )
    finally:
        await _close(run)


async def test_a_session_on_an_old_generation_can_still_lazy_import(
    tmp_path: Path, paper: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    data = tmp_path / "data"
    monkeypatch.setenv("MFTIK_DATA", str(data))
    monkeypatch.setenv(MFTIK_DEV_RELEASE, "1")
    env = NodeEnv(data)
    with env.lock():
        first = env.begin()
        (first / "extra_mod.py").write_text(
            'TOKEN = "from-gen-1"\n', encoding="utf-8"
        )
        env.commit(first, {"extra_mod": PackageRecord("1", "extra_mod", "manual")})
    with env.lock():
        second = env.begin()
        (second / "extra_mod.py").write_text(
            'TOKEN = "from-gen-2"\n', encoding="utf-8"
        )
        env.commit(second, {"extra_mod": PackageRecord("2", "extra_mod", "manual")})
    state = tmp_path / "state"
    trigger = tmp_path / "trigger"
    session_id = uuid.uuid4().hex[:6]
    run = await _open(
        tmp_path,
        paper,
        monkeypatch,
        source=_LAZY,
        session_id=session_id,
        restart="never",
        paras={"state": str(state), "trigger": str(trigger)},
        env_generation=1,
    )
    try:
        reply = await start_handler(run.orch)(_start_message(run.request))
        assert reply is not None and reply.type != "error"
        await run.orch.converge(session_id)

        async def _waiting() -> bool:
            return state.is_file() and state.read_text(encoding="utf-8") == "waiting"

        try:
            await _until(_waiting, seconds=5)
        except AssertionError:
            raise AssertionError(_stderr(run.work, session_id)) from None
        env.write_pinned_generations([1])
        with env.lock():
            third = env.begin()
            (third / "extra_mod.py").write_text(
                'TOKEN = "from-gen-3"\n', encoding="utf-8"
            )
            env.commit(
                third, {"extra_mod": PackageRecord("3", "extra_mod", "manual")}
            )
        assert (data / "env" / "gen-1" / "site-packages").is_dir()
        trigger.write_text("go\n", encoding="utf-8")

        async def _imported() -> bool:
            return state.is_file() and state.read_text(encoding="utf-8") == "from-gen-1"

        try:
            await _until(_imported, seconds=3)
        except AssertionError:
            seen = state.read_text(encoding="utf-8") if state.is_file() else ""
            raise AssertionError(
                f"state={seen}\n{_stderr(run.work, session_id)}"
            ) from None
    finally:
        await _close(run)


async def test_the_controller_process_never_imports_a_strategy_tree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    monkeypatch.setenv(MFTIK_DEV_RELEASE, "1")
    before = {name for name in sys.modules if name.startswith("_mftik_reg_")}
    added = RegistryStore(tmp_path).add({"strategy.py": _pinned(_V1)})
    key = qualify(added.origin, added.type)
    orch = StsOrchestrator(Supervisor(tmp_path / "work", plane="sts", instance="sts"))
    message = Envelope[dict].wrap(
        StsRegistrySyncRequest(
            trees=[
                StsRegistryTreeOp(
                    op="upsert",
                    origin=added.origin,
                    name=added.type,
                    digest=added.digest,
                    files={"strategy.py": _pinned(_V1)},
                )
            ],
            reload=True,
            retain=[key],
        ).model_dump(),
        type=STS_REGISTRY_SYNC,
        source="api",
    )
    reply = await registry_sync_handler(orch)(message)
    assert reply is not None
    spec = SessionSpec(
        session_id="abc123",
        instance="sts",
        strategy=key,
        strategy_digest=added.digest,
    )
    replica = TreeReplica(tmp_path)
    assert rehang_code(spec, replica=replica, release="0.1.0").failed is False
    assert deployable(spec, replica=replica, env=NodeEnv(tmp_path)).ok
    env_reply = await env_sync_handler(orch)(
        Envelope[dict].wrap(
            StsEnvSyncRequest().model_dump(),
            type=STS_ENV_SYNC,
            source="api",
        )
    )
    assert env_reply is not None
    after = {name for name in sys.modules if name.startswith("_mftik_reg_")}
    assert after == before
    for module in list(sys.modules.values()):
        path = str(getattr(module, "__file__", "") or "")
        assert "/registry/trees/" not in path.replace("\\", "/")
