"""ENV-11 rebuild / boot sequences that cannot be faked at the API."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import fakeredis.aioredis
import pytest
from mftik.broker import Broker, BrokerConfig
from mftik.envapply import ApplySpec, apply_packages
from mftik.environment import EnvStamp, NodeEnv
from mftik.protocol import StsCreateSessionRequest
from mftik.registry import RegistryStore
from mftik.strategy import Strategy
from mftik_sts.impl import load_local_registry, resolve
from mftik_sts.impl.noop import NoopStrategy
from mftik_sts.runtime_env import (
    IncompatibleEnvironment,
    attach_overlay,
    extras_names,
    refresh,
    reset_for_tests,
)
from mftik_sts.session import SessionManager

_NUMPY = """\
from mftik.strategy import Strategy

class UsesNumpy(Strategy):
    name = "uses_numpy"
    requires = ("numpy",)
"""


def _write_pkg(dest: Path, packages: dict[str, ApplySpec]) -> None:
    for name in packages:
        pkg = dest / name
        pkg.mkdir()
        (pkg / "__init__.py").write_text("ok\n")


@pytest.fixture(autouse=True)
def _isolate() -> None:
    reset_for_tests()
    yield
    reset_for_tests()


@pytest.fixture
async def broker() -> Broker:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test-env11"),
        redis_client=redis,
    )
    await client.connect()
    yield client
    await client.close()
    await redis.aclose()


class Rebuildable(Strategy):
    name = "uses_numpy"
    rebuildable = True


class FakeStsStore:
    def __init__(self) -> None:
        self.rows: dict[str, SimpleNamespace] = {}

    def seed(
        self,
        session_id: str,
        *,
        status: str = "interrupted",
        strategy: str = "uses_numpy",
        type: str | None = None,
        rebuild_count: int = 0,
    ) -> SimpleNamespace:
        row = SimpleNamespace(
            session_id=session_id,
            created_by=1,
            created_at=datetime.now(UTC),
            finished_at=datetime.now(UTC) - timedelta(seconds=1),
            status=status,
            reason="STS shut down while this was running",
            strategy=strategy,
            type=type,
            cid_slot=7,
            restart="always",
            rebuild_count=rebuild_count,
            td_api_ids=[],
            md_ids=[],
            st_paras={},
            st_facts={},
        )
        self.rows[session_id] = row
        return row

    async def persist_live(self, **kwargs: Any) -> SimpleNamespace:
        return self.seed(kwargs["session_id"], status="live")

    async def mark_finished(
        self, session_id: str, *, status: str = "done", reason: str | None = None
    ) -> SimpleNamespace | None:
        row = self.rows.get(session_id)
        if row is None:
            return None
        row.status = status
        row.reason = reason
        return row

    async def mark_live(self, session_id: str) -> SimpleNamespace | None:
        row = self.rows.get(session_id)
        if row is None:
            return None
        row.status = "live"
        return row

    async def bump_rebuild_count(self, session_id: str) -> int:
        row = self.rows[session_id]
        row.rebuild_count = int(row.rebuild_count or 0) + 1
        return row.rebuild_count

    async def reset_rebuild_count(self, session_id: str) -> SimpleNamespace | None:
        row = self.rows.get(session_id)
        if row is None:
            return None
        row.rebuild_count = 0
        return row

    async def list_sessions(
        self,
        *,
        status: str | None = "live",
        created_by: int | None = None,
        limit: int = 100,
    ) -> list[SimpleNamespace]:
        return [r for r in self.rows.values() if status is None or r.status == status]


@pytest.mark.asyncio
async def test_s3_restart_does_not_install(
    broker: Broker, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    installs = 0

    def counting(dest: Path, packages: dict[str, ApplySpec]) -> None:
        nonlocal installs
        installs += 1
        _write_pkg(dest, packages)

    apply_packages(
        NodeEnv(tmp_path),
        {"numpy": ApplySpec(version="1.0", dist="numpy")},
        installer=counting,
    )
    assert installs == 1
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _NUMPY}, applied_extras={"numpy": "1.0"})
    loaded, stamp = refresh(store, tmp_path)
    assert installs == 1
    assert stamp.generation == 1
    assert "numpy" in extras_names()
    import sys

    assert str(NodeEnv(tmp_path).site_packages(1)) in sys.path
    assert any(key.endswith("UsesNumpy") for key in loaded)
    resolve(loaded[0])

    db = FakeStsStore()
    db.seed("r-numpy", type=loaded[0], strategy="uses_numpy")
    manager = SessionManager(
        broker,
        heartbeat_interval=0.05,
        strategy_factory=lambda _name: Rebuildable(),
        persist_live=db.persist_live,
        mark_done=db.mark_finished,
        mark_live=db.mark_live,
        list_db_sessions=db.list_sessions,
        bump_rebuild_count=db.bump_rebuild_count,
        reset_rebuild_count=db.reset_rebuild_count,
    )
    rebuilt = await manager.rebuild_interrupted()
    assert rebuilt == ["r-numpy"]
    await manager.close_all()


@pytest.mark.asyncio
async def test_s7_rebuild_names_the_missing_extra(
    broker: Broker, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    attach_overlay(tmp_path)
    store = RegistryStore(tmp_path)
    added = store.add({"strategy.py": _NUMPY}, applied_extras={"numpy": "1.0"})
    key = f"{added.origin}::{added.type}"
    db = FakeStsStore()
    db.seed("r-env", type=key)
    manager = SessionManager(
        broker,
        heartbeat_interval=0.05,
        strategy_factory=lambda _name: NoopStrategy(),
        persist_live=db.persist_live,
        mark_done=db.mark_finished,
        mark_live=db.mark_live,
        list_db_sessions=db.list_sessions,
        bump_rebuild_count=db.bump_rebuild_count,
        reset_rebuild_count=db.reset_rebuild_count,
    )
    assert await manager.rebuild_interrupted() == []
    assert db.rows["r-env"].status == "interrupted"
    assert db.rows["r-env"].rebuild_count == 1


@pytest.mark.asyncio
async def test_s12_orphan_generation_is_not_on_sys_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    apply_packages(
        NodeEnv(tmp_path),
        {"numpy": ApplySpec(version="1.0", dist="numpy")},
        installer=_write_pkg,
    )
    env = NodeEnv(tmp_path)
    with env.lock():
        dest = env.begin()
        (dest / "orphan").mkdir()
        (dest / "orphan" / "__init__.py").write_text("nope\n")
    import sys

    attach_overlay(tmp_path)
    assert str(env.site_packages(1)) in sys.path
    assert str(dest) not in sys.path
    assert "numpy" in extras_names()
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _NUMPY}, applied_extras={"numpy": "1.0"})
    loaded = load_local_registry(store)
    assert any("UsesNumpy" in key for key in loaded)


@pytest.mark.asyncio
async def test_s14_mismatched_python_is_incompatible_not_an_import_error(
    broker: Broker, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    apply_packages(
        NodeEnv(tmp_path),
        {"numpy": ApplySpec(version="1.0", dist="numpy")},
        installer=_write_pkg,
    )
    env = NodeEnv(tmp_path)
    matching = env.read_stamp()
    env._write_stamp(
        EnvStamp(
            generation=matching.generation,
            python=(3, 11),
            platform=matching.platform,
            nbytes=matching.nbytes,
            packages=matching.packages,
        )
    )
    attach_overlay(tmp_path)
    assert extras_names() == frozenset()
    store = RegistryStore(tmp_path)
    added = store.add({"strategy.py": _NUMPY}, applied_extras={"numpy": "1.0"})
    key = f"{added.origin}::{added.type}"
    manager = SessionManager(
        broker,
        heartbeat_interval=0.05,
        strategy_factory=lambda _name: NoopStrategy(),
    )
    with pytest.raises(IncompatibleEnvironment, match="numpy") as caught:
        await manager.create_session(
            StsCreateSessionRequest(
                session_id="s1",
                created_by=1,
                strategy=key,
                type=key,
            )
        )
    assert "ImportError" not in type(caught.value).__name__
    assert manager.get("s1") is None


@pytest.mark.asyncio
async def test_s3_bundled_noop_still_deploys_on_a_bare_overlay(
    broker: Broker, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    attach_overlay(tmp_path)
    manager = SessionManager(broker, heartbeat_interval=0.05)
    result = await manager.create_session(
        StsCreateSessionRequest(session_id="noop-1", created_by=1, strategy="noop")
    )
    assert result.strategy == "noop"
    await manager.close_all()
