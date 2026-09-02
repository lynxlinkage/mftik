"""Deploy and rebuild name a missing extra, not a missing strategy."""

from __future__ import annotations

from pathlib import Path

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.envapply import ApplySpec, apply_packages
from mftik.environment import EnvStamp, NodeEnv
from mftik.protocol import StsCreateSessionRequest, TdAccountRef
from mftik.registry import RegistryStore
from mftik_sts.impl.noop import NoopStrategy
from mftik_sts.runtime_env import (
    IncompatibleEnvironment,
    attach_overlay,
    reset_for_tests,
)
from mftik_sts.session import SessionManager

_NUMPY = """\
from mftik.strategy import Strategy

class UsesNumpy(Strategy):
    name = "uses_numpy"
    requires = ("numpy",)
"""


@pytest.fixture(autouse=True)
def _isolate() -> None:
    reset_for_tests()
    yield
    reset_for_tests()


@pytest.fixture
async def broker() -> Broker:
    async with a_broker("test-env8") as client:
        yield client


def _plant_tree(tmp_path: Path, *, origin: str = "peer") -> str:
    store = RegistryStore(tmp_path)
    if origin not in {"public", "private"}:
        store.put_remote(origin, "http://peer:8000")
    added = store.add(
        {"strategy.py": _NUMPY},
        origin=origin,
        applied_extras={},
    )
    return f"{added.origin}::{added.type}"


def _manager(broker: Broker) -> SessionManager:
    return SessionManager(
        broker,
        heartbeat_interval=0.05,
        strategy_factory=lambda _name: NoopStrategy(),
    )


@pytest.mark.asyncio
async def test_deploy_refuses_missing_extras(
    broker: Broker, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    attach_overlay(tmp_path)
    key = _plant_tree(tmp_path)
    manager = _manager(broker)
    with pytest.raises(IncompatibleEnvironment, match="numpy"):
        await manager.create_session(
            StsCreateSessionRequest(
                session_id="s1",
                created_by=1,
                strategy=key,
                type=key,
            )
        )
    assert manager.get("s1") is None


@pytest.mark.asyncio
async def test_deploy_proceeds_when_extras_are_applied(
    broker: Broker, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))

    def plant(dest: Path, packages: dict[str, ApplySpec]) -> None:
        for name in packages:
            pkg = dest / name
            pkg.mkdir()
            (pkg / "__init__.py").write_text("ok\n")

    apply_packages(
        NodeEnv(tmp_path),
        {"numpy": ApplySpec(version="1.0", dist="numpy")},
        installer=plant,
    )
    attach_overlay(tmp_path)
    key = _plant_tree(tmp_path)
    manager = _manager(broker)
    result = await manager.create_session(
        StsCreateSessionRequest(
            session_id="s1",
            created_by=1,
            strategy=key,
            type=key,
        )
    )
    assert result.session_id == "s1"
    await manager.close_all()


@pytest.mark.asyncio
async def test_abi_mismatch_is_incompatible_not_unknown(
    broker: Broker, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))

    def plant(dest: Path, packages: dict[str, ApplySpec]) -> None:
        for name in packages:
            pkg = dest / name
            pkg.mkdir()
            (pkg / "__init__.py").write_text("ok\n")

    apply_packages(
        NodeEnv(tmp_path),
        {"numpy": ApplySpec(version="1.0", dist="numpy")},
        installer=plant,
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
    key = _plant_tree(tmp_path)
    manager = _manager(broker)
    with pytest.raises(IncompatibleEnvironment, match="numpy"):
        await manager.create_session(
            StsCreateSessionRequest(
                session_id="s1",
                created_by=1,
                strategy=key,
                type=key,
            )
        )


@pytest.mark.asyncio
async def test_missing_tree_is_still_unknown_strategy(
    broker: Broker, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    attach_overlay(tmp_path)
    manager = SessionManager(broker, heartbeat_interval=0.05)
    with pytest.raises(KeyError, match="unknown strategy"):
        await manager.create_session(
            StsCreateSessionRequest(
                session_id="s1",
                created_by=1,
                strategy="peer::Gone",
                type="peer::Gone",
            )
        )


@pytest.mark.asyncio
async def test_bundled_noop_on_a_bare_node(
    broker: Broker, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    attach_overlay(tmp_path)
    manager = SessionManager(broker, heartbeat_interval=0.05)
    result = await manager.create_session(
        StsCreateSessionRequest(
            session_id="noop-1",
            created_by=1,
            strategy="noop",
            td={"paper": TdAccountRef(api_id=1)},
        )
    )
    assert result.strategy == "noop"
    await manager.close_all()
