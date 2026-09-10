"""STS reads extras from the volume overlay, not by installing them."""

from __future__ import annotations

import asyncio
import logging
import shutil
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.envapply import ApplySpec, apply_packages
from mftik.environment import EnvStamp, NodeEnv
from mftik.protocol import (
    STS_ENV_SYNC,
    STS_REGISTRY_GENERATION,
    STS_REGISTRY_RELOAD,
    StsEnvPackagePin,
    StsEnvSyncRequest,
    StsEnvSyncRequestEnvelope,
    StsEnvSyncResult,
    StsRegistryGenerationRequest,
    StsRegistryGenerationRequestEnvelope,
    StsRegistryGenerationResult,
    StsRegistryReloadRequest,
    StsRegistryReloadRequestEnvelope,
    StsRegistryReloadResult,
    Topics,
)
from mftik.registry import RegistryStore
from mftik_sts.impl import _REGISTRY, resolve_class
from mftik_sts.impl.noop import NoopStrategy
from mftik_sts.rpc import dispatch
from mftik_sts.rpc.env import apply_requested, local_matches
from mftik_sts.runtime_env import (
    _ABI_MISMATCH,
    attach_overlay,
    extras_names,
    refresh,
    reset_for_tests,
)

_NUMPY_STRAT = """\
from mftik.strategy import Strategy
import numpy

class UsesNumpy(Strategy):
    name = "uses_numpy"
    requires = ("numpy",)
"""


@pytest.fixture(autouse=True)
def _isolate_overlay() -> None:
    before = dict(_REGISTRY)
    reset_for_tests()
    yield
    reset_for_tests()
    _REGISTRY.clear()
    _REGISTRY.update(before)


def _plant_numpy(dest: Path, packages: dict[str, ApplySpec]) -> None:
    pkg = dest / "numpy"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("version = 'stub'\n")


def _plant_exploding_numpy(dest: Path, packages: dict[str, ApplySpec]) -> None:
    pkg = dest / "numpy"
    pkg.mkdir()
    (pkg / "__init__.py").write_text(
        "raise RuntimeError('should not import overlay')\n"
    )


def test_boot_on_a_bare_volume_inserts_the_stamped_generation(
    tmp_path: Path,
) -> None:
    env = NodeEnv(tmp_path)
    loaded, stamp = refresh(data_dir=tmp_path)
    assert stamp.generation == 0
    # ``current`` is still maintained for a person reading the volume, but
    # what goes on ``sys.path`` is the generation the stamp names — so the
    # extras this process reports and the ones it can import cannot drift.
    assert env.current_path.is_symlink()
    assert str(env.site_packages(0)) in sys.path
    assert str(env.current_path) not in sys.path
    assert extras_names() == frozenset()
    assert loaded == []
    assert resolve_class("noop") is NoopStrategy


def test_boot_with_a_planted_overlay_loads_a_numpy_tree(tmp_path: Path) -> None:
    env = NodeEnv(tmp_path)
    apply_packages(
        env,
        {"numpy": ApplySpec(version="1.0", dist="numpy")},
        installer=_plant_numpy,
    )
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _NUMPY_STRAT})
    loaded, stamp = refresh(store, tmp_path)
    assert stamp.generation == 1
    assert extras_names() == frozenset({"numpy"})
    assert "private::UsesNumpy" in loaded
    assert resolve_class("private::UsesNumpy").name == "uses_numpy"


def test_mismatched_abi_does_not_import_the_overlay(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    env = NodeEnv(tmp_path)
    apply_packages(
        env,
        {"numpy": ApplySpec(version="1.0", dist="numpy")},
        installer=_plant_exploding_numpy,
    )
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
    with caplog.at_level(logging.WARNING):
        attach_overlay(tmp_path)
    assert _ABI_MISMATCH in caplog.text
    assert extras_names() == frozenset()
    assert str(env.current_path) not in sys.path
    with pytest.raises(ModuleNotFoundError):
        import numpy  # noqa: F401


def test_reload_moves_the_in_memory_stamp_and_loads_the_tree(
    tmp_path: Path,
) -> None:
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _NUMPY_STRAT})
    loaded, stamp = refresh(store, tmp_path)
    assert stamp.generation == 0
    assert extras_names() == frozenset()
    assert loaded == []
    with pytest.raises(KeyError):
        resolve_class("private::UsesNumpy")

    apply_packages(
        NodeEnv(tmp_path),
        {"numpy": ApplySpec(version="1.0", dist="numpy")},
        installer=_plant_numpy,
    )
    assert extras_names() == frozenset()
    with pytest.raises(KeyError):
        resolve_class("private::UsesNumpy")

    loaded, stamp = refresh(store, tmp_path)
    assert stamp.generation == 1
    assert extras_names() == frozenset({"numpy"})
    assert "private::UsesNumpy" in loaded
    assert resolve_class("private::UsesNumpy").name == "uses_numpy"


def test_sync_no_ops_when_the_stamp_already_matches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    env = NodeEnv(tmp_path)
    apply_packages(
        env,
        {"numpy": ApplySpec(version="1.0", dist="numpy")},
        installer=_plant_numpy,
    )
    pins = {"numpy": StsEnvPackagePin(version="1.0", dist="numpy")}
    assert local_matches(env, pins) is True
    calls: list[str] = []

    def record(dest: Path, packages: dict[str, ApplySpec]) -> None:
        calls.append("ran")
        _plant_numpy(dest, packages)

    import mftik_sts.rpc.env as env_rpc

    env_rpc.installer_for_sync = record
    try:
        apply_requested(
            StsEnvSyncRequest(generation=1, packages=pins, allow_disruptive=True)
        )
    finally:
        env_rpc.installer_for_sync = None
    assert calls == []
    assert env.read_stamp().generation == 1


def test_sync_installs_when_the_overlay_is_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    calls: list[str] = []

    def record(dest: Path, packages: dict[str, ApplySpec]) -> None:
        calls.append("ran")
        _plant_numpy(dest, packages)

    import mftik_sts.rpc.env as env_rpc

    env_rpc.installer_for_sync = record
    try:
        apply_requested(
            StsEnvSyncRequest(
                generation=3,
                packages={"numpy": StsEnvPackagePin(version="1.0", dist="numpy")},
                allow_disruptive=True,
            )
        )
    finally:
        env_rpc.installer_for_sync = None
    assert calls == ["ran"]
    stamp = NodeEnv(tmp_path).read_stamp()
    assert stamp.generation == 3
    assert stamp.packages["numpy"].version == "1.0"
    assert (tmp_path / "env" / "gen-3" / "site-packages" / "numpy").is_dir()


def test_sync_reinstalls_when_abi_mismatches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    env = NodeEnv(tmp_path)
    apply_packages(
        env,
        {"numpy": ApplySpec(version="1.0", dist="numpy")},
        installer=_plant_numpy,
    )
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
    pins = {"numpy": StsEnvPackagePin(version="1.0", dist="numpy")}
    assert local_matches(env, pins) is False
    calls: list[str] = []

    def record(dest: Path, packages: dict[str, ApplySpec]) -> None:
        calls.append("ran")
        _plant_numpy(dest, packages)

    import mftik_sts.rpc.env as env_rpc

    env_rpc.installer_for_sync = record
    try:
        apply_requested(
            StsEnvSyncRequest(generation=2, packages=pins, allow_disruptive=True)
        )
    finally:
        env_rpc.installer_for_sync = None
    assert calls == ["ran"]
    assert env.read_stamp().matches_runtime()


def test_amain_and_refresh_never_call_the_installer() -> None:
    import mftik_sts.app as app
    import mftik_sts.runtime_env as runtime_env

    for module in (app, runtime_env):
        text = Path(module.__file__).read_text(encoding="utf-8")
        assert "envapply" not in text
        assert "apply_packages" not in text
        assert "run_uv_installer" not in text


def test_refresh_does_not_invoke_uv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        "mftik.envapply.apply_packages",
        lambda *a, **k: calls.append("apply"),
    )
    monkeypatch.setattr(
        "mftik.envapply.run_uv_installer",
        lambda *a, **k: calls.append("uv"),
    )
    refresh(data_dir=tmp_path)
    assert calls == []


@pytest.fixture
async def broker() -> Broker:
    async with a_broker("test-envrpc") as client:
        yield client


@pytest.mark.asyncio
async def test_reload_rpc_returns_the_generation_it_now_believes(
    broker: Broker, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    apply_packages(
        NodeEnv(tmp_path),
        {"numpy": ApplySpec(version="1.0", dist="numpy")},
        installer=_plant_numpy,
    )
    stop = asyncio.Event()

    async def serve() -> None:
        async for req in broker.serve(Topics.STS, stop=stop):
            await dispatch(req, sessions=SimpleNamespace())

    task = asyncio.create_task(serve())
    try:
        reply = await broker.request(
            Topics.STS,
            StsRegistryReloadRequestEnvelope.wrap(
                StsRegistryReloadRequest(),
                type=STS_REGISTRY_RELOAD,
                source="test",
            ),
            timeout=5.0,
        )
        result = StsRegistryReloadResult.model_validate(reply.payload)
        assert result.generation == 1
        assert extras_names() == frozenset({"numpy"})
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_sync_rpc_installs_then_returns_the_generation(
    broker: Broker, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    import mftik_sts.rpc.env as env_rpc

    env_rpc.installer_for_sync = _plant_numpy
    stop = asyncio.Event()

    async def serve() -> None:
        async for req in broker.serve(Topics.STS, stop=stop):
            await dispatch(req, sessions=SimpleNamespace())

    task = asyncio.create_task(serve())
    try:
        reply = await broker.request(
            Topics.STS,
            StsEnvSyncRequestEnvelope.wrap(
                StsEnvSyncRequest(
                    generation=4,
                    packages={
                        "numpy": StsEnvPackagePin(version="1.0", dist="numpy")
                    },
                    allow_disruptive=True,
                ),
                type=STS_ENV_SYNC,
                source="test",
            ),
            timeout=5.0,
        )
        result = StsEnvSyncResult.model_validate(reply.payload)
        assert result.generation == 4
        assert result.packages["numpy"].version == "1.0"
        assert extras_names() == frozenset({"numpy"})
    finally:
        env_rpc.installer_for_sync = None
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_generation_rpc_is_read_only(
    broker: Broker, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    apply_packages(
        NodeEnv(tmp_path),
        {"numpy": ApplySpec(version="1.0", dist="numpy")},
        installer=_plant_numpy,
    )
    refresh_calls: list[str] = []
    real_refresh = refresh

    def wrapped_refresh(*args: object, **kwargs: object):  # noqa: ANN001
        refresh_calls.append("refresh")
        return real_refresh(*args, **kwargs)

    monkeypatch.setattr("mftik_sts.rpc.registry.refresh", wrapped_refresh)
    stop = asyncio.Event()

    async def serve() -> None:
        async for req in broker.serve(Topics.STS, stop=stop):
            await dispatch(req, sessions=SimpleNamespace())

    task = asyncio.create_task(serve())
    try:
        unread = await broker.request(
            Topics.STS,
            StsRegistryGenerationRequestEnvelope.wrap(
                StsRegistryGenerationRequest(),
                type=STS_REGISTRY_GENERATION,
                source="test",
            ),
            timeout=5.0,
        )
        unread_gen = StsRegistryGenerationResult.model_validate(
            unread.payload
        ).generation
        assert unread_gen == 0
        assert refresh_calls == []

        real_refresh(data_dir=tmp_path)
        adopted = await broker.request(
            Topics.STS,
            StsRegistryGenerationRequestEnvelope.wrap(
                StsRegistryGenerationRequest(),
                type=STS_REGISTRY_GENERATION,
                source="test",
            ),
            timeout=5.0,
        )
        adopted_gen = StsRegistryGenerationResult.model_validate(
            adopted.payload
        ).generation
        assert adopted_gen == 1
        assert refresh_calls == []

        apply_packages(
            NodeEnv(tmp_path),
            {
                "numpy": ApplySpec(version="1.0", dist="numpy"),
                "sklearn": ApplySpec(version="1.4", dist="scikit-learn"),
            },
            installer=_plant_numpy,
        )
        trailing = await broker.request(
            Topics.STS,
            StsRegistryGenerationRequestEnvelope.wrap(
                StsRegistryGenerationRequest(),
                type=STS_REGISTRY_GENERATION,
                source="test",
            ),
            timeout=5.0,
        )
        assert (
            StsRegistryGenerationResult.model_validate(trailing.payload).generation == 1
        )
        assert refresh_calls == []
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def test_the_stamp_not_the_symlink_decides_sys_path(tmp_path: Path) -> None:
    """``commit`` writes the stamp and retargets ``current`` as two steps.

    Between them the node is describable two ways, and the dangerous half is
    a removal: a process that read ``current`` would have the old overlay on
    ``sys.path`` while the stamp said the package was gone — or, with the
    steps the other way round, report a package it could no longer import,
    pass ``ensure_deployable``, and die on ``ModuleNotFoundError``. That is
    the failure this module exists to replace, so the path follows the stamp
    and a stale symlink changes nothing.
    """
    env = NodeEnv(tmp_path)
    apply_packages(
        env,
        {"numpy": ApplySpec(version="1.0", dist="numpy")},
        installer=_plant_numpy,
    )
    apply_packages(
        env,
        {},
        allow_disruptive=True,
        installer=_plant_numpy,
    )
    stamp_now = env.read_stamp()
    assert stamp_now.generation == 2
    assert stamp_now.packages == {}
    # Crash after the stamp, before the retarget: ``current`` still names the
    # generation that has numpy in it.
    env.current_path.unlink()
    env.current_path.symlink_to("gen-1/site-packages")

    stamp = attach_overlay(tmp_path)
    assert stamp.generation == 2
    assert extras_names() == frozenset()
    assert str(env.site_packages(2)) in sys.path
    assert str(env.site_packages(1)) not in sys.path


def test_a_stamp_naming_a_missing_generation_has_no_extras(
    tmp_path: Path,
) -> None:
    env = NodeEnv(tmp_path)
    apply_packages(
        env,
        {"numpy": ApplySpec(version="1.0", dist="numpy")},
        installer=_plant_exploding_numpy,
    )
    shutil.rmtree(env.site_packages(1).parent)

    stamp = attach_overlay(tmp_path)
    assert stamp.generation == 1, "the stamp is still what it says it is"
    assert extras_names() == frozenset(), "but this process has none of it"
    assert str(env.site_packages(1)) not in sys.path
