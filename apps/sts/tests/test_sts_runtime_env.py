"""STS reads extras from the volume overlay, not by installing them."""

from __future__ import annotations

import asyncio
import logging
import shutil
import sys
from pathlib import Path
from types import SimpleNamespace

import fakeredis.aioredis
import pytest
from mftik.broker import Broker, BrokerConfig
from mftik.envapply import ApplySpec, apply_packages
from mftik.environment import EnvStamp, NodeEnv
from mftik.protocol import (
    STS_REGISTRY_RELOAD,
    StsRegistryReloadRequest,
    StsRegistryReloadRequestEnvelope,
    StsRegistryReloadResult,
    Topics,
)
from mftik.registry import RegistryStore
from mftik_sts.impl import _REGISTRY, resolve_class
from mftik_sts.impl.noop import NoopStrategy
from mftik_sts.rpc import dispatch
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


@pytest.mark.asyncio
async def test_reload_rpc_returns_the_generation_it_now_believes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    apply_packages(
        NodeEnv(tmp_path),
        {"numpy": ApplySpec(version="1.0", dist="numpy")},
        installer=_plant_numpy,
    )
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    broker = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test-envrpc"),
        redis_client=redis,
    )
    await broker.connect()
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
        await broker.close()
        await redis.aclose()


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
