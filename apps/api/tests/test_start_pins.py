"""Start pins the registry digest and the env generation (B5-10)."""

from __future__ import annotations

from pathlib import Path

import pytest
from mftik.environment import NodeEnv, PackageRecord
from mftik.registry.qualify import qualify
from mftik.registry.store import RegistryStore
from mftik_api.orchestrate import resolve_start_pins

pytestmark = pytest.mark.component

_TINY = (
    "from mftik.strategy import Strategy\n"
    "class Tiny(Strategy):\n"
    "    pass\n"
)


def test_a_builtin_name_does_not_take_a_registry_digest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    RegistryStore(tmp_path).add({"strategy.py": _TINY})
    digest, generation = resolve_start_pins("NoopStrategy")
    assert digest is None
    assert generation is None


def test_a_qualified_name_pins_the_digest_and_the_stamp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    added = RegistryStore(tmp_path).add({"strategy.py": _TINY})
    env = NodeEnv(tmp_path)
    with env.lock():
        dest = env.begin()
        dest.mkdir(parents=True, exist_ok=True)
        env.commit(dest, {"numpy": PackageRecord("1.0", "numpy", "manual")})
    digest, generation = resolve_start_pins(qualify(added.origin, added.type))
    assert digest == added.digest
    assert generation == env.read_stamp().generation
    assert generation in env.read_pinned_generations()


def test_a_second_pin_does_not_drop_the_first(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    env = NodeEnv(tmp_path)
    env.write_pinned_generations([4])
    with env.lock():
        dest = env.begin()
        env.commit(dest, {})
    _digest, generation = resolve_start_pins("NoopStrategy")
    assert generation == env.read_stamp().generation
    assert 4 in env.read_pinned_generations()
    assert generation in env.read_pinned_generations()
