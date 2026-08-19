"""Stamp, generation directories, and the apply lock — no installer."""

from __future__ import annotations

from pathlib import Path

import pytest
from mftik.environment import (
    EnvironmentLocked,
    EnvStamp,
    NodeEnv,
    PackageRecord,
)


def test_missing_stamp_is_empty(tmp_path: Path) -> None:
    env = NodeEnv(tmp_path)
    stamp = env.read_stamp()
    assert stamp.generation == 0
    assert stamp.packages == {}
    assert env.extras_names() == frozenset()


def test_round_trip_preserves_fields(tmp_path: Path) -> None:
    env = NodeEnv(tmp_path)
    rec = PackageRecord(version="2.2.1", dist="numpy", source="manual")
    with env.lock():
        dest = env.begin()
        (dest / "numpy").mkdir()
        (dest / "numpy" / "__init__.py").write_text("x = 1\n")
        stamp = env.commit(dest, {"numpy": rec})
    assert stamp.generation == 1
    assert stamp.packages["numpy"] == rec
    loaded = env.read_stamp()
    assert loaded.generation == 1
    assert loaded.packages["numpy"].version == "2.2.1"
    assert loaded.packages["numpy"].source == "manual"
    assert loaded.python == stamp.python
    assert loaded.platform == stamp.platform
    assert loaded.nbytes == stamp.nbytes
    assert stamp.nbytes > 0


def test_commit_is_atomic_for_readers(tmp_path: Path) -> None:
    env = NodeEnv(tmp_path)
    rec = PackageRecord(version="1.0", dist="foo", source="manual")
    with env.lock():
        dest = env.begin()
        (dest / "foo.py").write_text("n = 1\n")
        env.commit(dest, {"foo": rec})
    stamp = env.read_stamp()
    target = (tmp_path / "env" / "current").resolve()
    assert target == (tmp_path / "env" / "gen-1" / "site-packages").resolve()
    assert stamp.generation == 1
    assert target.is_dir()


def test_abort_leaves_the_previous_generation(tmp_path: Path) -> None:
    env = NodeEnv(tmp_path)
    rec = PackageRecord(version="1.0", dist="foo", source="manual")
    with env.lock():
        first = env.begin()
        (first / "foo.py").write_text("n = 1\n")
        env.commit(first, {"foo": rec})
    with env.lock():
        second = env.begin()
        (second / "half.py").write_text("broken\n")
        env.abort(second)
    assert env.read_stamp().generation == 1
    assert (tmp_path / "env" / "current").resolve() == (
        tmp_path / "env" / "gen-1" / "site-packages"
    ).resolve()
    assert not (tmp_path / "env" / "gen-2").exists()


def test_stale_generation_is_not_reported_as_extras(tmp_path: Path) -> None:
    env = NodeEnv(tmp_path)
    leftover = tmp_path / "env" / "gen-9" / "site-packages" / "torch"
    leftover.mkdir(parents=True)
    (leftover / "__init__.py").write_text("x = 1\n")
    assert "torch" not in env.extras_names()
    assert env.read_stamp().generation == 0


def test_commit_drops_the_generation_before_previous(tmp_path: Path) -> None:
    env = NodeEnv(tmp_path)
    rec = PackageRecord(version="1.0", dist="foo", source="manual")
    for _ in range(3):
        with env.lock():
            dest = env.begin()
            (dest / "foo.py").write_text("n = 1\n")
            env.commit(dest, {"foo": rec})
    assert env.read_stamp().generation == 3
    assert (tmp_path / "env" / "gen-3").is_dir()
    assert (tmp_path / "env" / "gen-2").is_dir()
    assert not (tmp_path / "env" / "gen-1").exists()


def test_second_lock_raises(tmp_path: Path) -> None:
    env = NodeEnv(tmp_path)
    with env.lock():
        held = NodeEnv(tmp_path)
        with pytest.raises(EnvironmentLocked):
            with held.lock():
                pass


def test_ensure_current_creates_empty_gen0(tmp_path: Path) -> None:
    env = NodeEnv(tmp_path)
    current = env.ensure_current()
    assert current.is_symlink()
    assert (tmp_path / "env" / "gen-0" / "site-packages").is_dir()
    assert env.read_stamp().generation == 0


def test_mismatched_python_hides_extras(tmp_path: Path) -> None:
    env = NodeEnv(tmp_path)
    rec = PackageRecord(version="1.0", dist="numpy", source="manual")
    with env.lock():
        dest = env.begin()
        (dest / "numpy.py").write_text("x = 1\n")
        committed = env.commit(dest, {"numpy": rec})
    env._write_stamp(
        EnvStamp(
            generation=1,
            python=(3, 11),
            platform=committed.platform,
            nbytes=committed.nbytes,
            packages={"numpy": rec},
        )
    )
    assert env.extras_names() == frozenset()
    assert not env.read_stamp().matches_runtime()
