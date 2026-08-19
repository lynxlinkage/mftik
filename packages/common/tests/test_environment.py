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
    rec = _rec("2.2.1")
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
    rec = _rec("1.0")
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


def _rec(version: str) -> PackageRecord:
    return PackageRecord(version=version, dist="numpy", source="manual")


def test_the_stamp_survives_an_abort_of_the_generation_it_names(
    tmp_path: Path,
) -> None:
    """``commit`` publishes by writing the stamp, so abort must not undo it.

    ``ApplyInProgress.__exit__`` aborts whenever it was not told the commit
    succeeded. If the caller raises between a successful ``commit`` and that
    bookkeeping, the abort arrives for a generation the stamp is already
    naming — and deleting it is the one state nothing downstream recovers
    from: every reader is sent to a directory that is not there.
    """
    env = NodeEnv(tmp_path)
    with env.lock():
        dest = env.begin()
        (dest / "numpy").mkdir()
        (dest / "numpy" / "__init__.py").write_text("x = 1\n")
        env.commit(dest, {"numpy": _rec("1.0")})
        env.abort(dest)
    assert dest.is_dir()
    assert env.read_stamp().generation == 1
    assert env.overlay_for(env.read_stamp()) == dest


def test_abort_removes_a_generation_the_stamp_never_named(tmp_path: Path) -> None:
    env = NodeEnv(tmp_path)
    with env.lock():
        dest = env.begin()
        (dest / "half").mkdir()
        env.abort(dest)
    assert not dest.parent.exists()
    assert env.read_stamp().generation == 0


def test_bytes_counts_every_generation_still_on_disk(tmp_path: Path) -> None:
    """``bytes`` is what the overlay costs on the volume, not one generation.

    The retained predecessor is real disk on the same volume as the registry,
    which is the whole reason the number is reported at all. Measuring after
    the prune keeps the generation this apply just dropped out of it.
    """
    env = NodeEnv(tmp_path)
    with env.lock():
        first = env.begin()
        (first / "a.py").write_text("x = 1\n" * 200)
        stamp_one = env.commit(
            first, {"numpy": _rec("1.0")}
        )
    with env.lock():
        second = env.begin()
        (second / "b.py").write_text("y = 2\n" * 200)
        stamp_two = env.commit(
            second, {"numpy": _rec("2.0")}
        )
    assert first.is_dir(), "the predecessor is kept for a live process"
    assert stamp_two.nbytes > stamp_one.nbytes
    assert stamp_two.nbytes == _tree_bytes(env.root)


def test_a_dropped_generation_is_not_counted(tmp_path: Path) -> None:
    env = NodeEnv(tmp_path)
    generations = []
    for n in range(3):
        with env.lock():
            dest = env.begin()
            (dest / "pkg.py").write_text("z = 3\n" * 200)
            stamp = env.commit(
                dest, {"numpy": _rec(f"{n}.0")}
            )
            generations.append(dest.parent)
    assert not generations[0].exists()
    assert generations[1].is_dir() and generations[2].is_dir()
    assert stamp.nbytes == _tree_bytes(env.root)


def _tree_bytes(root: Path) -> int:
    total = 0
    for path in root.rglob("*"):
        if path.is_file() and not path.is_symlink():
            if path.name.endswith((".tmp", ".lock")):
                continue
            if "__pycache__" in path.parts:
                continue
            total += path.stat().st_size
    return total
