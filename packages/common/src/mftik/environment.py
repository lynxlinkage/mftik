"""Node extras overlay — stamp, generation directories, apply lock.

The registry copies source. This is the other half of a node's promise:
which third-party import names are actually on disk, under ``MFTIK_DATA/env``.
The stamp is the list. ``site-packages`` listings are not.
"""

from __future__ import annotations

import fcntl
import json
import os
import shutil
import sys
import sysconfig
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from mftik.registry.store import DATA_ENV, DEFAULT_DATA_DIR

STAMP_NAME = "applied.json"
LOCK_NAME = "apply.lock"
CURRENT_NAME = "current"
ENV_DIRNAME = "env"


class EnvironmentLocked(Exception):
    """Another apply holds ``apply.lock``. The API turns this into 409."""


@dataclass(frozen=True, slots=True)
class PackageRecord:
    version: str
    dist: str
    source: str

    def to_json(self) -> dict[str, str]:
        return {"version": self.version, "dist": self.dist, "source": self.source}

    @classmethod
    def from_json(cls, name: str, raw: object) -> PackageRecord:
        if isinstance(raw, str):
            return cls(version=raw, dist=name, source="manual")
        if not isinstance(raw, dict):
            raise ValueError(f"package {name!r} is not an object")
        version = str(raw.get("version") or "")
        dist = str(raw.get("dist") or name)
        source = str(raw.get("source") or "manual")
        return cls(version=version, dist=dist, source=source)


@dataclass(frozen=True, slots=True)
class EnvStamp:
    generation: int
    python: tuple[int, int]
    platform: str
    nbytes: int
    packages: dict[str, PackageRecord]

    @classmethod
    def empty(cls) -> EnvStamp:
        return cls(
            generation=0,
            python=_runtime_python(),
            platform=_runtime_platform(),
            nbytes=0,
            packages={},
        )

    def names(self) -> frozenset[str]:
        return frozenset(self.packages)

    def matches_runtime(self) -> bool:
        if self.generation == 0:
            return True
        return (
            self.python == _runtime_python()
            and self.platform == _runtime_platform()
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "generation": self.generation,
            "python": list(self.python),
            "platform": self.platform,
            "bytes": self.nbytes,
            "packages": {name: rec.to_json() for name, rec in self.packages.items()},
        }

    @classmethod
    def from_json(cls, raw: object) -> EnvStamp:
        if not isinstance(raw, dict):
            raise ValueError("stamp is not an object")
        py = raw.get("python") or _runtime_python()
        if isinstance(py, list) and len(py) >= 2:
            python = (int(py[0]), int(py[1]))
        else:
            python = _runtime_python()
        packages: dict[str, PackageRecord] = {}
        body = raw.get("packages") or {}
        if isinstance(body, dict):
            for name, item in body.items():
                packages[str(name)] = PackageRecord.from_json(str(name), item)
        return cls(
            generation=int(raw.get("generation") or 0),
            python=python,
            platform=str(raw.get("platform") or _runtime_platform()),
            nbytes=int(raw.get("bytes") or 0),
            packages=packages,
        )


class NodeEnv:
    """``$MFTIK_DATA/env`` — stamp, ``current``, ``gen-{N}``, lock."""

    def __init__(self, data_dir: str | Path) -> None:
        self.data_dir = Path(data_dir)
        self.root = self.data_dir / ENV_DIRNAME
        self.stamp_path = self.root / STAMP_NAME
        self.lock_path = self.root / LOCK_NAME
        self.current_path = self.root / CURRENT_NAME

    @classmethod
    def from_env(cls) -> NodeEnv:
        raw = os.getenv(DATA_ENV, DEFAULT_DATA_DIR).strip() or DEFAULT_DATA_DIR
        return cls(raw)

    def read_stamp(self) -> EnvStamp:
        if not self.stamp_path.is_file():
            return EnvStamp.empty()
        try:
            raw = json.loads(self.stamp_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            return EnvStamp.empty()
        try:
            return EnvStamp.from_json(raw)
        except (TypeError, ValueError):
            return EnvStamp.empty()

    def extras_names(self) -> frozenset[str]:
        stamp = self.read_stamp()
        if not stamp.matches_runtime():
            return frozenset()
        return stamp.names()

    def ensure_current(self) -> Path:
        """Create an empty ``gen-0`` overlay if this node has never applied."""
        self.root.mkdir(parents=True, exist_ok=True)
        if self.current_path.is_symlink() or self.current_path.exists():
            return self.current_path
        dest = self._site_packages(0)
        dest.mkdir(parents=True, exist_ok=True)
        _replace_symlink(self.current_path, _rel_current_target(0))
        return self.current_path

    @contextmanager
    def lock(self) -> Iterator[None]:
        self.root.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise EnvironmentLocked("another apply holds the lock") from exc
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def begin(self) -> Path:
        """Create ``gen-{N+1}/site-packages``. Caller must hold :meth:`lock`."""
        nxt = self.read_stamp().generation + 1
        gen = self.root / f"gen-{nxt}"
        if gen.exists():
            shutil.rmtree(gen)
        dest = gen / "site-packages"
        dest.mkdir(parents=True)
        return dest

    def abort(self, dest: Path) -> None:
        gen = dest.parent
        if gen.is_dir() and gen.parent == self.root and gen.name.startswith("gen-"):
            shutil.rmtree(gen, ignore_errors=True)

    def commit(
        self, dest: Path, packages: Mapping[str, PackageRecord]
    ) -> EnvStamp:
        """Publish ``dest`` as ``current`` and write the stamp atomically."""
        gen = dest.parent
        n = _generation_of(gen)
        rel = _rel_current_target(n)
        _replace_symlink(self.current_path, rel)
        stamp = EnvStamp(
            generation=n,
            python=_runtime_python(),
            platform=_runtime_platform(),
            nbytes=0,
            packages=dict(packages),
        )
        self._write_stamp(stamp)
        measured = EnvStamp(
            generation=stamp.generation,
            python=stamp.python,
            platform=stamp.platform,
            nbytes=_dir_size(self.root),
            packages=stamp.packages,
        )
        self._write_stamp(measured)
        stale = n - 2
        if stale >= 0:
            shutil.rmtree(self.root / f"gen-{stale}", ignore_errors=True)
        return measured

    def _site_packages(self, generation: int) -> Path:
        return self.root / f"gen-{generation}" / "site-packages"

    def _write_stamp(self, stamp: EnvStamp) -> None:
        payload = json.dumps(stamp.to_json(), indent=2, sort_keys=True) + "\n"
        tmp = self.stamp_path.with_name(STAMP_NAME + ".tmp")
        tmp.write_text(payload, encoding="utf-8")
        os.replace(tmp, self.stamp_path)


def _runtime_python() -> tuple[int, int]:
    return (sys.version_info[0], sys.version_info[1])


def _runtime_platform() -> str:
    return sysconfig.get_platform()


def _generation_of(gen_dir: Path) -> int:
    name = gen_dir.name
    if not name.startswith("gen-"):
        raise ValueError(f"not a generation directory: {gen_dir}")
    return int(name[len("gen-") :])


def _rel_current_target(generation: int) -> str:
    return f"gen-{generation}/site-packages"


def _replace_symlink(link: Path, target: str) -> None:
    tmp = link.with_name(link.name + ".tmp")
    if tmp.exists() or tmp.is_symlink():
        tmp.unlink()
    tmp.symlink_to(target)
    os.replace(tmp, link)


def _dir_size(root: Path) -> int:
    total = 0
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [n for n in dirnames if n != "__pycache__"]
        for name in filenames:
            if name.endswith((".tmp", ".lock")):
                continue
            path = Path(dirpath) / name
            if path.is_symlink():
                continue
            try:
                total += path.stat().st_size
            except OSError:
                continue
    return total
