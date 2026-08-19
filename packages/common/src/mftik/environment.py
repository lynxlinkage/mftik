"""Node extras overlay — stamp, generation directories, apply lock.

The registry copies source. This is the other half of a node's promise:
which third-party import names are actually on disk, under ``MFTIK_DATA/env``.
The stamp is the list. ``site-packages`` listings are not.
"""

from __future__ import annotations

import fcntl
import json
import logging
import os
import shutil
import sys
import sysconfig
from collections.abc import Collection, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from importlib.metadata import PathDistribution
from pathlib import Path
from typing import Any

from mftik.registry.store import DATA_ENV, DEFAULT_DATA_DIR

logger = logging.getLogger(__name__)

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
        dest = self.site_packages(0)
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
        gen = self.root / f"gen-{self._next_generation()}"
        dest = gen / "site-packages"
        dest.mkdir(parents=True)
        return dest

    def _next_generation(self) -> int:
        """One past the highest number the stamp *or* the disk knows about.

        Never derived from the stamp alone: ``commit`` retargets ``current``
        before it writes the stamp, so a crash in between leaves a generation
        on disk that the stamp has not caught up to. Reusing that number would
        install over — or, on a failed apply, delete — the directory ``current``
        is pointing at, which is the live overlay.
        """
        highest = self.read_stamp().generation
        for child in self.root.glob("gen-*"):
            try:
                highest = max(highest, _generation_of(child))
            except ValueError:
                continue
        return highest + 1

    def abort(self, dest: Path) -> None:
        gen = dest.parent
        if not (
            gen.is_dir()
            and gen.parent == self.root
            and gen.name.startswith("gen-")
        ):
            return
        try:
            number = _generation_of(gen)
        except ValueError:
            return
        if number == self.read_stamp().generation:
            # ``commit`` already published this generation. Removing it now
            # would leave the stamp naming a directory that is not there,
            # which is the one state nothing downstream can recover from.
            return
        shutil.rmtree(gen, ignore_errors=True)

    def commit(
        self, dest: Path, packages: Mapping[str, PackageRecord]
    ) -> EnvStamp:
        """Publish ``dest`` as ``current`` and write the stamp atomically."""
        gen = dest.parent
        n = _generation_of(gen)
        previous = self.read_stamp().generation
        # Prune first, then measure: ``bytes`` is what this overlay costs on
        # the volume it shares with the registry, which is every generation
        # still on disk — and not the one this apply is about to drop. Keep
        # the predecessor a live process may still be importing from; aborted
        # applies burn numbers, so "n - 2" is not it.
        self._prune_generations(keep={n, previous})
        stamp = EnvStamp(
            generation=n,
            python=_runtime_python(),
            platform=_runtime_platform(),
            nbytes=_dir_size(self.root),
            packages=dict(packages),
        )
        # The stamp is the only durable publish. STS derives its ``sys.path``
        # entry from ``generation``, so nothing here is visible until this
        # line lands, and a crash before it leaves the node whole on the
        # previous generation rather than half on each.
        self._write_stamp(stamp)
        # ``current`` is for a person reading the volume, and is allowed to
        # lag. It must never fail the apply: this generation is already
        # published, and raising now would send ``__exit__`` off to abort an
        # overlay the stamp is naming.
        try:
            _replace_symlink(self.current_path, _rel_current_target(n))
        except OSError:
            logger.warning(
                "extras published as gen-%d but %s could not be retargeted",
                n,
                self.current_path,
            )
        return stamp

    def _prune_generations(self, *, keep: set[int]) -> None:
        for child in self.root.glob("gen-*"):
            try:
                number = _generation_of(child)
            except ValueError:
                continue
            if number not in keep:
                shutil.rmtree(child, ignore_errors=True)

    def site_packages(self, generation: int) -> Path:
        return self.root / f"gen-{generation}" / "site-packages"

    def overlay_for(self, stamp: EnvStamp) -> Path | None:
        """The directory ``stamp`` names, or ``None`` if it is not on disk.

        The caller derives ``sys.path`` from this rather than from
        ``current``: the stamp is the switch, so the extras a process
        reports and the extras it can import cannot drift apart.
        """
        dest = self.site_packages(stamp.generation)
        return dest if dest.is_dir() else None

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

def unapproved_present(env: NodeEnv, stamp: EnvStamp) -> dict[str, str]:
    """Import name → version for overlay distributions no stamp row claims.

    A resolver installs dependencies. They land in the same directory that
    goes on ``sys.path``, so they are importable — but ``requires`` is
    checked against the stamp, so declaring one is refused. Telling the Owner
    "this node does not have numpy" when numpy is in that directory sends
    them to install something they already have, quite possibly at a
    different pin, which turns a no-op into a re-resolve of everything.

    Best effort by construction: the key here is the distribution name, which
    is the import name for numpy, pandas and scipy and is not for
    scikit-learn. A name this cannot map simply does not appear, and the
    caller says "not installed" rather than guessing.
    """
    approved = {normalize_dist(rec.dist) for rec in stamp.packages.values()}
    live = resolved_dists(env.site_packages(stamp.generation))
    return {
        dist.replace("-", "_"): version
        for dist, version in live.items()
        if dist not in approved and dist.replace("-", "_").isidentifier()
    }


def describe_missing(
    missing: Collection[str], present: Mapping[str, str] | None = None
) -> str:
    """Name each missing extra and say which kind of missing it is.

    The two are not the same problem and do not have the same fix. Absent
    means install it. Present-but-unapproved means approve it — a no-op at
    the version already on disk, which is why the version is in the sentence.
    """
    present = present or {}
    parts: list[str] = []
    for name in sorted(missing):
        version = present.get(name)
        if version:
            parts.append(
                f"{name} (on this node at {version} as a dependency, but not "
                f"an approved extra — approve it to declare it)"
            )
        else:
            parts.append(f"{name} (not on this node)")
    return ", ".join(parts)


def normalize_dist(name: str) -> str:
    """PyPI-normalised distribution name, for comparing what is on disk."""
    return name.replace("_", "-").lower()

def resolved_dists(site_packages: Path) -> dict[str, str]:
    """Every distribution actually in a generation, normalised name → version.

    The stamp records what the Owner asked for. A resolver installs that plus
    whatever it depends on, and those are the ones nobody declared and nobody
    can see: ``pandas`` alone brings numpy, python-dateutil, pytz and tzdata.
    """
    out: dict[str, str] = {}
    for info in site_packages.glob("*.dist-info"):
        try:
            dist = PathDistribution(info)
            name = dist.metadata["Name"]
            if not name:
                continue
            out[normalize_dist(name)] = dist.version or ""
        except (OSError, KeyError, ValueError):
            continue
    return out


def disruptive_dists(
    previous: Mapping[str, str], incoming: Mapping[str, str]
) -> tuple[str, ...]:
    """Distributions the new generation changes or drops.

    Comparing the *requested* names cannot see this. Adding ``scipy`` is not
    a change to any stamped name, but the resolver rebuilds the whole target
    and may hand back a different numpy — and the reload that follows swings
    ``sys.path`` to it while a live session still holds the old one in
    ``sys.modules``. Whether an apply may disturb the interpreter is a
    question about what lands on disk, not about what was typed.
    """
    changed = [
        name
        for name, version in previous.items()
        if incoming.get(name) != version
    ]
    return tuple(sorted(changed))

