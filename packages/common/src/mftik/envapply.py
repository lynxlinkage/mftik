"""Install a target extras set into a new generation directory.

STS never calls this. The API does, then asks STS to reload. The installer
writes ``gen-{N}/site-packages`` only; :meth:`NodeEnv.commit` is what
makes that generation visible.
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from importlib.metadata import PathDistribution
from pathlib import Path
from types import TracebackType
from typing import Self

from mftik.environment import EnvStamp, NodeEnv, PackageRecord
from mftik.registry.gate import PROVIDED_BY_NODE

APPLY_TIMEOUT_S = 600
_UV = os.environ.get("UV", "uv")


class EnvironmentDisruptive(Exception):
    """The target set changes or removes a stamped extra without permission."""

    def __init__(self, names: tuple[str, ...]) -> None:
        self.names = names
        super().__init__(
            "changing or removing "
            + ", ".join(names)
            + " is disruptive — pass allow_disruptive if the caller has checked"
        )


class EnvironmentInvalid(Exception):
    """A package name this node must not install under."""


def check_package_name(name: str) -> None:
    """Refuse an import name the node already answers to.

    ``gate.py`` refuses a *strategy* that declares ``requires = ("json",)``.
    This is the same rule one layer up, where the Owner types the name into
    the API — and it is the layer that matters, because the overlay goes on
    the front of ``sys.path`` for the whole STS process. A distribution
    installed as ``json`` or ``mftik`` would shadow the real module for every
    session, including the ones already running.
    """
    if not name.isidentifier():
        raise EnvironmentInvalid(
            f"{name!r} is not a Python identifier — the key is the import "
            f"name (sklearn); put the PyPI name (scikit-learn) in dist"
        )
    if name in PROVIDED_BY_NODE:
        raise EnvironmentInvalid(
            f"{name!r} is provided by the node — an extra installed under "
            f"that import name would shadow it for every session"
        )


class ApplyFailed(Exception):
    """The installer failed; the previous generation is still current."""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class ApplySpec:
    version: str
    dist: str
    source: str = "manual"

    def requirement(self) -> str:
        if not self.version:
            raise ApplyFailed(f"{self.dist} has no version pin")
        return f"{self.dist}=={self.version}"


@dataclass(frozen=True, slots=True)
class ApplyResult:
    stamp: EnvStamp
    restart_required: bool


Installer = Callable[[Path, Mapping[str, ApplySpec]], None]


class ApplyInProgress:
    """Hold the apply lock from install through a caller-chosen commit.

    The API re-checks live sessions after the installer returns and before
    ``current`` is retargeted. That gap is why this is not just
    :func:`apply_packages`.
    """

    def __init__(
        self,
        env: NodeEnv,
        packages: Mapping[str, ApplySpec],
        *,
        allow_disruptive: bool = False,
        installer: Installer | None = None,
    ) -> None:
        self.env = env
        self.packages = dict(packages)
        self.allow_disruptive = allow_disruptive
        self.installer = installer or run_uv_installer
        self.changed: tuple[str, ...] = ()
        self.dest: Path | None = None
        self._lock_cm: object | None = None
        self._committed = False

    def __enter__(self) -> Self:
        for name, spec in self.packages.items():
            check_package_name(name)
            spec.requirement()
        self.changed = disruptive_names(self.env.read_stamp(), self.packages)
        if self.changed and not self.allow_disruptive:
            raise EnvironmentDisruptive(self.changed)
        lock_cm = self.env.lock()
        lock_cm.__enter__()
        self._lock_cm = lock_cm
        try:
            dest = self.env.begin()
            self.dest = dest
            if self.packages:
                self.installer(dest, self.packages)
        except Exception:
            if self.dest is not None:
                self.env.abort(self.dest)
            lock_cm.__exit__(*sys.exc_info())
            self._lock_cm = None
            raise
        return self

    def commit(self) -> ApplyResult:
        if self.dest is None:
            raise RuntimeError("ApplyInProgress is not active")
        stamp = self.env.commit(
            self.dest, _records_from_target(self.dest, self.packages)
        )
        self._committed = True
        return ApplyResult(stamp=stamp, restart_required=bool(self.changed))

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if not self._committed and self.dest is not None:
            self.env.abort(self.dest)
        lock_cm = self._lock_cm
        if lock_cm is not None:
            lock_cm.__exit__(exc_type, exc, tb)  # type: ignore[union-attr]


def apply_packages(
    env: NodeEnv,
    packages: Mapping[str, ApplySpec],
    *,
    allow_disruptive: bool = False,
    installer: Installer | None = None,
    before_commit: Callable[[], None] | None = None,
) -> ApplyResult:
    """Replace the overlay with ``packages``. Pins are ``==``."""
    with ApplyInProgress(
        env,
        packages,
        allow_disruptive=allow_disruptive,
        installer=installer,
    ) as pending:
        if before_commit is not None:
            before_commit()
        return pending.commit()


def run_uv_installer(dest: Path, packages: Mapping[str, ApplySpec]) -> None:
    """``uv pip install --target dest --only-binary=:all:`` each pin."""
    reqs = [spec.requirement() for spec in packages.values()]
    cmd = [
        _UV,
        "pip",
        "install",
        "--target",
        str(dest),
        "--python",
        sys.executable,
        "--only-binary",
        ":all:",
        *reqs,
    ]
    index = os.environ.get("UV_INDEX_URL", "").strip()
    if index:
        cmd.extend(["--index-url", index])
    try:
        completed = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
            timeout=APPLY_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired as exc:
        raise ApplyFailed(
            f"installer exceeded {APPLY_TIMEOUT_S}s"
        ) from exc
    except OSError as exc:
        raise ApplyFailed(f"installer could not start: {exc}") from exc
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        raise ApplyFailed(detail or f"installer exited {completed.returncode}")


def disruptive_names(
    current: EnvStamp, packages: Mapping[str, ApplySpec]
) -> tuple[str, ...]:
    changed: list[str] = []
    for name, rec in current.packages.items():
        spec = packages.get(name)
        if spec is None or spec.version != rec.version or spec.dist != rec.dist:
            changed.append(name)
    return tuple(changed)


def _records_from_target(
    dest: Path, requested: Mapping[str, ApplySpec]
) -> dict[str, PackageRecord]:
    found: dict[str, PathDistribution] = {}
    for info in dest.glob("*.dist-info"):
        try:
            dist = PathDistribution(info)
            key = _norm_dist(dist.metadata["Name"] or info.name)
        except (OSError, KeyError, ValueError):
            continue
        found[key] = dist
    records: dict[str, PackageRecord] = {}
    for name, spec in requested.items():
        installed = found.get(_norm_dist(spec.dist))
        records[name] = PackageRecord(
            version=installed.version if installed else spec.version,
            dist=spec.dist,
            source=spec.source,
        )
    return records


def _norm_dist(name: str) -> str:
    return name.replace("_", "-").lower()
