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

from mftik.environment import EnvStamp, NodeEnv, PackageRecord

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


def apply_packages(
    env: NodeEnv,
    packages: Mapping[str, ApplySpec],
    *,
    allow_disruptive: bool = False,
    installer: Installer | None = None,
) -> ApplyResult:
    """Replace the overlay with ``packages``. Pins are ``==``."""
    for spec in packages.values():
        spec.requirement()
    current = env.read_stamp()
    changed = _disruptive_names(current, packages)
    if changed and not allow_disruptive:
        raise EnvironmentDisruptive(changed)
    install = installer or run_uv_installer
    with env.lock():
        dest = env.begin()
        try:
            if packages:
                install(dest, packages)
        except Exception:
            env.abort(dest)
            raise
        stamp = env.commit(dest, _records_from_target(dest, packages))
    return ApplyResult(stamp=stamp, restart_required=bool(changed))


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


def _disruptive_names(
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
