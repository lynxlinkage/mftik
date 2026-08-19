"""Install a target extras set into a new generation directory.

STS never calls this. The API does, then asks STS to reload. The installer
writes ``gen-{N}/site-packages`` only; :meth:`NodeEnv.commit` is what
makes that generation visible.
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Self

from mftik.environment import (
    EnvStamp,
    NodeEnv,
    PackageRecord,
    disruptive_dists,
    normalize_dist,
    resolved_dists,
)
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


class EnvironmentMissing(Exception):
    """A delete named an extra the locked stamp no longer has."""

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(f"no extra named {name!r} on this node")


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
    """One package to install.

    ``version`` is ``None`` when the Owner did not pin it and the resolver
    should choose. That is a property of the *request* only: commit stamps
    whatever came back, so every later apply rebuilds from an exact pin and
    an untouched row cannot drift when something else is added.

    ``None`` and ``""`` are different on purpose. Empty means a version was
    expected and is missing — a peer that published names without pins
    (``envimport``) — and that must fail, not quietly install the latest.
    """

    version: str | None
    dist: str
    source: str = "manual"

    def requirement(self) -> str:
        if self.version is None:
            return self.dist
        if not self.version:
            raise ApplyFailed(f"{self.dist} has no version pin")
        return f"{self.dist}=={self.version}"


@dataclass(frozen=True, slots=True)
class ApplyResult:
    stamp: EnvStamp
    restart_required: bool


Installer = Callable[[Path, Mapping[str, ApplySpec]], None]


def specs_from_stamp(stamp: EnvStamp) -> dict[str, ApplySpec]:
    return {
        name: ApplySpec(version=rec.version, dist=rec.dist, source=rec.source)
        for name, rec in stamp.packages.items()
    }


def merge_packages(
    stamp: EnvStamp,
    *,
    replace: Mapping[str, ApplySpec] | None = None,
    upsert: Mapping[str, ApplySpec] | None = None,
    remove: Collection[str] = (),
    missing_ok: bool = False,
) -> dict[str, ApplySpec]:
    """Build the target set from a stamp. Callers hold the apply lock."""
    if replace is not None:
        return dict(replace)
    packages = specs_from_stamp(stamp)
    if upsert:
        packages.update(dict(upsert))
    for name in remove:
        if name not in packages:
            if missing_ok:
                continue
            raise EnvironmentMissing(name)
        del packages[name]
    return packages


class ApplyInProgress:
    """Hold the apply lock from install through a caller-chosen commit.

    The API re-checks live sessions after the installer returns and before
    ``current`` is retargeted. That gap is why this is not just
    :func:`apply_packages`.

    ``packages`` is a full replace (PUT). ``upsert`` / ``remove`` are
    merged into the stamp *after* the lock is held, so two tabs that
    both read ``{numpy}`` and add different names do not silently drop
    one of the adds.
    """

    def __init__(
        self,
        env: NodeEnv,
        packages: Mapping[str, ApplySpec] | None = None,
        *,
        upsert: Mapping[str, ApplySpec] | None = None,
        remove: Collection[str] | None = None,
        allow_disruptive: bool = False,
        installer: Installer | None = None,
    ) -> None:
        if packages is not None and (upsert is not None or remove):
            raise TypeError("replace cannot be combined with upsert/remove")
        if packages is None and upsert is None and not remove:
            raise TypeError("ApplyInProgress needs replace, upsert, or remove")
        self.env = env
        self._replace = dict(packages) if packages is not None else None
        self._upsert = dict(upsert) if upsert is not None else None
        self._remove = frozenset(remove or ())
        self.packages: dict[str, ApplySpec] = {}
        self.allow_disruptive = allow_disruptive
        self.installer = installer or run_uv_installer
        self.changed: tuple[str, ...] = ()
        self.dest: Path | None = None
        self._lock_cm: object | None = None
        self._committed = False
        #: Distributions the resolver changed or dropped, including ones
        #: nobody named. Only knowable after the installer has run.
        self.resolved_changed: tuple[str, ...] = ()

    def _intent_specs(self) -> Mapping[str, ApplySpec]:
        if self._replace is not None:
            return self._replace
        return self._upsert or {}

    @property
    def disruptive(self) -> tuple[str, ...]:
        """Everything this apply would move under a running interpreter.

        Declared names the Owner is changing, plus distributions the resolver
        changed underneath them. The caller asks live sessions about this,
        not about ``changed`` alone.
        """
        return tuple(sorted(set(self.changed) | set(self.resolved_changed)))

    def __enter__(self) -> Self:
        for name, spec in self._intent_specs().items():
            check_package_name(name)
            spec.requirement()
        lock_cm = self.env.lock()
        lock_cm.__enter__()
        self._lock_cm = lock_cm
        try:
            stamp = self.env.read_stamp()
            self.packages = merge_packages(
                stamp,
                replace=self._replace,
                upsert=self._upsert,
                remove=self._remove,
            )
            self.changed = disruptive_names(stamp, self.packages)
            if self.changed and not self.allow_disruptive:
                raise EnvironmentDisruptive(self.changed)
            previous = resolved_dists(self.env.site_packages(stamp.generation))
            dest = self.env.begin()
            self.dest = dest
            if self.packages:
                self.installer(dest, self.packages)
            # After the installer, because until it runs nothing knows what
            # the resolver chose. Not a refusal here: the generation is not
            # published yet, so the caller can still ask about live sessions
            # and abort a directory that cost nothing to throw away.
            self.resolved_changed = disruptive_dists(
                previous, resolved_dists(dest)
            )
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
        return ApplyResult(
            stamp=stamp, restart_required=bool(self.disruptive)
        )

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
        raise ApplyFailed(
            (detail or f"installer exited {completed.returncode}")
            + _dist_hint(packages)
        )


def _dist_hint(packages: Mapping[str, ApplySpec]) -> str:
    """Point at the import-name/PyPI-name split when a resolve fails.

    ``sklearn`` is on PyPI — as a deprecation shim with no wheels — so asking
    for it gets "no usable wheels", which is true and unhelpful. The package
    wanted is ``scikit-learn``.

    A mapping of the known ones would be the catalog this epic refuses to
    keep. This says what the mechanism is and lets the Owner apply it: only
    for names sent without a distribution of their own, and only after a
    failure that has already left the stamp untouched.
    """
    same = sorted(name for name, spec in packages.items() if spec.dist == name)
    if not same:
        return ""
    return (
        "\n\nAsked the index for: "
        + ", ".join(same)
        + ". A package's PyPI name is often not its import name — sklearn is "
        "published as scikit-learn. Pass the distribution explicitly if that "
        "is what happened, e.g. --dist scikit-learn."
    )


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
    """What the stamp records: the requested names, at installed versions.

    Only the requested ones. Whatever else the resolver dragged in is on
    disk and importable, but it is not an approved extra — see
    ``unapproved_present``, which is how the Owner finds out.
    """
    found = resolved_dists(dest)
    records: dict[str, PackageRecord] = {}
    for name, spec in requested.items():
        version = found.get(normalize_dist(spec.dist)) or spec.version
        if not version:
            # An unpinned request whose installer reported nothing. The stamp
            # must not carry a row that cannot be reinstalled: the next apply
            # rebuilds the whole set from it.
            raise ApplyFailed(
                f"installer did not report a version for {spec.dist!r}"
            )
        records[name] = PackageRecord(
            version=version, dist=spec.dist, source=spec.source
        )
    return records

