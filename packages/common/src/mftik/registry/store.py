"""On-disk registry — ``public/`` served, ``private/`` not, ``pulled/`` copied in."""

from __future__ import annotations

import json
import os
import re
import shutil
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from mftik.registry.digest import digest_files
from mftik.registry.errors import RegistryConflict, RegistryError
from mftik.registry.files import normalize_files
from mftik.registry.gate import StrategyClass, check_files
from mftik.registry.qualify import (
    OWN_ORIGINS,
    PRIVATE_ORIGIN,
    PUBLIC_ORIGIN,
    RESERVED_REMOTE_NAMES,
)

DATA_ENV = "MFT_DATA"
DEFAULT_DATA_DIR = ".mft"
_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
_TYPE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DEFAULT_REQUIRES = "0.1.0"


@dataclass(frozen=True, slots=True)
class AddedStrategy:
    """What ``add`` wrote. The digest is of the ``.py`` files."""

    name: str
    type: str
    digest: str
    requires_mft: str
    files: tuple[str, ...]
    path: str
    origin: str = PRIVATE_ORIGIN


@dataclass(frozen=True, slots=True)
class Remote:
    name: str
    url: str
    #: The registry key this peer issued us, if it asks for one. A peer that
    #: publishes openly needs none, so this stays optional — but once a peer
    #: locks its source dump, pulling from it without this is a 401.
    token: str | None = None


class RegistryStore:
    """Published ``public/``, local-only ``private/``, and ``pulled/`` copies."""

    def __init__(self, data_dir: str | Path) -> None:
        self.data_dir = Path(data_dir)
        self.registry_dir = self.data_dir / "registry"
        self.public_dir = self.registry_dir / PUBLIC_ORIGIN
        self.private_dir = self.registry_dir / PRIVATE_ORIGIN
        self.pulled_dir = self.registry_dir / "pulled"
        self.remotes_path = self.registry_dir / "remotes.toml"
        #: ``(resolved dest, origin)`` → ``(stamp, record)``. Stamp is
        #: ``(py count, max mtime_ns)`` so an in-place edit invalidates.
        self._tree_cache: dict[
            tuple[str, str], tuple[tuple[int, int], AddedStrategy | None]
        ] = {}

    @classmethod
    def from_env(cls) -> RegistryStore:
        raw = os.getenv(DATA_ENV, DEFAULT_DATA_DIR).strip() or DEFAULT_DATA_DIR
        return cls(raw)

    def add(
        self,
        files: Mapping[str, str | bytes],
        *,
        replace: bool = False,
        origin: str = PRIVATE_ORIGIN,
    ) -> AddedStrategy:
        """Validate, hash, and copy ``.py`` files into public, private, or pulled."""
        _check_origin(origin)
        normalised = normalize_files(files)
        classes = check_files(normalised)
        chosen = _pick_class(classes)
        if not chosen.name:
            raise RegistryError(
                f"{chosen.type} has no name — set class attribute name = \"...\""
            )
        _check_name(chosen.name)
        _check_type(chosen.type)
        requires = chosen.requires_mft or _DEFAULT_REQUIRES
        digest = digest_files(normalised)
        dest = self._dest(origin, chosen.name)
        if dest.exists() and not replace:
            raise RegistryConflict(
                f"strategy {chosen.name!r} is already in the {_where(origin)}"
            )
        self._commit(dest, normalised)
        return AddedStrategy(
            name=chosen.name,
            type=chosen.type,
            digest=digest,
            requires_mft=requires,
            files=tuple(sorted(normalised)),
            path=str(dest),
            origin=origin,
        )

    def list_public(self) -> list[AddedStrategy]:
        """Trees this node publishes. Peers pull from here."""
        return self._list_dir(self.public_dir, origin=PUBLIC_ORIGIN)

    def list_private(self) -> list[AddedStrategy]:
        """Trees that stay on this node."""
        return self._list_dir(self.private_dir, origin=PRIVATE_ORIGIN)

    def list_pulled(self) -> list[AddedStrategy]:
        """Copies under ``pulled/{remote}/{name}/``. Unknown remotes still load."""
        if not self.pulled_dir.is_dir():
            return []
        out: list[AddedStrategy] = []
        for remote_dir in sorted(self.pulled_dir.iterdir()):
            if not remote_dir.is_dir() or remote_dir.name.startswith("."):
                continue
            try:
                _check_remote_name(remote_dir.name)
            except RegistryError:
                continue
            out.extend(self._list_dir(remote_dir, origin=remote_dir.name))
        return out

    def list_all(self) -> list[AddedStrategy]:
        return self.list_public() + self.list_private() + self.list_pulled()

    def get_public(self, name: str) -> AddedStrategy | None:
        return self._get_own(name, origin=PUBLIC_ORIGIN)

    def get_private(self, name: str) -> AddedStrategy | None:
        return self._get_own(name, origin=PRIVATE_ORIGIN)

    def _get_own(self, name: str, *, origin: str) -> AddedStrategy | None:
        dest = self._dest(origin, name)
        if not dest.is_dir():
            return None
        return self._read_tree(dest, origin=origin)

    def read_contents(self, rec: AddedStrategy) -> dict[str, str]:
        """Source files for a tree."""
        dest = Path(rec.path)
        out: dict[str, str] = {}
        for rel in rec.files:
            body = (dest / rel).read_bytes()
            try:
                out[rel] = body.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise RegistryError(f"{rel} is not UTF-8: {exc}") from exc
        return out

    def get_remote(self, name: str) -> Remote | None:
        return _load_remotes(self.remotes_path).get(name)

    def list_pulled_from(self, origin: str) -> list[AddedStrategy]:
        return [rec for rec in self.list_pulled() if rec.origin == origin]

    def list_remotes(self) -> list[Remote]:
        remotes = _load_remotes(self.remotes_path)
        return [remotes[name] for name in sorted(remotes)]

    def put_remote(self, name: str, url: str, token: str | None = None) -> Remote:
        _check_remote_name(name)
        remotes = _load_remotes(self.remotes_path)
        remote = Remote(name=name, url=url, token=token or None)
        remotes[name] = remote
        self.registry_dir.mkdir(parents=True, exist_ok=True)
        self._write_remotes(remotes)
        return remote

    def _write_remotes(self, remotes: Mapping[str, Remote]) -> None:
        """Write the file, then narrow it. It holds peers' bearer tokens.

        Order matters: created first with whatever the umask allows and
        chmod'd after, there is a window where it is world-readable. Opening
        it at 0600 and writing into that handle has no such window.
        """
        path = self.remotes_path
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(_dump_remotes(remotes))
        # An existing file keeps its old mode through O_CREAT, so say it again.
        os.chmod(path, 0o600)

    def drop_remote(self, name: str) -> Remote:
        """Forget a peer: drop the URL and the pulled copy."""
        _check_remote_name(name)
        remotes = _load_remotes(self.remotes_path)
        remote = remotes.pop(name, None)
        if remote is None:
            raise RegistryError(f"unknown remote: {name}")
        if remotes:
            self._write_remotes(remotes)
        elif self.remotes_path.is_file():
            self.remotes_path.unlink()
        pulled = self.pulled_dir / name
        if pulled.is_dir():
            shutil.rmtree(pulled)
        self._tree_cache = {
            key: val for key, val in self._tree_cache.items() if key[1] != name
        }
        return remote

    def _dest(self, origin: str, name: str) -> Path:
        if origin == PUBLIC_ORIGIN:
            return self.public_dir / name
        if origin == PRIVATE_ORIGIN:
            return self.private_dir / name
        return self.pulled_dir / origin / name

    def _list_dir(self, root: Path, *, origin: str) -> list[AddedStrategy]:
        if not root.is_dir():
            return []
        out: list[AddedStrategy] = []
        for dest in sorted(root.iterdir()):
            if not dest.is_dir() or dest.name.startswith("."):
                continue
            rec = self._read_tree(dest, origin=origin)
            if rec is not None:
                out.append(rec)
        return out

    def _commit(self, dest: Path, files: Mapping[str, bytes]) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.parent / f".tmp-{dest.name}-{os.getpid()}"
        if tmp.exists():
            shutil.rmtree(tmp)
        tmp.mkdir()
        try:
            for path, body in files.items():
                target = tmp / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(body)
            if dest.exists():
                shutil.rmtree(dest)
            tmp.rename(dest)
        except Exception:
            if tmp.exists():
                shutil.rmtree(tmp, ignore_errors=True)
            raise


    def _read_tree(self, dest: Path, *, origin: str) -> AddedStrategy | None:
        """Scan a tree. Directory name is identity; class ``name`` must match."""
        try:
            resolved = str(dest.resolve())
        except OSError:
            return None
        key = (resolved, origin)
        stamp = _tree_stamp(dest)
        if stamp is not None:
            cached = self._tree_cache.get(key)
            if cached is not None and cached[0] == stamp:
                return cached[1]
        rec = _scan_tree(dest, origin=origin)
        stamp = _tree_stamp(dest)
        if stamp is not None:
            self._tree_cache[key] = (stamp, rec)
        return rec


def _tree_stamp(dest: Path) -> tuple[int, int] | None:
    """``.py`` count and newest mtime, plus the directory itself."""
    try:
        newest = dest.stat().st_mtime_ns
        n_py = 0
        for py in dest.rglob("*.py"):
            if "__pycache__" in py.parts:
                continue
            n_py += 1
            newest = max(newest, py.stat().st_mtime_ns)
    except OSError:
        return None
    return (n_py, newest)


def _scan_tree(dest: Path, *, origin: str) -> AddedStrategy | None:
    files: dict[str, bytes] = {}
    try:
        for py in dest.rglob("*.py"):
            if "__pycache__" in py.parts:
                continue
            files[py.relative_to(dest).as_posix()] = py.read_bytes()
    except OSError:
        return None
    if not files:
        return None
    try:
        normalised = normalize_files(files)
        chosen = _pick_class(check_files(normalised))
        if chosen.name != dest.name:
            return None
        digest = digest_files(normalised)
    except RegistryError:
        return None
    return AddedStrategy(
        name=dest.name,
        type=chosen.type,
        digest=digest,
        requires_mft=chosen.requires_mft or _DEFAULT_REQUIRES,
        files=tuple(sorted(normalised)),
        path=str(dest),
        origin=origin,
    )


def _pick_class(classes: list[StrategyClass]) -> StrategyClass:
    if not classes:
        raise RegistryError(
            "no Strategy subclass found — import Strategy from "
            "mftik_sts.strategy and subclass it"
        )
    if len(classes) > 1:
        known = ", ".join(sorted(c.type for c in classes))
        raise RegistryError(
            f"multiple Strategy subclasses ({known}) — one tree, one subclass"
        )
    return classes[0]


def _check_name(name: str) -> None:
    if not _NAME.match(name):
        raise RegistryError(
            f"strategy name {name!r} must be lowercase "
            f"[a-z][a-z0-9_]* (e.g. macd_dollar)"
        )


def _check_type(type_name: str) -> None:
    if not _TYPE.match(type_name):
        raise RegistryError(
            f"strategy type {type_name!r} must be a Python identifier"
        )


def _check_origin(origin: str) -> None:
    _check_name(origin)
    if origin in OWN_ORIGINS:
        return
    _check_remote_name(origin)


def _check_remote_name(name: str) -> None:
    _check_name(name)
    if name in RESERVED_REMOTE_NAMES:
        raise RegistryError(f"remote name {name!r} is reserved")


def _where(origin: str) -> str:
    if origin in OWN_ORIGINS:
        return f"{origin} registry"
    return f"pulled/{origin}"


def _load_remotes(path: Path) -> dict[str, Remote]:
    """Read the file, in either shape it has had.

    A peer used to be ``name = "url"`` and is now a table, because a URL is no
    longer all we keep about one. Both are accepted: a node that has been
    running since before registry keys existed has the flat form on disk, and
    rewriting it on read would be a migration nobody asked for — it happens on
    the next write instead.
    """
    if not path.is_file():
        return {}
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError, UnicodeDecodeError):
        return {}
    remotes = data.get("remotes")
    if not isinstance(remotes, dict):
        return {}
    out: dict[str, Remote] = {}
    for key, value in remotes.items():
        if not isinstance(key, str):
            continue
        if isinstance(value, str):
            out[key] = Remote(name=key, url=value)
        elif isinstance(value, dict):
            url = value.get("url")
            token = value.get("token")
            if isinstance(url, str):
                out[key] = Remote(
                    name=key,
                    url=url,
                    token=token if isinstance(token, str) and token else None,
                )
    return out


def _dump_remotes(remotes: Mapping[str, Remote]) -> str:
    lines: list[str] = []
    for name in sorted(remotes):
        remote = remotes[name]
        lines.append(f"[remotes.{name}]")
        lines.append(f"url = {json.dumps(remote.url)}")
        if remote.token:
            lines.append(f"token = {json.dumps(remote.token)}")
        lines.append("")
    return "\n".join(lines)
