"""On-disk registry — ``public/`` served, ``private/`` not, ``pulled/`` copied in."""

from __future__ import annotations

import contextlib
import json
import os
import shutil
import tomllib
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from mftik.registry.digest import digest_files
from mftik.registry.errors import (
    RegistryConflict,
    RegistryDigestMismatch,
    RegistryError,
)
from mftik.registry.files import TEMPLATE_NAME, normalize_files, read_tree
from mftik.registry.gate import check_files
from mftik.registry.inspect import check_name, check_type, inspect_files, pick_class
from mftik.registry.qualify import (
    OWN_ORIGINS,
    PRIVATE_ORIGIN,
    PUBLIC_ORIGIN,
    RESERVED_REMOTE_NAMES,
)

DATA_ENV = "MFTIK_DATA"
DEFAULT_DATA_DIR = ".mftik"
_DEFAULT_REQUIRES = "0.1.0"


@dataclass(frozen=True, slots=True)
class AddedStrategy:
    """What ``add`` wrote. The digest is of the ``.py`` files.

    ``files`` may also list a root ``strategy.yml``. That sidecar is the
    deploy template; it is not part of the digest.
    """

    #: Class name. The directory this tree is stored under, and the same
    #: string as :attr:`type`.
    name: str
    #: Class name. With :attr:`origin`, the qualified registry key.
    type: str
    digest: str
    requires_mftik: str
    #: Import names the chosen class declared. Empty when it needs only
    #: the stdlib and the SDK. Derived at scan time, like ``requires_mftik``.
    requires: tuple[str, ...]
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
        applied_extras: Mapping[str, str] | None = None,
        present_extras: Mapping[str, str] | None = None,
        expect_digest: str | None = None,
    ) -> AddedStrategy:
        """Validate, hash, and copy ``.py`` files and optional ``strategy.yml``.

        A nested ``strategy.yml`` has already been lifted to the tree root
        by :func:`inspect_files`. Own origins check ``requires`` against
        ``applied_extras`` when the caller passes one. A remote origin
        already in ``remotes.toml`` does not — that incompatibility is a
        deploy error, not a store refusal.

        ``present_extras`` is what the volume holds unapproved, passed in
        because this store reads neither Postgres nor the overlay. It only
        shapes the refusal: "not here" and "here but not approved" are
        different problems with different fixes, and ``mftik push`` is where
        a person reads the difference.
        """
        _check_origin(origin)
        inspected = inspect_files(files)
        chosen = inspected.cls
        normalised = inspected.files
        requires = chosen.requires_mftik or _DEFAULT_REQUIRES
        if (
            applied_extras is not None
            and origin in OWN_ORIGINS
        ):
            missing = [name for name in chosen.requires if name not in applied_extras]
            if missing:
                from mftik.environment import describe_missing

                raise RegistryError(
                    "this node cannot run that tree: "
                    + describe_missing(missing, present_extras)
                )
        digest = digest_files(normalised)
        if expect_digest and digest != expect_digest:
            # Before any rename. A mismatch must not replace a tree that
            # already loads, and must not leave the rejected bytes behind.
            raise RegistryDigestMismatch(
                f"digest mismatch: tree is {digest}, request said {expect_digest}"
            )
        type_name = chosen.type
        root = self._origin_root(origin)
        hit = _casefold_entry(root, type_name)
        if hit is not None and hit != type_name:
            raise RegistryConflict(
                f"strategy {type_name!r} collides with directory {hit!r} "
                f"in the {_where(origin)}"
            )
        if hit == type_name and not replace:
            raise RegistryConflict(
                f"strategy {type_name!r} is already in the {_where(origin)}"
            )
        dest = self._dest(origin, type_name)
        self._commit(dest, normalised, expect_digest=expect_digest or None)
        return AddedStrategy(
            name=type_name,
            type=chosen.type,
            digest=digest,
            requires_mftik=requires,
            requires=chosen.requires,
            files=tuple(sorted(normalised)),
            path=str(dest),
            origin=origin,
        )

    def remove(self, name: str, *, origin: str = PRIVATE_ORIGIN) -> AddedStrategy:
        """Delete one of this node's own trees. Returns what was deleted.

        Own origins only. A pulled copy is not this node's to delete one tree
        of — it is a copy of what a peer publishes, and removing a single
        strategy from it would leave a partial mirror that the next diff
        reports as missing and the next connect silently restores.
        :meth:`drop_remote` is how a pulled tree goes away.
        """
        if origin not in OWN_ORIGINS:
            raise RegistryError(
                f"{origin!r} is a pulled copy — disconnect the remote instead "
                f"of deleting one of its strategies"
            )
        check_type(name)
        dest = self._dest(origin, name)
        rec = self._read_tree(dest, origin=origin) if dest.is_dir() else None
        if rec is None:
            raise RegistryError(f"no {origin} strategy named {name!r}")
        # Resolved before the delete: the cache is keyed on the resolved path,
        # and a directory that is already gone cannot be resolved to build the
        # key that has to come out.
        key = (str(dest.resolve()), origin)
        shutil.rmtree(dest)
        self._tree_cache.pop(key, None)
        return rec

    def discard(self, name: str, *, origin: str) -> None:
        """Remove one tree of any origin. A missing tree is success.

        ``remove`` stays own-only, so an HTTP delete cannot punch a hole in
        a peer mirror one strategy at a time. Sync uses this for a pulled
        copy: the remote record lives on the API, and this volume may not
        have ``remotes.toml`` at all.
        """
        _check_origin(origin)
        check_type(name)
        dest = self._dest(origin, name)
        if not dest.is_dir():
            return
        key = (str(dest.resolve()), origin)
        shutil.rmtree(dest)
        self._tree_cache.pop(key, None)

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

    def read_template(self, rec: AddedStrategy) -> str | None:
        """The sidecar ``strategy.yml``, if this tree shipped one."""
        if TEMPLATE_NAME not in rec.files:
            return None
        dest = Path(rec.path) / TEMPLATE_NAME
        try:
            return dest.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            return None

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

    def _origin_root(self, origin: str) -> Path:
        if origin == PUBLIC_ORIGIN:
            return self.public_dir
        if origin == PRIVATE_ORIGIN:
            return self.private_dir
        return self.pulled_dir / origin

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

    def _commit(
        self,
        dest: Path,
        files: Mapping[str, bytes],
        *,
        expect_digest: str | None = None,
    ) -> None:
        """Replace ``dest`` by renaming a private temp directory onto it.

        The temp name is unique, not ``pid``. Two STS containers on one
        volume are both pid 1, and a shared ``.tmp-{name}-1`` lets one
        writer delete the other's half-written tree — and then delete
        ``dest`` itself. The previous tree is renamed aside and put back
        if the swap fails, so a crash between the two renames does not
        leave the key with no directory. ``expect_digest`` is checked on
        the temp directory before that swap. A rename that loses to the
        other writer is retried; a digest mismatch is not.
        """
        last: OSError | None = None
        for _attempt in range(8):
            try:
                self._commit_once(dest, files, expect_digest=expect_digest)
                return
            except RegistryDigestMismatch:
                raise
            except OSError as exc:
                last = exc
        assert last is not None
        raise last

    def _commit_once(
        self,
        dest: Path,
        files: Mapping[str, bytes],
        *,
        expect_digest: str | None,
    ) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        token = uuid.uuid4().hex
        tmp = dest.parent / f".tmp-{dest.name}-{token}"
        old = dest.parent / f".old-{dest.name}-{token}"
        tmp.mkdir()
        try:
            for path, body in files.items():
                target = tmp / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(body)
            if expect_digest and _digest_written(tmp) != expect_digest:
                raise RegistryDigestMismatch(
                    f"digest mismatch: wrote {dest.name}, "
                    f"request said {expect_digest}"
                )
            if dest.exists():
                dest.rename(old)
            tmp.rename(dest)
            if old.exists():
                shutil.rmtree(old, ignore_errors=True)
            self._drop_cache(dest)
        except Exception:
            if tmp.exists():
                shutil.rmtree(tmp, ignore_errors=True)
            if old.exists():
                if not dest.exists():
                    with contextlib.suppress(OSError):
                        old.rename(dest)
                else:
                    shutil.rmtree(old, ignore_errors=True)
            raise

    def _drop_cache(self, dest: Path) -> None:
        # A replace that lands in the same mtime bucket would otherwise
        # keep serving the digest from before the write.
        try:
            resolved = str(dest.resolve())
        except OSError:
            return
        self._tree_cache = {
            key: val
            for key, val in self._tree_cache.items()
            if key[0] != resolved
        }


    def _read_tree(self, dest: Path, *, origin: str) -> AddedStrategy | None:
        """Scan a tree. The directory name is the class name."""
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


def _digest_written(root: Path) -> str:
    """Digest of the ``.py`` files actually on disk under ``root``."""
    files: dict[str, bytes] = {}
    for py in root.rglob("*.py"):
        if "__pycache__" in py.parts:
            continue
        files[py.relative_to(root).as_posix()] = py.read_bytes()
    return digest_files(files)


def _tree_stamp(dest: Path) -> tuple[int, int] | None:
    """Source-file count and newest mtime, plus the directory itself.

    ``strategy.yml`` counts: editing the template must invalidate the
    cache even when no ``.py`` mtime moved.
    """
    try:
        newest = dest.stat().st_mtime_ns
        n = 0
        for py in dest.rglob("*.py"):
            if "__pycache__" in py.parts:
                continue
            n += 1
            newest = max(newest, py.stat().st_mtime_ns)
        template = dest / TEMPLATE_NAME
        if template.is_file():
            n += 1
            newest = max(newest, template.stat().st_mtime_ns)
    except OSError:
        return None
    return (n, newest)


def _scan_tree(dest: Path, *, origin: str) -> AddedStrategy | None:
    try:
        files = read_tree(dest)
    except (OSError, RegistryError):
        return None
    if not files:
        return None
    try:
        normalised = normalize_files(files)
        chosen = pick_class(check_files(normalised))
        # A directory left under the old short name, or renamed ahead of this
        # build, is not a strategy this process can see. Returning nothing —
        # rather than the class — is what makes starting the wrong build
        # against the wrong directories look like an empty registry.
        if chosen.type != dest.name:
            return None
        digest = digest_files(normalised)
    except RegistryError:
        return None
    return AddedStrategy(
        name=dest.name,
        type=chosen.type,
        digest=digest,
        requires_mftik=chosen.requires_mftik or _DEFAULT_REQUIRES,
        requires=chosen.requires,
        files=tuple(sorted(normalised)),
        path=str(dest),
        origin=origin,
    )


def _casefold_entry(root: Path, type_name: str) -> str | None:
    """The real directory name that casefolds to ``type_name``, if one exists.

    ``Path.exists`` is the wrong question on a case-insensitive volume:
    ``tiny`` being present makes ``Path("Tiny").exists()`` true, and two
    class names that differ only by case would silently share one directory.
    """
    if not root.is_dir():
        return None
    folded = type_name.casefold()
    for entry in os.listdir(root):
        if entry.startswith(".") or not (root / entry).is_dir():
            continue
        if entry.casefold() == folded:
            return entry
    return None


def _check_origin(origin: str) -> None:
    check_name(origin)
    if origin in OWN_ORIGINS:
        return
    _check_remote_name(origin)


def _check_remote_name(name: str) -> None:
    check_name(name)
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
