"""Per-STS object store — one relative path, one file of opaque bytes.

Each STS process keeps this on its own volume (``STS_ARTIFACT_DIR``, default
``/var/lib/mftik/artifacts``). A strategy in that process reads and writes
the files directly. The API never opens the directory; it asks the STS that
holds it, the same way an event log is read.

A key is the relative path. ``weights/model.pt`` and
``sessions/{session_id}/weights/model.pt`` are different keys. ``read``
returns the key it was given and does not fall back to the other one.

Operator verbs (``put``, ``rm``, and the catalog ``ls``) refuse a key under
``sessions/``. A tree the operator cannot see is one they must not be able
to delete. A strategy ``write`` may use that prefix: that is how a session
keeps its own object without replacing the upload.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import secrets
import stat
import threading
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import IO, TYPE_CHECKING

if TYPE_CHECKING:
    from mftik.strategy.base import Strategy

DIR_ENV = "STS_ARTIFACT_DIR"
DEFAULT_DIR = "/var/lib/mftik/artifacts"

#: How long an unfinished upload may sit before the reaper unlinks it.
PART_IDLE_S = 3600.0

#: Prefix of keys a session writes for itself. Hidden from the catalog, and
#: refused to operator put / rm.
SESSIONS = "sessions"

_DIGEST_PREFIX = "sha256:"

#: Entries kept in the digest cache. It is keyed by ``(size, mtime_ns, inode)``,
#: so every version of every object it has hashed is a new key and nothing but
#: a delete ever removes one. An STS process lives for weeks and a strategy
#: that checkpoints every minute would otherwise grow this without end.
_DIGEST_CACHE_MAX = 4096


class BadArtifactKey(ValueError):
    """The key is not a relative path this store will accept."""


class ArtifactNotFound(LookupError):
    """A valid key that names no object."""


class ArtifactUploadError(LookupError):
    """``chunk`` / ``commit`` / ``abort`` named a token this process does not hold."""


@dataclass(frozen=True)
class ArtifactMeta:
    """What a listing or a ``stat`` can say without the body."""

    path: str
    size: int
    #: Seconds since the epoch, the file's mtime.
    mtime: float
    digest: str


@dataclass(frozen=True)
class ArtifactObject(ArtifactMeta):
    """A ``read``: the metadata plus the bytes."""

    body: bytes


@dataclass
class _Upload:
    key: str
    path: Path
    lock: threading.Lock


@dataclass
class _StreamWrite:
    """A part file a strategy is writing. Commit checks it is still this file."""

    key: str
    path: Path
    handle: IO[bytes]
    dev: int
    ino: int


def artifact_dir() -> Path:
    """Where this process keeps objects.

    Unset is the default path, not "off". An object store that is absent
    cannot accept an upload, and that is not a legal mode the way a missing
    event log is.
    """
    raw = os.environ.get(DIR_ENV, "").strip()
    return Path(raw) if raw else Path(DEFAULT_DIR)


def is_session_key(key: str) -> bool:
    """Whether ``key`` is the sessions tree, which operators do not touch."""
    return key == SESSIONS or key.startswith(SESSIONS + "/")


def check_key(key: str) -> str:
    """Refuse a key that is not a relative path of ordinary segments.

    Absolute, empty, ``.``, ``..``, a NUL or a control character. After this
    the path is still resolved against the store root, which is what catches
    a symlink already planted in the tree.
    """
    if not isinstance(key, str) or key == "":
        raise BadArtifactKey("artifact key is empty")
    if key.startswith("/") or key.startswith("\\"):
        raise BadArtifactKey(f"artifact key must be relative, got {key!r}")
    for char in key:
        if ord(char) < 32 or ord(char) == 127:
            raise BadArtifactKey(f"artifact key contains a control character: {key!r}")
    parts = key.split("/")
    if any(part == "" for part in parts):
        raise BadArtifactKey(f"artifact key has an empty segment: {key!r}")
    if any(part in {".", ".."} for part in parts):
        raise BadArtifactKey(f"artifact key must not contain '.' or '..': {key!r}")
    return key


class ArtifactStore:
    """The files under one root, and the uploads this process has open."""

    def __init__(self, root: Path) -> None:
        self.root = root
        #: ``(size, mtime_ns, inode)`` → digest. A restart drops it; the next
        #: listing recomputes. A replace inserts the digest it already hashed,
        #: so the common path hashes once.
        self._digests: dict[tuple[int, int, int], str] = {}
        self._uploads: dict[str, _Upload] = {}
        self._lock = threading.Lock()

    def list_catalog(self) -> list[ArtifactMeta]:
        """Uploaded objects. The ``sessions/`` tree is not in this list."""
        return [row for row in self._list() if not is_session_key(row.path)]

    def list_session(self, session_id: str) -> list[ArtifactMeta]:
        """Objects under ``sessions/{session_id}/``."""
        check_key(session_id)
        if "/" in session_id:
            raise BadArtifactKey(
                f"session id is not a single path segment: {session_id!r}"
            )
        prefix = f"{SESSIONS}/{session_id}/"
        return [row for row in self._list() if row.path.startswith(prefix)]

    def stat(self, key: str) -> ArtifactMeta | None:
        """Metadata for ``key``, or None when the listing has no such object.

        Matched against the directory listing, not against a path built from
        the string: a name the listing would not offer is not an object.
        """
        found = self._listed(key)
        if found is None:
            return None
        return self._meta(key, found)

    def read(self, key: str) -> ArtifactObject | None:
        """The object at ``key``, or None when it is not there."""
        found = self._listed(key)
        if found is None:
            return None
        # One copy of the body, not a list of blocks and then a join of them:
        # this is already the call that admits to holding a whole checkpoint,
        # and holding it twice at the peak is what ``reading`` exists to avoid.
        with found.open("rb") as handle:
            st = os.fstat(handle.fileno())
            body = handle.read()
        text = _DIGEST_PREFIX + hashlib.sha256(body).hexdigest()
        self._cache(st, text)
        return ArtifactObject(
            path=key,
            size=len(body),
            mtime=st.st_mtime,
            digest=text,
            body=body,
        )

    def read_at(self, key: str, offset: int, length: int) -> tuple[bytes, bool]:
        """One slice of ``key``. The bool is whether this slice reaches the end.

        Raises :class:`ArtifactNotFound` when the key is valid and absent.
        A download is streaming a key the caller already believes exists.
        """
        if offset < 0 or length < 0:
            raise BadArtifactKey(
                f"artifact read offset and length must be >= 0, got {offset}, {length}"
            )
        found = self._listed(key)
        if found is None:
            raise ArtifactNotFound(key)
        with found.open("rb") as handle:
            handle.seek(offset)
            data = handle.read(length)
        size = found.stat().st_size
        return data, offset + len(data) >= size

    def write(self, key: str, body: bytes) -> ArtifactMeta:
        """Replace ``key`` with ``body``.

        The previous object stays until this finishes.
        """
        dest = self._destination(key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        token = secrets.token_hex(8)
        part = dest.parent / f".{dest.name}.{token}.part"
        digest = hashlib.sha256()
        try:
            with part.open("wb") as handle:
                digest.update(body)
                handle.write(body)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(part, dest)
        except OSError:
            # A key that collides with a directory, a full disk: the object
            # that was there is untouched, and the part must not be left
            # behind for the sweeper to find an hour later.
            part.unlink(missing_ok=True)
            raise
        _fsync_dir(dest.parent)
        return self._remember(key, dest, _DIGEST_PREFIX + digest.hexdigest())

    def open_read(self, key: str) -> IO[bytes]:
        """Open ``key`` for reading, or raise when the listing has no such object.

        The name is one the directory walk would offer. A caller that streams
        the file does not also hold the body as ``bytes``.
        """
        found = self._listed(key)
        if found is None:
            raise ArtifactNotFound(key)
        return found.open("rb")

    def open_write(self, key: str) -> _StreamWrite:
        """Create a part file for ``key`` and return it still open.

        The caller writes the body. :meth:`commit_stream` replaces the key;
        :meth:`abort_stream` unlinks the part. A ``sessions/`` key is allowed:
        this is the strategy, not an operator upload.
        """
        dest = self._destination(key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        token = secrets.token_hex(8)
        part = dest.parent / f".{dest.name}.{token}.part"
        handle = part.open("w+b")
        st = os.fstat(handle.fileno())
        return _StreamWrite(
            key=key, path=part, handle=handle, dev=st.st_dev, ino=st.st_ino
        )

    def commit_stream(self, opened: _StreamWrite) -> ArtifactMeta:
        """fsync the part, hash it, and replace ``key`` with it.

        The part must still be the regular file this call opened. A symlink
        planted at that path is not replaced onto the key.
        """
        try:
            self._same_part(opened)
            if not opened.handle.closed:
                opened.handle.flush()
                os.fsync(opened.handle.fileno())
                opened.handle.seek(0)
                digest = self._hash_handle(opened.handle)
                opened.handle.flush()
                os.fsync(opened.handle.fileno())
                opened.handle.close()
            else:
                with opened.path.open("rb") as handle:
                    digest = self._hash_handle(handle)
                    os.fsync(handle.fileno())
            self._same_part(opened)
            dest = self._destination(opened.key)
            dest.parent.mkdir(parents=True, exist_ok=True)
            os.replace(opened.path, dest)
        except Exception:
            self.abort_stream(opened)
            raise
        _fsync_dir(dest.parent)
        return self._remember(opened.key, dest, digest)

    def abort_stream(self, opened: _StreamWrite) -> None:
        """Close the part and unlink it when it is still the file we opened."""
        if not opened.handle.closed:
            opened.handle.close()
        try:
            st = os.lstat(opened.path)
        except OSError:
            return
        ours = stat.S_ISREG(st.st_mode) and (st.st_dev, st.st_ino) == (
            opened.dev,
            opened.ino,
        )
        if ours or stat.S_ISLNK(st.st_mode):
            try:
                os.unlink(opened.path)
            except OSError:
                return

    def remove(self, key: str) -> None:
        """Unlink an uploaded object. A ``sessions/`` key is refused.

        The name is what goes: ``unlink`` never follows a symlink, so
        removing one takes the directory entry away and leaves the object it
        pointed at under its own key.
        """
        self._refuse_session_key(key)
        found = self._listed(key)
        if found is None:
            raise ArtifactNotFound(key)
        self._forget(found)
        found.unlink()

    def begin(self, key: str) -> str:
        """Open a part file for ``key`` and return its token.

        The part's name carries the token, so two uploads of one key do not
        share a temporary path. A ``sessions/`` key is refused: this is the
        operator upload, and that tree belongs to the session that writes it.
        """
        self._refuse_session_key(key)
        dest = self._destination(key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        token = secrets.token_hex(16)
        part = dest.parent / f".{dest.name}.{token}.part"
        part.touch()
        with self._lock:
            self._uploads[token] = _Upload(key=key, path=part, lock=threading.Lock())
        return token

    def chunk(self, token: str, offset: int, data: bytes) -> None:
        """Write ``data`` at ``offset`` in the part ``token`` names.

        The offset is explicit so a retried chunk overwrites the same bytes
        instead of appending a second copy.
        """
        if offset < 0:
            raise BadArtifactKey(f"artifact chunk offset must be >= 0, got {offset}")
        upload = self._upload(token)
        with upload.lock:
            with upload.path.open("r+b") as handle:
                handle.seek(offset)
                handle.write(data)
                handle.flush()

    def commit(self, token: str) -> ArtifactMeta:
        """fsync the part, hash it, and replace the key with it."""
        upload = self._take_upload(token)
        with upload.lock:
            try:
                digest = hashlib.sha256()
                with upload.path.open("rb") as handle:
                    for block in iter(lambda: handle.read(1024 * 1024), b""):
                        digest.update(block)
                    os.fsync(handle.fileno())
                dest = self._destination(upload.key)
                dest.parent.mkdir(parents=True, exist_ok=True)
                os.replace(upload.path, dest)
            except Exception:
                # The token is already off the registry, so nothing else will
                # ever name this part. Drop it here rather than leave it for
                # the sweeper an hour from now.
                upload.path.unlink(missing_ok=True)
                raise
        _fsync_dir(dest.parent)
        return self._remember(upload.key, dest, _DIGEST_PREFIX + digest.hexdigest())

    def abort(self, token: str) -> None:
        """Unlink the part. An unknown token is already gone."""
        upload = self._take_upload(token, missing_ok=True)
        if upload is None:
            return
        with upload.lock:
            upload.path.unlink(missing_ok=True)

    def sweep_parts(
        self, *, idle_s: float = PART_IDLE_S, now: float | None = None
    ) -> int:
        """Unlink part files that have not been written for ``idle_s`` seconds."""
        if not self.root.is_dir():
            return 0
        moment = time.time() if now is None else now
        removed = 0
        root = self.root.resolve()
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [name for name in dirnames if not name.startswith(".")]
            for name in filenames:
                if not _is_part_name(name):
                    continue
                path = Path(dirpath) / name
                try:
                    mtime = path.stat().st_mtime
                except OSError:
                    continue
                if moment - mtime < idle_s:
                    continue
                token = _token_of(name)
                if token is not None:
                    self._take_upload(token, missing_ok=True)
                try:
                    path.unlink()
                except OSError:
                    continue
                removed += 1
        return removed

    def _list(self) -> list[ArtifactMeta]:
        rows: list[ArtifactMeta] = []
        for key, path in self._walk():
            try:
                rows.append(self._meta(key, path))
            except OSError:
                # Unlinked between the walk and the stat — a delete on the
                # other STS subject, or a session replacing its own object.
                # A row that is gone is left out, not raised out of the whole
                # listing.
                continue
        rows.sort(key=lambda row: row.path)
        return rows

    def _walk(self) -> list[tuple[str, Path]]:
        """Objects the directory actually holds, as ``(key, walked path)``.

        The key is the name the directory entry carries, not the name of
        whatever it points at. A symlink is not folded into its target: with
        ``link -> data.bin`` in the tree those are two keys, ``ls`` shows
        both, and removing one leaves the other.

        A leading-dot name is a part file, not an object. ``resolve`` is the
        escape check only — see :meth:`_is_object`.
        """
        if not self.root.is_dir():
            return []
        root = self.root.resolve()
        found: list[tuple[str, Path]] = []
        for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
            dirnames[:] = [name for name in dirnames if not name.startswith(".")]
            for name in filenames:
                if name.startswith("."):
                    continue
                path = Path(dirpath) / name
                if not self._is_object(path):
                    continue
                found.append((path.relative_to(root).as_posix(), path))
        return found

    def _is_object(self, path: Path) -> bool:
        """Whether the entry at ``path`` is one of this store's objects.

        ``resolve`` is the escape check and nothing else: the target has to
        be a regular file under the root, which rules out a symlink pointing
        out of the store and a dangling one. It does not rename the entry —
        what the object is called is the name it is filed under.
        """
        root = self.root.resolve()
        try:
            resolved = path.resolve()
            resolved.relative_to(root)
        except (OSError, ValueError):
            return False
        return resolved.is_file()

    def _listed(self, key: str) -> Path | None:
        """The entry ``key`` names, or None when the listing would not offer it.

        Looked up directly rather than by walking the tree: a download asks
        for one slice at a time, and walking every object on the disk once per
        256 KiB of a checkpoint is the whole store's cost per chunk.

        Which makes agreeing with :meth:`_walk` this method's whole job, and
        the agreement is reached the same way the walk reaches it rather than
        by comparing resolved names. Every directory above the last segment
        must be a real directory: ``os.walk(followlinks=False)`` does not
        descend into a symlinked one, so a key underneath it is a name the
        listing never offers. The last segment is then put to
        :meth:`_is_object`, which resolves it only to check it stays inside.

        The path returned is the walked one, so a caller that opens it follows
        the link to the bytes and a caller that unlinks it takes the name away
        and leaves the target alone.
        """
        check_key(key)
        parts = key.split("/")
        if any(part.startswith(".") for part in parts):
            return None
        if not self.root.is_dir():
            return None
        path = self.root.resolve()
        for part in parts[:-1]:
            path = path / part
            if path.is_symlink() or not path.is_dir():
                return None
        path = path / parts[-1]
        return path if self._is_object(path) else None

    def _destination(self, key: str) -> Path:
        """The absolute path ``key`` will occupy, once it exists.

        The directories above the last segment are resolved and checked to be
        under the root. That is the check a string rule cannot make: a symlink
        already in the tree.

        The last segment is not resolved. POSIX ``rename`` does not follow a
        symlink in its final component, so the ``os.replace`` that lands the
        object swaps this directory entry for a regular file. Writing ``link``
        therefore replaces ``link`` and leaves the file it pointed at alone,
        which is the same name-is-the-name rule ``remove`` follows.
        """
        check_key(key)
        self.root.mkdir(parents=True, exist_ok=True)
        root = self.root.resolve()
        dest = root.joinpath(key)
        try:
            parent = dest.parent.resolve()
            parent.relative_to(root)
        except (OSError, ValueError) as exc:
            raise BadArtifactKey(f"{key} is outside the artifact store") from exc
        return parent / dest.name

    def _meta(self, key: str, path: Path) -> ArtifactMeta:
        st = path.stat()
        return ArtifactMeta(
            path=key,
            size=st.st_size,
            mtime=st.st_mtime,
            digest=self._digest(path, st),
        )

    def _digest(self, path: Path, st: os.stat_result) -> str:
        cache_key = (st.st_size, st.st_mtime_ns, st.st_ino)
        with self._lock:
            cached = self._digests.get(cache_key)
        if cached is not None:
            return cached
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
        text = _DIGEST_PREFIX + digest.hexdigest()
        self._cache(st, text)
        return text

    def _cache(self, st: os.stat_result, digest: str) -> None:
        """Remember one digest, and forget the oldest when the cache is full.

        Insertion order is eviction order: the entry least recently written is
        the one whose object is least likely to be asked for again.
        """
        with self._lock:
            self._digests[(st.st_size, st.st_mtime_ns, st.st_ino)] = digest
            while len(self._digests) > _DIGEST_CACHE_MAX:
                self._digests.pop(next(iter(self._digests)))

    def _remember(self, key: str, path: Path, digest: str) -> ArtifactMeta:
        st = path.stat()
        self._cache(st, digest)
        return ArtifactMeta(path=key, size=st.st_size, mtime=st.st_mtime, digest=digest)

    def _forget(self, path: Path) -> None:
        try:
            st = path.stat()
        except OSError:
            return
        with self._lock:
            self._digests.pop((st.st_size, st.st_mtime_ns, st.st_ino), None)

    def _same_part(self, opened: _StreamWrite) -> None:
        try:
            st = os.lstat(opened.path)
        except OSError as exc:
            raise ArtifactUploadError(opened.key) from exc
        if not stat.S_ISREG(st.st_mode) or (st.st_dev, st.st_ino) != (
            opened.dev,
            opened.ino,
        ):
            raise BadArtifactKey(
                f"{opened.key} part file is no longer the file the store opened"
            )

    @staticmethod
    def _hash_handle(handle: IO[bytes]) -> str:
        digest = hashlib.sha256()
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
        return _DIGEST_PREFIX + digest.hexdigest()

    def _refuse_session_key(self, key: str) -> None:
        check_key(key)
        if is_session_key(key):
            raise BadArtifactKey(
                f"{key} is under sessions/ — that tree is written by the session, "
                "not by an upload"
            )

    def _upload(self, token: str) -> _Upload:
        with self._lock:
            upload = self._uploads.get(token)
        if upload is None:
            raise ArtifactUploadError(token)
        return upload

    def _take_upload(self, token: str, *, missing_ok: bool = False) -> _Upload | None:
        with self._lock:
            upload = self._uploads.pop(token, None)
        if upload is None and not missing_ok:
            raise ArtifactUploadError(token)
        return upload


_store: ArtifactStore | None = None
_store_root: Path | None = None


def get_store() -> ArtifactStore:
    """The store for this process. A changed ``STS_ARTIFACT_DIR`` starts a new one."""
    global _store, _store_root
    root = artifact_dir()
    if _store is None or _store_root != root:
        _store = ArtifactStore(root)
        _store_root = root
    return _store


def reset_store() -> None:
    """Drop the process store. Tests that point ``STS_ARTIFACT_DIR`` elsewhere."""
    global _store, _store_root
    _store = None
    _store_root = None


def _is_part_name(name: str) -> bool:
    return name.startswith(".") and name.endswith(".part")


def _fsync_dir(path: Path) -> None:
    """Make the rename itself durable, not only the bytes it renamed.

    ``os.replace`` of an fsync'd part is atomic, but a crash before the
    directory entry reaches the disk can still lose it — and ``commit`` has
    already handed the operator a digest by then. Best effort: a filesystem
    that refuses to open a directory is not a reason to fail the write.
    """
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


class StrategyArtifacts:
    """The store, as a strategy calls it.

    Direct file operations on this machine — the strategy is in the STS
    process. The bytes move on a worker thread so a large checkpoint does
    not stall the event loop. Every call raises until :meth:`bind`, the way
    ``remember`` does: an unbound strategy has no session, and a write that
    guessed ``sessions/None/...`` would be a real key.
    """

    def __init__(self) -> None:
        self._strategy: Strategy | None = None

    def bind(self, strategy: Strategy) -> None:
        self._strategy = strategy

    def _bound(self) -> None:
        if self._strategy is None or self._strategy.session is None:
            raise RuntimeError("strategy artifacts are not bound to a session")

    async def read(self, key: str) -> ArtifactObject | None:
        self._bound()
        return await asyncio.to_thread(get_store().read, key)

    async def stat(self, key: str) -> ArtifactMeta | None:
        self._bound()
        return await asyncio.to_thread(get_store().stat, key)

    async def write(self, key: str, body: bytes) -> ArtifactMeta:
        self._bound()
        return await asyncio.to_thread(get_store().write, key, body)

    @asynccontextmanager
    async def reading(self, key: str) -> AsyncIterator[IO[bytes]]:
        """Open ``key`` for a streaming read.

        The file is the object the directory lists. A missing key raises
        :class:`ArtifactNotFound` before the block runs. Close happens here;
        the body reads the file, usually from :func:`asyncio.to_thread`.
        """
        self._bound()
        handle = await asyncio.to_thread(get_store().open_read, key)
        try:
            yield handle
        finally:
            await asyncio.to_thread(handle.close)

    @asynccontextmanager
    async def writing(self, key: str) -> AsyncIterator[IO[bytes]]:
        """Open a part file. The block writes it; leaving the block replaces ``key``.

        An exception unlinks the part and leaves the previous object. The file
        object is the one to hand to ``torch.save`` inside
        :func:`asyncio.to_thread` — the body is not assembled as ``bytes`` first.
        """
        self._bound()
        opened = await asyncio.to_thread(get_store().open_write, key)
        try:
            yield opened.handle
        except BaseException:
            await asyncio.to_thread(get_store().abort_stream, opened)
            raise
        else:
            try:
                await asyncio.to_thread(get_store().commit_stream, opened)
            except BaseException:
                await asyncio.to_thread(get_store().abort_stream, opened)
                raise


def _token_of(name: str) -> str | None:
    """The token in ``.{filename}.{token}.part``, when the token looks like one."""
    stem = name[1:] if name.startswith(".") else name
    if stem.endswith(".part"):
        stem = stem[: -len(".part")]
    filename, dot, token = stem.rpartition(".")
    if not dot or not filename or not token:
        return None
    if len(token) < 8 or any(char not in "0123456789abcdef" for char in token):
        return None
    return token
