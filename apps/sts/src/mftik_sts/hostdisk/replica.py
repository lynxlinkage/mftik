"""Digest-addressed strategy trees on an STS volume (F39, §5.7).

Trees live at ``registry/trees/<digest>/``. The name → digest index is a
separate file. Putting a new digest does not overwrite another tree, and
pointing a name at a new digest does not delete the previous one.
"""

from __future__ import annotations

import json
import os
import re
import shutil
from collections.abc import Collection, Mapping
from pathlib import Path

from mftik.registry.digest import DIGEST_PREFIX, digest_files
from mftik.registry.errors import RegistryDigestMismatch
from mftik.registry.files import normalize_files

#: ``sha256:`` plus 64 hex characters. The column ``strategy_digest`` is
#: this wide.
_DIGEST = re.compile(rf"^{re.escape(DIGEST_PREFIX)}[0-9a-f]{{64}}$")

TREES_DIRNAME = "trees"
INDEX_NAME = "index.json"


def require_digest(value: object) -> str:
    """Return ``value`` when it is ``sha256:<64 hex>``."""
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise ValueError("digest must be sha256:<64 hex>")
    return value


class TreeReplica:
    """The STS copy of the strategy trees, addressed by digest (F39).

    **State authority (§3.3).** The API's registry store is the authority
    for which names exist and which digest is current. This object is the
    copy on the STS volume. The controller is the only writer, and only
    from the API's fan-out and the boot catch-up. A session worker reads
    a tree; it does not publish one.

    **Invariants.**

    * A tree is stored at ``registry/trees/<digest>/`` and is not replaced
      when a later push of the same name arrives. The index moves; the
      bytes stay until :meth:`gc`.
    * :meth:`gc` deletes a digest only when it is neither in ``keep`` nor
      the index's current digest for some name. ``keep`` is the pin set.
    * This class does not import strategy code.
    """

    def __init__(self, data_dir: str | Path) -> None:
        self.data_dir = Path(data_dir)
        self.registry_dir = self.data_dir / "registry"
        self.trees_dir = self.registry_dir / TREES_DIRNAME
        self.index_path = self.registry_dir / INDEX_NAME

    def put(self, digest: str, files: Mapping[str, str | bytes]) -> Path:
        """Write ``files`` at ``digest`` if that tree is not already here.

        ``digest`` has to be :func:`mftik.registry.digest.digest_files` of
        the normalised ``.py`` files. A mismatch is refused before anything
        is renamed, so a bad payload cannot replace a tree that is already
        on disk. A second put of the same digest leaves the directory
        untouched.
        """
        digest = require_digest(digest)
        normalised = normalize_files(files)
        actual = digest_files(normalised)
        if actual != digest:
            raise RegistryDigestMismatch(
                f"digest mismatch: tree is {actual}, request said {digest}"
            )
        dest = self.tree_path(digest)
        if dest.is_dir():
            return dest
        self.trees_dir.mkdir(parents=True, exist_ok=True)
        tmp = self.trees_dir / f".{digest}.tmp"
        if tmp.exists():
            shutil.rmtree(tmp)
        tmp.mkdir()
        try:
            for rel, body in normalised.items():
                path = tmp / rel
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(body)
            os.rename(tmp, dest)
        except Exception:
            shutil.rmtree(tmp, ignore_errors=True)
            raise
        return dest

    def bind(self, name: str, digest: str) -> None:
        """Point ``name`` at ``digest``. The previous tree is left in place.

        ``digest`` has to already be on disk. The index is the only thing
        this changes, which is how a push of the same name publishes a new
        version without covering the one a session is pinned to.
        """
        if not isinstance(name, str) or name == "" or "/" in name or "\\" in name:
            raise ValueError("name must be a non-empty strategy name")
        digest = require_digest(digest)
        if not self.tree_path(digest).is_dir():
            raise ValueError(f"digest {digest} is not on this disk")
        index = self._read_index()
        index[name] = digest
        self._write_index(index)

    def current(self, name: str) -> str | None:
        """The digest the index currently names, or ``None``."""
        if not isinstance(name, str):
            raise ValueError("name must be a string")
        return self._read_index().get(name)

    def path_of(self, digest: str) -> Path | None:
        """The tree directory, or ``None`` when this disk does not have it."""
        digest = require_digest(digest)
        path = self.tree_path(digest)
        return path if path.is_dir() else None

    def tree_path(self, digest: str) -> Path:
        """``registry/trees/<digest>``, whether or not it exists yet."""
        return self.trees_dir / require_digest(digest)

    def gc(self, keep: Collection[str]) -> tuple[str, ...]:
        """Delete trees that are neither pinned nor current in the index.

        ``keep`` is the digests non-terminal sessions pin. The index's
        current digest for every name is kept as well (§5.7): a version
        nobody is running yet is still the one the next start will use.
        Returns the digests removed, in name order.
        """
        if isinstance(keep, str) or not isinstance(keep, Collection):
            raise TypeError("keep must be a collection of digests")
        retained = set(keep) | set(self._read_index().values())
        if not self.trees_dir.is_dir():
            return ()
        removed: list[str] = []
        for child in sorted(self.trees_dir.iterdir(), key=lambda path: path.name):
            if child.name.startswith("."):
                if child.is_dir():
                    shutil.rmtree(child, ignore_errors=True)
                else:
                    child.unlink(missing_ok=True)
                continue
            if not child.is_dir() or child.name in retained:
                continue
            shutil.rmtree(child)
            removed.append(child.name)
        return tuple(removed)

    def _read_index(self) -> dict[str, str]:
        if not self.index_path.is_file():
            return {}
        try:
            raw = json.loads(self.index_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            return {}
        if not isinstance(raw, dict):
            return {}
        index: dict[str, str] = {}
        for key, value in raw.items():
            if isinstance(key, str) and isinstance(value, str) and key != "":
                index[key] = value
        return index

    def _write_index(self, index: Mapping[str, str]) -> None:
        self.registry_dir.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(dict(sorted(index.items())), indent=2) + "\n"
        tmp = self.index_path.with_name(INDEX_NAME + ".tmp")
        tmp.write_text(payload, encoding="utf-8")
        os.replace(tmp, self.index_path)
