"""Normalise a strategy file map before hashing, scanning, or writing."""

from __future__ import annotations

import re
from collections.abc import Mapping

from mftik.registry.errors import RegistryError

_SKIP_DIR = frozenset({"__pycache__", ".git"})
_SKIP_SUFFIX = (".pyc", ".pyo")
_PATH = re.compile(r"^[A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)*$")
MAX_FILES = 64
MAX_FILE_BYTES = 1_048_576


def normalize_files(files: Mapping[str, str | bytes]) -> dict[str, bytes]:
    """POSIX relative ``.py`` paths, UTF-8 bytes, no parent traversal or junk.

    Skips ``__pycache__``, ``*.pyc``, and anything that is not Python so a
    copy-paste of a working tree does not change the digest.
    """
    if not files:
        raise RegistryError("strategy has no files")
    out: dict[str, bytes] = {}
    for raw_path, raw_body in files.items():
        path = _collapse(raw_path.replace("\\", "/").strip().lstrip("/"))
        if not path:
            raise RegistryError(f"illegal path: {raw_path!r}")
        if any(part in _SKIP_DIR for part in path.split("/")):
            continue
        if path.endswith(_SKIP_SUFFIX):
            continue
        if not path.endswith(".py"):
            continue
        if not _PATH.match(path):
            raise RegistryError(
                f"illegal path {raw_path!r} — use relative POSIX names "
                f"(letters, digits, ._-/)"
            )
        if path in out:
            raise RegistryError(f"duplicate path: {path}")
        body = raw_body.encode("utf-8") if isinstance(raw_body, str) else raw_body
        if len(body) > MAX_FILE_BYTES:
            raise RegistryError(f"{path} is larger than {MAX_FILE_BYTES} bytes")
        out[path] = body
    if len(out) > MAX_FILES:
        raise RegistryError(f"too many files ({len(out)}); max is {MAX_FILES}")
    if not out:
        raise RegistryError("strategy has no .py files")
    return out


def _collapse(path: str) -> str:
    parts: list[str] = []
    for part in path.split("/"):
        if part in {"", "."}:
            continue
        if part == "..":
            raise RegistryError(f"path must not contain '..': {path!r}")
        parts.append(part)
    return "/".join(parts)
