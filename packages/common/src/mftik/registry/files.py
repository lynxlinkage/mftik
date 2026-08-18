"""Normalise a strategy file map before hashing, scanning, or writing."""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path

from mftik.registry.errors import RegistryError

_SKIP_DIR = frozenset({"__pycache__", ".git"})
_SKIP_SUFFIX = (".pyc", ".pyo")
_PATH = re.compile(r"^[A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)*$")
MAX_FILES = 64
MAX_FILE_BYTES = 1_048_576
#: The only non-Python file a tree may ship. Nested copies and other
#: names are junk — identity is the ``.py`` files, and this sidecar is
#: the deploy template the picker starts from.
TEMPLATE_NAME = "strategy.yml"


def normalize_files(files: Mapping[str, str | bytes]) -> dict[str, bytes]:
    """POSIX relative ``.py`` paths plus an optional root ``strategy.yml``.

    Skips ``__pycache__``, ``*.pyc``, and anything that is not Python or
    the deploy template so a copy-paste of a working tree does not change
    the digest. The digest itself only hashes ``.py`` files.
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
        if not path.endswith(".py") and path != TEMPLATE_NAME:
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
    if not any(path.endswith(".py") for path in out):
        raise RegistryError("strategy has no .py files")
    return out


def read_tree(root: Path) -> dict[str, bytes]:
    """Collect ``.py`` files and an optional root ``strategy.yml``.

    Skips ``__pycache__``. Does not normalise: that is
    :func:`normalize_files`, which is what refuses junk paths and empty
    trees. A missing directory is a refusal here so a caller does not have
    to distinguish "nothing there" from "nothing worth copying".
    """
    root = Path(root)
    if not root.is_dir():
        raise RegistryError(f"strategy tree does not exist: {root}")
    files: dict[str, bytes] = {}
    for py in root.rglob("*.py"):
        if "__pycache__" in py.parts:
            continue
        files[py.relative_to(root).as_posix()] = py.read_bytes()
    template = root / TEMPLATE_NAME
    if template.is_file():
        files[TEMPLATE_NAME] = template.read_bytes()
    return files


def _collapse(path: str) -> str:
    parts: list[str] = []
    for part in path.split("/"):
        if part in {"", "."}:
            continue
        if part == "..":
            raise RegistryError(f"path must not contain '..': {path!r}")
        parts.append(part)
    return "/".join(parts)
