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
#: The only non-Python file a tree may ship. Other names are junk —
#: identity is the ``.py`` files, and this sidecar is the deploy template
#: the picker starts from. A file at the tree root always wins; a single
#: copy deeper in (next to a packaged class) is lifted to that name.
TEMPLATE_NAME = "strategy.yml"
_NESTED_TEMPLATE = "/" + TEMPLATE_NAME


def normalize_files(files: Mapping[str, str | bytes]) -> dict[str, bytes]:
    """POSIX relative ``.py`` paths plus an optional ``strategy.yml``.

    Skips ``__pycache__``, ``*.pyc``, and anything that is not Python or
    the deploy template so a copy-paste of a working tree does not change
    the digest. The digest itself only hashes ``.py`` files. A nested
    ``strategy.yml`` is stored under :data:`TEMPLATE_NAME` so the registry
    and the picker always look in one place.
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
        if not path.endswith(".py") and not _is_template(path):
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
    _collapse_templates(out)
    if len(out) > MAX_FILES:
        raise RegistryError(f"too many files ({len(out)}); max is {MAX_FILES}")
    if not any(path.endswith(".py") for path in out):
        raise RegistryError("strategy has no .py files")
    return out


def find_template(root: Path) -> Path | None:
    """The tree's deploy document, or None.

    ``strategy.yml`` at the root always wins. If it is absent, exactly one
    copy deeper in the tree is accepted — a package that put the sidecar
    next to the class rather than at the directory the CLI was pointed at.
    Two or more cannot be guessed between.
    """
    root = Path(root)
    at_root = root / TEMPLATE_NAME
    if at_root.is_file():
        return at_root
    found: list[Path] = []
    for path in root.rglob(TEMPLATE_NAME):
        rel = path.relative_to(root)
        if any(part in _SKIP_DIR for part in rel.parts):
            continue
        if not path.is_file():
            continue
        found.append(path)
    if len(found) > 1:
        listed = ", ".join(sorted(p.relative_to(root).as_posix() for p in found))
        raise RegistryError(
            f"multiple {TEMPLATE_NAME} files ({listed}) — "
            f"put one at the tree root"
        )
    return found[0] if found else None


def read_tree(root: Path) -> dict[str, bytes]:
    """Collect ``.py`` files and an optional ``strategy.yml``.

    A nested template is stored under :data:`TEMPLATE_NAME`. Skips
    ``__pycache__``. Does not normalise: that is :func:`normalize_files`,
    which is what refuses junk paths and empty trees. A missing directory
    is a refusal here so a caller does not have to distinguish "nothing
    there" from "nothing worth copying".
    """
    root = Path(root)
    if not root.is_dir():
        raise RegistryError(f"strategy tree does not exist: {root}")
    files: dict[str, bytes] = {}
    for py in root.rglob("*.py"):
        if "__pycache__" in py.parts:
            continue
        files[py.relative_to(root).as_posix()] = py.read_bytes()
    template = find_template(root)
    if template is not None:
        files[TEMPLATE_NAME] = template.read_bytes()
    return files


def _is_template(path: str) -> bool:
    return path == TEMPLATE_NAME or path.endswith(_NESTED_TEMPLATE)


def _collapse_templates(files: dict[str, bytes]) -> None:
    """Keep at most one ``strategy.yml``, always under :data:`TEMPLATE_NAME`."""
    nested = sorted(p for p in files if p != TEMPLATE_NAME and _is_template(p))
    if TEMPLATE_NAME in files:
        for path in nested:
            del files[path]
        return
    if not nested:
        return
    if len(nested) > 1:
        raise RegistryError(
            f"multiple {TEMPLATE_NAME} files ({', '.join(nested)}) — "
            f"put one at the tree root"
        )
    files[TEMPLATE_NAME] = files.pop(nested[0])


def _collapse(path: str) -> str:
    parts: list[str] = []
    for part in path.split("/"):
        if part in {"", "."}:
            continue
        if part == "..":
            raise RegistryError(f"path must not contain '..': {path!r}")
        parts.append(part)
    return "/".join(parts)
