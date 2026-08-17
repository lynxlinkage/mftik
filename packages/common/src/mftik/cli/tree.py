"""A strategy directory on this machine.

``check``, ``push`` and ``run`` all start here, so a path that is not a
tree is refused in the same words by each of them.
"""

from __future__ import annotations

from pathlib import Path

from mftik.cli.client import CliError
from mftik.registry.errors import RegistryError
from mftik.registry.files import read_tree
from mftik.registry.inspect import Inspected, inspect_files

#: What ``run`` deploys when the operator gave no document. An empty ``sts``
#: is a valid starting point — ``on_initialized`` is what refuses a bad one.
EMPTY_YAML = "sts: {}\n"


def require_tree(path: str | Path) -> Path:
    """The directory a command was pointed at, or a sentence saying it is not."""
    root = Path(path)
    if root.is_file():
        raise CliError(f"{root} is a file — give the strategy directory")
    if not root.is_dir():
        raise CliError(f"strategy tree does not exist: {root}")
    return root


def inspect_tree(root: Path) -> Inspected:
    """Gate and naming, as :func:`inspect_files` does for ``add``."""
    try:
        return inspect_files(read_tree(root))
    except RegistryError as exc:
        raise CliError(str(exc)) from exc


def cfg_path(cfg: str | None, root: Path) -> Path | None:
    """The document to parse, if one was given or lives next to the tree."""
    if cfg is not None:
        path = Path(cfg)
        if not path.is_file():
            raise CliError(f"strategy.yml does not exist: {path}")
        return path
    candidate = root / "strategy.yml"
    if candidate.is_file():
        return candidate
    return None


def read_yaml(cfg: str | None, root: Path) -> str:
    """Text to deploy. ``EMPTY_YAML`` when no document was found."""
    path = cfg_path(cfg, root)
    if path is None:
        return EMPTY_YAML
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise CliError(f"cannot read {path}: {exc}") from exc


def tree_texts(inspected: Inspected) -> dict[str, str]:
    """The file map ``POST /registry/v1/add`` wants — UTF-8 text, not bytes."""
    out: dict[str, str] = {}
    for path, body in inspected.files.items():
        try:
            out[path] = body.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise CliError(f"{path} is not UTF-8: {exc}") from exc
    return out
