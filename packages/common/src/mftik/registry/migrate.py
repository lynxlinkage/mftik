"""Move own registry trees onto their class name, and drop pulled copies.

Run this while STS is stopped. ``_scan_tree`` treats a directory whose name
is not the class as absent, and it does that by returning nothing. A process
started on the wrong side of this rename — an old STS after the directories
moved, or a new one before they have — loads an empty registry and does not
say why.

``pulled/`` is removed rather than renamed. Those trees are copies. The next
``connect`` fetches them again, under the class name the peer now publishes.
"""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path

from mftik.registry.errors import RegistryError
from mftik.registry.files import normalize_files, read_tree
from mftik.registry.gate import check_files
from mftik.registry.inspect import check_type, pick_class
from mftik.registry.store import RegistryStore


@dataclass(frozen=True, slots=True)
class RegistryMigration:
    """What one successful run changed. Both tuples are empty when it was a no-op."""

    renamed: tuple[tuple[str, str], ...]
    removed_pulled: tuple[str, ...]


def class_type_of(dest: Path) -> str:
    """The class name in ``dest``, without asking the directory to match it."""
    try:
        files = read_tree(dest)
    except OSError as exc:
        raise RegistryError(f"cannot read {dest}: {exc}") from exc
    if not files:
        raise RegistryError(f"{dest} has no strategy files")
    normalised = normalize_files(files)
    chosen = pick_class(check_files(normalised))
    check_type(chosen.type)
    return chosen.type


def migrate_registry(data_dir: str | Path) -> RegistryMigration:
    """Rename ``public/`` and ``private/`` trees. Delete ``pulled/``.

    Refuses the whole batch when two trees in one origin would land on
    directory names that compare equal ignoring case, or when a tree cannot
    be read. Nothing is renamed and ``pulled/`` stays until that is clean.
    """
    store = RegistryStore(data_dir)
    problems: list[str] = []
    moves: list[tuple[Path, Path]] = []
    for root in (store.public_dir, store.private_dir):
        moves.extend(_plan_root(root, problems))
    if problems:
        raise RegistryError(
            "registry migration refused:\n" + "\n".join(problems)
        )
    renamed = tuple(_rename_all(moves))
    removed = _drop_pulled(store.pulled_dir)
    return RegistryMigration(renamed=renamed, removed_pulled=tuple(removed))


def _plan_root(root: Path, problems: list[str]) -> list[tuple[Path, Path]]:
    if not root.is_dir():
        return []
    entries = _dir_names(root)
    planned: list[tuple[str, str]] = []
    for actual in entries:
        try:
            type_name = class_type_of(root / actual)
        except RegistryError as exc:
            problems.append(str(exc))
            continue
        planned.append((actual, type_name))
    if not _targets_ok(root, entries, planned, problems):
        return []
    return [
        (root / actual, root / type_name)
        for actual, type_name in planned
        if actual != type_name
    ]


def _targets_ok(
    root: Path,
    entries: list[str],
    planned: list[tuple[str, str]],
    problems: list[str],
) -> bool:
    """Record collisions. False means this root must not be renamed."""
    ok = True
    by_fold: dict[str, list[str]] = {}
    sources = {actual: type_name for actual, type_name in planned}
    for actual, type_name in planned:
        by_fold.setdefault(type_name.casefold(), []).append(
            f"{actual} -> {type_name}"
        )
    for group in by_fold.values():
        if len(group) > 1:
            problems.append(
                f"{root}: class names collide ignoring case: {', '.join(group)}"
            )
            ok = False
    for actual, type_name in planned:
        if actual == type_name:
            continue
        for name in entries:
            if name == actual or name.casefold() != type_name.casefold():
                continue
            dest = sources.get(name)
            if dest is None or dest.casefold() == type_name.casefold():
                problems.append(
                    f"{root / actual} would become {type_name!r}, which "
                    f"collides with {name!r}"
                )
                ok = False
    return ok


def _rename_all(moves: list[tuple[Path, Path]]) -> list[tuple[str, str]]:
    """Park every source under a temp name, then land it on the class name.

    A rename that only changes case has to leave the original name first.
    On a case-insensitive volume ``tiny`` and ``Tiny`` are one directory, and
    ``rename`` onto the name it already has does not change the stored case.
    Moving everything aside first also breaks a cycle where one's target is
    the other's current name.
    """
    parked: list[tuple[Path, Path, str]] = []
    for src, dst in moves:
        tmp = src.with_name(f".tmp-migrate-{os.getpid()}-{src.name}")
        src.rename(tmp)
        parked.append((tmp, dst, str(src)))
    renamed: list[tuple[str, str]] = []
    for tmp, dst, old in parked:
        if _casefold_taken(dst.parent, dst.name):
            raise RegistryError(
                f"cannot rename {old} to {dst.name}: that name appeared "
                "while the migration was moving trees"
            )
        tmp.rename(dst)
        renamed.append((old, str(dst)))
    return renamed


def _casefold_taken(root: Path, type_name: str) -> bool:
    folded = type_name.casefold()
    for entry in os.listdir(root):
        if entry.startswith("."):
            continue
        if entry.casefold() == folded and (root / entry).is_dir():
            return True
    return False


def _drop_pulled(pulled: Path) -> list[str]:
    if not pulled.is_dir():
        return []
    removed: list[str] = []
    for entry in sorted(os.listdir(pulled)):
        path = pulled / entry
        if entry.startswith(".") or not path.is_dir():
            continue
        shutil.rmtree(path)
        removed.append(entry)
    return removed


def _dir_names(root: Path) -> list[str]:
    return sorted(
        entry
        for entry in os.listdir(root)
        if not entry.startswith(".") and (root / entry).is_dir()
    )
