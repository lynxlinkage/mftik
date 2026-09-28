"""Move own registry trees onto their class name, and drop pulled copies.

Run this while STS is stopped. ``_scan_tree`` treats a directory whose name
is not the class as absent, and it does that by returning nothing. A process
started on the wrong side of this rename — an old STS after the directories
moved, or a new one before they have — loads an empty registry and does not
say why.

``pulled/`` is removed rather than renamed. Those trees are copies. The next
``connect`` fetches them again, under the class name the peer now publishes.

The batch is all or nothing. A tree is parked under a dot-prefixed temp name
before it lands on the class name, and every listing skips dot entries — so a
run that stopped half way would not leave a registry half renamed, it would
leave one with strategies missing. Anything that fails puts every tree back
under the name it had, and a run that is killed outright is picked up by the
next one, which recovers what it finds parked before it plans anything.
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

#: What a tree is called while it is between its old name and its class name.
#: Dot-prefixed so nothing serves a tree mid-rename, and stamped with the pid
#: so the run that parked it is on the directory.
TMP_PREFIX = ".tmp-migrate-"


@dataclass(frozen=True, slots=True)
class RegistryMigration:
    """What one successful run changed. Every tuple is empty when it was a no-op."""

    renamed: tuple[tuple[str, str], ...]
    removed_pulled: tuple[str, ...]
    #: ``(temp path, restored path)`` for each tree an interrupted earlier run
    #: had left parked. Not a rename anyone asked for — it is the state that
    #: run was in the middle of, undone.
    recovered: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class _Parked:
    """One tree moved aside: where it was, where it is now, where it goes."""

    src: Path
    tmp: Path
    dst: Path


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

    Trees an interrupted earlier run left parked are put back first, before
    anything is planned. Planning around them instead would read a registry
    those strategies are absent from and report that it had nothing to do.
    """
    store = RegistryStore(data_dir)
    roots = (store.public_dir, store.private_dir)
    recovered = tuple(_unpark_leftovers(roots))
    problems: list[str] = []
    moves: list[tuple[Path, Path]] = []
    for root in roots:
        moves.extend(_plan_root(root, problems))
    if problems:
        raise RegistryError(
            "registry migration refused:\n" + "\n".join(problems)
        )
    renamed = tuple(_rename_all(moves))
    removed = _drop_pulled(store.pulled_dir)
    return RegistryMigration(
        renamed=renamed,
        removed_pulled=tuple(removed),
        recovered=recovered,
    )


def _unpark_leftovers(roots: tuple[Path, ...]) -> list[tuple[str, str]]:
    """Restore trees an earlier run parked and never landed.

    A process killed between the two phases leaves ``.tmp-migrate-<pid>-tiny``
    behind. Nothing lists it, so ``tiny`` is not half migrated — it is gone,
    and a rerun that ignored it would say the registry already uses class
    names. The original name is in the directory name, which is the only
    record of it anywhere.

    Refuses rather than guesses when that name cannot be read back, or when
    something else now holds it: the tree is still on disk either way, and a
    person moving it back knows which of the two is the one to keep.
    """
    restored: list[tuple[str, str]] = []
    for root in roots:
        if not root.is_dir():
            continue
        for entry in sorted(os.listdir(root)):
            if not entry.startswith(TMP_PREFIX):
                continue
            tmp = root / entry
            if not tmp.is_dir():
                continue
            original = _parked_name(entry)
            if original is None:
                raise RegistryError(
                    f"{tmp} was left behind by an interrupted migration and "
                    "does not say what it was called; rename it to the "
                    "class name by hand, then run this again"
                )
            taken = _taken_by(root, original)
            if taken is not None:
                raise RegistryError(
                    f"{tmp} was left behind by an interrupted migration, and "
                    f"its name {original!r} is now held by {taken!r}; decide "
                    "which tree to keep by hand, then run this again"
                )
            dest = root / original
            tmp.rename(dest)
            restored.append((str(tmp), str(dest)))
    return restored


def _parked_name(entry: str) -> str | None:
    """The directory name a temp entry was parked from, or None."""
    _, _, rest = entry[len(TMP_PREFIX) :].partition("-")
    return rest or None


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

    Anything that goes wrong in either phase puts every tree back under the
    name it had and re-raises. A batch that stopped part way through the
    second phase would leave the rest parked under dot-prefixed names that no
    listing shows, so "half renamed" reads as "some strategies are missing".
    """
    parked: list[_Parked] = []
    landed: list[_Parked] = []
    try:
        for src, dst in moves:
            tmp = src.with_name(f"{TMP_PREFIX}{os.getpid()}-{src.name}")
            src.rename(tmp)
            parked.append(_Parked(src=src, tmp=tmp, dst=dst))
        for item in parked:
            taken = _taken_by(item.dst.parent, item.dst.name)
            if taken is not None:
                raise RegistryError(
                    f"cannot rename {item.src} to {item.dst.name}: that name "
                    "appeared while the migration was moving trees"
                )
            item.tmp.rename(item.dst)
            landed.append(item)
    except BaseException as exc:
        # BaseException rather than Exception: a Ctrl-C between the phases is
        # the likeliest way this ends part way through, and it leaves exactly
        # the state the rollback exists to undo. A SIGKILL is out of reach —
        # the next run recovers that one.
        _roll_back(parked, landed, exc)
        raise
    return [(str(item.src), str(item.dst)) for item in landed]


def _roll_back(
    parked: list[_Parked], landed: list[_Parked], cause: BaseException
) -> None:
    """Put every tree back under the name it came in with.

    Two phases again, and for the same reason the migration has two: one
    tree's old name can be another's class name, so a tree that already
    landed goes back to its temp name before anything claims an old name.

    A rollback that cannot finish says so instead of letting the original
    error stand alone — what is on disk then matters more than what went
    wrong, and the next run recovers whatever is still parked.
    """
    failed: list[str] = []
    for item in landed:
        try:
            item.dst.rename(item.tmp)
        except OSError as exc:
            failed.append(f"{item.dst} could not go back to {item.tmp}: {exc}")
    for item in parked:
        if not item.tmp.is_dir():
            continue
        try:
            item.tmp.rename(item.src)
        except OSError as exc:
            failed.append(f"{item.tmp} could not go back to {item.src}: {exc}")
    if failed:
        raise RegistryError(
            f"registry migration failed ({cause}) and could not be undone:\n"
            + "\n".join(failed)
            + "\nthose trees are parked and no listing shows them; run this "
            "again to recover them"
        ) from cause


def _taken_by(root: Path, type_name: str) -> str | None:
    """The directory whose name casefolds to ``type_name``, if there is one."""
    folded = type_name.casefold()
    for entry in os.listdir(root):
        if entry.startswith("."):
            continue
        if entry.casefold() == folded and (root / entry).is_dir():
            return entry
    return None


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
