"""Import a registry tree as an isolated package and return its strategy class.

Each tree is loaded under ``_mftik_reg_{source}_{name}_{tag}`` so two
strategies can both ship ``helpers.py`` without one clobbering the other
in ``sys.modules``. The directory is on ``sys.path`` only while that tree
is executing, which is what lets a flat ``from helpers import N`` resolve;
afterwards those top-level aliases are dropped.

The tag covers the tree's *contents* as well as its path. A path alone is
stable across an edit, so re-loading a strategy that had been replaced on
disk returned the module already in ``sys.modules`` — the old code, from a
directory that no longer held it. Nothing noticed, because the load
succeeded. Including the digest makes an edited tree a different module,
which is what it is.
"""

from __future__ import annotations

import hashlib
import importlib.machinery
import importlib.util
import sys
from pathlib import Path

from mftik.registry.errors import RegistryError


def load_class(
    dest: Path,
    *,
    type_name: str,
    source: str,
    name: str,
    digest: str | None = None,
) -> type:
    """Load ``dest`` and return the class named ``type_name``.

    ``digest`` is the tree's content hash, as :func:`mftik.registry.digest`
    computes it and the store records it. Pass it whenever you have one: it
    is what makes a replaced tree load its new code instead of the module a
    previous load left in ``sys.modules``. Omitting it keeps the old
    path-only behaviour, which is right for a caller loading a tree that
    cannot have changed under it — and wrong for anything reloading.
    """
    dest = dest.resolve()
    if not dest.is_dir():
        raise RegistryError(f"strategy tree does not exist: {dest}")
    # Path so two tmp trees named ``tiny`` in tests do not collide; digest so
    # the same path holding different source is a different module.
    seed = str(dest) if digest is None else f"{dest}\0{digest}"
    tag = hashlib.sha256(seed.encode()).hexdigest()[:12]
    pkg = f"_mftik_reg_{source}_{name}_{tag}"
    if pkg not in sys.modules:
        try:
            _load_tree(dest, pkg)
        except Exception:
            # A tree that failed because an extra was missing must be
            # retryable after the overlay appears. Leaving the half-imported
            # package in ``sys.modules`` would make every later load skip
            # ``_load_tree`` and look like the same permanent failure.
            _drop_package(pkg)
            raise
    return _class_from_package(pkg, type_name)


def _load_tree(dest: Path, pkg: str) -> None:
    spec = importlib.machinery.ModuleSpec(pkg, loader=None, is_package=True)
    spec.submodule_search_locations = [str(dest)]
    module = importlib.util.module_from_spec(spec)
    sys.modules[pkg] = module
    init_py = dest / "__init__.py"
    if init_py.is_file():
        init_spec = importlib.util.spec_from_file_location(
            pkg, init_py, submodule_search_locations=[str(dest)]
        )
        if init_spec is None or init_spec.loader is None:
            raise RegistryError(f"cannot import {init_py}")
        module = importlib.util.module_from_spec(init_spec)
        sys.modules[pkg] = module
        init_spec.loader.exec_module(module)

    before = set(sys.modules)
    sys.path.insert(0, str(dest))
    try:
        for py in _py_files(dest):
            mod_name = _mod_name(pkg, dest, py)
            if mod_name in sys.modules:
                continue
            file_spec = importlib.util.spec_from_file_location(mod_name, py)
            if file_spec is None or file_spec.loader is None:
                raise RegistryError(f"cannot import {py}")
            mod = importlib.util.module_from_spec(file_spec)
            sys.modules[mod_name] = mod
            file_spec.loader.exec_module(mod)
    finally:
        try:
            sys.path.remove(str(dest))
        except ValueError:
            pass
        for key in list(sys.modules):
            if key in before or key == pkg or key.startswith(pkg + "."):
                continue
            origin = getattr(sys.modules[key], "__file__", None)
            if origin is not None and _is_under(Path(origin), dest):
                sys.modules.pop(key, None)


def _py_files(dest: Path) -> list[Path]:
    files = [
        py
        for py in dest.rglob("*.py")
        if "__pycache__" not in py.parts
    ]
    # Parents before children so ``pkg/__init__.py`` lands before ``pkg/mod.py``.
    return sorted(files, key=lambda p: len(p.relative_to(dest).parts))


def _mod_name(pkg: str, dest: Path, py: Path) -> str:
    parts = list(py.relative_to(dest).with_suffix("").parts)
    if parts[-1] == "__init__":
        parts = parts[:-1]
    if not parts:
        return pkg
    return pkg + "." + ".".join(parts)


def _is_under(path: Path, dest: Path) -> bool:
    try:
        path.resolve().relative_to(dest)
    except ValueError:
        return False
    return True


def _drop_package(pkg: str) -> None:
    for key in list(sys.modules):
        if key == pkg or key.startswith(pkg + "."):
            sys.modules.pop(key, None)


def _class_from_package(pkg: str, type_name: str) -> type:
    found: list[type] = []
    for key, mod in list(sys.modules.items()):
        if key != pkg and not key.startswith(pkg + "."):
            continue
        for obj in vars(mod).values():
            if (
                isinstance(obj, type)
                and obj.__name__ == type_name
                and getattr(obj, "__module__", "").startswith(pkg)
            ):
                found.append(obj)
    if not found:
        raise RegistryError(
            f"class {type_name!r} was not found in loaded strategy {pkg}"
        )
    return found[0]
