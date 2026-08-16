"""Import a registry tree as an isolated package and return its strategy class.

Each tree is loaded under ``_mft_reg_{source}_{name}_{pathhash}`` so two
strategies can both ship ``helpers.py`` without one clobbering the other
in ``sys.modules``. The directory is on ``sys.path`` only while that tree
is executing, which is what lets a flat ``from helpers import N`` resolve;
afterwards those top-level aliases are dropped.
"""

from __future__ import annotations

import hashlib
import importlib.machinery
import importlib.util
import sys
from pathlib import Path

from mft.registry.errors import RegistryError


def load_class(
    dest: Path,
    *,
    type_name: str,
    source: str,
    name: str,
) -> type:
    """Load ``dest`` and return the class named ``type_name``."""
    dest = dest.resolve()
    if not dest.is_dir():
        raise RegistryError(f"strategy tree does not exist: {dest}")
    # Path is in the package name so two tmp trees named ``tiny`` in tests —
    # and a replace on disk after a process restart — do not reuse a stale
    # module from ``sys.modules``.
    tag = hashlib.sha256(str(dest).encode()).hexdigest()[:12]
    pkg = f"_mft_reg_{source}_{name}_{tag}"
    if pkg not in sys.modules:
        _load_tree(dest, pkg)
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
