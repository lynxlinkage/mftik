"""Static import check — the registry copies source, not a venv.

Every node already has the stdlib and the MFTIK SDK (``mftik``, ``mftik_sts``).
A third-party import must be declared on a ``Strategy`` subclass as
``requires``; the destination node's applied extras decide whether it is
actually present. Dynamic imports (``importlib``, ``__import__``) are
refused because this scan would not see the module they load.

``check_files`` is two-pass: it collects every subclass and the union of
their ``requires`` before it judges imports. A helper file that imports
``numpy`` is legal when a later file's class declared it.

Relative imports and other files in the same tree are allowed: they travel
with the copy.
"""

from __future__ import annotations

import ast
import sys
from collections.abc import Mapping
from dataclasses import dataclass, field

from mftik.registry.errors import RegistryError

#: Top-level packages the host node provides. Shadowing them with a local
#: file would make a pull load the strategy's copy instead of the SDK.
HOST_PACKAGES = frozenset({"mftik", "mftik_sts"})
_DYNAMIC_MODULES = frozenset({"importlib", "runpy"})
_STDLIB = frozenset(sys.stdlib_module_names) | {"__future__"}
#: Import names the node itself answers to. A strategy may not declare one
#: in ``requires`` and the Owner may not install an extra under one: the
#: overlay sits ahead of both on ``sys.path``, so a distribution named
#: ``json`` or ``mftik`` would shadow the real module for every session in
#: the process. Both refusals read this one set.
PROVIDED_BY_NODE = _STDLIB | HOST_PACKAGES
#: Where ``Strategy`` may be imported from. ``mftik.strategy`` is where the
#: class lives and what a strategy written against the installed package
#: imports; ``mftik_sts.strategy`` is where it lived, and what every tree
#: already on disk — this node's and every copy pulled from a peer — still
#: says. A scan that knew only one of them would stop recognising half the
#: strategies in existence as strategies.
_STRATEGY_MODULES = frozenset({"mftik.strategy", "mftik_sts.strategy"})
_STRATEGY_ATTR = "Strategy"


@dataclass
class StrategyClass:
    """One ``Strategy`` subclass found in the tree."""

    type: str
    filename: str
    name: str | None
    requires_mftik: str | None = None
    requires: tuple[str, ...] = ()


@dataclass
class _ImportState:
    #: Local name → fully-qualified module (``import mftik_sts.strategy as st``).
    modules: dict[str, str] = field(default_factory=dict)
    #: Local names that refer to ``mftik_sts.strategy.Strategy``.
    strategy_aliases: set[str] = field(default_factory=set)


def check_files(files: Mapping[str, bytes]) -> list[StrategyClass]:
    """Validate imports and return every ``Strategy`` subclass.

    Two passes: parse and collect subclasses (and the union of their
    ``requires``), then judge every import against that union. Raises
    :class:`RegistryError` on the first disallowed import, dynamic load,
    or unreadable Python. An empty list means no subclass was found —
    the caller turns that into a refusal.
    """
    local_modules = _local_modules(files)
    for host in sorted(HOST_PACKAGES):
        if host in local_modules:
            raise RegistryError(
                f"{host!r} is provided by the node — do not ship a local "
                f"module of that name"
            )
    parsed = _parse_trees(files)
    found: list[StrategyClass] = []
    for path, tree in parsed:
        state = _ImportState()
        _walk(
            tree,
            path=path,
            package=_package_of(path),
            local=local_modules,
            state=state,
            extras=None,
        )
        found.extend(_strategy_classes(tree, path=path, state=state))
    extras = _declared_extras(found, local=local_modules)
    for path, tree in parsed:
        state = _ImportState()
        _walk(
            tree,
            path=path,
            package=_package_of(path),
            local=local_modules,
            state=state,
            extras=extras,
        )
    return found


def _parse_trees(files: Mapping[str, bytes]) -> list[tuple[str, ast.Module]]:
    parsed: list[tuple[str, ast.Module]] = []
    for path in sorted(p for p in files if p.endswith(".py")):
        try:
            source = files[path].decode("utf-8")
        except UnicodeDecodeError as exc:
            raise RegistryError(f"{path} is not UTF-8: {exc}") from exc
        try:
            tree = ast.parse(source, filename=path)
        except SyntaxError as exc:
            raise RegistryError(f"{path} is not valid Python: {exc}") from exc
        parsed.append((path, tree))
    return parsed


def _declared_extras(
    found: list[StrategyClass], *, local: set[str]
) -> frozenset[str]:
    extras: set[str] = set()
    for cls in found:
        extras.update(cls.requires)
    for name in sorted(extras):
        if not name.isidentifier():
            raise RegistryError(
                f"requires entry {name!r} is not a Python identifier — "
                f"use the import name (sklearn), not the dist name "
                f"(scikit-learn)"
            )
        if name in PROVIDED_BY_NODE:
            # The overlay goes on the front of ``sys.path``, so an extra
            # installed under a stdlib or SDK name would shadow it for the
            # whole STS process — and the import was already allowed anyway.
            raise RegistryError(
                f"requires entry {name!r} is already provided by the node — "
                f"list only third-party import names"
            )
        if name in local:
            raise RegistryError(
                f"{name!r} is a file in this tree — do not list it in "
                f"requires; that would shadow the node's extra of the "
                f"same name"
            )
    return frozenset(extras)


def _local_modules(files: Mapping[str, bytes]) -> set[str]:
    """Dotted module names implied by ``.py`` paths, including parents."""
    modules: set[str] = set()
    for path in files:
        if not path.endswith(".py"):
            continue
        parts = path[: -len(".py")].split("/")
        if parts[-1] == "__init__":
            parts = parts[:-1]
        if not parts or parts == [""]:
            continue
        for i in range(len(parts)):
            modules.add(".".join(parts[: i + 1]))
    return modules


def _package_of(path: str) -> str:
    """The package a relative import is resolved against (``__package__``)."""
    parts = path[: -len(".py")].split("/")
    parts = parts[:-1]
    return ".".join(p for p in parts if p)


def _walk(
    tree: ast.AST,
    *,
    path: str,
    package: str,
    local: set[str],
    state: _ImportState,
    extras: frozenset[str] | None,
) -> None:
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.If) and _is_type_checking(node.test):
            continue
        if isinstance(node, ast.Import):
            for alias in node.names:
                _check_module(alias.name, path=path, local=local, extras=extras)
                if alias.asname:
                    state.modules[alias.asname] = alias.name
                else:
                    top = alias.name.split(".", 1)[0]
                    state.modules[top] = top
            continue
        if isinstance(node, ast.ImportFrom):
            _handle_import_from(
                node,
                path=path,
                package=package,
                local=local,
                state=state,
                extras=extras,
            )
            continue
        if isinstance(node, ast.Call) and _is_dynamic_import(node):
            raise RegistryError(
                f"{path}: dynamic import is refused — the scan cannot see "
                f"what it loads"
            )
        _walk(
            node,
            path=path,
            package=package,
            local=local,
            state=state,
            extras=extras,
        )


def _handle_import_from(
    node: ast.ImportFrom,
    *,
    path: str,
    package: str,
    local: set[str],
    state: _ImportState,
    extras: frozenset[str] | None,
) -> None:
    if node.level:
        try:
            module = _resolve_relative(package, node.module, node.level)
        except ValueError as exc:
            raise RegistryError(f"{path}: {exc}") from exc
        if module not in local and not any(
            item.startswith(module + ".") for item in local
        ):
            raise RegistryError(
                f"{path}: relative import {module!r} is not in this strategy"
            )
        _bind_from(node, module=module, state=state)
        return
    if not node.module:
        raise RegistryError(f"{path}: from-import is missing a module")
    _check_module(node.module, path=path, local=local, extras=extras)
    _bind_from(node, module=node.module, state=state)


def _bind_from(node: ast.ImportFrom, *, module: str, state: _ImportState) -> None:
    for alias in node.names:
        bound = alias.asname or alias.name
        if module in _STRATEGY_MODULES and alias.name == _STRATEGY_ATTR:
            state.strategy_aliases.add(bound)
        else:
            state.modules[bound] = f"{module}.{alias.name}"


def _resolve_relative(package: str, module: str | None, level: int) -> str:
    """Resolve like Python: dots are relative to ``__package__``, not ``__name__``."""
    if not package:
        raise ValueError(
            "relative import needs a package — put the files in a directory "
            "with __init__.py, or import the sibling by name"
        )
    parts = package.split(".")
    up = level - 1
    if up > len(parts):
        raise ValueError(
            "relative import goes beyond the top of this strategy — "
            "put the files in a package (directory with __init__.py), "
            "or import a sibling by name"
        )
    if up:
        parts = parts[:-up]
    if module:
        parts.extend(module.split("."))
    resolved = ".".join(p for p in parts if p)
    if not resolved:
        raise ValueError(
            "relative import goes beyond the top of this strategy — "
            "put the files in a package (directory with __init__.py), "
            "or import a sibling by name"
        )
    return resolved


def _check_module(
    module: str,
    *,
    path: str,
    local: set[str],
    extras: frozenset[str] | None,
) -> None:
    top = module.split(".", 1)[0]
    if module in local or top in local:
        return
    if top in _DYNAMIC_MODULES:
        raise RegistryError(
            f"{path}: {module!r} is refused — dynamic imports cannot be scanned"
        )
    if top in _STDLIB:
        return
    if top in HOST_PACKAGES:
        return
    if extras is None:
        return
    if top in extras:
        return
    raise RegistryError(
        f"{path}: import {module!r} is not allowed — a strategy may use the "
        f"stdlib, mftik, mftik_sts, files in this tree, or a name listed "
        f"in requires"
    )


def _is_type_checking(test: ast.expr) -> bool:
    if isinstance(test, ast.Name) and test.id == "TYPE_CHECKING":
        return True
    return (
        isinstance(test, ast.Attribute)
        and test.attr == "TYPE_CHECKING"
        and isinstance(test.value, ast.Name)
        and test.value.id == "typing"
    )


def _is_dynamic_import(node: ast.Call) -> bool:
    func = node.func
    if isinstance(func, ast.Name) and func.id == "__import__":
        return True
    if isinstance(func, ast.Name) and func.id == "import_module":
        return True
    return (
        isinstance(func, ast.Attribute)
        and func.attr == "import_module"
        and isinstance(func.value, ast.Name)
        and func.value.id == "importlib"
    )


def _strategy_classes(
    tree: ast.Module, *, path: str, state: _ImportState
) -> list[StrategyClass]:
    found: list[StrategyClass] = []
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        if not any(_is_strategy_base(base, state) for base in node.bases):
            continue
        found.append(
            StrategyClass(
                type=node.name,
                filename=path,
                name=_class_str_attr(node, "name"),
                requires_mftik=_class_str_attr(node, "requires_mftik"),
                requires=_class_str_seq(node, "requires"),
            )
        )
    return found


def _is_strategy_base(base: ast.expr, state: _ImportState) -> bool:
    if isinstance(base, ast.Name):
        return base.id in state.strategy_aliases
    if isinstance(base, ast.Attribute) and base.attr == _STRATEGY_ATTR:
        return _expr_module(base.value, state) in _STRATEGY_MODULES
    return False


def _expr_module(node: ast.expr, state: _ImportState) -> str | None:
    if isinstance(node, ast.Name):
        if node.id in state.modules:
            return state.modules[node.id]
        return node.id
    if isinstance(node, ast.Attribute):
        parent = _expr_module(node.value, state)
        if parent is None:
            return None
        return f"{parent}.{node.attr}"
    return None


def _assignment_value(node: ast.ClassDef, attr: str) -> ast.expr | None:
    for stmt in node.body:
        target: ast.expr | None = None
        value: ast.expr | None = None
        if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1:
            target, value = stmt.targets[0], stmt.value
        elif isinstance(stmt, ast.AnnAssign) and stmt.value is not None:
            target, value = stmt.target, stmt.value
        if target is None or value is None:
            continue
        if not isinstance(target, ast.Name) or target.id != attr:
            continue
        return value
    return None


def _class_str_attr(node: ast.ClassDef, attr: str) -> str | None:
    value = _assignment_value(node, attr)
    if value is None:
        return None
    if isinstance(value, ast.Constant) and isinstance(value.value, str):
        return value.value
    return None


def _class_str_seq(node: ast.ClassDef, attr: str) -> tuple[str, ...]:
    value = _assignment_value(node, attr)
    if value is None:
        return ()
    if isinstance(value, ast.Constant) and isinstance(value.value, str):
        return (value.value,)
    if isinstance(value, (ast.Tuple, ast.List)):
        names: list[str] = []
        for elt in value.elts:
            if not isinstance(elt, ast.Constant) or not isinstance(elt.value, str):
                raise RegistryError(
                    f"{attr} must be a string or a sequence of strings"
                )
            names.append(elt.value)
        return tuple(names)
    raise RegistryError(f"{attr} must be a string or a sequence of strings")
