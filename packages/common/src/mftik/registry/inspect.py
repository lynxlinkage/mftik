"""What a tree must be before it is a strategy.

The import gate says whether the files are copyable. These rules say whether
they name a strategy the registry can store: one subclass, and a Python
identifier for that class. The class name is the tree's identity — the
directory it is stored under, and the second half of its qualified key.
A ``name = "..."`` attribute on the class is ignored. A shipped
``strategy.yml`` must parse. ``add`` and ``mftik check`` both run this, so a
tree that one accepts the other cannot refuse for a different reason.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass

from mftik.protocol.strategy_yml import StrategyYamlError, parse_strategy_yml
from mftik.registry.errors import RegistryError
from mftik.registry.files import TEMPLATE_NAME, normalize_files
from mftik.registry.gate import StrategyClass, check_files

_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
_TYPE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True, slots=True)
class Inspected:
    """A tree that passed the gate and the naming rules."""

    files: dict[str, bytes]
    cls: StrategyClass
    #: The class name. This is the directory key, same string as ``cls.type``.
    name: str


def inspect_files(files: Mapping[str, str | bytes]) -> Inspected:
    """Normalise, scan, and name-check. Raises :class:`RegistryError`."""
    normalised = normalize_files(files)
    _check_template(normalised)
    chosen = pick_class(check_files(normalised))
    check_type(chosen.type)
    return Inspected(files=normalised, cls=chosen, name=chosen.type)


def _check_template(files: Mapping[str, bytes]) -> None:
    """A shipped ``strategy.yml`` must parse. Absence is fine."""
    body = files.get(TEMPLATE_NAME)
    if body is None:
        return
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RegistryError(f"{TEMPLATE_NAME} is not UTF-8: {exc}") from exc
    try:
        parse_strategy_yml(text)
    except StrategyYamlError as exc:
        raise RegistryError(f"{TEMPLATE_NAME}: {exc}") from exc


def pick_class(classes: list[StrategyClass]) -> StrategyClass:
    if not classes:
        raise RegistryError(
            "no Strategy subclass found — import Strategy from "
            "mftik.strategy and subclass it"
        )
    if len(classes) > 1:
        known = ", ".join(sorted(c.type for c in classes))
        raise RegistryError(
            f"multiple Strategy subclasses ({known}) — one tree, one subclass"
        )
    return classes[0]


def check_name(name: str) -> None:
    """Origin and remote names. A tree is identified with :func:`check_type`."""
    if not _NAME.match(name):
        raise RegistryError(
            f"name {name!r} must be lowercase [a-z][a-z0-9_]* (e.g. node1)"
        )


def check_type(type_name: str) -> None:
    if not _TYPE.match(type_name):
        raise RegistryError(
            f"strategy type {type_name!r} must be a Python identifier"
        )
