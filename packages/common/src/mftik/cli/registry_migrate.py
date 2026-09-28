"""Rename on-disk registry trees onto their class name.

Offline, and only safe while STS is not running. The scan that loads a tree
drops a directory whose name is not the class, without an error. Starting
either build against the other layout looks like the registry is empty.
"""

from __future__ import annotations

import argparse
import os

from mftik.cli.client import CliError
from mftik.registry.errors import RegistryError
from mftik.registry.migrate import migrate_registry
from mftik.registry.store import DATA_ENV, DEFAULT_DATA_DIR


def registry_migrate(args: argparse.Namespace) -> int:
    raw = args.data or os.getenv(DATA_ENV, DEFAULT_DATA_DIR)
    data = raw.strip() or DEFAULT_DATA_DIR
    try:
        result = migrate_registry(data)
    except RegistryError as exc:
        raise CliError(str(exc)) from exc
    if not result.renamed and not result.removed_pulled:
        print(f"{data}: registry already uses class names")
        return 0
    for old, new in result.renamed:
        print(f"renamed {old} -> {new}")
    for name in result.removed_pulled:
        print(f"removed pulled/{name} — connect again to fetch it")
    return 0
