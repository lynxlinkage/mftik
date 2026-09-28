"""Rename on-disk registry trees onto their class name.

Offline, and only safe while STS is not running. The scan that loads a tree
drops a directory whose name is not the class, without an error. Starting
either build against the other layout looks like the registry is empty.

All or nothing: a run that fails leaves the registry as it was, and one that
is killed outright leaves trees parked under a temp name that the next run
recovers. Either way the command has to exit 0 before the new build starts.
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
    except OSError as exc:
        # A rename that the filesystem refused. The trees are back where they
        # were, and this is the sentence that says the deploy must not go on.
        raise CliError(f"{data}: registry migration failed: {exc}") from exc
    if not result.renamed and not result.removed_pulled and not result.recovered:
        print(f"{data}: registry already uses class names")
        return 0
    for old, new in result.recovered:
        print(f"recovered {old} -> {new}, left by an interrupted run")
    for old, new in result.renamed:
        print(f"renamed {old} -> {new}")
    for name in result.removed_pulled:
        print(f"removed pulled/{name} — connect again to fetch it")
    return 0
