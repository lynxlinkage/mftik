"""Copy a strategy tree into a node's private registry.

The files land on disk in a different process from the one that imports
them. ``POST /registry/v1/add`` tells STS to reload and answers ``loaded``
so this command can say whether a deploy would work — storing the tree is
only half of a push.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from mftik.cli.client import Client, CliError, connected
from mftik.cli.tree import inspect_tree, require_tree, tree_texts
from mftik.registry.files import TEMPLATE_NAME


def push(args: argparse.Namespace) -> int:
    root = require_tree(args.path)
    _, client = connected(args.profile)
    with client:
        report_push(push_tree(client, root))
    return 0


def push_tree(client: Client, root: Path) -> dict[str, Any]:
    """``POST /registry/v1/add`` for ``root``. Always private, always replace."""
    inspected = inspect_tree(root)
    return client.post(
        "/registry/v1/add",
        json_body={
            "files": tree_texts(inspected),
            "replace": True,
            "origin": "private",
        },
    )


def report_push(out: dict[str, Any]) -> dict[str, Any]:
    """Print what was stored. Raises if STS did not load it."""
    print(
        f"pushed {out.get('name')} type={out.get('type')} "
        f"origin={out.get('origin', 'private')}"
    )
    digest = out.get("digest")
    if digest:
        print(f"    {digest}")
    files = out.get("files") or []
    if TEMPLATE_NAME in files:
        print(f"    template {TEMPLATE_NAME}")
    if out.get("loaded"):
        return out
    raise CliError(out.get("load_error") or "stored but not deployable")
