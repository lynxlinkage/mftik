"""Delete a strategy tree from a node's own registry.

The files leave disk in a different process from the one that imported
them. ``DELETE /registry/v1/strategies/{name}`` tells STS to reload and
answers ``unloaded`` so this command can say whether a deploy would still
resolve the type — removing the tree is only half of a delete.
"""

from __future__ import annotations

import argparse
from typing import Any
from urllib.parse import quote

from mftik.cli.client import Client, CliError, connected


def rm(args: argparse.Namespace) -> int:
    _, client = connected(args.profile)
    with client:
        report_rm(rm_strategy(client, args.name, origin=args.origin))
    return 0


def rm_strategy(client: Client, name: str, *, origin: str) -> dict[str, Any]:
    """``DELETE /registry/v1/strategies/{name}`` for one of this node's trees."""
    return client.delete(
        f"/registry/v1/strategies/{quote(name, safe='')}",
        params={"origin": origin},
    )


def report_rm(out: dict[str, Any]) -> dict[str, Any]:
    """Print what was deleted. Raises if STS still answers to it."""
    print(
        f"removed {out.get('name')} type={out.get('type')} "
        f"origin={out.get('origin', 'private')}"
    )
    digest = out.get("digest")
    if digest:
        print(f"    {digest}")
    if out.get("unloaded"):
        return out
    raise CliError(out.get("unload_error") or "deleted but still deployable")
