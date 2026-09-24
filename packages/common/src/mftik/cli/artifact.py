"""``mftik artifact`` — one STS's uploaded objects, from a terminal.

The same routes as the Artifact page. ``put`` sends the laptop file as one
HTTP body; the API slices it. ``ls`` is the uploaded tree, and ``put`` and
``rm`` refuse a key under ``sessions/``: that tree belongs to a running
session, and a key the catalog does not list is not one an operator deletes.
"""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Any

from mftik.cli.client import Client, CliError, connected
from mftik.cli.output import table

#: A checkpoint is not a request. The default client timeout is for a JSON
#: round trip; this one is for the body actually leaving the laptop.
_PUT_TIMEOUT_S = 3600.0


def _bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n / (1024 * 1024):.1f} MB"


def _when(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def _catalog_key(path: str) -> str:
    """A key an operator may put or remove.

    ``sessions/`` is written by a strategy. Listing hides it, so removing it
    from here would delete a running session's file with nothing left to show
    that it is gone.
    """
    if path == "sessions" or path.startswith("sessions/"):
        raise CliError(
            f"{path}: a key under sessions/ belongs to a session; "
            "the catalog cannot put or remove it"
        )
    return path


def _instance(client: Client, named: str | None) -> str | None:
    """The STS this verb is aimed at.

    One declared STS is that one. Several have no default disk: ``sts-jp``
    and ``sts-tw`` do not share a volume, and a put with no instance has no
    single place to land.
    """
    if named:
        return named
    body = client.get("/instances", params={"domain": "sts"})
    names = [
        str(row["name"])
        for row in (body.get("instances") or [])
        if isinstance(row, dict) and row.get("name")
    ]
    if len(names) > 1:
        raise CliError(
            "more than one STS is declared; name one with --instance: "
            + ", ".join(names)
        )
    return names[0] if names else None


def _params(instance: str | None, path: str | None = None) -> dict[str, str]:
    params: dict[str, str] = {}
    if instance:
        params["instance"] = instance
    if path is not None:
        params["path"] = path
    return params


def show(args: argparse.Namespace) -> int:
    """The uploaded objects on one STS. ``sessions/`` is not in this list."""
    _, client = connected(args.profile)
    with client:
        instance = _instance(client, args.instance)
        body = client.get("/sts/artifacts", params=_params(instance))
    rows = body.get("objects") or []
    if not rows:
        where = instance or "the shared STS"
        print(f"no artifacts on {where}")
        return 0
    print(
        table(
            ("PATH", "SIZE", "MTIME", "DIGEST"),
            (
                (
                    row.get("path", ""),
                    _bytes(int(row.get("size") or 0)),
                    _when(float(row.get("mtime") or 0)),
                    row.get("digest", ""),
                )
                for row in rows
            ),
        )
    )
    return 0


def put(args: argparse.Namespace) -> int:
    """Replace one key with a file from this machine."""
    key = _catalog_key(args.key)
    local = Path(args.file)
    if not local.is_file():
        raise CliError(f"{local}: not a file")
    _, client = connected(args.profile, timeout=_PUT_TIMEOUT_S)
    with client, local.open("rb") as handle:
        instance = _instance(client, args.instance)
        landed: dict[str, Any] = client.put(
            "/sts/artifacts",
            params=_params(instance, key),
            content=handle,
        )
    where = landed.get("instance") or instance or ""
    suffix = f" on {where}" if where else ""
    print(
        f"{landed.get('path', key)}  {_bytes(int(landed.get('size') or 0))}  "
        f"{landed.get('digest', '')}{suffix}"
    )
    return 0


def remove(args: argparse.Namespace) -> int:
    """Remove one uploaded key."""
    key = _catalog_key(args.key)
    _, client = connected(args.profile)
    with client:
        instance = _instance(client, args.instance)
        client.delete("/sts/artifacts", params=_params(instance, key))
    where = f" on {instance}" if instance else ""
    print(f"removed {key}{where}")
    return 0
