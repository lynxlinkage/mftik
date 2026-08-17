"""The nodes this machine knows about."""

from __future__ import annotations

import argparse

from mftik.cli import config
from mftik.cli.output import table


def list_profiles(args: argparse.Namespace) -> int:
    """Every connected node, with the default marked."""
    loaded = config.load()
    if not loaded.profiles:
        print("no nodes connected — run: mftik connect <url>")
        return 0

    rows = []
    for name in sorted(loaded.profiles):
        profile = loaded.profiles[name]
        rows.append(
            (
                "*" if name == loaded.default else "",
                name,
                profile.url,
                "key" if profile.token else "none",
            )
        )
    print(table(("", "NAME", "URL", "AUTH"), rows))
    return 0


def disconnect(args: argparse.Namespace) -> int:
    """Forget a node, and the key it issued.

    The key is only forgotten here — the node still has the row. Revoking it
    there is a separate act, and one this cannot do on a node it has just
    dropped the credential for, so it says so rather than implying otherwise.
    """
    dropped = config.drop(args.name)
    print(f"disconnected {dropped.name} ({dropped.url})")
    if dropped.token:
        print(
            "The key is gone from this machine but still live on the node — "
            "revoke it there if it should stop working."
        )
    return 0
