"""``mftik env`` — the extras this node has applied, from a terminal.

The same surface as the Settings page, because it is the same endpoints. What
a terminal changes is the shape of the answer, not the rules: a version may be
left out and the node stamps what it resolved to; a dependency is approved at
the version already on disk; import previews before it installs.

Subcommands rather than flags on one command. ``add`` and ``rm`` are writes
that reinstall the whole overlay, ``deps`` is a read, and giving them one
parser would make the dangerous ones reachable by a typo in the safe one.
"""

from __future__ import annotations

import argparse
from typing import Any

from mftik.cli.client import Client, CliError, connected
from mftik.cli.output import table


def _get(client: Client) -> dict[str, Any]:
    return client.get("/environment")


def _report(env: dict[str, Any]) -> None:
    """The stamp, then anything that makes it not the whole story."""
    packages = env.get("packages") or {}
    generation = env.get("generation", 0)
    size = _bytes(env.get("bytes") or 0)
    print(f"generation {generation}  {len(packages)} extra(s)  {size}")

    if not env.get("abi_ok", True):
        print(
            f"    ABI MISMATCH: stamped for python "
            f"{_py(env.get('python'))} / {env.get('platform')}, running "
            f"{_py(env.get('runtime_python'))} / {env.get('runtime_platform')}."
        )
        print("    Extras count as absent until you apply again on this image.")
    if env.get("restart_required"):
        print("    RESTART REQUIRED: STS is still on an older generation.")
    if env.get("load_error"):
        print(f"    {env['load_error']}")

    if packages:
        rows = [
            (
                name,
                rec.get("version", ""),
                rec.get("dist", ""),
                rec.get("source", ""),
            )
            for name, rec in sorted(packages.items())
        ]
        print()
        print(table(("IMPORT", "VERSION", "DIST", "SOURCE"), rows))

    broken = env.get("broken") or []
    if broken:
        print()
        for row in broken:
            needs = ", ".join(row.get("requires") or ())
            print(
                f"no longer deployable: {row.get('origin')}::{row.get('type')} "
                f"needs {needs}"
            )


def _py(parts: Any) -> str:
    if isinstance(parts, list) and parts:
        return ".".join(str(p) for p in parts[:2])
    return "?"


def _bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n / (1024 * 1024):.1f} MB"


def show(args: argparse.Namespace) -> int:
    """What this node has applied."""
    _, client = connected(args.profile)
    with client:
        env = _get(client)
    if not (env.get("packages") or {}):
        print("no extras applied — this node is the image: stdlib and the SDK")
        if env.get("generation"):
            _report(env)
        return 0
    _report(env)
    return 0


def deps(args: argparse.Namespace) -> int:
    """What the resolver installed that nobody approved.

    Importable and undeclarable: ``requires`` is checked against the stamp, so
    a tree naming one of these is refused while the package sits on the very
    ``sys.path`` the refusal came from. ``NEEDED BY`` is what tells the one
    worth approving from the two levels of plumbing under it.
    """
    _, client = connected(args.profile)
    with client:
        env = _get(client)
    rows = [row for row in (env.get("installed") or []) if not row.get("approved")]
    if not rows:
        print("nothing unapproved on the overlay")
        return 0
    print(
        table(
            ("DIST", "VERSION", "NEEDED BY", "APPROVE AS"),
            [
                (
                    row.get("dist", ""),
                    row.get("version", ""),
                    ", ".join(row.get("needed_by") or ()) or "-",
                    row.get("suggested_name") or "(import name differs)",
                )
                for row in rows
            ],
        )
    )
    return 0


def add(args: argparse.Namespace) -> int:
    """Apply one extra. Omitting the version lets the node's resolver pick."""
    _, client = connected(args.profile)
    body: dict[str, Any] = {"name": args.name}
    if args.version:
        body["version"] = args.version
    if args.dist:
        body["dist"] = args.dist
    if args.force:
        body["force"] = True
    with client:
        env = client.post("/environment/packages", json_body=body)
    rec = (env.get("packages") or {}).get(args.name, {})
    print(
        f"applied {args.name}=={rec.get('version', '?')} "
        f"(generation {env.get('generation')})"
    )
    _after_write(env)
    return 0


def approve(args: argparse.Namespace) -> int:
    """Stamp a dependency that is already installed, at the version installed.

    A no-op for the installer — the files are there — so nothing is
    reinstalled and no live session is disturbed. What it changes is that a
    strategy may now name it in ``requires``.
    """
    _, client = connected(args.profile)
    with client:
        env = _get(client)
        row = _unapproved_row(env, args.dist)
        name = args.name or row.get("suggested_name")
        if not name:
            raise CliError(
                f"{row['dist']} does not give a usable import name — pass one, "
                f"e.g. mftik env approve {row['dist']} --name dateutil"
            )
        env = client.post(
            "/environment/packages",
            json_body={
                "name": name,
                "version": row["version"],
                "dist": row["dist"],
                "source": "dependency",
                **({"force": True} if args.force else {}),
            },
        )
    print(f"approved {name}=={row['version']} (dist {row['dist']})")
    _after_write(env)
    return 0


def _unapproved_row(env: dict[str, Any], dist: str) -> dict[str, Any]:
    installed = env.get("installed") or []
    for row in installed:
        if row.get("dist") == dist:
            if row.get("approved"):
                raise CliError(f"{dist} is already an approved extra")
            return row
    raise CliError(f"{dist} is not on this node's overlay — see: mftik env deps")


def rm(args: argparse.Namespace) -> int:
    """Remove an extra. Prints the trees that stop being deployable."""
    _, client = connected(args.profile)
    path = f"/environment/packages/{args.name}"
    if args.force:
        path += "?force=true"
    with client:
        env = client.delete(path)
    print(f"removed {args.name} (generation {env.get('generation')})")
    _after_write(env)
    return 0


def _after_write(env: dict[str, Any]) -> None:
    """What the operator has to do next, if anything."""
    if env.get("restart_required"):
        print(
            "    restart the STS container: a package it had already imported "
            "moved, and no reload evicts it"
        )
    if not env.get("loaded", True):
        print(f"    {env.get('load_error') or 'STS did not reload'}")
    for row in env.get("broken") or []:
        needs = ", ".join(row.get("requires") or ())
        print(
            f"    no longer deployable: {row.get('origin')}::{row.get('type')} "
            f"needs {needs}"
        )


def import_from_peer(args: argparse.Namespace) -> int:
    """Preview a peer's extras, and install them only when told twice.

    Without ``--confirm`` this reads the peer's ``/info`` and prints the diff.
    The names come from another node; installing them straight off its say-so
    would let a typosquat onto this node's trading ``sys.path``.
    """
    _, client = connected(args.profile)
    body: dict[str, Any] = {"url": args.url}
    if args.token:
        body["token"] = args.token
    if args.dist:
        body["dist"] = dict(_dist_pair(item) for item in args.dist)
    if args.force:
        body["force"] = True
    if args.confirm:
        body["confirm"] = True
    with client:
        out = client.post("/environment/import", json_body=body)

    added = out.get("added") or []
    kept = out.get("kept") or []
    conflicts = out.get("conflicts") or []
    if added or kept or conflicts:
        print(
            table(
                ("NAME", "STATUS", "VERSION", "DIST", "NOTE"),
                [
                    (
                        row.get("name", ""),
                        row.get("status", ""),
                        row.get("version") if row.get("pinned", True) else "-",
                        row.get("dist", ""),
                        _row_note(row),
                    )
                    for row in [*added, *kept, *conflicts]
                ],
            )
        )
    else:
        print("this peer advertises no extras")

    if out.get("applied"):
        env = out.get("environment") or {}
        print(f"\napplied — generation {env.get('generation')}")
        _after_write(env)
        return 0
    if not args.confirm and added:
        _print_blockers(out)
        print("\nnothing installed — re-run with --confirm to install")
    return 0


def _row_note(row: dict[str, Any]) -> str:
    if not row.get("pinned", True):
        return "peer sent no version — needs their registry key"
    if row.get("guessed"):
        return f"dist guessed; set with --dist {row.get('name')}=<pypi-name>"
    if row.get("status") == "conflict":
        return f"this node has {row.get('local_version')}"
    return ""


def _print_blockers(out: dict[str, Any]) -> None:
    if out.get("unpinned"):
        print(
            f"\n{', '.join(out['unpinned'])}: the peer published names without "
            "versions. Ask that node for a registry key and pass --token."
        )
    if out.get("guessed"):
        print(
            f"\n{', '.join(out['guessed'])}: the PyPI name was assumed from the "
            "import name. Correct it with --dist before confirming."
        )
    if out.get("conflicts"):
        names = ", ".join(row.get("name", "") for row in out["conflicts"])
        print(
            f"\n{names}: this node has a different version applied. Change it "
            "here first — importing must not move a pin under a running strategy."
        )


def _dist_pair(item: str) -> tuple[str, str]:
    name, _, dist = item.partition("=")
    if not name or not dist:
        raise CliError(f"--dist wants name=pypi-name, got {item!r}")
    return name, dist
