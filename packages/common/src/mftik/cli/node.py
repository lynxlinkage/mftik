"""Write out a node you can host yourself.

The maintainers' own production compose is not this: it expects
Postgres and Redis to be substrate somebody else runs, and a standalone
Traefik to own :443 and route to it. Neither is true on a laptop, and neither
should have to be true to try the thing.

So the template here brings its own database, its own broker, and its own
edge. The edge is not optional — the browser asks for ``/api`` and opens
``ws://<this host>/ws`` on whatever host served the page, so something has to
put the API and the UI on one origin. Caddy does it in five lines; see the
Caddyfile this writes.

The templates ship inside the wheel rather than being fetched, so
``mftik node init`` works on a machine that cannot reach GitHub and cannot
hand you a compose file written for a different version of the images than
the client you are holding.
"""

from __future__ import annotations

import argparse
import os
import re
import secrets
from importlib.resources import files
from pathlib import Path

from mftik.cli.client import CliError

#: Files copied out verbatim. ``.env`` is rendered instead — it has a
#: generated password in it and is written narrow.
_VERBATIM = ("docker-compose.yml", "Caddyfile")

#: A compose project name has to be a DNS label, and so does the container
#: prefix it becomes.
_PROJECT = re.compile(r"^[a-z0-9][a-z0-9_-]*$")


def node_init(args: argparse.Namespace) -> int:
    root = Path(args.path)
    project = _project_name(args.project or root.resolve().name)

    written = [root / name for name in (*_VERBATIM, ".env")]
    existing = [p for p in written if p.exists()]
    if existing and not args.force:
        listed = ", ".join(str(p) for p in existing)
        raise CliError(f"{listed} already exists — pass --force to overwrite")

    root.mkdir(parents=True, exist_ok=True)
    for name in _VERBATIM:
        (root / name).write_text(_template(name), encoding="utf-8")

    env_path = root / ".env"
    body = _template("env").format(
        version=args.tag,
        port=args.port,
        project=project,
        password=secrets.token_urlsafe(24),
    )
    # It holds the database password. Opened at 0600 and written into that
    # handle rather than created under the umask and narrowed after, which
    # leaves a window where it is world-readable — the same shape as the
    # profile store and the registry's remotes.toml.
    fd = os.open(env_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(body)
    os.chmod(env_path, 0o600)

    for path in written:
        print(f"created {path}")

    where = "" if str(root) in {".", ""} else f"cd {root} && "
    print(
        f"\n  {where}docker compose pull\n"
        f"  {where}docker compose run --rm migrate\n"
        f"  {where}docker compose run --rm seed\n"
        f"  {where}docker compose up -d\n"
        f"\n  mftik connect http://localhost:{args.port} --setup\n"
        f"  mftik init ./my-strategy\n"
        f"  mftik run ./my-strategy\n"
    )
    if args.tag == "latest":
        print(
            "The images are :latest, which moves under you on the next "
            "release.\nPin MFTIK_VERSION in .env once this node matters."
        )
    return 0


def _template(name: str) -> str:
    try:
        return (files("mftik.cli") / "templates" / name).read_text(
            encoding="utf-8"
        )
    except (FileNotFoundError, OSError) as exc:  # pragma: no cover
        raise CliError(
            f"the {name} template is missing from this install: {exc}"
        ) from exc


def _project_name(raw: str) -> str:
    name = re.sub(r"[^a-z0-9_-]+", "-", raw.strip().lower()).strip("-_")
    if not _PROJECT.match(name or ""):
        raise CliError(
            f"cannot make a compose project name out of {raw!r} — pass --project"
        )
    return name
