"""What is running, what it is saying, and how to stop it.

``ps`` / ``logs`` / ``stop`` act on sessions the node already has, so a
Ctrl-C out of ``logs -f`` drops the socket and leaves the session alone —
this command did not start it and has no business ending it.

``run`` shares :func:`follow_logs` and does *not* share that. It started the
session, it says so on attach, and its Ctrl-C stops it. The difference is
whose session it is, not what the socket does.
"""

from __future__ import annotations

import argparse

from mftik.cli.client import Client, connected
from mftik.cli.output import table


def list_sessions(args: argparse.Namespace) -> int:
    _, client = connected(args.profile)
    with client:
        body = client.get("/sts/sessions")
    sessions = body.get("sessions") or []
    if not sessions:
        print("no live sessions")
        return 0
    rows = [
        (
            str(row.get("session_id") or ""),
            str(row.get("strategy") or ""),
            str(row.get("status") or ""),
        )
        for row in sessions
    ]
    print(table(("SESSION", "STRATEGY", "STATUS"), rows))
    return 0


def stop_session(args: argparse.Namespace) -> int:
    _, client = connected(args.profile)
    with client:
        out = client.post(f"/sts/sessions/{args.session_id}/stop")
    print(
        f"stopped {out.get('session_id', args.session_id)} "
        f"status={out.get('status')}"
    )
    return 0


def logs(args: argparse.Namespace) -> int:
    _, client = connected(args.profile)
    with client:
        if args.follow:
            follow_logs(client, args.session_id)
            return 0
        body = client.get(f"/logs/sts/{args.session_id}")
    rows = list(body.get("logs") or [])
    # The API pages newest-first. A dump is read top to bottom in time.
    for row in reversed(rows):
        level = str(row.get("level") or "info")
        message = str(row.get("message") or "")
        print(f"{level}  {message}")
    return 0


def follow_logs(client: Client, session_id: str) -> None:
    """Tail ``/ws/sts/{session_id}`` until it closes or is interrupted.

    Ctrl-C propagates rather than being swallowed here, because what it should
    mean depends on who is calling: ``logs -f`` lets it end the command, and
    ``run`` turns it into a stop.
    """
    client.follow_sts_logs(session_id)
