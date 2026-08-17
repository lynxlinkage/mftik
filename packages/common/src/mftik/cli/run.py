"""Push, deploy, and tail. The loop a person is in when they edit a strategy.

A separate push is the step they forget, so this does it unless asked not to.

Ctrl-C stops the session. That is the whole reason this command reads the way
it does: a strategy is placing orders, somebody is watching it in the
foreground, and the key they reach for when they want it to stop has to stop
it. Detaching instead would leave a live position behind on the strength of a
keystroke that every other program treats as "end this". A second Ctrl-C, once
the stop is already going out, leaves the session running and says so loudly —
that is the escape hatch, and it is the one that has to be typed twice.

``--no-follow`` never attaches, so it never stops anything either; it prints
the session id and how to end it.
"""

from __future__ import annotations

import argparse

from mftik.cli.client import Client, CliError, connected
from mftik.cli.exits import EXIT_INTERRUPTED
from mftik.cli.push import push_tree, report_push
from mftik.cli.sessions import follow_logs
from mftik.cli.tree import inspect_tree, read_yaml, require_tree
from mftik.protocol.strategy_yml import StrategyYamlError, parse_strategy_yml
from mftik.registry.qualify import PRIVATE_ORIGIN, qualify

#: What a deploy answers when the session is up. Anything else means the
#: strategy refused its configuration or finished during ``on_start``, and
#: there is no live log to attach to.
_LIVE = "live"


def run(args: argparse.Namespace) -> int:
    root = require_tree(args.path)
    inspected = inspect_tree(root)
    yaml_text = read_yaml(args.cfg, root)
    # Parsed here as well as there. The node would refuse the same document,
    # but only after the tree has been copied into its registry — and a push
    # that lands for a deploy that cannot is a confusing half-step.
    try:
        parse_strategy_yml(yaml_text)
    except StrategyYamlError as exc:
        raise CliError(str(exc)) from exc

    key = qualify(PRIVATE_ORIGIN, inspected.cls.type)
    _, client = connected(args.profile)
    with client:
        if not args.no_push:
            report_push(push_tree(client, root))

        deployed = client.post(f"/sts/deploy/{key}", json_body={"yaml": yaml_text})
        session_id = deployed["session_id"]
        status = str(deployed.get("status") or _LIVE)
        print(f"running {key} session={session_id}")

        if status != _LIVE:
            # It started and stopped inside the deploy call. Attaching would
            # hang on a socket for a session that has already gone.
            print(f"  session is {status} — nothing to follow")
            return 0

        if args.no_follow:
            print(f"  left running — stop it with: mftik stop {session_id}")
            return 0

        print("  ^C stops this session")
        try:
            follow_logs(client, session_id)
        except KeyboardInterrupt:
            return _stop_on_interrupt(client, session_id)
    return 0


def _stop_on_interrupt(client: Client, session_id: str) -> int:
    """Send the stop the Ctrl-C asked for, and report what became of it.

    The second Ctrl-C lands here, while the stop is in flight. It leaves the
    session running, which is worth saying at length: the strategy is still
    holding whatever it was holding, and nothing else is going to mention it.
    """
    print(f"\nstopping {session_id} (^C again to leave it running)")
    try:
        out = client.post(f"/sts/sessions/{session_id}/stop")
    except KeyboardInterrupt:
        print(
            f"\nleft {session_id} running. It is still trading.\n"
            f"Stop it with: mftik stop {session_id}"
        )
        return EXIT_INTERRUPTED
    except CliError as exc:
        # The stop did not land, and the session is presumed up. Saying so is
        # the whole value here — a failure that read as "stopped" would be the
        # worst possible outcome of pressing Ctrl-C.
        raise CliError(
            f"could not stop {session_id}: {exc}\n"
            f"It may still be running — check with: mftik ps"
        ) from exc
    print(f"stopped {session_id} status={out.get('status')}")
    return EXIT_INTERRUPTED
