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

``--wait`` / ``--no-wait`` are the F12 surface (IF-15). Passing either one
prints that it is not implemented and exits 1, and does not deploy. Leaving
both off keeps the behaviour above, so a person can still start a session
the way they do today. The plan's default is ``--wait``: watch status until
``running`` or ``failed``, then tail. ``--no-wait`` prints the session id
and returns. B4-08 makes that true and is what flips the default.
:func:`run_wait_action` is that decision, and it raises
``NotImplementedError("IF-15")`` until then.

**State authority (§3.3): this command holds none.** Session status belongs
to the STS controller's Supervisor. Once ``--wait`` exists, this command
reads that status and then tails a log. It does not write the status, and
it does not store ``strategy_digest`` or ``env_generation`` (F39, IF-16).
"""

from __future__ import annotations

import argparse
from typing import Literal

from mftik.cli.client import (
    DEFAULT_TIMEOUT_S,
    Client,
    CliError,
    NodeUnreachable,
    connected,
    is_environment_refusal,
)
from mftik.cli.exits import EXIT_ERROR, EXIT_INTERRUPTED
from mftik.cli.output import fail
from mftik.cli.push import push_tree, report_push
from mftik.cli.sessions import follow_logs
from mftik.cli.tree import inspect_tree, read_yaml, require_tree
from mftik.protocol.strategy_yml import StrategyYamlError, parse_strategy_yml
from mftik.registry.qualify import PRIVATE_ORIGIN, qualify

#: What a deploy answers when the session is up. Anything else means the
#: strategy refused its configuration or finished during ``on_start``, and
#: there is no live log to attach to.
_LIVE = "live"

#: How long the deploy POST may take. IF-15 settles this as one ordinary
#: HTTP hop, the same budget every other command uses. F12 makes deploy
#: answer 202 once the session is accepted, so this is not a budget for
#: ``on_start``. Watching until ``running`` or ``failed`` is ``--wait``,
#: and that watch is not this timer (B4-08).
_DEPLOY_HTTP_TIMEOUT_S = DEFAULT_TIMEOUT_S

#: What :func:`run_wait_action` returns. ``wait`` means keep watching,
#: ``tail`` means the session has reached ``running`` or ``failed``,
#: ``return_id`` means ``--no-wait`` is done.
RunWaitStep = Literal["wait", "tail", "return_id"]


def run_wait_action(status: str, *, wait: bool) -> RunWaitStep:
    """What ``mftik run`` does with one status snapshot (F12, §5.2).

    * **W1.** ``wait`` true: ``running`` or ``failed`` → ``"tail"``.
      ``pending``, ``starting`` and ``restarting`` → ``"wait"`` (keep
      watching). The deploy's 202 is ``starting``; that is not the end
      of the watch.
    * **W2.** ``wait`` false (``--no-wait``): ``"return_id"`` for every
      status. No tail.
    * **W3.** This is not the deploy POST's timer. That timer is
      :data:`_DEPLOY_HTTP_TIMEOUT_S`, one HTTP hop.

    ``stopping`` and ``done`` are not decided here. Not implemented
    (IF-15). B4-08 performs the watch.
    """
    del status, wait
    raise NotImplementedError("IF-15")


def _deploy_may_be_live(exc: BaseException) -> str:
    return (
        f"{exc}\n"
        "A session may already be live — check with: mftik ps\n"
        "Do not run again until you know."
    )


def run(args: argparse.Namespace) -> int:
    # ``None`` means neither flag was passed. Today's deploy-and-follow
    # stays on that path. Either explicit flag is the IF-15 surface: it
    # does not deploy, because the watch it names is B4-08's.
    if getattr(args, "wait", None) is not None:
        fail("run --wait / --no-wait is not implemented (IF-15)")
        return EXIT_ERROR

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
    _, client = connected(args.profile, timeout=_DEPLOY_HTTP_TIMEOUT_S)
    with client:
        if not args.no_push:
            report_push(push_tree(client, root))

        try:
            deployed = client.post(
                f"/sts/deploy/{key}", json_body={"yaml": yaml_text}
            )
        except KeyboardInterrupt:
            # The POST is in flight. STS may already have created the
            # session; this side will never hear the id.
            raise CliError(
                "interrupted during deploy.\n"
                "A session may already be live — check with: mftik ps\n"
                "Do not run again until you know."
            ) from None
        except NodeUnreachable as exc:
            raise NodeUnreachable(_deploy_may_be_live(exc)) from exc
        except CliError as exc:
            # A missing extra never created a session. Saying one may already
            # be live turns an environment problem into a hunt for a ghost.
            if is_environment_refusal(exc):
                raise
            raise CliError(_deploy_may_be_live(exc)) from exc
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
