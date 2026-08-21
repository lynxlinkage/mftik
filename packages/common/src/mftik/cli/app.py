"""``mftik`` — the command line for a node you host yourself.

Subcommands are registered in one table rather than as a chain of ``elif``:
adding one is a row and a function, and the help text is generated from the
same place the dispatch reads, so the two cannot disagree.

argparse rather than a CLI framework on purpose. Every service in this
workspace installs this package, so a dependency added for the client lands
in each of their images too — and the parsing this needs is what argparse is
for.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version

from mftik.cli import alert as alert_cmd
from mftik.cli import check as check_cmd
from mftik.cli import connect as connect_cmd
from mftik.cli import env as env_cmd
from mftik.cli import init as init_cmd
from mftik.cli import node as node_cmd
from mftik.cli import profiles, sessions
from mftik.cli import push as push_cmd
from mftik.cli import run as run_cmd
from mftik.cli.client import CliError, NodeUnreachable
from mftik.cli.config import ConfigError
from mftik.cli.exits import EXIT_ERROR, EXIT_INTERRUPTED, EXIT_UNREACHABLE
from mftik.cli.output import fail

__all__ = [
    "EXIT_ERROR",
    "EXIT_INTERRUPTED",
    "EXIT_UNREACHABLE",
    "COMMANDS",
    "Command",
    "build_parser",
    "main",
]


@dataclass(frozen=True, slots=True)
class Command:
    name: str
    help: str
    #: Adds this command's own arguments to its subparser.
    setup: Callable[[argparse.ArgumentParser], None]
    run: Callable[[argparse.Namespace], int]


def _setup_nothing(parser: argparse.ArgumentParser) -> None:
    del parser  # takes nothing


def _setup_disconnect(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("name", help="profile to forget (see: mftik profiles)")


def _setup_check(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("path", help="strategy directory")
    parser.add_argument(
        "cfg",
        nargs="?",
        default=None,
        help=(
            "strategy.yml to validate with on_initialized; defaults to "
            "<path>/strategy.yml if that file exists"
        ),
    )
    parser.add_argument(
        "--traceback",
        action="store_true",
        help=(
            "show frames when the strategy's own code raises, at import or "
            "in on_initialized"
        ),
    )
    parser.add_argument(
        "--against",
        action="store_true",
        help=(
            "also ask a node whether it has the extras this tree declares; "
            "everything else stays offline"
        ),
    )


def _setup_env(parser: argparse.ArgumentParser) -> None:
    """``env`` has its own verbs, and they are not equally safe.

    ``add`` and ``rm`` reinstall the whole overlay into a new generation;
    ``deps`` reads. One parser with flags would put the first two a typo away
    from the third.

    The handler goes on ``_env_run``, not ``_run``: argparse fills a nested
    parser's defaults only when the attribute is absent, and the outer command
    has already set ``_run`` by then.
    """
    verbs = parser.add_subparsers(dest="env_command", metavar="<verb>")

    listed = verbs.add_parser("list", help="what this node has applied")
    listed.set_defaults(_env_run=env_cmd.show)

    dependencies = verbs.add_parser(
        "deps", help="what came along that nobody approved"
    )
    dependencies.set_defaults(_env_run=env_cmd.deps)

    added = verbs.add_parser("add", help="apply one extra")
    added.add_argument("name", help="import name, e.g. numpy (not scikit-learn)")
    added.add_argument(
        "--version",
        default=None,
        help="omit to let the node's resolver pick; the stamp keeps what it picked",
    )
    added.add_argument(
        "--dist", default=None, help="PyPI name when it differs from the import name"
    )
    added.add_argument(
        "--force",
        action="store_true",
        help="apply even though sessions are live (they keep the old modules)",
    )
    added.set_defaults(_env_run=env_cmd.add)

    approved = verbs.add_parser(
        "approve", help="stamp an already-installed dependency"
    )
    approved.add_argument("dist", help="distribution name (see: mftik env deps)")
    approved.add_argument(
        "--name", default=None, help="import name, when it differs from the dist"
    )
    approved.add_argument("--force", action="store_true", help=argparse.SUPPRESS)
    approved.set_defaults(_env_run=env_cmd.approve)

    removed = verbs.add_parser("rm", help="remove an extra")
    removed.add_argument("name", help="import name as the stamp lists it")
    removed.add_argument(
        "--force",
        action="store_true",
        help="remove even though sessions are live",
    )
    removed.set_defaults(_env_run=env_cmd.rm)

    imported = verbs.add_parser(
        "import", help="preview a peer's extras, and install them with --confirm"
    )
    imported.add_argument("url", help="the peer, e.g. https://peer.example.com")
    imported.add_argument(
        "--token", default=None, help="registry key that peer issued you"
    )
    imported.add_argument(
        "--dist",
        action="append",
        default=None,
        metavar="NAME=PYPI",
        help="correct a dist the peer did not send, e.g. sklearn=scikit-learn",
    )
    imported.add_argument(
        "--confirm", action="store_true", help="install; without this it only reads"
    )
    imported.add_argument("--force", action="store_true", help=argparse.SUPPRESS)
    imported.set_defaults(_env_run=env_cmd.import_from_peer)


def _run_env(args: argparse.Namespace) -> int:
    run = getattr(args, "_env_run", None)
    if run is None:
        print("usage: mftik env {list,deps,add,approve,rm,import}")
        return EXIT_ERROR
    return run(args)


def _setup_alert(parser: argparse.ArgumentParser) -> None:
    """``alert`` owns the whole graph, including ``source`` and ``matcher``.

    Singular, and nested, for the same reason ``env`` is: this is a namespace
    with verbs, not a listing. ``source`` and ``matcher`` are verbs of it
    rather than top-level commands because neither does anything alone — a
    Source with no Matcher is a subscription nobody reads.

    As with ``env``, the handler goes on ``_alert_run``: argparse fills a
    nested parser's defaults only when the attribute is absent, and the outer
    command has already set ``_run``.
    """
    verbs = parser.add_subparsers(dest="alert_command", metavar="<verb>")

    listed = verbs.add_parser("list", help="every Alert, with its masked webhook")
    listed.set_defaults(_alert_run=alert_cmd.list_alerts)

    graph = verbs.add_parser("graph", help="the whole wiring, as a tree")
    graph.set_defaults(_alert_run=alert_cmd.show_graph)

    added = verbs.add_parser(
        "add", help="create an Alert; the URL is prompted for, never an argument"
    )
    added.add_argument("--name", required=True, help="Owner label, unique")
    added.add_argument(
        "--webhook-url-stdin",
        action="store_true",
        help="read the URL from stdin instead of prompting (for scripts)",
    )
    added.add_argument(
        "--window",
        type=int,
        default=None,
        metavar="SECONDS",
        help="quiesce window; one POST per window (default 30)",
    )
    added.add_argument(
        "--max-events", type=int, default=None, help="lines in the embed (default 15)"
    )
    added.add_argument(
        "--max-buffer",
        type=int,
        default=None,
        help="hard cap on buffered events per window (default 200)",
    )
    added.add_argument(
        "--no-dedupe",
        action="store_true",
        help="keep identical messages apart in the window",
    )
    added.add_argument(
        "--disabled", action="store_true", help="create it, but drop injects for now"
    )
    added.set_defaults(_alert_run=alert_cmd.add_alert)

    removed = verbs.add_parser("rm", help="delete an Alert, its wires and deliveries")
    removed.add_argument("alert_id", type=int)
    removed.set_defaults(_alert_run=alert_cmd.rm_alert)

    tested = verbs.add_parser("test", help="fire a fixed embed at the webhook")
    tested.add_argument("alert_id", type=int)
    tested.set_defaults(_alert_run=alert_cmd.test_alert)

    fired = verbs.add_parser("deliveries", help="the fire log for one Alert")
    fired.add_argument("alert_id", type=int)
    fired.set_defaults(_alert_run=alert_cmd.deliveries)

    types = verbs.add_parser("types", help="what an sts selector may be")
    types.set_defaults(_alert_run=alert_cmd.list_types)

    wired = verbs.add_parser("wire", help="draw one edge of the graph")
    _setup_wire_flags(wired)
    wired.set_defaults(_alert_run=alert_cmd.wire)

    unwired = verbs.add_parser("unwire", help="remove one edge; the nodes stay")
    _setup_wire_flags(unwired)
    unwired.set_defaults(_alert_run=alert_cmd.unwire)

    _setup_alert_source(verbs.add_parser("source", help="the log streams to watch"))
    _setup_alert_matcher(verbs.add_parser("matcher", help="the judgements to apply"))


def _alert_group_usage(
    name: str, verbs: str
) -> Callable[[argparse.Namespace], int]:
    """The fallback for a group named without a verb.

    Without this, ``mftik alert source`` falls through to the outer command's
    handler and is told about ``list,graph,add,…`` — the verbs of the wrong
    noun. A group answers for itself.
    """

    def _usage(_args: argparse.Namespace) -> int:
        print(f"usage: mftik alert {name} {verbs}")
        return EXIT_ERROR

    return _usage


def _setup_wire_flags(parser: argparse.ArgumentParser) -> None:
    """Two of the three, naming one legal edge. ``alert.py`` refuses the rest."""
    parser.add_argument("--source", type=int, default=None)
    parser.add_argument("--matcher", type=int, default=None)
    parser.add_argument("--alert", type=int, default=None)


def _setup_alert_source(parser: argparse.ArgumentParser) -> None:
    parser.set_defaults(_alert_run=_alert_group_usage("source", "{list,add,rm}"))
    verbs = parser.add_subparsers(dest="source_command", metavar="<verb>")

    listed = verbs.add_parser("list", help="every Source")
    listed.set_defaults(_alert_run=alert_cmd.list_sources)

    added = verbs.add_parser("add", help="subscribe to a stream")
    added.add_argument("--domain", required=True, choices=("sts", "td", "md"))
    added.add_argument(
        "--selector",
        required=True,
        help=(
            "for sts a strategy type (see: mftik alert types) or *; "
            "for td an api_id; for md a venue"
        ),
    )
    added.set_defaults(_alert_run=alert_cmd.add_source)

    removed = verbs.add_parser("rm", help="delete a Source and its wires")
    removed.add_argument("source_id", type=int)
    removed.set_defaults(_alert_run=alert_cmd.rm_source)


def _setup_alert_matcher(parser: argparse.ArgumentParser) -> None:
    parser.set_defaults(_alert_run=_alert_group_usage("matcher", "{list,add,rm}"))
    verbs = parser.add_subparsers(dest="matcher_command", metavar="<verb>")

    listed = verbs.add_parser("list", help="every Matcher, and what it judges")
    listed.set_defaults(_alert_run=alert_cmd.list_matchers)

    added = verbs.add_parser("add", help="create a judgement")
    added.add_argument("--name", required=True, help="Owner label, unique")
    added.add_argument("--kind", required=True, choices=("level", "regex", "extract"))
    added.add_argument(
        "--level",
        action="append",
        default=None,
        help="for --kind level; repeatable, e.g. --level warn --level error",
    )
    added.add_argument("--pattern", default=None, help="for --kind regex / extract")
    added.add_argument("--group", type=int, default=1, help="capture group to read")
    added.add_argument(
        "--as",
        dest="as_",
        default="float",
        choices=("float", "int", "str"),
        help="how to read the captured group before comparing",
    )
    added.add_argument(
        "--op", default=">", choices=(">", ">=", "<", "<=", "==", "!=")
    )
    added.add_argument("--value", default=None, help="the right-hand side")
    added.set_defaults(_alert_run=alert_cmd.add_matcher)

    removed = verbs.add_parser("rm", help="delete a Matcher and its wires")
    removed.add_argument("matcher_id", type=int)
    removed.set_defaults(_alert_run=alert_cmd.rm_matcher)


def _run_alert(args: argparse.Namespace) -> int:
    run = getattr(args, "_alert_run", None)
    if run is None:
        print(
            "usage: mftik alert "
            "{list,graph,add,rm,test,deliveries,types,wire,unwire,source,matcher}"
        )
        return EXIT_ERROR
    return run(args)


def _setup_init(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "path",
        nargs="?",
        default=".",
        help="directory to scaffold into; defaults to the current one",
    )
    parser.add_argument(
        "--name",
        default=None,
        help="strategy name; defaults to a slug of the directory name",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help=(
            "do not ask a node for its accounts and instruments; leaves "
            "placeholders in strategy.yml"
        ),
    )
    parser.add_argument(
        "--force", action="store_true", help="overwrite existing files"
    )


def _setup_node_init(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "path",
        nargs="?",
        default=".",
        help="directory to write the stack into; defaults to the current one",
    )
    parser.add_argument(
        "--tag",
        default="latest",
        help=(
            "image tag to run, from the repository's releases. Not the version "
            "of this package — they are numbered separately"
        ),
    )
    parser.add_argument(
        "--port", default="8080", help="port the node answers on (default 8080)"
    )
    parser.add_argument(
        "--project",
        default=None,
        help="compose project name; defaults to a slug of the directory name",
    )
    parser.add_argument(
        "--force", action="store_true", help="overwrite existing files"
    )


def _setup_push(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("path", help="strategy directory")


def _setup_run(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("path", help="strategy directory")
    parser.add_argument(
        "cfg",
        nargs="?",
        default=None,
        help=(
            "strategy.yml to deploy; defaults to <path>/strategy.yml if "
            "that file exists, otherwise an empty sts block"
        ),
    )
    parser.add_argument(
        "--no-push",
        action="store_true",
        help="deploy what is already on the node, without copying the tree",
    )
    parser.add_argument(
        "--no-follow",
        action="store_true",
        help="print the session id and exit, without tailing its log",
    )


def _setup_session(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("session_id", help="session to act on")


def _setup_logs(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("session_id", help="session whose log to print")
    parser.add_argument(
        "-f",
        "--follow",
        action="store_true",
        help="tail the live stream instead of printing the stored page",
    )


def _setup_connect(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("url", help="the node's URL, e.g. https://node.example.com")
    parser.add_argument(
        "--name",
        default=None,
        help="what to call this node here; defaults to its hostname",
    )
    parser.add_argument(
        "--token",
        default=None,
        help=(
            "use an existing API key instead of signing in. This is the "
            "non-interactive path, and the one CI should take"
        ),
    )
    parser.add_argument(
        "--setup",
        action="store_true",
        help=(
            "claim an unclaimed node, becoming its Owner. Refused on a node "
            "that already has one"
        ),
    )
    parser.add_argument(
        "--keep-default",
        action="store_true",
        help="do not make this the default profile",
    )


COMMANDS: tuple[Command, ...] = (
    Command(
        name="connect",
        help="authenticate this machine against a node",
        setup=_setup_connect,
        run=connect_cmd.connect,
    ),
    Command(
        name="whoami",
        help="who this machine is, to the node it is pointed at",
        setup=_setup_nothing,
        run=connect_cmd.whoami,
    ),
    Command(
        name="profiles",
        help="list the nodes this machine is connected to",
        setup=_setup_nothing,
        run=profiles.list_profiles,
    ),
    Command(
        name="disconnect",
        help="forget a node and the key it issued",
        setup=_setup_disconnect,
        run=profiles.disconnect,
    ),
    Command(
        name="check",
        help="the import gate and on_initialized, offline",
        setup=_setup_check,
        run=check_cmd.check,
    ),
    Command(
        name="env",
        help="the third-party packages this node has applied",
        setup=_setup_env,
        run=_run_env,
    ),
    Command(
        name="alert",
        help="the Source → Matcher → Alert graph that reaches a Discord webhook",
        setup=_setup_alert,
        run=_run_alert,
    ),
    Command(
        name="node-init",
        help="write a docker compose stack that hosts a whole node",
        setup=_setup_node_init,
        run=node_cmd.node_init,
    ),
    Command(
        name="init",
        help="scaffold a strategy that runs as generated",
        setup=_setup_init,
        run=init_cmd.init,
    ),
    Command(
        name="push",
        help="copy a strategy tree into the node's private registry",
        setup=_setup_push,
        run=push_cmd.push,
    ),
    Command(
        name="run",
        help="push, deploy, and tail the session's log",
        setup=_setup_run,
        run=run_cmd.run,
    ),
    Command(
        name="ps",
        help="list live sessions",
        setup=_setup_nothing,
        run=sessions.list_sessions,
    ),
    Command(
        name="logs",
        help="print a session's log",
        setup=_setup_logs,
        run=sessions.logs,
    ),
    Command(
        name="stop",
        help="stop a live session",
        setup=_setup_session,
        run=sessions.stop_session,
    ),
)


def _version() -> str:
    try:
        return version("mftik")
    except PackageNotFoundError:
        # Running from a source tree that was never installed. Worth saying
        # so plainly rather than reporting a version that is not on disk.
        return "unknown (not installed)"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mftik",
        description="Client for a self-hosted MFTIK trading node.",
    )
    parser.add_argument(
        "--version", action="version", version=f"mftik {_version()}"
    )
    parser.add_argument(
        "--profile",
        default=None,
        help=(
            "which connected node to act on; defaults to MFTIK_PROFILE, "
            "then to the last one connected"
        ),
    )
    subparsers = parser.add_subparsers(dest="command", metavar="<command>")
    for command in COMMANDS:
        sub = subparsers.add_parser(command.name, help=command.help)
        command.setup(sub)
        sub.set_defaults(_run=command.run)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    run = getattr(args, "_run", None)
    if run is None:
        parser.print_help()
        return EXIT_ERROR

    try:
        return run(args)
    except NodeUnreachable as exc:
        fail(str(exc))
        return EXIT_UNREACHABLE
    except (CliError, ConfigError) as exc:
        fail(str(exc))
        return EXIT_ERROR
    except KeyboardInterrupt:
        # Newline first: the ^C landed at the end of whatever was being
        # printed, and without this the shell prompt continues that line.
        print(file=sys.stderr)
        fail("interrupted")
        return EXIT_INTERRUPTED


if __name__ == "__main__":
    raise SystemExit(main())
