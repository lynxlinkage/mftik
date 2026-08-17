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

from mftik.cli import check as check_cmd
from mftik.cli import connect as connect_cmd
from mftik.cli import init as init_cmd
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
