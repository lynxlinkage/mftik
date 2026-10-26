"""``python -m mftik.pack`` (E04-D071). Success prints the manifest digest."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Never

from mftik.pack.build import Options, build_and_push, build_image, parse_env
from mftik.pack.error import PackError
from mftik.pack.pins import DEFAULT_PYTHON


class _Parser(argparse.ArgumentParser):
    """Usage errors are ``PackError`` so ``main`` can print one line and exit 1."""

    def error(self, message: str) -> Never:
        raise PackError(message)


def execute(argv: list[str] | None = None) -> str:
    """Pack, and push unless ``-no-push``. Returns the image manifest digest.

    ``-no-push`` still requires ``-repo`` (the image name is checked) and does
    not open a connection to that registry.
    """
    parser = _Parser(prog="mftik.pack")
    parser.add_argument("-dir", dest="directory", required=True)
    parser.add_argument("-requirements", default="")
    parser.add_argument("-target", required=True)
    parser.add_argument("-name", required=True)
    parser.add_argument("-repo", dest="repository", required=True)
    parser.add_argument("-tag", default="")
    parser.add_argument("-env", action="append", default=[])
    parser.add_argument("-plain-http", dest="plain_http", action="store_true")
    parser.add_argument("-python", default=DEFAULT_PYTHON)
    parser.add_argument("-wheel", default="")
    parser.add_argument("-no-push", dest="no_push", action="store_true")
    args = parser.parse_args(argv)
    options = Options(
        directory=Path(args.directory),
        requirements=Path(args.requirements) if args.requirements else None,
        target=args.target,
        name=args.name,
        repository=args.repository,
        tag=args.tag,
        env=parse_env(list(args.env)),
        plain_http=bool(args.plain_http),
        python=args.python,
        wheel=Path(args.wheel) if args.wheel else None,
    )
    if args.no_push:
        return build_image(options).digest
    return build_and_push(options)


def main(argv: list[str] | None = None) -> None:
    """CLI entry. ``PackError`` is one line on stderr and exit status 1."""
    try:
        digest = execute(argv)
    except PackError as exc:
        print(f"mftik.pack: {exc}", file=sys.stderr)
        raise SystemExit(1) from None
    print(digest)
