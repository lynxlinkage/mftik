"""``mftik.run`` and ``python -m mftik.run module:Class`` (PROTOCOL §8.2)."""

from __future__ import annotations

import importlib
import os
import sys
import traceback

from mftik._mftik import _run, _run_load_failed
from mftik._version import package_version
from mftik.strategy import Strategy


def resolve_release(release: str | None) -> str:
    """``Hello.release``: argument, then ``MFTIK_RELEASE``, then the package version."""
    if release:
        return release
    from_env = os.environ.get("MFTIK_RELEASE", "")
    if from_env:
        return from_env
    return package_version()


def format_load_error(spec: str, exc: BaseException) -> str:
    """``Done.error`` when the entrypoint cannot build the strategy (E04-D012)."""
    return f"load {spec}: {type(exc).__name__}: {exc}"


def load_strategy(spec: str) -> Strategy:
    """Import ``module:Class``, construct it with no arguments, and check the type."""
    module_name, sep, class_name = spec.partition(":")
    if sep != ":" or not module_name or not class_name or ":" in class_name:
        raise ValueError(f"want module:Class, got {spec!r}")
    module = importlib.import_module(module_name)
    cls = getattr(module, class_name)
    obj = cls()
    if not isinstance(obj, Strategy):
        raise TypeError(f"{spec} is not a mftik.Strategy")
    return obj


def run(
    strategy: Strategy,
    release: str | None = None,
    *,
    version: str = "",
    commit: str = "",
) -> None:
    """Host ``strategy`` until the process exits. Does not return.

    ``version`` and ``commit`` are the strategy's log fields. They do not
    replace ``Hello.release``.
    """
    if not isinstance(strategy, Strategy):
        raise TypeError("strategy must be an instance of mftik.Strategy")
    if not isinstance(version, str):
        raise TypeError("version must be str")
    if not isinstance(commit, str):
        raise TypeError("commit must be str")
    _run(strategy, resolve_release(release), version, commit)


def configure_line_buffering() -> None:
    """Make stdout and stderr line buffered (E04-D002)."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue
        try:
            reconfigure(line_buffering=True)
        except (AttributeError, OSError, ValueError):
            continue


def _enter_load_failed(spec: str, exc: BaseException) -> None:
    _write_traceback(exc)
    _run_load_failed(format_load_error(spec, exc), resolve_release(None))


def _write_traceback(exc: BaseException) -> None:
    try:
        traceback.print_exception(exc, file=sys.stderr)
    except (OSError, ValueError):
        return


def main(argv: list[str] | None = None) -> None:
    """``python -m mftik.run module:Class``."""
    configure_line_buffering()
    args = list(sys.argv if argv is None else argv)
    if len(args) != 2:
        spec = args[1] if len(args) > 1 else ""
        _enter_load_failed(spec, TypeError("usage: python -m mftik.run module:Class"))
        return
    spec = args[1]
    try:
        strategy = load_strategy(spec)
    except BaseException as exc:
        _enter_load_failed(spec, exc)
        return
    run(strategy)


if __name__ == "__main__":
    main()
