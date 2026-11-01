"""STS Python SDK. Protocol logic and IO stay in the Rust crates."""

from __future__ import annotations

import os
import sys

from mftik._mftik import (
    AlgoNotDeclared,
    BalanceEvent,
    BookUpdate,
    BookView,
    Context,
    ContextError,
    Error,
    Feed,
    FillEvent,
    Info,
    Log,
    Md,
    NoFeed,
    NotInInit,
    Order,
    OrderEvent,
    Origin,
    PositionEvent,
    Poster,
    ReadError,
    ReconEvent,
    RejectedRequest,
    RejectEvent,
    ReplayError,
    ReplayReport,
    Stopping,
    TapeQuery,
    Td,
    UniversalTicker,
    UnknownAccount,
    UnknownUid,
    common,
    log,
    md,
    td,
)
from mftik._version import package_version
from mftik.strategy import Strategy

from . import topic
from . import volume as volume

sys.modules["mftik.common"] = common
sys.modules["mftik.common.v1"] = common.v1
sys.modules["mftik.md"] = md
sys.modules["mftik.md.v1"] = md.v1
sys.modules["mftik.td"] = td
sys.modules["mftik.td.v1"] = td.v1

__all__ = [
    "AlgoNotDeclared",
    "BalanceEvent",
    "BookUpdate",
    "BookView",
    "Context",
    "ContextError",
    "Error",
    "Feed",
    "FillEvent",
    "Info",
    "Log",
    "Md",
    "NoFeed",
    "NotInInit",
    "Order",
    "OrderEvent",
    "Origin",
    "PositionEvent",
    "Poster",
    "ReadError",
    "ReconEvent",
    "RejectEvent",
    "RejectedRequest",
    "ReplayError",
    "ReplayReport",
    "Stopping",
    "Strategy",
    "TapeQuery",
    "Td",
    "UniversalTicker",
    "topic",
    "UnknownAccount",
    "UnknownUid",
    "common",
    "log",
    "md",
    "replay",
    "run",
    "td",
    "volume",
]

__version__ = package_version()


def replay(
    strategy_cls: type[Strategy],
    dir: str | os.PathLike[str],
    *,
    incarnation: int | None = None,
    volume_dir: str | os.PathLike[str] | None = None,
    input_dir: str | os.PathLike[str] | None = None,
) -> ReplayReport:
    """Run ``strategy_cls`` over one incarnation of the event log in ``dir``.

    ``dir`` is the session volume's ``eventlog`` directory. The class is
    constructed with no arguments. ``None`` options use the Rust defaults:
    newest incarnation, empty volume and input directories.
    """
    from mftik._mftik import _replay

    if not isinstance(strategy_cls, type) or not issubclass(strategy_cls, Strategy):
        raise TypeError("strategy must be a subclass of mftik.Strategy")
    if incarnation is None:
        incarnation = 0
    elif isinstance(incarnation, bool) or not isinstance(incarnation, int):
        raise TypeError("incarnation must be int")
    obj = strategy_cls()
    if not isinstance(obj, Strategy):
        raise TypeError("strategy must be a subclass of mftik.Strategy")
    return _replay(
        obj,
        _path_str(dir, "dir"),
        incarnation,
        "" if volume_dir is None else _path_str(volume_dir, "volume_dir"),
        "" if input_dir is None else _path_str(input_dir, "input_dir"),
    )


def _path_str(value: str | os.PathLike[str], name: str) -> str:
    text = os.fspath(value)
    if isinstance(text, bytes):
        raise TypeError(f"{name} must be a str, not bytes")
    return text


def __getattr__(name: str):
    # ``python -m mftik.run`` imports this package first. Importing ``mftik.run``
    # at package import time would leave it in ``sys.modules`` before that
    # entrypoint executes, so the function is bound on first use.
    if name == "run":
        from mftik.run import run as entry

        # Importing the submodule assigns this name to the module. Put the
        # function back; otherwise the next lookup is the module.
        globals()["run"] = entry
        return entry
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
