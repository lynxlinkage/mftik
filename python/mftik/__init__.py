"""STS Python SDK. Protocol logic and IO stay in the Rust crates."""

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
    Stopping,
    TapeQuery,
    Td,
    UniversalTicker,
    UnknownAccount,
    UnknownUid,
    common,
    md,
    td,
)
from mftik._version import package_version
from mftik.strategy import Strategy

from . import topic

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
    "Stopping",
    "Strategy",
    "TapeQuery",
    "Td",
    "UniversalTicker",
    "topic",
    "UnknownAccount",
    "UnknownUid",
    "common",
    "md",
    "run",
    "td",
]

__version__ = package_version()


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
