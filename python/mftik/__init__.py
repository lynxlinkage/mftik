"""Python SDK. Protocol logic and IO stay in the Rust crates."""

import sys

from mftik._mftik import (
    AlgoNotDeclared,
    BalanceEvent,
    BookUpdate,
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
    "ReadError",
    "ReconEvent",
    "RejectEvent",
    "RejectedRequest",
    "Stopping",
    "TapeQuery",
    "Td",
    "UniversalTicker",
    "UnknownAccount",
    "UnknownUid",
    "common",
    "md",
    "td",
]
