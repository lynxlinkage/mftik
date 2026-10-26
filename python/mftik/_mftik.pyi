from __future__ import annotations

import enum
from collections.abc import Callable
from typing import Any, Self, final

import mftik.common as common
import mftik.md as md
import mftik.td as td
from mftik.common.v1 import OrderType, Side, TimeInForce
from mftik.md.v1 import Cursor
from mftik.td.v1 import Balance, Fill, OrderCause, Position, Recon
from mftik.td.v1 import Order as TdOrder

__all__ = [
    "Error",
    "NotInInit",
    "UnknownUid",
    "UnknownAccount",
    "AlgoNotDeclared",
    "NoFeed",
    "Stopping",
    "ReadError",
    "ContextError",
    "Md",
    "Td",
    "Feed",
    "BookUpdate",
    "OrderEvent",
    "FillEvent",
    "PositionEvent",
    "BalanceEvent",
    "ReconEvent",
    "RejectEvent",
    "UniversalTicker",
    "Info",
    "Order",
    "TapeQuery",
    "Origin",
    "RejectedRequest",
    "common",
    "md",
    "td",
    "Context",
    "Poster",
    "_run",
    "_run_load_failed",
    "_registered_hooks",
]

class Error(Exception): ...
class NotInInit(Error): ...
class UnknownUid(Error): ...
class UnknownAccount(Error): ...
class AlgoNotDeclared(Error): ...
class NoFeed(Error): ...
class Stopping(Error): ...
class ReadError(Error): ...
class ContextError(Error): ...

class Origin(enum.IntEnum):
    from_td = 1
    local_reject = 2
    timeout = 3

class RejectedRequest(enum.IntEnum):
    cancel = 1
    batch_cancel = 2

@final
class Md:
    uid: int
    md_ts_ns: int
    conn_epoch: int
    seq: int
    def __new__(
        cls, *, uid: int = ..., md_ts_ns: int = ..., conn_epoch: int = ..., seq: int = ...
    ) -> Self: ...
    def __eq__(self, other: object, /) -> bool: ...
    def __repr__(self) -> str: ...

@final
class Td:
    api_id: int
    incarnation: int
    seq: int
    def __new__(cls, *, api_id: int = ..., incarnation: int = ..., seq: int = ...) -> Self: ...
    def __eq__(self, other: object, /) -> bool: ...
    def __repr__(self) -> str: ...

@final
class Feed:
    md: Md
    data: Any
    def __eq__(self, other: object, /) -> bool: ...
    def __repr__(self) -> str: ...

@final
class BookUpdate:
    md: Md
    exch_ts_ns: int
    def __new__(cls, *, md: Md, exch_ts_ns: int = ...) -> Self: ...
    def __eq__(self, other: object, /) -> bool: ...
    def __repr__(self) -> str: ...

@final
class OrderEvent:
    td: Td
    origin: Origin
    order: TdOrder
    cause: OrderCause
    code: int
    reason: str
    def __eq__(self, other: object, /) -> bool: ...
    def __repr__(self) -> str: ...

@final
class FillEvent:
    td: Td
    fill: Fill
    def __eq__(self, other: object, /) -> bool: ...
    def __repr__(self) -> str: ...

@final
class PositionEvent:
    td: Td
    positions: tuple[Position, ...]
    def __eq__(self, other: object, /) -> bool: ...
    def __repr__(self) -> str: ...

@final
class BalanceEvent:
    td: Td
    balances: tuple[Balance, ...]
    def __eq__(self, other: object, /) -> bool: ...
    def __repr__(self) -> str: ...

@final
class ReconEvent:
    td: Td
    recon: Recon
    def __eq__(self, other: object, /) -> bool: ...
    def __repr__(self) -> str: ...

@final
class RejectEvent:
    td: Td
    request: RejectedRequest
    client_order_ids: tuple[str, ...]
    code: int
    reason: str
    def __eq__(self, other: object, /) -> bool: ...
    def __repr__(self) -> str: ...

@final
class UniversalTicker:
    venue: str
    category: str
    symbol: str
    def __new__(cls, *, venue: str, category: str, symbol: str) -> Self: ...
    def __eq__(self, other: object, /) -> bool: ...
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

@final
class Info:
    uid: int
    ticker: UniversalTicker
    exch_ticker: str
    base: str
    quote: str
    contract_size: float
    contract_value_ccy: str
    price_ccy: str
    settle: str
    expiry_ns: int
    strike: float
    option_type: str
    status: str
    filters: dict[str, float]
    updated_at_ns: int
    def __new__(
        cls,
        *,
        ticker: UniversalTicker,
        uid: int = ...,
        exch_ticker: str = ...,
        base: str = ...,
        quote: str = ...,
        contract_size: float = ...,
        contract_value_ccy: str = ...,
        price_ccy: str = ...,
        settle: str = ...,
        expiry_ns: int = ...,
        strike: float = ...,
        option_type: str = ...,
        status: str = ...,
        filters: dict[str, float] | None = ...,
        updated_at_ns: int = ...,
    ) -> Self: ...
    def is_tradable(self) -> bool: ...
    def price_tick(self) -> float: ...
    def linear(self) -> bool: ...
    def inverse(self) -> bool: ...
    def quanto(self) -> bool: ...
    def coin_margined(self) -> bool: ...
    def u_margined(self) -> bool: ...
    def __eq__(self, other: object, /) -> bool: ...
    def __repr__(self) -> str: ...

@final
class Order:
    api_id: int
    algo_id: int
    uid: int
    side: Side
    order_type: OrderType
    tif: TimeInForce
    qty: float | None
    quote_qty: float | None
    price: float | None
    recv_window_ms: int
    reduce_only: bool
    params: dict[str, str]
    def __new__(
        cls,
        *,
        api_id: int = ...,
        algo_id: int = ...,
        uid: int = ...,
        side: int = ...,
        order_type: int = ...,
        tif: int = ...,
        qty: float | None = ...,
        quote_qty: float | None = ...,
        price: float | None = ...,
        recv_window_ms: int = ...,
        reduce_only: bool = ...,
        params: dict[str, str] | None = ...,
    ) -> Self: ...
    def __eq__(self, other: object, /) -> bool: ...
    def __repr__(self) -> str: ...

@final
class TapeQuery:
    limit: int
    before: Cursor | None
    since_ns: int
    def __new__(
        cls, *, limit: int = ..., before: Cursor | None = ..., since_ns: int = ...
    ) -> Self: ...
    def __eq__(self, other: object, /) -> bool: ...
    def __repr__(self) -> str: ...

@final
class Context:
    def session_id(self, /) -> str: ...
    def params(self, /) -> bytes: ...
    def exit(self, /) -> None: ...
    def fail(self, /, reason: str) -> None: ...
    def volume_dir(self, /) -> str: ...
    def input_dir(self, /, name: str) -> str: ...
    def after(self, /, delay: float, callback: Callable[[Context], object]) -> None: ...
    def post(self, /, callback: Callable[[Context], object]) -> None: ...
    def poster(self, /) -> Poster: ...

@final
class Poster:
    def post(self, /, callback: Callable[[Context], object]) -> None: ...

def _run(strategy: object, release: str) -> None: ...
def _run_load_failed(message: str, release: str) -> None: ...
def _registered_hooks(strategy: object) -> list[str]: ...
