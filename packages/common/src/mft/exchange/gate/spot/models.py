"""Gate spot WebSocket v4 message models.

These mirror Gate's wire format field-for-field — including the single-letter
keys and the ``BTC_USDT`` pair spelling. Where a shared ``mft.exchange`` model
is a faithful fit, a ``to_*`` converter is provided; where Gate has no
equivalent (candlesticks, best bid/ask, depth diffs) there is deliberately no
converter, because flattening those into a normalized shape would lose the
sequencing and windowing that make them usable.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from mft.exchange.models import (
    Balance,
    BookLevel,
    Fill,
    Order,
    OrderBook,
    OrderStatus,
    OrderType,
    Side,
    Ticker,
    Trade,
)

#: Gate requires user-supplied order ids to start with ``t-``.
TEXT_PREFIX = "t-"


def to_text(client_order_id: str) -> str:
    """Wrap a client order id in Gate's ``text`` form."""
    if client_order_id.startswith(TEXT_PREFIX):
        return client_order_id
    return f"{TEXT_PREFIX}{client_order_id}"


def from_text(text: str | None) -> str | None:
    """Unwrap Gate's ``text`` back to a client order id.

    Gate stamps its own orders ``apiv4`` and uses ``-`` for "unset", neither of
    which is a client order id.
    """
    if not text or text in ("apiv4", "-"):
        return None
    if text.startswith(TEXT_PREFIX):
        return text[len(TEXT_PREFIX) :]
    return text


def _secs(value: Any) -> float:
    """Gate sends ms as int or as a string with a fractional tail."""
    if value is None or value == "":
        return 0.0
    return float(value) / 1000.0


def _levels(rows: list[Any] | None) -> list[BookLevel]:
    out: list[BookLevel] = []
    for row in rows or []:
        if len(row) < 2:
            continue
        out.append(BookLevel(price=Decimal(str(row[0])), qty=Decimal(str(row[1]))))
    return out


class GateMessage(BaseModel):
    """Base for wire models: tolerant of new fields, immutable once parsed."""

    model_config = ConfigDict(frozen=True, populate_by_name=True, extra="ignore")


# --- public ----------------------------------------------------------------


class GateTicker(GateMessage):
    """``spot.tickers`` — rolling 24h stats plus top of book."""

    currency_pair: str
    last: Decimal
    lowest_ask: Decimal | None = None
    highest_bid: Decimal | None = None
    change_percentage: Decimal | None = None
    base_volume: Decimal | None = None
    quote_volume: Decimal | None = None
    high_24h: Decimal | None = None
    low_24h: Decimal | None = None

    def to_ticker(self) -> Ticker:
        """Requires both sides quoted; Gate omits them on an empty book."""
        return Ticker(
            symbol=self.currency_pair,
            bid=self.highest_bid if self.highest_bid is not None else self.last,
            ask=self.lowest_ask if self.lowest_ask is not None else self.last,
            last=self.last,
        )


class GateTrade(GateMessage):
    """``spot.trades`` — public tape. ``side`` is the taker's side."""

    id: int
    create_time: int
    create_time_ms: str | float
    side: Side
    currency_pair: str
    amount: Decimal
    price: Decimal

    def to_trade(self) -> Trade:
        return Trade(
            trade_id=str(self.id),
            symbol=self.currency_pair,
            price=self.price,
            qty=self.amount,
            side=self.side,
            ts=_secs(self.create_time_ms),
        )


class GateCandlestick(GateMessage):
    """``spot.candlesticks`` — no shared equivalent; kept Gate-native.

    ``n`` carries ``"<interval>_<pair>"``; ``w`` marks the closing tick of the
    window, which is the only safe point to treat a bar as final.
    """

    t: int
    v: Decimal = Decimal("0")
    c: Decimal
    h: Decimal
    low: Decimal = Field(alias="l")
    o: Decimal
    n: str
    a: Decimal = Decimal("0")
    w: bool = False

    @property
    def interval(self) -> str:
        return self.n.split("_", 1)[0]

    @property
    def currency_pair(self) -> str:
        parts = self.n.split("_", 1)
        return parts[1] if len(parts) > 1 else ""

    @property
    def open_time(self) -> float:
        return float(self.t)

    @property
    def window_closed(self) -> bool:
        return self.w


class GateBookTicker(GateMessage):
    """``spot.book_ticker`` — best bid/ask only. No shared equivalent."""

    t: int
    u: int
    s: str
    bid: Decimal = Field(alias="b")
    bid_size: Decimal = Field(alias="B")
    ask: Decimal = Field(alias="a")
    ask_size: Decimal = Field(alias="A")

    @property
    def currency_pair(self) -> str:
        return self.s

    @property
    def ts(self) -> float:
        return _secs(self.t)


class GateOrderBook(GateMessage):
    """``spot.order_book`` — capped-depth snapshot pushed on a timer."""

    t: int
    last_update_id: int = Field(alias="lastUpdateId")
    s: str
    bids: list[Any] = Field(default_factory=list)
    asks: list[Any] = Field(default_factory=list)

    def to_order_book(self) -> OrderBook:
        return OrderBook(
            symbol=self.s,
            bids=_levels(self.bids),
            asks=_levels(self.asks),
            ts=_secs(self.t),
        )


class GateOrderBookUpdate(GateMessage):
    """``spot.order_book_update`` — depth diff, not a book.

    Deliberately has no ``to_order_book()``: a diff is only meaningful applied
    to a snapshot in ``U``/``u`` sequence order. A zero ``qty`` deletes a level.
    """

    t: int
    e: str = "depthUpdate"
    event_time: int = Field(default=0, alias="E")
    s: str
    first_id: int = Field(alias="U")
    last_id: int = Field(alias="u")
    bids: list[Any] = Field(default_factory=list, alias="b")
    asks: list[Any] = Field(default_factory=list, alias="a")

    @property
    def currency_pair(self) -> str:
        return self.s

    @property
    def ts(self) -> float:
        return _secs(self.t)

    def bid_levels(self) -> list[BookLevel]:
        return _levels(self.bids)

    def ask_levels(self) -> list[BookLevel]:
        return _levels(self.asks)

    def follows(self, last_applied_id: int) -> bool:
        """Whether this diff slots onto a book already at ``last_applied_id``."""
        return self.first_id <= last_applied_id + 1 <= self.last_id


# --- private ---------------------------------------------------------------


class GateOrderUpdate(GateMessage):
    """``spot.orders`` — order lifecycle.

    Status lives across two fields: ``event`` (put/update/finish) and, once
    finished, ``finish_as``. Neither alone is enough.
    """

    id: str
    text: str | None = None
    currency_pair: str
    type: OrderType = OrderType.LIMIT
    side: Side
    amount: Decimal
    price: Decimal | None = None
    left: Decimal = Decimal("0")
    filled_amount: Decimal | None = None
    filled_total: Decimal | None = None
    avg_deal_price: Decimal | None = None
    fee: Decimal = Decimal("0")
    fee_currency: str = ""
    time_in_force: str | None = None
    account: str | None = None
    create_time_ms: str | float | None = None
    update_time_ms: str | float | None = None
    event: str = ""
    finish_as: str | None = None

    @property
    def client_order_id(self) -> str | None:
        return from_text(self.text)

    @property
    def filled_qty(self) -> Decimal:
        if self.filled_amount is not None:
            return self.filled_amount
        return self.amount - self.left

    @property
    def status(self) -> OrderStatus:
        if self.event == "finish":
            if self.finish_as == "filled":
                return OrderStatus.FILLED
            if self.finish_as in ("cancelled", "ioc", "stp"):
                return OrderStatus.CANCELED
            # No explicit finish_as: a fully consumed order is a fill.
            return OrderStatus.FILLED if self.left == 0 else OrderStatus.CANCELED
        if self.event == "put":
            return OrderStatus.OPEN
        return (
            OrderStatus.PARTIALLY_FILLED
            if self.filled_qty > 0
            else OrderStatus.OPEN
        )

    def to_order(self) -> Order:
        return Order(
            order_id=self.id,
            client_order_id=self.client_order_id,
            symbol=self.currency_pair,
            side=self.side,
            type=self.type,
            status=self.status,
            qty=self.amount,
            price=self.price,
            filled_qty=self.filled_qty,
            avg_price=self.avg_deal_price or None,
            ts=_secs(self.update_time_ms or self.create_time_ms),
        )


class GateUserTrade(GateMessage):
    """``spot.usertrades`` — own executions. ``role`` is maker/taker."""

    id: int
    order_id: str
    currency_pair: str
    create_time_ms: str | float
    side: Side
    amount: Decimal
    role: str = ""
    price: Decimal
    fee: Decimal = Decimal("0")
    fee_currency: str = ""
    point_fee: Decimal = Decimal("0")
    gt_fee: Decimal = Decimal("0")
    text: str | None = None

    @property
    def client_order_id(self) -> str | None:
        return from_text(self.text)

    def to_fill(self) -> Fill:
        return Fill(
            fill_id=str(self.id),
            order_id=self.order_id,
            client_order_id=self.client_order_id,
            symbol=self.currency_pair,
            side=self.side,
            price=self.price,
            qty=self.amount,
            fee=self.fee,
            fee_asset=self.fee_currency,
            ts=_secs(self.create_time_ms),
        )


class GateOrderAck(GateMessage):
    """Reply to a trading call (``spot.order_place`` / ``order_cancel`` / ...).

    Not the same shape as :class:`GateOrderUpdate`: the push channel reports
    lifecycle through ``event`` + ``finish_as``, whereas a call reply carries a
    plain ``status`` (``open`` / ``closed`` / ``cancelled``). ``succeeded`` is
    only present on batch cancels, where individual legs can fail.
    """

    id: str = ""
    text: str | None = None
    currency_pair: str = ""
    type: OrderType = OrderType.LIMIT
    side: Side = Side.BUY
    amount: Decimal = Decimal("0")
    price: Decimal | None = None
    left: Decimal = Decimal("0")
    filled_total: Decimal | None = None
    avg_deal_price: Decimal | None = None
    status: str = ""
    finish_as: str | None = None
    time_in_force: str | None = None
    account: str | None = None
    create_time_ms: str | float | None = None
    update_time_ms: str | float | None = None
    succeeded: bool | None = None
    label: str | None = None
    message: str | None = None

    @property
    def client_order_id(self) -> str | None:
        return from_text(self.text)

    @property
    def filled_qty(self) -> Decimal:
        return self.amount - self.left

    @property
    def order_status(self) -> OrderStatus:
        if self.status == "closed":
            return OrderStatus.FILLED
        if self.status == "cancelled":
            return OrderStatus.CANCELED
        if self.finish_as in ("cancelled", "ioc", "stp"):
            return OrderStatus.CANCELED
        if self.finish_as == "filled":
            return OrderStatus.FILLED
        return (
            OrderStatus.PARTIALLY_FILLED
            if self.filled_qty > 0
            else OrderStatus.OPEN
        )

    def to_order(self) -> Order:
        return Order(
            order_id=self.id,
            client_order_id=self.client_order_id,
            symbol=self.currency_pair,
            side=self.side,
            type=self.type,
            status=self.order_status,
            qty=self.amount,
            price=self.price,
            filled_qty=self.filled_qty,
            avg_price=self.avg_deal_price or None,
            ts=_secs(self.update_time_ms or self.create_time_ms),
        )


class GateBalance(GateMessage):
    """``spot.balances`` — pushed on every balance-moving event.

    ``change_type`` says what moved it (``order-create``, ``trade``, ...), which
    the shared ``Balance`` has nowhere to put.
    """

    currency: str
    total: Decimal
    available: Decimal
    freeze: Decimal = Decimal("0")
    change: Decimal = Decimal("0")
    freeze_change: Decimal = Decimal("0")
    change_type: str = ""
    timestamp_ms: str | float | None = None

    @property
    def ts(self) -> float:
        return _secs(self.timestamp_ms)

    def to_balance(self) -> Balance:
        return Balance(
            asset=self.currency,
            free=self.available,
            locked=self.freeze,
        )


__all__ = [
    "TEXT_PREFIX",
    "GateBalance",
    "GateBookTicker",
    "GateCandlestick",
    "GateMessage",
    "GateOrderAck",
    "GateOrderBook",
    "GateOrderBookUpdate",
    "GateOrderUpdate",
    "GateTicker",
    "GateTrade",
    "GateUserTrade",
    "from_text",
    "to_text",
]
