"""Exchange domain models shared by all venue adapters."""

from __future__ import annotations

import time
import uuid
from decimal import Decimal
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


def _id() -> str:
    return uuid.uuid4().hex


def _ts() -> float:
    return time.time()


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"


class OrderType(StrEnum):
    MARKET = "market"
    LIMIT = "limit"


class OrderStatus(StrEnum):
    NEW = "new"
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"


class Instrument(BaseModel):
    """A tradeable instrument and the restrictions on trading it.

    ``tick_size`` / ``lot_size`` are the price and quantity steps; ``min_qty``
    and ``min_notional`` are floors an order has to clear. ``None`` means the
    venue publishes no such floor.
    """

    model_config = ConfigDict(frozen=True)

    symbol: str
    base: str
    quote: str
    tick_size: Decimal = Decimal("0.01")
    lot_size: Decimal = Decimal("0.0001")
    min_qty: Decimal | None = None
    min_notional: Decimal | None = None


class Ticker(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    bid: Decimal
    ask: Decimal
    last: Decimal
    ts: float = Field(default_factory=_ts)


class Trade(BaseModel):
    model_config = ConfigDict(frozen=True)

    trade_id: str = Field(default_factory=_id)
    symbol: str
    price: Decimal
    qty: Decimal
    side: Side
    ts: float = Field(default_factory=_ts)


class Kline(BaseModel):
    """One candle. ``closed`` marks the final tick of the window.

    Venues re-push the in-progress candle on every change, so an unclosed
    ``Kline`` for the same ``open_time`` supersedes the one before it — only
    ``closed`` candles are safe to append to a series.
    """

    model_config = ConfigDict(frozen=True)

    symbol: str
    interval: str
    open_time: float
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal = Decimal("0")
    quote_volume: Decimal = Decimal("0")
    closed: bool = False
    ts: float = Field(default_factory=_ts)


class BookLevel(BaseModel):
    model_config = ConfigDict(frozen=True)

    price: Decimal
    qty: Decimal


class BestQuote(BaseModel):
    """Top of book with sizes — pushed on every best bid/ask change.

    Distinct from :class:`Ticker`, which carries 24h stats on a venue-chosen
    cadence: this is the quote alone, at book speed, and it has the resting
    sizes a :class:`Ticker` does not.
    """

    model_config = ConfigDict(frozen=True)

    symbol: str
    bid: Decimal
    bid_qty: Decimal
    ask: Decimal
    ask_qty: Decimal
    ts: float = Field(default_factory=_ts)


class OrderBook(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    bids: list[BookLevel]
    asks: list[BookLevel]
    ts: float = Field(default_factory=_ts)


class Balance(BaseModel):
    model_config = ConfigDict(frozen=True)

    asset: str
    free: Decimal
    locked: Decimal = Decimal("0")

    @property
    def total(self) -> Decimal:
        return self.free + self.locked


class Order(BaseModel):
    model_config = ConfigDict(frozen=True)

    order_id: str = Field(default_factory=_id)
    client_order_id: str | None = None
    symbol: str
    side: Side
    type: OrderType
    status: OrderStatus
    qty: Decimal
    price: Decimal | None = None
    filled_qty: Decimal = Decimal("0")
    avg_price: Decimal | None = None
    ts: float = Field(default_factory=_ts)


class Fill(BaseModel):
    model_config = ConfigDict(frozen=True)

    fill_id: str = Field(default_factory=_id)
    order_id: str
    client_order_id: str | None = None
    symbol: str
    side: Side
    price: Decimal
    qty: Decimal
    fee: Decimal = Decimal("0")
    fee_asset: str = ""
    ts: float = Field(default_factory=_ts)


class PlaceOrderRequest(BaseModel):
    """Common order fields, plus venue-specific extras in ``params``.

    ``symbol`` is canonical (``BTCUSDT``); the adapter renders the venue's
    spelling. ``params`` carries what has no cross-venue meaning — Gate's
    ``account`` / ``time_in_force`` / ``iceberg`` / ``stp_act``, for instance.
    Each adapter reads the keys it understands and ignores the rest, so a
    request stays portable even when it carries hints for one venue.
    """

    model_config = ConfigDict(frozen=True)

    symbol: str
    side: Side
    type: OrderType
    qty: Decimal
    price: Decimal | None = None
    client_order_id: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
