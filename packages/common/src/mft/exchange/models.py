"""Exchange domain models shared by all venue adapters."""

from __future__ import annotations

import time
import uuid
from decimal import Decimal
from enum import StrEnum

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
    model_config = ConfigDict(frozen=True)

    symbol: str
    base: str
    quote: str
    tick_size: Decimal = Decimal("0.01")
    lot_size: Decimal = Decimal("0.0001")


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


class BookLevel(BaseModel):
    model_config = ConfigDict(frozen=True)

    price: Decimal
    qty: Decimal


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
    model_config = ConfigDict(frozen=True)

    symbol: str
    side: Side
    type: OrderType
    qty: Decimal
    price: Decimal | None = None
    client_order_id: str | None = None
