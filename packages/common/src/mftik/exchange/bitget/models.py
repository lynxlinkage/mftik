"""Bitget UTA v3 wire models — account pushes, market pushes, and call replies.

Numbers arrive as strings. Empty means unset. Sides are lowercase
``buy``/``sell``. Timestamps are milliseconds.

**V5:** funding and open interest ride the ticker row (``fundingRate``,
``openInterest``) on both the REST snapshot and the public ``ticker``
push. They are a second pump on a shared wire identity, not a second
``SUBSCRIBE``.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field

from mftik.exchange.bitget.protocol import category_of
from mftik.exchange.models import (
    Balance,
    BestQuote,
    BookLevel,
    Fill,
    FundingRate,
    Kline,
    Liquidation,
    OpenInterest,
    Order,
    OrderBook,
    OrderStatus,
    OrderType,
    Side,
    Ticker,
    Trade,
)
from mftik.exchange.oms import Position
from mftik.exchange.tickers import Category, UniversalTicker

_STATUS: dict[str, OrderStatus] = {
    "LIVE": OrderStatus.NEW,
    "NEW": OrderStatus.NEW,
    "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
    "PARTIAL_FILL": OrderStatus.PARTIALLY_FILLED,
    "FILLED": OrderStatus.FILLED,
    "CANCELLED": OrderStatus.CANCELED,
    "CANCELED": OrderStatus.CANCELED,
    "REJECTED": OrderStatus.REJECTED,
}

_TYPE: dict[str, OrderType] = {
    "LIMIT": OrderType.LIMIT,
    "MARKET": OrderType.MARKET,
}

#: Post-only refusals that arrive as a cancellation rather than a REST
#: reject. Public so ``mftik_td.errors`` can hold a matching entry.
CANCEL_REFUSALS: dict[str, str] = {
    "post_only_would_take": "The post-only order will take liquidity",
}


def status_of(value: str | None) -> OrderStatus:
    return _STATUS.get((value or "").upper(), OrderStatus.UNKNOWN)


def type_of(value: str | None) -> OrderType:
    return _TYPE.get((value or "").upper(), OrderType.LIMIT)


def _dec(value: Any) -> Any:
    if value is None or value == "":
        return Decimal("0")
    return value


def _opt_dec(value: Any) -> Any:
    if value is None or value == "":
        return None
    return value


def _lower(value: Any) -> Any:
    return value.lower() if isinstance(value, str) else value


def _secs(value: Any) -> Any:
    if value is None or value == "":
        return 0.0
    try:
        return float(value) / 1000.0
    except (TypeError, ValueError):
        return 0.0


Dec = Annotated[Decimal, BeforeValidator(_dec)]
OptDec = Annotated[Decimal | None, BeforeValidator(_opt_dec)]
VenueSide = Annotated[Side, BeforeValidator(_lower)]
Ms = Annotated[float, BeforeValidator(_secs)]


def _levels(rows: Any) -> list[BookLevel]:
    out: list[BookLevel] = []
    for row in rows or []:
        if len(row) < 2:
            continue
        try:
            out.append(
                BookLevel(price=Decimal(str(row[0])), qty=Decimal(str(row[1])))
            )
        except (InvalidOperation, ValueError):
            continue
    return out


class BitgetMessage(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True, extra="ignore")


# --- private ---------------------------------------------------------------


class BitgetOrderUpdate(BitgetMessage):
    """One row of the UTA ``order`` topic — and of unfilled / history REST."""

    category: str = ""
    symbol: str = ""
    order_id: str = Field(default="", alias="orderId")
    client_oid: str = Field(default="", alias="clientOid")
    side: VenueSide = Side.BUY
    order_type: str = Field(default="limit", alias="orderType")
    status: str = Field(default="", alias="orderStatus")
    price: Dec = Decimal("0")
    qty: Dec = Decimal("0")
    acc_base: Dec = Field(default=Decimal("0"), alias="accBaseVolume")
    avg_price: OptDec = Field(default=None, alias="avgPrice")
    pos_side: str = Field(default="", alias="posSide")
    reduce_only: str = Field(default="", alias="reduceOnly")
    cancel_reason: str = Field(default="", alias="cancelReason")
    created_time: Ms = Field(default=0.0, alias="createdTime")
    updated_time: Ms = Field(default=0.0, alias="updatedTime")

    @property
    def client_order_id(self) -> str | None:
        return self.client_oid or None

    @property
    def order_status(self) -> OrderStatus:
        status = status_of(self.status)
        if status is OrderStatus.CANCELED and self.refusal:
            return OrderStatus.REJECTED
        return status

    @property
    def refusal(self) -> str:
        reason = self.cancel_reason.strip()
        words = CANCEL_REFUSALS.get(reason)
        if words:
            return words
        return ""

    def to_order(self, ticker: UniversalTicker) -> Order:
        return Order(
            universal_ticker=str(ticker),
            order_id=self.order_id,
            client_order_id=self.client_order_id,
            side=self.side,
            type=type_of(self.order_type),
            status=self.order_status,
            qty=self.qty,
            price=self.price or None,
            filled_qty=self.acc_base,
            avg_price=self.avg_price,
            reject_reason=self.refusal,
            ts=self.updated_time or self.created_time,
        )


class BitgetOrderAck(BitgetMessage):
    order_id: str = Field(default="", alias="orderId")
    client_oid: str = Field(default="", alias="clientOid")

    @property
    def client_order_id(self) -> str | None:
        return self.client_oid or None


class BitgetFill(BitgetMessage):
    category: str = ""
    symbol: str = ""
    order_id: str = Field(default="", alias="orderId")
    client_oid: str = Field(default="", alias="clientOid")
    exec_id: str = Field(default="", alias="execId")
    side: VenueSide = Side.BUY
    price: Dec = Field(default=Decimal("0"), alias="execPrice")
    qty: Dec = Field(default=Decimal("0"), alias="execQty")
    fee: Dec = Field(default=Decimal("0"), alias="fee")
    fee_coin: str = Field(default="", alias="feeCoin")
    ts: Ms = Field(default=0.0, alias="execTime")

    @property
    def is_fill(self) -> bool:
        return self.qty > 0

    def to_fill(self, ticker: UniversalTicker) -> Fill:
        fee = self.fee
        if fee < 0:
            fee = -fee
        return Fill(
            universal_ticker=str(ticker),
            fill_id=self.exec_id,
            order_id=self.order_id,
            client_order_id=self.client_oid or None,
            side=self.side,
            price=self.price,
            qty=self.qty,
            fee=fee,
            fee_asset=self.fee_coin,
            ts=self.ts,
        )


class BitgetAsset(BitgetMessage):
    """One UTA asset row — never a funding-account coin (V9 / I7)."""

    coin: str = ""
    available: Dec = Decimal("0")
    frozen: Dec = Decimal("0")
    locked: Dec = Decimal("0")

    def to_balance(self) -> Balance | None:
        if not self.coin:
            return None
        locked = self.frozen + self.locked
        return Balance(asset=self.coin.upper(), free=self.available, locked=locked)


class BitgetAccount(BitgetMessage):
    """UTA ``account`` push — a list of asset rows, or a wrapper around one."""

    coin: str = ""
    available: Dec = Decimal("0")
    frozen: Dec = Decimal("0")
    locked: Dec = Decimal("0")
    assets: list[BitgetAsset] = Field(default_factory=list)

    def to_balances(self) -> list[Balance]:
        rows = list(self.assets)
        if self.coin:
            rows.append(
                BitgetAsset(
                    coin=self.coin,
                    available=self.available,
                    frozen=self.frozen,
                    locked=self.locked,
                )
            )
        out: list[Balance] = []
        for row in rows:
            balance = row.to_balance()
            if balance is not None:
                out.append(balance)
        return out


class BitgetPosition(BitgetMessage):
    category: str = ""
    symbol: str = ""
    hold_side: str = Field(default="", alias="holdSide")
    pos_side: str = Field(default="", alias="posSide")
    total: Dec = Decimal("0")
    size: Dec = Decimal("0")
    avg_price: OptDec = Field(default=None, alias="openPriceAvg")
    upl: Dec = Field(default=Decimal("0"), alias="unrealisedPnl")

    @property
    def side(self) -> str:
        return (self.hold_side or self.pos_side).lower()

    @property
    def signed_size(self) -> Decimal:
        qty = self.total or self.size
        if self.side == "short":
            return -abs(qty)
        if self.side == "long":
            return abs(qty)
        return qty

    def to_position(self, ticker: UniversalTicker) -> Position:
        return Position(
            universal_ticker=str(ticker),
            qty=self.signed_size,
            entry_price=self.avg_price,
            unrealised_pnl=self.upl,
        )


class BitgetSettings(BitgetMessage):
    uid: str = ""
    account_mode: str = Field(default="", alias="accountMode")
    hold_mode: str = Field(default="", alias="holdMode")
    account_level: str = Field(default="", alias="accountLevel")
    asset_mode: str = Field(default="", alias="assetMode")


# --- public ----------------------------------------------------------------


class BitgetPublicTrade(BitgetMessage):
    """``publicTrade`` — compact field names on the wire."""

    symbol: str = ""
    price: Dec = Field(default=Decimal("0"), alias="p")
    qty: Dec = Field(default=Decimal("0"), alias="v")
    side: VenueSide = Field(default=Side.BUY, alias="S")
    trade_id: str = Field(default="", alias="i")
    ts: Ms = Field(default=0.0, alias="T")

    def to_trade(self, ticker: UniversalTicker) -> Trade:
        return Trade(
            universal_ticker=str(ticker),
            trade_id=self.trade_id,
            price=self.price,
            qty=self.qty,
            side=self.side,
            ts=self.ts,
        )


class BitgetTicker(BitgetMessage):
    """REST tickers row and the public ``ticker`` push. V5 lives here."""

    symbol: str = ""
    category: str = ""
    last_price: OptDec = Field(default=None, alias="lastPrice")
    bid: OptDec = Field(default=None, alias="bid1Price")
    bid_qty: OptDec = Field(default=None, alias="bid1Size")
    ask: OptDec = Field(default=None, alias="ask1Price")
    ask_qty: OptDec = Field(default=None, alias="ask1Size")
    high_24h: OptDec = Field(default=None, alias="highPrice24h")
    low_24h: OptDec = Field(default=None, alias="lowPrice24h")
    volume_24h: OptDec = Field(default=None, alias="volume24h")
    mark_price: OptDec = Field(default=None, alias="markPrice")
    index_price: OptDec = Field(default=None, alias="indexPrice")
    funding_rate: OptDec = Field(default=None, alias="fundingRate")
    open_interest: OptDec = Field(default=None, alias="openInterest")
    ts: Ms = 0.0

    @property
    def quoted(self) -> bool:
        return self.last_price is not None or (
            self.bid is not None and self.ask is not None
        )

    def to_ticker(self, ticker: UniversalTicker, *, ts: float = 0.0) -> Ticker:
        last = self.last_price or Decimal("0")
        bid = self.bid if self.bid else last
        ask = self.ask if self.ask else last
        fields: dict[str, Any] = {} if ts <= 0 else {"ts": ts}
        return Ticker(
            universal_ticker=str(ticker), bid=bid, ask=ask, last=last, **fields
        )

    def to_best_quote(
        self, ticker: UniversalTicker, *, ts: float = 0.0
    ) -> BestQuote | None:
        if not self.bid or not self.ask or not self.bid_qty or not self.ask_qty:
            return None
        return BestQuote(
            universal_ticker=str(ticker),
            bid=self.bid,
            bid_qty=self.bid_qty,
            ask=self.ask,
            ask_qty=self.ask_qty,
            ts=ts or self.ts,
        )

    def to_funding_rate(
        self, ticker: UniversalTicker, *, ts: float = 0.0
    ) -> FundingRate | None:
        if self.funding_rate is None:
            return None
        return FundingRate(
            universal_ticker=str(ticker),
            rate=self.funding_rate,
            ts=ts or self.ts,
        )

    def to_open_interest(
        self, ticker: UniversalTicker, *, ts: float = 0.0
    ) -> OpenInterest | None:
        if self.open_interest is None:
            return None
        return OpenInterest(
            universal_ticker=str(ticker),
            qty=self.open_interest,
            ts=ts or self.ts,
        )


class BitgetOrderBook(BitgetMessage):
    symbol: str = ""
    bids: list[Any] = Field(default_factory=list, alias="b")
    asks: list[Any] = Field(default_factory=list, alias="a")
    ts: Ms = 0.0

    def bid_levels(self) -> list[BookLevel]:
        return _levels(self.bids)

    def ask_levels(self) -> list[BookLevel]:
        return _levels(self.asks)

    def to_order_book(self, ticker: UniversalTicker) -> OrderBook:
        return OrderBook(
            universal_ticker=str(ticker),
            bids=self.bid_levels(),
            asks=self.ask_levels(),
            ts=self.ts,
        )

    def to_best_quote(
        self, ticker: UniversalTicker, *, ts: float = 0.0
    ) -> BestQuote | None:
        bids = self.bid_levels()
        asks = self.ask_levels()
        if not bids or not asks:
            return None
        return BestQuote(
            universal_ticker=str(ticker),
            bid=bids[0].price,
            bid_qty=bids[0].qty,
            ask=asks[0].price,
            ask_qty=asks[0].qty,
            ts=ts or self.ts,
        )


class BitgetLiquidation(BitgetMessage):
    symbol: str = ""
    side: VenueSide = Side.BUY
    price: Dec = Decimal("0")
    amount: Dec = Decimal("0")
    ts: Ms = 0.0

    def to_liquidation(self, ticker: UniversalTicker) -> Liquidation:
        qty = self.amount
        if self.price > 0:
            qty = self.amount / self.price
        return Liquidation(
            universal_ticker=str(ticker),
            price=self.price,
            qty=qty,
            side=self.side,
            ts=self.ts,
        )


def kline_from_row(
    row: list[Any], ticker: UniversalTicker, interval: str
) -> Kline:
    ts = float(row[0] or 0) / 1000.0 if row else 0.0
    open_ = Decimal(str(row[1])) if len(row) > 1 else Decimal("0")
    high = Decimal(str(row[2])) if len(row) > 2 else Decimal("0")
    low = Decimal(str(row[3])) if len(row) > 3 else Decimal("0")
    close = Decimal(str(row[4])) if len(row) > 4 else Decimal("0")
    volume = Decimal(str(row[5])) if len(row) > 5 else Decimal("0")
    return Kline(
        universal_ticker=str(ticker),
        interval=interval,
        open_time=ts,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
    )


def order_book_from_result(result: dict[str, Any], ticker: UniversalTicker) -> OrderBook:
    return BitgetOrderBook.model_validate(result).to_order_book(ticker)


def inbound_category(row_category: str, default: Category) -> Category:
    return category_of(row_category, default)


__all__ = [
    "CANCEL_REFUSALS",
    "BitgetAccount",
    "BitgetAsset",
    "BitgetFill",
    "BitgetLiquidation",
    "BitgetOrderAck",
    "BitgetOrderBook",
    "BitgetOrderUpdate",
    "BitgetPosition",
    "BitgetPublicTrade",
    "BitgetSettings",
    "BitgetTicker",
    "category_of",
    "inbound_category",
    "kline_from_row",
    "order_book_from_result",
    "status_of",
    "type_of",
]
