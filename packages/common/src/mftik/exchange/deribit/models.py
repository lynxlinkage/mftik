"""Deribit wire models — account pushes, market pushes, and call replies.

Numbers arrive as numbers or strings. Sides are lowercase ``buy``/``sell``.
Timestamps are milliseconds.

**V5:** funding and open interest ride the ticker row (``current_funding``,
``funding_8h``, ``open_interest``) on both the REST snapshot and the
public ``ticker`` push. They are a second pump on a shared wire identity,
not a second ``SUBSCRIBE``.

**V9:** balances come from a per-currency summary. ``free`` is
``available_funds``; ``locked`` is ``max(0, equity - available_funds)``.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field

from mftik.exchange.deribit.protocol import category_of, is_linear_perp
from mftik.exchange.models import (
    Balance,
    BestQuote,
    BookLevel,
    Fill,
    FundingRate,
    Kline,
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
    "OPEN": OrderStatus.NEW,
    "FILLED": OrderStatus.FILLED,
    "REJECTED": OrderStatus.REJECTED,
    "CANCELLED": OrderStatus.CANCELED,
    "CANCELED": OrderStatus.CANCELED,
    "UNTRIGGERED": OrderStatus.NEW,
    "ARCHIVE": OrderStatus.CANCELED,
}

_TYPE: dict[str, OrderType] = {
    "LIMIT": OrderType.LIMIT,
    "MARKET": OrderType.MARKET,
}


def status_of(value: str | None) -> OrderStatus:
    raw = (value or "").upper()
    if raw == "OPEN":
        return OrderStatus.NEW
    return _STATUS.get(raw, OrderStatus.UNKNOWN)


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
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    if number > 1e12:
        return number / 1000.0
    return number


Dec = Annotated[Decimal, BeforeValidator(_dec)]
OptDec = Annotated[Decimal | None, BeforeValidator(_opt_dec)]
VenueSide = Annotated[Side, BeforeValidator(_lower)]
Ms = Annotated[float, BeforeValidator(_secs)]


def _levels(rows: Any) -> list[BookLevel]:
    out: list[BookLevel] = []
    for row in rows or []:
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            continue
        # Incremental book: ["new"|"change"|"delete", price, amount]
        if len(row) >= 3 and isinstance(row[0], str):
            action, price, qty = row[0], row[1], row[2]
            if str(action).casefold() == "delete":
                qty = 0
        else:
            price, qty = row[0], row[1]
        try:
            out.append(
                BookLevel(price=Decimal(str(price)), qty=Decimal(str(qty)))
            )
        except (InvalidOperation, ValueError):
            continue
    return out


class DeribitMessage(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True, extra="ignore")


# --- private ---------------------------------------------------------------


class DeribitOrderUpdate(DeribitMessage):
    """One order object — buy/sell result, open-order row, or user.orders."""

    instrument_name: str = ""
    order_id: str = ""
    label: str = ""
    direction: VenueSide = Side.BUY
    order_type: str = ""
    order_state: str = ""
    price: Dec = Decimal("0")
    amount: Dec = Decimal("0")
    filled_amount: Dec = Decimal("0")
    average_price: OptDec = None
    reduce_only: bool = False
    creation_timestamp: Ms = 0.0
    last_update_timestamp: Ms = 0.0

    @property
    def client_order_id(self) -> str | None:
        return self.label or None

    @property
    def side(self) -> Side:
        return self.direction

    def to_order(self, ticker: UniversalTicker) -> Order:
        filled = self.filled_amount
        status = status_of(self.order_state)
        if status is OrderStatus.NEW and filled > 0:
            status = OrderStatus.PARTIALLY_FILLED
        return Order(
            universal_ticker=str(ticker),
            order_id=self.order_id,
            client_order_id=self.client_order_id,
            side=self.side,
            type=type_of(self.order_type),
            status=status,
            qty=self.amount,
            price=self.price or None,
            filled_qty=filled,
            avg_price=self.average_price,
            ts=self.last_update_timestamp or self.creation_timestamp,
        )


class DeribitOrderAck(DeribitMessage):
    """``private/buy`` / ``private/sell`` wraps the order under ``order``."""

    order: DeribitOrderUpdate | None = None
    order_id: str = ""
    label: str = ""

    @property
    def client_order_id(self) -> str | None:
        if self.order is not None:
            return self.order.client_order_id
        return self.label or None

    def placed(self) -> DeribitOrderUpdate:
        if self.order is not None:
            return self.order
        return DeribitOrderUpdate(order_id=self.order_id, label=self.label)


class DeribitFill(DeribitMessage):
    instrument_name: str = ""
    trade_id: str = ""
    order_id: str = ""
    label: str = ""
    direction: VenueSide = Side.BUY
    price: Dec = Decimal("0")
    amount: Dec = Decimal("0")
    fee: Dec = Decimal("0")
    fee_currency: str = ""
    timestamp: Ms = 0.0

    @property
    def is_fill(self) -> bool:
        return self.amount > 0

    def to_fill(self, ticker: UniversalTicker) -> Fill:
        fee = self.fee
        if fee < 0:
            fee = -fee
        return Fill(
            universal_ticker=str(ticker),
            fill_id=self.trade_id,
            order_id=self.order_id,
            client_order_id=self.label or None,
            side=self.direction,
            price=self.price,
            qty=self.amount,
            fee=fee,
            fee_asset=self.fee_currency,
            ts=self.timestamp,
        )


class DeribitSummary(DeribitMessage):
    """One currency row of ``get_account_summaries`` / ``user.portfolio``."""

    currency: str = ""
    balance: Dec = Decimal("0")
    equity: Dec = Decimal("0")
    available_funds: Dec = Decimal("0")
    available_withdrawal_funds: Dec = Decimal("0")
    spot_reserve: Dec = Decimal("0")
    initial_margin: Dec = Decimal("0")
    maintenance_margin: Dec = Decimal("0")
    margin_model: str = ""
    portfolio_margining_enabled: bool = False
    cross_collateral_enabled: bool = False

    def to_balance(self) -> Balance | None:
        if not self.currency:
            return None
        locked = self.equity - self.available_funds
        if locked < 0:
            locked = Decimal("0")
        return Balance(
            asset=self.currency.upper(),
            free=self.available_funds,
            locked=locked,
        )


class DeribitAccountSummaries(DeribitMessage):
    id: int | None = None
    type: str = ""
    summaries: list[DeribitSummary] = Field(default_factory=list)

    def to_balances(self) -> list[Balance]:
        out: list[Balance] = []
        for row in self.summaries:
            balance = row.to_balance()
            if balance is not None:
                out.append(balance)
        return out

    def margin_model(self) -> str:
        for row in self.summaries:
            if row.margin_model:
                return row.margin_model
        return ""


class DeribitPosition(DeribitMessage):
    instrument_name: str = ""
    kind: str = ""
    instrument_type: str = ""
    future_type: str = ""
    settlement_period: str = ""
    size: Dec = Decimal("0")
    average_price: OptDec = None
    floating_profit_loss: Dec = Decimal("0")
    direction: str = ""

    @property
    def is_linear_perp(self) -> bool:
        if self.kind and self.kind.casefold() not in {"", "future"}:
            return False
        if self.settlement_period or self.instrument_type or self.future_type:
            return is_linear_perp(
                instrument_type=self.instrument_type,
                future_type=self.future_type,
                settlement_period=self.settlement_period,
                kind=self.kind,
            )
        # get_positions often omits settlement_period; a linear name has `_`.
        return "_" in self.instrument_name and self.instrument_name.endswith(
            "-PERPETUAL"
        )

    def to_position(self, ticker: UniversalTicker) -> Position:
        return Position(
            universal_ticker=str(ticker),
            qty=self.size,
            entry_price=self.average_price,
            unrealised_pnl=self.floating_profit_loss,
        )


# --- public ----------------------------------------------------------------


class DeribitPublicTrade(DeribitMessage):
    instrument_name: str = ""
    trade_id: str = ""
    price: Dec = Decimal("0")
    amount: Dec = Decimal("0")
    direction: VenueSide = Side.BUY
    timestamp: Ms = 0.0

    def to_trade(self, ticker: UniversalTicker) -> Trade:
        return Trade(
            universal_ticker=str(ticker),
            trade_id=self.trade_id,
            price=self.price,
            qty=self.amount,
            side=self.direction,
            ts=self.timestamp,
        )


class DeribitTicker(DeribitMessage):
    """REST ``public/ticker`` and the public ``ticker`` push. V5 lives here."""

    instrument_name: str = ""
    last_price: OptDec = None
    best_bid_price: OptDec = None
    best_bid_amount: OptDec = None
    best_ask_price: OptDec = None
    best_ask_amount: OptDec = None
    current_funding: OptDec = None
    funding_8h: OptDec = None
    open_interest: OptDec = None
    timestamp: Ms = 0.0

    @property
    def quoted(self) -> bool:
        return self.last_price is not None or (
            self.best_bid_price is not None and self.best_ask_price is not None
        )

    def to_ticker(self, ticker: UniversalTicker, *, ts: float = 0.0) -> Ticker:
        last = self.last_price or Decimal("0")
        bid = self.best_bid_price if self.best_bid_price else last
        ask = self.best_ask_price if self.best_ask_price else last
        fields: dict[str, Any] = {} if ts <= 0 else {"ts": ts}
        return Ticker(
            universal_ticker=str(ticker), bid=bid, ask=ask, last=last, **fields
        )

    def to_best_quote(
        self, ticker: UniversalTicker, *, ts: float = 0.0
    ) -> BestQuote | None:
        if (
            not self.best_bid_price
            or not self.best_ask_price
            or not self.best_bid_amount
            or not self.best_ask_amount
        ):
            return None
        return BestQuote(
            universal_ticker=str(ticker),
            bid=self.best_bid_price,
            bid_qty=self.best_bid_amount,
            ask=self.best_ask_price,
            ask_qty=self.best_ask_amount,
            ts=ts or self.timestamp,
        )

    def to_funding_rate(
        self, ticker: UniversalTicker, *, ts: float = 0.0
    ) -> FundingRate | None:
        rate = self.current_funding
        if rate is None:
            rate = self.funding_8h
        if rate is None:
            return None
        return FundingRate(
            universal_ticker=str(ticker),
            rate=rate,
            ts=ts or self.timestamp,
        )

    def to_open_interest(
        self, ticker: UniversalTicker, *, ts: float = 0.0
    ) -> OpenInterest | None:
        if self.open_interest is None:
            return None
        return OpenInterest(
            universal_ticker=str(ticker),
            qty=self.open_interest,
            ts=ts or self.timestamp,
        )


class DeribitQuote(DeribitMessage):
    instrument_name: str = ""
    best_bid_price: OptDec = None
    best_bid_amount: OptDec = None
    best_ask_price: OptDec = None
    best_ask_amount: OptDec = None
    timestamp: Ms = 0.0

    def to_best_quote(
        self, ticker: UniversalTicker, *, ts: float = 0.0
    ) -> BestQuote | None:
        if (
            not self.best_bid_price
            or not self.best_ask_price
            or not self.best_bid_amount
            or not self.best_ask_amount
        ):
            return None
        return BestQuote(
            universal_ticker=str(ticker),
            bid=self.best_bid_price,
            bid_qty=self.best_bid_amount,
            ask=self.best_ask_price,
            ask_qty=self.best_ask_amount,
            ts=ts or self.timestamp,
        )


class DeribitOrderBook(DeribitMessage):
    instrument_name: str = ""
    bids: list[Any] = Field(default_factory=list)
    asks: list[Any] = Field(default_factory=list)
    change_id: int | None = None
    prev_change_id: int | None = None
    timestamp: Ms = 0.0

    def bid_levels(self) -> list[BookLevel]:
        return _levels(self.bids)

    def ask_levels(self) -> list[BookLevel]:
        return _levels(self.asks)

    def to_order_book(self, ticker: UniversalTicker) -> OrderBook:
        return OrderBook(
            universal_ticker=str(ticker),
            bids=self.bid_levels(),
            asks=self.ask_levels(),
            ts=self.timestamp,
        )


def kline_from_tick(
    row: dict[str, Any],
    ticker: UniversalTicker,
    interval: str,
) -> Kline | None:
    """One ``chart.trades`` push row → a candle."""
    tick = row.get("tick")
    if tick is None:
        return None
    try:
        ts = float(tick)
    except (TypeError, ValueError):
        return None
    if ts > 1e12:
        ts = ts / 1000.0

    def _dec(key: str) -> Decimal:
        value = row.get(key)
        if value is None or value == "":
            return Decimal("0")
        return Decimal(str(value))

    return Kline(
        universal_ticker=str(ticker),
        interval=interval,
        open_time=ts,
        open=_dec("open"),
        high=_dec("high"),
        low=_dec("low"),
        close=_dec("close"),
        volume=_dec("volume"),
    )


def kline_from_chart(
    *,
    ticker: UniversalTicker,
    interval: str,
    ticks: list[Any],
    opens: list[Any],
    highs: list[Any],
    lows: list[Any],
    closes: list[Any],
    volumes: list[Any],
) -> list[Kline]:
    """``public/get_tradingview_chart_data`` arrays → oldest-first candles."""
    out: list[Kline] = []
    for index, tick in enumerate(ticks or []):
        try:
            ts = float(tick)
        except (TypeError, ValueError):
            continue
        if ts > 1e12:
            ts = ts / 1000.0

        def _at(values: list[Any], default: str = "0") -> Decimal:
            if index >= len(values):
                return Decimal(default)
            return Decimal(str(values[index] or default))

        out.append(
            Kline(
                universal_ticker=str(ticker),
                interval=interval,
                open_time=ts,
                open=_at(opens),
                high=_at(highs),
                low=_at(lows),
                close=_at(closes),
                volume=_at(volumes),
            )
        )
    return out


def order_book_from_result(
    result: dict[str, Any], ticker: UniversalTicker
) -> OrderBook:
    return DeribitOrderBook.model_validate(result).to_order_book(ticker)


def inbound_category(kind: str, default: Category) -> Category:
    return category_of(kind, default=default)


__all__ = [
    "DeribitAccountSummaries",
    "DeribitFill",
    "DeribitOrderAck",
    "DeribitOrderBook",
    "DeribitOrderUpdate",
    "DeribitPosition",
    "DeribitPublicTrade",
    "DeribitQuote",
    "DeribitSummary",
    "DeribitTicker",
    "category_of",
    "inbound_category",
    "kline_from_chart",
    "kline_from_tick",
    "order_book_from_result",
    "status_of",
    "type_of",
]
