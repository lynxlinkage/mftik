"""Gate USDT-perpetual wire models.

Sizes on the wire are **contracts**. Every ``to_*`` converter takes the
instrument's ``contract_size`` (quanto multiplier) and emits the shared
models in **base**, which is what :class:`PlaceOrderRequest.qty` uses.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from mftik.exchange.gate.spot.models import TEXT_PREFIX, from_text, to_text
from mftik.exchange.models import (
    Balance,
    BestQuote,
    BookLevel,
    Fill,
    FundingRate,
    Kline,
    Liquidation,
    Order,
    OrderBook,
    OrderStatus,
    OrderType,
    Side,
    Ticker,
    Trade,
)
from mftik.exchange.oms import Position
from mftik.exchange.tickers import UniversalTicker


def _dec(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    return Decimal(str(value))


def _ts(value: Any) -> float:
    """Seconds if the venue sent seconds; divide when it sent milliseconds."""
    if value is None or value == "":
        return 0.0
    number = float(value)
    return number / 1000.0 if number > 1e12 else number


def contracts_to_base(size: Decimal, contract_size: Decimal) -> Decimal:
    return size * contract_size


def base_to_contracts(qty: Decimal, contract_size: Decimal) -> Decimal:
    if contract_size <= 0:
        raise ValueError(f"contract_size must be positive, got {contract_size}")
    return qty / contract_size


def signed_contracts(side: Side, contracts: Decimal) -> Decimal:
    """Positive size is buy; negative is sell."""
    return contracts if side is Side.BUY else -contracts


def format_size(value: Decimal) -> str:
    """Decimal → wire string without scientific notation (``-1E+1``)."""
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def side_of_size(size: Decimal) -> Side:
    return Side.SELL if size < 0 else Side.BUY


def _levels(rows: list[Any] | None, contract_size: Decimal) -> list[BookLevel]:
    out: list[BookLevel] = []
    for row in rows or []:
        if len(row) < 2:
            continue
        out.append(
            BookLevel(
                price=Decimal(str(row[0])),
                qty=contracts_to_base(Decimal(str(row[1])), contract_size),
            )
        )
    return out


class GateMessage(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True, extra="ignore")


class GateFuturesTicker(GateMessage):
    """``futures.tickers`` — last/mark/funding; no bid/ask on this channel."""

    contract: str
    last: Decimal
    mark_price: Decimal | None = None
    index_price: Decimal | None = None
    funding_rate: Decimal | None = None
    high_24h: Decimal | None = None
    low_24h: Decimal | None = None
    volume_24h_base: Decimal | None = None
    volume_24h_quote: Decimal | None = None
    t: int | None = None

    def to_ticker(self, ticker: UniversalTicker) -> Ticker:
        # No quote on this push — last stands in so a Ticker is still honest
        # about "a price", and ``stream_best_quote`` is the BBO.
        return Ticker(
            universal_ticker=str(ticker),
            bid=self.last,
            ask=self.last,
            last=self.last,
        )

    def to_funding_rate(
        self, ticker: UniversalTicker, *, ts: float
    ) -> FundingRate | None:
        """The still-moving prediction, when this push named one.

        ``ts`` is the frame stamp (or local receive) the caller already chose;
        the row's ``t`` is optional and is not used here.
        """
        if self.funding_rate is None:
            return None
        return FundingRate(
            universal_ticker=str(ticker),
            rate=self.funding_rate,
            ts=ts,
        )


class GateFuturesTrade(GateMessage):
    """``futures.trades`` — public tape. Size sign is the taker's side."""

    id: int | str
    contract: str
    size: Decimal
    price: Decimal
    create_time: int | float | None = None
    create_time_ms: int | float | str | None = None
    is_internal: bool = False

    def to_trade(self, ticker: UniversalTicker, contract_size: Decimal) -> Trade:
        return Trade(
            trade_id=str(self.id),
            universal_ticker=str(ticker),
            price=self.price,
            qty=contracts_to_base(abs(self.size), contract_size),
            side=side_of_size(self.size),
            ts=_ts(self.create_time_ms or self.create_time),
        )


class GateFuturesCandlestick(GateMessage):
    """``futures.candlesticks``. ``n`` is ``"<interval>_<contract>"``."""

    t: int
    v: Decimal = Decimal("0")
    c: Decimal
    h: Decimal
    low: Decimal = Field(alias="l")
    o: Decimal
    n: str = ""
    sum: Decimal = Decimal("0")
    w: bool = False

    @property
    def interval(self) -> str:
        return self.n.split("_", 1)[0] if self.n else ""

    @property
    def contract(self) -> str:
        parts = self.n.split("_", 1)
        return parts[1] if len(parts) > 1 else ""

    def to_kline(
        self,
        ticker: UniversalTicker,
        interval: str,
        contract_size: Decimal,
    ) -> Kline:
        return Kline(
            universal_ticker=str(ticker),
            interval=interval,
            open_time=float(self.t),
            open=self.o,
            high=self.h,
            low=self.low,
            close=self.c,
            volume=contracts_to_base(self.v, contract_size),
            quote_volume=self.sum,
            closed=self.w,
        )


class GateFuturesBookTicker(GateMessage):
    """``futures.book_ticker`` — best bid/ask."""

    t: int | None = None
    contract: str = Field(default="", alias="s")
    bid: Decimal = Field(alias="b")
    bid_size: Decimal = Field(alias="B")
    ask: Decimal = Field(alias="a")
    ask_size: Decimal = Field(alias="A")

    def to_best_quote(
        self, ticker: UniversalTicker, contract_size: Decimal
    ) -> BestQuote:
        return BestQuote(
            universal_ticker=str(ticker),
            bid=self.bid,
            bid_qty=contracts_to_base(self.bid_size, contract_size),
            ask=self.ask,
            ask_qty=contracts_to_base(self.ask_size, contract_size),
            ts=_ts(self.t),
        )


class GateFuturesOrderBook(GateMessage):
    """``futures.order_book`` — capped-depth snapshot."""

    t: int | None = None
    contract: str = Field(default="", alias="s")
    id: int | None = None
    bids: list[Any] = Field(default_factory=list)
    asks: list[Any] = Field(default_factory=list)

    def to_order_book(
        self, ticker: UniversalTicker, contract_size: Decimal
    ) -> OrderBook:
        return OrderBook(
            universal_ticker=str(ticker),
            bids=_levels(self.bids, contract_size),
            asks=_levels(self.asks, contract_size),
            ts=_ts(self.t),
        )


class GateFuturesLiquidation(GateMessage):
    """``futures.public_liquidates`` — other accounts being closed out."""

    contract: str
    price: Decimal
    size: Decimal
    time: int | float | None = None

    def to_liquidation(
        self, ticker: UniversalTicker, contract_size: Decimal
    ) -> Liquidation:
        # Size sign is the liquidated position: negative = long closed out
        # (they were buying to close? Gate: negative size is sell). The shared
        # model says side is the liquidated position's side — long closed is
        # buy. Gate's signed size is the *order* that liquidated: negative is
        # a sell, which closes a long.
        return Liquidation(
            universal_ticker=str(ticker),
            price=self.price,
            qty=contracts_to_base(abs(self.size), contract_size),
            side=Side.BUY if self.size < 0 else Side.SELL,
            ts=_ts(self.time),
        )


def _order_type(price: Decimal | None) -> OrderType:
    if price is None or price == 0:
        return OrderType.MARKET
    return OrderType.LIMIT


def _order_status(
    *,
    status: str,
    finish_as: str | None,
    size: Decimal,
    left: Decimal,
) -> OrderStatus:
    finished = finish_as or ""
    if status == "finished" or finished:
        if finished == "filled" or (not finished and left == 0):
            return OrderStatus.FILLED
        return OrderStatus.CANCELED
    consumed = abs(size) - abs(left)
    if consumed > 0:
        return OrderStatus.PARTIALLY_FILLED
    return OrderStatus.NEW


class GateFuturesOrder(GateMessage):
    """Order row shared by ``futures.orders``, ``order_place`` and REST."""

    id: str | int = ""
    text: str | None = None
    contract: str = ""
    size: Decimal = Decimal("0")
    left: Decimal = Decimal("0")
    price: Decimal | None = None
    fill_price: Decimal | None = None
    status: str = ""
    finish_as: str | None = None
    tif: str | None = None
    is_reduce_only: bool = False
    reduce_only: bool = False
    create_time: int | float | None = None
    update_time: int | float | None = None
    finish_time: int | float | None = None
    mkfr: Decimal | None = None
    tkfr: Decimal | None = None

    @property
    def client_order_id(self) -> str | None:
        return from_text(self.text)

    @property
    def side(self) -> Side:
        return side_of_size(self.size)

    @property
    def order_type(self) -> OrderType:
        return _order_type(self.price)

    def to_order(self, ticker: UniversalTicker, contract_size: Decimal) -> Order:
        qty = contracts_to_base(abs(self.size), contract_size)
        filled = contracts_to_base(abs(self.size) - abs(self.left), contract_size)
        return Order(
            order_id=str(self.id),
            client_order_id=self.client_order_id,
            universal_ticker=str(ticker),
            side=self.side,
            type=self.order_type,
            status=_order_status(
                status=self.status,
                finish_as=self.finish_as,
                size=self.size,
                left=self.left,
            ),
            qty=qty,
            price=None if self.order_type is OrderType.MARKET else self.price,
            filled_qty=filled,
            avg_price=self.fill_price or None,
            ts=_ts(self.update_time or self.finish_time or self.create_time),
        )


class GateFuturesUserTrade(GateMessage):
    """``futures.usertrades`` / REST my_trades."""

    id: str | int = ""
    order_id: str | int = ""
    contract: str = ""
    size: Decimal = Decimal("0")
    price: Decimal = Decimal("0")
    role: str = ""
    text: str | None = None
    fee: Decimal = Decimal("0")
    point_fee: Decimal = Decimal("0")
    create_time: int | float | None = None

    @property
    def client_order_id(self) -> str | None:
        return from_text(self.text)

    def to_fill(self, ticker: UniversalTicker, contract_size: Decimal) -> Fill:
        return Fill(
            fill_id=str(self.id),
            order_id=str(self.order_id),
            client_order_id=self.client_order_id,
            universal_ticker=str(ticker),
            side=side_of_size(self.size),
            price=self.price,
            qty=contracts_to_base(abs(self.size), contract_size),
            fee=abs(self.fee),
            fee_asset="",
            ts=_ts(self.create_time),
        )


class GateFuturesPosition(GateMessage):
    """``futures.positions`` / REST positions. ``size`` is signed."""

    contract: str
    size: Decimal = Decimal("0")
    entry_price: Decimal | None = None
    unrealised_pnl: Decimal | None = None
    leverage: Decimal | None = None
    margin: Decimal | None = None
    liq_price: Decimal | None = None

    def to_position(
        self, ticker: UniversalTicker, contract_size: Decimal
    ) -> Position:
        return Position(
            universal_ticker=str(ticker),
            qty=contracts_to_base(self.size, contract_size),
            entry_price=self.entry_price,
            unrealised_pnl=self.unrealised_pnl,
        )


class GateFuturesBalance(GateMessage):
    """``futures.balances`` / REST accounts."""

    currency: str = "USDT"
    total: Decimal = Decimal("0")
    available: Decimal = Decimal("0")
    unrealised_pnl: Decimal | None = None

    def to_balance(self) -> Balance:
        locked = self.total - self.available
        if locked < 0:
            locked = Decimal("0")
        return Balance(asset=self.currency, free=self.available, locked=locked)


__all__ = [
    "TEXT_PREFIX",
    "GateFuturesBalance",
    "GateFuturesBookTicker",
    "GateFuturesCandlestick",
    "GateFuturesLiquidation",
    "GateFuturesOrder",
    "GateFuturesOrderBook",
    "GateFuturesPosition",
    "GateFuturesTicker",
    "GateFuturesTrade",
    "GateFuturesUserTrade",
    "base_to_contracts",
    "contracts_to_base",
    "format_size",
    "from_text",
    "side_of_size",
    "signed_contracts",
    "to_text",
]
