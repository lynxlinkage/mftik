"""Binance spot wire models — market streams, user data, and call replies.

These mirror Binance's wire format field-for-field, single-letter keys and all,
and keep its ``BTCUSDT`` spelling. Where a shared ``mft.exchange`` model is a
faithful fit there is a ``to_*`` converter; where Binance has no equivalent
(depth diffs, balance deltas) there deliberately is none, because flattening
those loses the sequencing that makes them usable.

Three Binance-specific readings are worth knowing before trusting the
converters:

* **``m`` is the maker flag, not a side.** On the tape ``m: true`` means the
  *buyer* was the resting maker, so the aggressor sold. Reading it as "buy"
  inverts every trade on the feed.
* **``executionReport`` is both the order feed and the fill feed.** Binance has
  no separate user-trade stream; a fill is a report with ``x == "TRADE"``, and
  its ``l``/``L`` are that fill alone while ``z``/``Z`` are the order's running
  totals. Using ``z`` where ``l`` belongs double-counts every partial.
* **Average price is ``Z / z``**, not a field. Binance publishes the quote
  total and the base total and leaves the division to us — and ``z`` is zero
  until something fills, so the division has to be guarded.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field

from mft.exchange.models import (
    AggTrade,
    Balance,
    BookLevel,
    Fill,
    Kline,
    Order,
    OrderBook,
    OrderStatus,
    OrderType,
    Side,
    Ticker,
    Trade,
)
from mft.exchange.tickers import UniversalTicker

#: Binance order status → ours. ``EXPIRED`` covers both a time-in-force that
#: ran out and an order the venue killed, and ``EXPIRED_IN_MATCH`` is a
#: self-trade prevention kill; all three end an order without filling it, which
#: is what CANCELED means here.
_STATUS: dict[str, OrderStatus] = {
    "NEW": OrderStatus.NEW,
    "PENDING_NEW": OrderStatus.PENDING_NEW,
    "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
    "FILLED": OrderStatus.FILLED,
    "CANCELED": OrderStatus.CANCELED,
    "PENDING_CANCEL": OrderStatus.PENDING_CANCEL,
    "REJECTED": OrderStatus.REJECTED,
    "EXPIRED": OrderStatus.CANCELED,
    "EXPIRED_IN_MATCH": OrderStatus.CANCELED,
}

#: Binance order type → ours. ``LIMIT_MAKER`` is a limit order that must rest;
#: the "must rest" part is a time-in-force in our vocabulary
#: (:attr:`~mft.exchange.models.TimeInForce.POST_ONLY`), so the type it maps to
#: here is plain LIMIT and the intent is carried on the request instead. The
#: stop variants have no shared equivalent at all and are read as the leg they
#: turn into once triggered.
_TYPE: dict[str, OrderType] = {
    "MARKET": OrderType.MARKET,
    "LIMIT": OrderType.LIMIT,
    "LIMIT_MAKER": OrderType.LIMIT,
    "STOP_LOSS": OrderType.MARKET,
    "STOP_LOSS_LIMIT": OrderType.LIMIT,
    "TAKE_PROFIT": OrderType.MARKET,
    "TAKE_PROFIT_LIMIT": OrderType.LIMIT,
}


def status_of(value: str | None) -> OrderStatus:
    """Binance's ``status`` / ``X``, or UNKNOWN for one we have no name for.

    UNKNOWN rather than a guess: an unrecognised status means the venue told us
    something this platform cannot interpret, and
    :attr:`~mft.exchange.models.OrderStatus.UNKNOWN` is exactly the state that
    says "ask again" instead of inventing a lifecycle event.
    """
    return _STATUS.get((value or "").upper(), OrderStatus.UNKNOWN)


def type_of(value: str | None) -> OrderType:
    return _TYPE.get((value or "").upper(), OrderType.LIMIT)


def side_of(is_buyer_maker: bool) -> Side:
    """The aggressor's side on a public trade.

    ``m`` says whether the buyer was the maker. If they were, the taker — whose
    side the tape reports — was the seller.
    """
    return Side.SELL if is_buyer_maker else Side.BUY


def _lower(value: Any) -> Any:
    return value.lower() if isinstance(value, str) else value


#: Binance spells sides ``BUY``/``SELL``; :class:`~mft.exchange.models.Side` is
#: lowercase. Folded on the way in rather than at each use, so a model field
#: annotated with this parses the venue's casing and holds ours.
VenueSide = Annotated[Side, BeforeValidator(_lower)]


def _secs(ms: Any) -> float:
    """Binance timestamps are milliseconds, as ints, everywhere."""
    if ms is None or ms == "":
        return 0.0
    return float(ms) / 1000.0


def _levels(rows: list[Any] | None) -> list[BookLevel]:
    out: list[BookLevel] = []
    for row in rows or []:
        if len(row) < 2:
            continue
        out.append(BookLevel(price=Decimal(str(row[0])), qty=Decimal(str(row[1]))))
    return out


def _avg(quote_total: Decimal, base_total: Decimal) -> Decimal | None:
    """``Z / z`` — None while nothing has filled, rather than a zero price."""
    if base_total <= 0:
        return None
    return quote_total / base_total


class BinanceMessage(BaseModel):
    """Base for wire models: tolerant of new fields, immutable once parsed."""

    model_config = ConfigDict(frozen=True, populate_by_name=True, extra="ignore")


# --- market streams --------------------------------------------------------


class BinanceAggTrade(BinanceMessage):
    """``<symbol>@aggTrade`` — the tape, same-price fills coalesced.

    One message can represent many matches (``f``..``l``), which is why the id
    on the resulting :class:`~mft.exchange.models.Trade` is the aggregate id
    and not any single trade's.
    """

    e: str = "aggTrade"
    event_time: int = Field(default=0, alias="E")
    s: str
    a: int
    p: Decimal
    q: Decimal
    first_id: int = Field(default=0, alias="f")
    last_id: int = Field(default=0, alias="l")
    trade_time: int = Field(default=0, alias="T")
    m: bool = False

    @property
    def symbol(self) -> str:
        return self.s

    @property
    def ts(self) -> float:
        return _secs(self.trade_time or self.event_time)

    def to_trade(self, ticker: UniversalTicker) -> Trade:
        """The print as a plain tape entry, aggregation dropped.

        ``trade_id`` is the aggregate's id, which is not a trade id on this
        venue — nothing downstream can look it up as one. Prefer
        :meth:`to_agg_trade`, which says what the id is.
        """
        return Trade(
            universal_ticker=str(ticker),
            trade_id=str(self.a),
            price=self.p,
            qty=self.q,
            side=side_of(self.m),
            ts=self.ts,
        )

    def to_agg_trade(self, ticker: UniversalTicker) -> AggTrade:
        """The print with its match range kept, so the count survives."""
        return AggTrade(
            universal_ticker=str(ticker),
            trade_id=str(self.a),
            price=self.p,
            qty=self.q,
            side=side_of(self.m),
            ts=self.ts,
            first_trade_id=str(self.first_id),
            last_trade_id=str(self.last_id),
        )


class BinanceTrade(BinanceMessage):
    """``<symbol>@trade`` — the raw tape, one message per match."""

    e: str = "trade"
    event_time: int = Field(default=0, alias="E")
    s: str
    t: int
    p: Decimal
    q: Decimal
    trade_time: int = Field(default=0, alias="T")
    m: bool = False

    @property
    def symbol(self) -> str:
        return self.s

    @property
    def ts(self) -> float:
        return _secs(self.trade_time or self.event_time)

    def to_trade(self, ticker: UniversalTicker) -> Trade:
        return Trade(
            universal_ticker=str(ticker),
            trade_id=str(self.t),
            price=self.p,
            qty=self.q,
            side=side_of(self.m),
            ts=self.ts,
        )


class BinanceKlineWindow(BinanceMessage):
    """The ``k`` block of a kline push — the candle itself.

    ``x`` marks the closing tick of the window, which is the only point at
    which a bar is final; everything before it is the same bar re-pushed.
    """

    t: int
    close_time: int = Field(default=0, alias="T")
    s: str
    i: str
    o: Decimal
    c: Decimal
    h: Decimal
    low: Decimal = Field(alias="l")
    v: Decimal = Decimal("0")
    quote_volume: Decimal = Field(default=Decimal("0"), alias="q")
    n: int = 0
    x: bool = False


class BinanceKlineEvent(BinanceMessage):
    """``<symbol>@kline_<interval>`` — the envelope around one candle."""

    e: str = "kline"
    event_time: int = Field(default=0, alias="E")
    s: str
    k: BinanceKlineWindow

    @property
    def symbol(self) -> str:
        return self.s

    @property
    def interval(self) -> str:
        """Binance's spelling of the window, echoed back from the subscribe."""
        return self.k.i

    def to_kline(self, ticker: UniversalTicker) -> Kline:
        """The candle in shared form, still in Binance's interval spelling.

        The interval is translated a layer up, in
        :class:`~mft.exchange.binance.spot.public.BinanceSpotPublicClient`, so
        that this model stays a faithful reading of the wire.
        """
        return Kline(
            universal_ticker=str(ticker),
            interval=self.k.i,
            # ``t`` is the window's open time, in milliseconds.
            open_time=_secs(self.k.t),
            open=self.k.o,
            high=self.k.h,
            low=self.k.low,
            close=self.k.c,
            volume=self.k.v,
            quote_volume=self.k.quote_volume,
            closed=self.k.x,
        )


class BinanceTicker(BinanceMessage):
    """``<symbol>@ticker`` — rolling 24h stats plus top of book."""

    e: str = "24hrTicker"
    event_time: int = Field(default=0, alias="E")
    s: str
    price_change: Decimal | None = Field(default=None, alias="p")
    change_percent: Decimal | None = Field(default=None, alias="P")
    last: Decimal = Field(alias="c")
    bid: Decimal | None = Field(default=None, alias="b")
    bid_qty: Decimal | None = Field(default=None, alias="B")
    ask: Decimal | None = Field(default=None, alias="a")
    ask_qty: Decimal | None = Field(default=None, alias="A")
    o: Decimal | None = None
    h: Decimal | None = None
    low: Decimal | None = Field(default=None, alias="l")
    v: Decimal | None = None
    q: Decimal | None = None

    @property
    def symbol(self) -> str:
        return self.s

    def to_ticker(self, ticker: UniversalTicker) -> Ticker:
        """Falls back to ``last`` on whichever side is unquoted.

        Binance publishes ``0`` rather than omitting a side on an empty book,
        and a zero bid would read as a real price a caller could cross.
        """
        bid = self.bid if self.bid else self.last
        ask = self.ask if self.ask else self.last
        return Ticker(
            universal_ticker=str(ticker),
            bid=bid,
            ask=ask,
            last=self.last,
            ts=_secs(self.event_time),
        )


class BinanceBookTicker(BinanceMessage):
    """``<symbol>@bookTicker`` — best bid/ask on every change.

    Carries no timestamp of its own. ``u`` is the book update id, which orders
    the messages but is not a time, so anything needing one has to stamp
    arrival — see
    :meth:`~mft.exchange.binance.spot.public.BinanceSpotPublicClient.stream_best_quote`.
    """

    u: int
    s: str
    bid: Decimal = Field(alias="b")
    bid_size: Decimal = Field(alias="B")
    ask: Decimal = Field(alias="a")
    ask_size: Decimal = Field(alias="A")

    @property
    def symbol(self) -> str:
        return self.s


class BinanceDepth(BinanceMessage):
    """``<symbol>@depth<levels>`` and the ``depth`` call reply — a whole book.

    No symbol in the payload, on the stream or off it: the partial-depth push
    identifies its instrument only through the combined endpoint's ``stream``
    wrapper, and the WebSocket API reply only through the request. So
    :meth:`to_order_book` is told which symbol it is rather than reading one.
    """

    last_update_id: int = Field(default=0, alias="lastUpdateId")
    bids: list[Any] = Field(default_factory=list)
    asks: list[Any] = Field(default_factory=list)

    def to_order_book(
        self, ticker: UniversalTicker, *, ts: float | None = None
    ) -> OrderBook:
        """The book under ``ticker``, stamped ``ts`` or with arrival time.

        Binance dates neither form of this payload — a book carries only
        ``lastUpdateId`` — so ``ts`` is whatever the caller knows, and arrival
        time is the honest default when it knows nothing better.
        """
        fields: dict[str, Any] = {} if ts is None else {"ts": ts}
        return OrderBook(
            universal_ticker=str(ticker),
            bids=_levels(self.bids),
            asks=_levels(self.asks),
            **fields,
        )


class BinanceDepthUpdate(BinanceMessage):
    """``<symbol>@depth`` — a depth diff, not a book.

    Deliberately has no ``to_order_book()``: a diff is only meaningful applied
    to a snapshot in ``U``/``u`` sequence order, and a zero ``qty`` deletes a
    level rather than setting it to nothing.
    """

    e: str = "depthUpdate"
    event_time: int = Field(default=0, alias="E")
    s: str
    first_id: int = Field(alias="U")
    last_id: int = Field(alias="u")
    bids: list[Any] = Field(default_factory=list, alias="b")
    asks: list[Any] = Field(default_factory=list, alias="a")

    @property
    def symbol(self) -> str:
        return self.s

    @property
    def ts(self) -> float:
        return _secs(self.event_time)

    def bid_levels(self) -> list[BookLevel]:
        return _levels(self.bids)

    def ask_levels(self) -> list[BookLevel]:
        return _levels(self.asks)

    def follows(self, last_applied_id: int) -> bool:
        """Whether this diff slots onto a book already at ``last_applied_id``."""
        return self.first_id <= last_applied_id + 1 <= self.last_id


# --- user data stream ------------------------------------------------------


class BinanceExecutionReport(BinanceMessage):
    """``executionReport`` — Binance's only order event, and its only fill one.

    ``x`` (execution type) says what happened, ``X`` (order status) says where
    the order now stands, and only the pair is enough: a partial fill is
    ``x=TRADE`` with ``X=PARTIALLY_FILLED``, while a cancel that raced a fill is
    ``x=CANCELED`` with ``X=CANCELED`` and a non-zero ``z``.
    """

    e: str = "executionReport"
    event_time: int = Field(default=0, alias="E")
    s: str
    client_order_id_raw: str = Field(default="", alias="c")
    side: VenueSide = Field(alias="S")
    order_type: str = Field(default="LIMIT", alias="o")
    time_in_force: str = Field(default="", alias="f")
    q: Decimal = Decimal("0")
    p: Decimal = Decimal("0")
    orig_client_order_id: str = Field(default="", alias="C")
    exec_type: str = Field(default="", alias="x")
    order_status: str = Field(default="", alias="X")
    reject_reason: str = Field(default="NONE", alias="r")
    order_id: int = Field(default=0, alias="i")
    last_qty: Decimal = Field(default=Decimal("0"), alias="l")
    filled_qty_total: Decimal = Field(default=Decimal("0"), alias="z")
    last_price: Decimal = Field(default=Decimal("0"), alias="L")
    commission: Decimal = Field(default=Decimal("0"), alias="n")
    commission_asset: str | None = Field(default=None, alias="N")
    transact_time: int = Field(default=0, alias="T")
    trade_id: int = Field(default=-1, alias="t")
    order_time: int = Field(default=0, alias="O")
    quote_total: Decimal = Field(default=Decimal("0"), alias="Z")
    last_quote: Decimal = Field(default=Decimal("0"), alias="Y")
    quote_order_qty: Decimal = Field(default=Decimal("0"), alias="Q")

    @property
    def symbol(self) -> str:
        return self.s

    @property
    def client_order_id(self) -> str | None:
        """The id we gave the order, on any report about it.

        On a cancel report ``c`` is the id of the *cancel request* and ``C``
        holds the original order's — so reading ``c`` alone would lose track of
        the order at the one moment it matters.
        """
        return self.orig_client_order_id or self.client_order_id_raw or None

    @property
    def status(self) -> OrderStatus:
        return status_of(self.order_status)

    @property
    def type(self) -> OrderType:
        return type_of(self.order_type)

    @property
    def avg_price(self) -> Decimal | None:
        return _avg(self.quote_total, self.filled_qty_total)

    @property
    def is_fill(self) -> bool:
        """Whether this report carries an execution, not just a state change."""
        return self.exec_type.upper() == "TRADE" and self.last_qty > 0

    def to_order(self, ticker: UniversalTicker) -> Order:
        return Order(
            universal_ticker=str(ticker),
            order_id=str(self.order_id),
            client_order_id=self.client_order_id,
            side=self.side,
            type=self.type,
            status=self.status,
            qty=self.q,
            # A market order reports price 0; None reads as "no limit price",
            # which is what it is.
            price=self.p or None,
            filled_qty=self.filled_qty_total,
            avg_price=self.avg_price,
            ts=_secs(self.transact_time or self.event_time),
        )

    def to_fill(self, ticker: UniversalTicker) -> Fill:
        """This report's own execution — ``l``/``L``, never the running totals.

        Only meaningful when :attr:`is_fill`; callers filter first.
        """
        return Fill(
            universal_ticker=str(ticker),
            fill_id=str(self.trade_id),
            order_id=str(self.order_id),
            client_order_id=self.client_order_id,
            side=self.side,
            price=self.last_price,
            qty=self.last_qty,
            fee=self.commission,
            fee_asset=self.commission_asset or "",
            ts=_secs(self.transact_time or self.event_time),
        )


class BinanceAccountBalance(BinanceMessage):
    """One asset inside an ``outboundAccountPosition``."""

    a: str
    f: Decimal = Decimal("0")
    locked: Decimal = Field(default=Decimal("0"), alias="l")

    def to_balance(self) -> Balance:
        return Balance(asset=self.a, free=self.f, locked=self.locked)


class BinanceAccountPosition(BinanceMessage):
    """``outboundAccountPosition`` — every balance that just moved.

    A snapshot of the affected assets, not a delta, so each entry can be taken
    as the asset's new truth.
    """

    e: str = "outboundAccountPosition"
    event_time: int = Field(default=0, alias="E")
    last_update: int = Field(default=0, alias="u")
    balances: list[BinanceAccountBalance] = Field(default_factory=list, alias="B")

    @property
    def ts(self) -> float:
        return _secs(self.event_time)

    def to_balances(self) -> list[Balance]:
        return [row.to_balance() for row in self.balances]


class BinanceBalanceUpdate(BinanceMessage):
    """``balanceUpdate`` — a deposit, withdrawal or transfer.

    A *delta* (``d``), which is why there is no ``to_balance()``: the shared
    :class:`~mft.exchange.models.Balance` states a position, and turning a
    delta into one needs the balance it applies to. The
    ``outboundAccountPosition`` that accompanies the same movement carries
    that, so this event is information about *why*, not *what*.
    """

    e: str = "balanceUpdate"
    event_time: int = Field(default=0, alias="E")
    asset: str = Field(alias="a")
    delta: Decimal = Field(alias="d")
    clear_time: int = Field(default=0, alias="T")

    @property
    def ts(self) -> float:
        return _secs(self.event_time)


# --- call replies ----------------------------------------------------------


class BinanceOrderFill(BinanceMessage):
    """One leg inside ``order.place``'s ``fills`` array.

    Only present when the order traded on arrival, and only on the reply — the
    same executions arrive again as ``executionReport`` pushes, so a consumer
    reading both must dedupe on ``tradeId``.
    """

    price: Decimal = Decimal("0")
    qty: Decimal = Decimal("0")
    commission: Decimal = Decimal("0")
    commission_asset: str = Field(default="", alias="commissionAsset")
    trade_id: int = Field(default=-1, alias="tradeId")


class BinanceOrderAck(BinanceMessage):
    """The reply to ``order.place`` / ``order.cancel`` / ``order.status``.

    A different shape from :class:`BinanceExecutionReport` — a reply states the
    order's totals (``origQty`` / ``executedQty`` / ``cummulativeQuoteQty``)
    while a push describes an event that happened to it. The misspelling of
    ``cummulativeQuoteQty`` is Binance's and is load-bearing on the wire.
    """

    symbol: str = ""
    order_id: int = Field(default=0, alias="orderId")
    client_order_id: str = Field(default="", alias="clientOrderId")
    orig_client_order_id: str = Field(default="", alias="origClientOrderId")
    transact_time: int = Field(default=0, alias="transactTime")
    price: Decimal = Decimal("0")
    orig_qty: Decimal = Field(default=Decimal("0"), alias="origQty")
    executed_qty: Decimal = Field(default=Decimal("0"), alias="executedQty")
    quote_qty: Decimal = Field(default=Decimal("0"), alias="cummulativeQuoteQty")
    status: str = ""
    time_in_force: str = Field(default="", alias="timeInForce")
    type: str = "LIMIT"
    side: VenueSide = Side.BUY
    time: int = 0
    update_time: int = Field(default=0, alias="updateTime")
    working_time: int = Field(default=0, alias="workingTime")
    fills: list[BinanceOrderFill] = Field(default_factory=list)

    @property
    def order_client_id(self) -> str | None:
        """The id the *order* was placed with.

        ``order.cancel`` answers with the cancel request's id in
        ``clientOrderId`` and the order's in ``origClientOrderId``; every other
        reply leaves the latter empty. Preferring it therefore reads the order's
        own id in both cases.
        """
        return self.orig_client_order_id or self.client_order_id or None

    @property
    def order_status(self) -> OrderStatus:
        return status_of(self.status)

    @property
    def avg_price(self) -> Decimal | None:
        return _avg(self.quote_qty, self.executed_qty)

    @property
    def ts(self) -> float:
        return _secs(
            self.transact_time or self.update_time or self.working_time or self.time
        )

    def to_order(self, ticker: UniversalTicker) -> Order:
        return Order(
            universal_ticker=str(ticker),
            order_id=str(self.order_id),
            client_order_id=self.order_client_id,
            side=self.side,
            type=type_of(self.type),
            status=self.order_status,
            qty=self.orig_qty,
            price=self.price or None,
            filled_qty=self.executed_qty,
            avg_price=self.avg_price,
            ts=self.ts,
        )

    def to_fills(self, ticker: UniversalTicker) -> list[Fill]:
        """The executions this reply reported, if the order traded on arrival."""
        return [
            Fill(
                universal_ticker=str(ticker),
                fill_id=str(leg.trade_id),
                order_id=str(self.order_id),
                client_order_id=self.order_client_id,
                side=self.side,
                price=leg.price,
                qty=leg.qty,
                fee=leg.commission,
                fee_asset=leg.commission_asset,
                ts=self.ts,
            )
            for leg in self.fills
        ]


class BinanceAccountInfo(BinanceMessage):
    """``account.status`` — permissions plus the whole balance sheet."""

    account_type: str = Field(default="", alias="accountType")
    can_trade: bool = Field(default=True, alias="canTrade")
    update_time: int = Field(default=0, alias="updateTime")
    balances: list[dict[str, Any]] = Field(default_factory=list)

    def to_balances(self) -> list[Balance]:
        return [
            Balance(
                asset=str(row.get("asset", "")),
                free=Decimal(str(row.get("free", "0") or "0")),
                locked=Decimal(str(row.get("locked", "0") or "0")),
            )
            for row in self.balances
        ]


def kline_from_row(
    row: list[Any], ticker: UniversalTicker, interval: str
) -> Kline:
    """One row of the ``klines`` reply — a positional array, not an object.

    Binance's column order *is* OHLC, unlike some venues, but the two volumes
    sit either side of a second timestamp, so they cannot be read positionally
    from the OHLC block::

        [0] open time, ms      [5] volume, base
        [1] open               [6] close time, ms
        [2] high               [7] quote volume
        [3] low                [8] trade count
        [4] close              ...

    A row from this endpoint is always a closed window except the last one,
    which is the bar in progress — and the reply says nothing about which is
    which. It is reported as closed here and the caller drops or keeps the tail
    knowing the interval, because guessing from a timestamp would be wrong
    exactly at the boundary that matters.
    """
    if len(row) < 8:
        raise ValueError(
            f"kline row for {ticker} {interval} has {len(row)} columns, "
            f"expected at least 8: {row!r}"
        )
    return Kline(
        universal_ticker=str(ticker),
        interval=interval,
        open_time=_secs(row[0]),
        open=Decimal(str(row[1])),
        high=Decimal(str(row[2])),
        low=Decimal(str(row[3])),
        close=Decimal(str(row[4])),
        volume=Decimal(str(row[5])),
        quote_volume=Decimal(str(row[7])),
        closed=True,
    )


__all__ = [
    "BinanceAccountBalance",
    "BinanceAccountInfo",
    "BinanceAccountPosition",
    "BinanceAggTrade",
    "BinanceBalanceUpdate",
    "BinanceBookTicker",
    "BinanceDepth",
    "BinanceDepthUpdate",
    "BinanceExecutionReport",
    "BinanceKlineEvent",
    "BinanceKlineWindow",
    "BinanceMessage",
    "BinanceOrderAck",
    "BinanceOrderFill",
    "BinanceTicker",
    "BinanceTrade",
    "VenueSide",
    "kline_from_row",
    "side_of",
    "status_of",
    "type_of",
]
