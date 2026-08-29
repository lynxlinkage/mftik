"""Bybit v5 wire models — account pushes, market pushes, and call replies.

These mirror Bybit's wire format field-for-field and keep its ``BTCUSDT``
spelling. Where a shared ``mftik.exchange`` model is a faithful fit there is a
``to_*`` converter; where Bybit has no equivalent (book deltas, the ticker
delta push) there deliberately is none, because flattening those loses the
sequencing that makes them usable.

Four Bybit-specific readings are worth knowing before trusting the converters:

* **Everything numeric is a string, and "not applicable" is the empty
  string.** ``avgPrice`` is ``""`` on an order that has not traded, not ``0``
  and not absent, so every number here goes through a validator that reads
  ``""`` as unset rather than raising.
* **Fills are their own topic.** Unlike Binance, where a fill is an order
  event, Bybit pushes ``order`` and ``execution`` separately — and only the
  execution carries the fee. An ``execution`` with ``execType != "Trade"`` is
  not a fill at all: funding payments and ADL settlements arrive on the same
  topic.
* **Sides are ``Buy``/``Sell``, and on the tape ``S`` is the aggressor.** No
  maker flag to invert, unlike Binance's ``m``.
* **The ticker topic is two different messages.** Spot pushes snapshots with
  no bid or ask in them; the derivative books push deltas carrying only the
  fields that changed. :class:`BybitTicker` therefore has almost no required
  field, and :meth:`BybitTicker.to_ticker` falls back to the last price on a
  side the venue did not quote.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field

from mftik.exchange.models import (
    Balance,
    BestQuote,
    BookLevel,
    Fill,
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
from mftik.exchange.tickers import Category, UniversalTicker

#: Bybit order status → ours.
#:
#: ``Untriggered`` and ``Triggered`` belong to conditional orders: the first is
#: parked at the venue waiting for its trigger and the second has just entered
#: the book. Both are live orders that can still be cancelled, which is what
#: NEW means here. ``Deactivated`` is a conditional order that will never
#: trigger, and ``PartiallyFilledCanceled`` is a partially filled order whose
#: remainder was cancelled — both end the order, which is CANCELED.
_STATUS: dict[str, OrderStatus] = {
    "CREATED": OrderStatus.NEW,
    "NEW": OrderStatus.NEW,
    "UNTRIGGERED": OrderStatus.NEW,
    "TRIGGERED": OrderStatus.NEW,
    "PARTIALLYFILLED": OrderStatus.PARTIALLY_FILLED,
    "FILLED": OrderStatus.FILLED,
    "CANCELLED": OrderStatus.CANCELED,
    "PARTIALLYFILLEDCANCELED": OrderStatus.CANCELED,
    "DEACTIVATED": OrderStatus.CANCELED,
    "REJECTED": OrderStatus.REJECTED,
}

#: Bybit order type → ours. Bybit says ``UNKNOWN`` for an order placed through
#: a channel it does not classify, and that is a real value on the wire.
_TYPE: dict[str, OrderType] = {
    "MARKET": OrderType.MARKET,
    "LIMIT": OrderType.LIMIT,
}

#: The execution type that means a real fill. The others on this topic are
#: account movements against a position — funding, ADL, liquidation, delivery —
#: which are not something an order did.
EXEC_TYPE_TRADE = "Trade"


#: Bybit's ``category`` → ours. The inverse of
#: :data:`~mftik.exchange.bybit.protocol._PRODUCT_BY_CATEGORY`, and the reason a
#: unified session can resolve a row it did not subscribe by book: every
#: account payload says which book it came from.
_CATEGORY_OF: dict[str, Category] = {
    "spot": Category.SPOT,
    "linear": Category.PERP,
    "inverse": Category.PERP,
    "option": Category.OPTION,
}


def category_of(value: str | None, default: Category) -> Category:
    """The market a row came from, or ``default`` where it names none.

    Bybit stamps ``category`` on every order, execution and position, which is
    what lets one connection report the whole account without guessing. A row
    that omits it — some older payload shapes do — falls back to the book the
    connector was built for.
    """
    return _CATEGORY_OF.get((value or "").lower(), default)


def status_of(value: str | None) -> OrderStatus:
    """Bybit's ``orderStatus``, or UNKNOWN for one we have no name for.

    UNKNOWN rather than a guess: an unrecognised status means the venue told us
    something this platform cannot interpret, and
    :attr:`~mftik.exchange.models.OrderStatus.UNKNOWN` is exactly the state that
    says "ask again" instead of inventing a lifecycle event.
    """
    return _STATUS.get((value or "").upper(), OrderStatus.UNKNOWN)


def type_of(value: str | None) -> OrderType:
    return _TYPE.get((value or "").upper(), OrderType.LIMIT)


def _dec(value: Any) -> Any:
    """Read Bybit's numbers, treating the empty string as zero."""
    if value is None or value == "":
        return Decimal("0")
    return value


def _opt_dec(value: Any) -> Any:
    """Read a number Bybit may leave unset, keeping "unset" as ``None``."""
    if value is None or value == "":
        return None
    return value


def _lower(value: Any) -> Any:
    return value.lower() if isinstance(value, str) else value


def _secs(value: Any) -> Any:
    """Bybit timestamps are milliseconds, as strings on most payloads."""
    if value is None or value == "":
        return 0.0
    try:
        return float(value) / 1000.0
    except (TypeError, ValueError):
        return 0.0


#: A number that is always present, with ``""`` read as zero.
Dec = Annotated[Decimal, BeforeValidator(_dec)]
#: A number the venue may leave unset; ``""`` becomes ``None``.
OptDec = Annotated[Decimal | None, BeforeValidator(_opt_dec)]
#: Bybit spells sides ``Buy``/``Sell``; :class:`~mftik.exchange.models.Side` is
#: lowercase. Folded on the way in, so a model field annotated with this parses
#: the venue's casing and holds ours.
VenueSide = Annotated[Side, BeforeValidator(_lower)]
#: A millisecond timestamp, held as float seconds like every other ``ts`` here.
Ms = Annotated[float, BeforeValidator(_secs)]


def _levels(rows: Any) -> list[BookLevel]:
    out: list[BookLevel] = []
    for row in rows or []:
        if len(row) < 2:
            continue
        try:
            out.append(BookLevel(price=Decimal(str(row[0])), qty=Decimal(str(row[1]))))
        except (InvalidOperation, ValueError):
            continue
    return out


def _avg(
    value: Decimal | None, quote_total: Decimal, base_total: Decimal
) -> Decimal | None:
    """``avgPrice`` if Bybit filled it in, else the division it would have done.

    Bybit publishes ``avgPrice`` on most order payloads but leaves it empty on
    some spot ones, where ``cumExecValue / cumExecQty`` is the same number.
    ``None`` while nothing has filled, rather than a zero price.
    """
    if value is not None and value > 0:
        return value
    if base_total <= 0:
        return None
    return quote_total / base_total


class BybitMessage(BaseModel):
    """Base for wire models: tolerant of new fields, immutable once parsed."""

    model_config = ConfigDict(frozen=True, populate_by_name=True, extra="ignore")


# --- private topics --------------------------------------------------------


class BybitOrderUpdate(BybitMessage):
    """One row of the ``order`` topic — and of ``GET /v5/order/realtime``.

    The push and the REST row are the same shape on this venue, which is why
    one model serves both and the recon path needs no second converter.

    ``orderLinkId`` is Bybit's name for the id *we* gave the order. It is the
    only id that exists before the venue answers, so it is what the order path
    addresses an order by until an ``orderId`` comes back.
    """

    category: str = ""
    symbol: str = ""
    order_id: str = Field(default="", alias="orderId")
    order_link_id: str = Field(default="", alias="orderLinkId")
    side: VenueSide = Side.BUY
    order_type: str = Field(default="Limit", alias="orderType")
    order_status: str = Field(default="", alias="orderStatus")
    time_in_force: str = Field(default="", alias="timeInForce")
    price: Dec = Decimal("0")
    qty: Dec = Decimal("0")
    market_unit: str = Field(default="", alias="marketUnit")
    cum_exec_qty: Dec = Field(default=Decimal("0"), alias="cumExecQty")
    cum_exec_value: Dec = Field(default=Decimal("0"), alias="cumExecValue")
    cum_exec_fee: Dec = Field(default=Decimal("0"), alias="cumExecFee")
    avg_price_raw: OptDec = Field(default=None, alias="avgPrice")
    leaves_qty: Dec = Field(default=Decimal("0"), alias="leavesQty")
    reject_reason: str = Field(default="", alias="rejectReason")
    reduce_only: bool = Field(default=False, alias="reduceOnly")
    stop_order_type: str = Field(default="", alias="stopOrderType")
    created_time: Ms = Field(default=0.0, alias="createdTime")
    updated_time: Ms = Field(default=0.0, alias="updatedTime")

    @property
    def client_order_id(self) -> str | None:
        return self.order_link_id or None

    @property
    def status(self) -> OrderStatus:
        return status_of(self.order_status)

    @property
    def type(self) -> OrderType:
        return type_of(self.order_type)

    @property
    def avg_price(self) -> Decimal | None:
        return _avg(self.avg_price_raw, self.cum_exec_value, self.cum_exec_qty)

    @property
    def ts(self) -> float:
        return self.updated_time or self.created_time

    def to_order(self, ticker: UniversalTicker) -> Order:
        """The order under ``ticker`` — the identity the caller resolved.

        Passed in rather than read off ``symbol``: this payload carries Bybit's
        spelling and nothing that says which book it is on, and an :class:`
        ~mftik.exchange.models.Order` states the instrument. The connector has
        both by the time it converts.
        """
        quote_sized = self.market_unit == "quoteCoin"
        filled = self.cum_exec_qty
        return Order(
            universal_ticker=str(ticker),
            order_id=self.order_id,
            client_order_id=self.client_order_id,
            side=self.side,
            type=self.type,
            status=self.status,
            # ``qty`` on a quote-sized market order is quote; do not copy it
            # into the shared model's base quantity.
            qty=filled if quote_sized else self.qty,
            quote_qty=self.qty if quote_sized else None,
            # A market order reports price 0; None reads as "no limit price",
            # which is what it is.
            price=self.price or None,
            filled_qty=filled,
            avg_price=self.avg_price,
            ts=self.ts,
        )


class BybitExecution(BybitMessage):
    """One row of the ``execution`` topic — and of ``GET /v5/execution/list``.

    Only a row with ``execType == "Trade"`` is a fill; see :attr:`is_fill`.
    ``feeCurrency`` is the asset the fee was taken in, which on spot is the one
    received (base on a buy, quote on a sell) and on the derivative books is
    the settle coin.
    """

    category: str = ""
    symbol: str = ""
    order_id: str = Field(default="", alias="orderId")
    order_link_id: str = Field(default="", alias="orderLinkId")
    exec_id: str = Field(default="", alias="execId")
    side: VenueSide = Side.BUY
    exec_price: Dec = Field(default=Decimal("0"), alias="execPrice")
    exec_qty: Dec = Field(default=Decimal("0"), alias="execQty")
    exec_value: Dec = Field(default=Decimal("0"), alias="execValue")
    exec_fee: Dec = Field(default=Decimal("0"), alias="execFee")
    exec_type: str = Field(default="", alias="execType")
    exec_time: Ms = Field(default=0.0, alias="execTime")
    fee_currency: str = Field(default="", alias="feeCurrency")
    fee_rate: OptDec = Field(default=None, alias="feeRate")
    is_maker: bool = Field(default=False, alias="isMaker")
    closed_size: Dec = Field(default=Decimal("0"), alias="closedSize")
    leaves_qty: Dec = Field(default=Decimal("0"), alias="leavesQty")
    order_qty: Dec = Field(default=Decimal("0"), alias="orderQty")
    order_price: Dec = Field(default=Decimal("0"), alias="orderPrice")
    order_type: str = Field(default="", alias="orderType")
    seq: int = 0

    @property
    def client_order_id(self) -> str | None:
        return self.order_link_id or None

    @property
    def is_fill(self) -> bool:
        """Whether this row is an execution against an order of ours."""
        return self.exec_type == EXEC_TYPE_TRADE and self.exec_qty > 0

    def to_fill(self, ticker: UniversalTicker) -> Fill:
        """This execution alone — never a running total.

        Only meaningful when :attr:`is_fill`; callers filter first.
        """
        return Fill(
            universal_ticker=str(ticker),
            fill_id=self.exec_id,
            order_id=self.order_id,
            client_order_id=self.client_order_id,
            side=self.side,
            price=self.exec_price,
            qty=self.exec_qty,
            fee=self.exec_fee,
            fee_asset=self.fee_currency,
            ts=self.exec_time,
        )


class BybitWalletCoin(BybitMessage):
    """One asset inside a ``wallet`` push or a wallet-balance reply."""

    coin: str = ""
    equity: Dec = Decimal("0")
    wallet_balance: Dec = Field(default=Decimal("0"), alias="walletBalance")
    free: OptDec = None
    locked: Dec = Decimal("0")
    available_to_withdraw: OptDec = Field(default=None, alias="availableToWithdraw")
    total_order_im: Dec = Field(default=Decimal("0"), alias="totalOrderIM")
    total_position_im: Dec = Field(default=Decimal("0"), alias="totalPositionIM")

    def to_balance(self) -> Balance:
        """The asset as a shared :class:`~mftik.exchange.models.Balance`.

        Bybit names the spendable part differently per account type: a classic
        spot account has ``free``, a unified account has
        ``availableToWithdraw`` — and leaves it empty for a coin that is
        collateralising something, where the honest reading is the wallet
        balance less what is held. That subtraction is the fallback rather than
        the rule, because the venue's own number accounts for borrowing and
        margin in ways this arithmetic cannot.
        """
        spendable = self.free
        if spendable is None:
            spendable = self.available_to_withdraw
        if spendable is None:
            held = self.locked + self.total_order_im
            spendable = self.wallet_balance - held
        if spendable < 0:
            spendable = Decimal("0")
        return Balance(
            asset=self.coin,
            free=spendable,
            locked=self.locked or self.total_order_im,
        )


class BybitWallet(BybitMessage):
    """One row of the ``wallet`` topic — a whole account, not a delta."""

    account_type: str = Field(default="", alias="accountType")
    total_equity: OptDec = Field(default=None, alias="totalEquity")
    coin: list[BybitWalletCoin] = Field(default_factory=list)

    def to_balances(self) -> list[Balance]:
        return [row.to_balance() for row in self.coin if row.coin]


class BybitPosition(BybitMessage):
    """One row of the ``position`` topic — and of ``GET /v5/position/list``.

    Nothing on the spot book has one of these: Bybit reports spot holdings as
    wallet balances, and a position only exists on the contract books.
    """

    category: str = ""
    symbol: str = ""
    #: ``Buy`` long, ``Sell`` short, and empty once the position is closed —
    #: which is why this is a plain string rather than a :class:`Side`.
    side: str = ""
    size: Dec = Decimal("0")
    position_value: Dec = Field(default=Decimal("0"), alias="positionValue")
    entry_price: OptDec = Field(default=None, alias="entryPrice")
    avg_price: OptDec = Field(default=None, alias="avgPrice")
    leverage: OptDec = None
    unrealised_pnl: Dec = Field(default=Decimal("0"), alias="unrealisedPnl")
    position_idx: int = Field(default=0, alias="positionIdx")
    updated_time: Ms = Field(default=0.0, alias="updatedTime")

    @property
    def signed_size(self) -> Decimal:
        """Size with direction: negative when short, zero when flat.

        Bybit reports ``size`` unsigned and puts the direction in ``side``,
        while :class:`~mftik.exchange.oms.Position` states one signed quantity.
        """
        return -self.size if self.side.lower() == "sell" else self.size

    def to_position(self, ticker: UniversalTicker) -> Position:
        return Position(
            universal_ticker=str(ticker),
            qty=self.signed_size,
            entry_price=self.entry_price or self.avg_price,
            unrealised_pnl=self.unrealised_pnl,
        )


# --- public topics ---------------------------------------------------------


class BybitPublicTrade(BybitMessage):
    """One row of ``publicTrade.<symbol>`` — the tape.

    ``S`` is the **aggressor's** side, so there is nothing to invert: a row
    marked ``Buy`` is a buyer lifting the offer.
    """

    trade_time: Ms = Field(default=0.0, alias="T")
    s: str = ""
    side: VenueSide = Field(default=Side.BUY, alias="S")
    v: Dec = Decimal("0")
    p: Dec = Decimal("0")
    trade_id: str = Field(default="", alias="i")
    #: ``PlusTick`` / ``MinusTick`` — the direction against the previous price.
    tick_direction: str = Field(default="", alias="L")
    block_trade: bool = Field(default=False, alias="BT")

    @property
    def symbol(self) -> str:
        return self.s

    def to_trade(self, ticker: UniversalTicker) -> Trade:
        return Trade(
            universal_ticker=str(ticker),
            trade_id=self.trade_id,
            price=self.p,
            qty=self.v,
            side=self.side,
            ts=self.trade_time,
        )


class BybitLiquidation(BybitMessage):
    """One row of ``allLiquidation.<symbol>`` — a forced close on the contracts.

    ``S`` is the **liquidated position's** side, not the aggressor: ``Buy``
    means a long was closed out. ``p`` is the bankruptcy price Bybit reported
    for the event.
    """

    updated_time: Ms = Field(default=0.0, alias="T")
    s: str = ""
    side: VenueSide = Field(default=Side.BUY, alias="S")
    v: Dec = Decimal("0")
    p: Dec = Decimal("0")

    @property
    def symbol(self) -> str:
        return self.s

    def to_liquidation(self, ticker: UniversalTicker) -> Liquidation:
        return Liquidation(
            universal_ticker=str(ticker),
            price=self.p,
            qty=self.v,
            side=self.side,
            ts=self.updated_time,
        )


class BybitTicker(BybitMessage):
    """``tickers.<symbol>``, and one row of ``GET /v5/market/tickers``.

    Almost every field is optional, and that is the payload's doing rather than
    caution: the derivative books push this topic as a **delta** carrying only
    what changed, so a message with nothing but ``symbol`` and ``fundingRate``
    is normal. Spot pushes whole snapshots — but without any bid or ask, which
    the REST form of the same row does have.
    """

    symbol: str = ""
    last_price: OptDec = Field(default=None, alias="lastPrice")
    bid: OptDec = Field(default=None, alias="bid1Price")
    bid_qty: OptDec = Field(default=None, alias="bid1Size")
    ask: OptDec = Field(default=None, alias="ask1Price")
    ask_qty: OptDec = Field(default=None, alias="ask1Size")
    high_24h: OptDec = Field(default=None, alias="highPrice24h")
    low_24h: OptDec = Field(default=None, alias="lowPrice24h")
    prev_price_24h: OptDec = Field(default=None, alias="prevPrice24h")
    volume_24h: OptDec = Field(default=None, alias="volume24h")
    turnover_24h: OptDec = Field(default=None, alias="turnover24h")
    price_24h_pcnt: OptDec = Field(default=None, alias="price24hPcnt")
    mark_price: OptDec = Field(default=None, alias="markPrice")
    index_price: OptDec = Field(default=None, alias="indexPrice")
    funding_rate: OptDec = Field(default=None, alias="fundingRate")

    @property
    def quoted(self) -> bool:
        """Whether this payload carries a price at all.

        A ticker delta that changed only the funding rate has no price in it,
        and turning that into a :class:`~mftik.exchange.models.Ticker` would mean
        inventing one. Callers skip an unquoted row rather than publish it.
        """
        return self.last_price is not None

    def to_ticker(self, ticker: UniversalTicker, *, ts: float = 0.0) -> Ticker:
        """The row as a shared ticker, falling back to last on an unquoted side.

        Spot's push carries no bid or ask at all, and a zero on either side
        would read as a real price a caller could cross.
        """
        last = self.last_price or Decimal("0")
        bid = self.bid if self.bid else last
        ask = self.ask if self.ask else last
        fields: dict[str, Any] = {} if ts <= 0 else {"ts": ts}
        return Ticker(
            universal_ticker=str(ticker), bid=bid, ask=ask, last=last, **fields
        )


class BybitOrderBook(BybitMessage):
    """One ``orderbook.<depth>.<symbol>`` payload — a snapshot **or** a delta.

    Bybit sends one ``snapshot`` and then ``delta`` pushes against it, and both
    arrive in this shape; only the frame's ``type`` says which. There is no
    ``to_order_book()`` on a delta for the same reason Binance's depth diff has
    none: a delta is meaningful only applied to a snapshot in ``u`` order, and
    a zero quantity deletes a level rather than setting it to nothing. Folding
    the two into whole books is :class:`~mftik.exchange.bybit.feed.BybitBook`'s
    job.

    The exception is ``orderbook.1``, which Bybit pushes as a snapshot every
    time — so the top of book needs no folding at all.
    """

    s: str = ""
    bids: list[Any] = Field(default_factory=list, alias="b")
    asks: list[Any] = Field(default_factory=list, alias="a")
    #: Update id. Increments by one per message on a stream that has not
    #: restarted; a jump means a push was missed and the book must be rebuilt.
    u: int = 0
    #: Cross-sequence number, ordering this book against other topics.
    seq: int = 0

    @property
    def symbol(self) -> str:
        return self.s

    def bid_levels(self) -> list[BookLevel]:
        return _levels(self.bids)

    def ask_levels(self) -> list[BookLevel]:
        return _levels(self.asks)

    def to_order_book(
        self, ticker: UniversalTicker, *, ts: float = 0.0
    ) -> OrderBook:
        """The payload as a whole book. Only correct for a ``snapshot``."""
        fields: dict[str, Any] = {} if ts <= 0 else {"ts": ts}
        return OrderBook(
            universal_ticker=str(ticker),
            bids=self.bid_levels(),
            asks=self.ask_levels(),
            **fields,
        )

    def to_best_quote(
        self, ticker: UniversalTicker, *, ts: float = 0.0
    ) -> BestQuote | None:
        """Top of book, or ``None`` if this payload has no both-sided quote.

        For ``orderbook.1`` a one-sided payload means that side is empty, and a
        quote with a zero on it is not something a caller can act on.
        """
        bids = self.bid_levels()
        asks = self.ask_levels()
        if not bids or not asks:
            return None
        fields: dict[str, Any] = {} if ts <= 0 else {"ts": ts}
        return BestQuote(
            universal_ticker=str(ticker),
            bid=bids[0].price,
            bid_qty=bids[0].qty,
            ask=asks[0].price,
            ask_qty=asks[0].qty,
            **fields,
        )


class BybitKline(BybitMessage):
    """One row of ``kline.<interval>.<symbol>``.

    Carries no symbol: the topic names it, so :meth:`to_kline` is told which
    instrument it is rather than reading one. ``confirm`` marks the closing
    tick of the window — everything before it is the same bar re-pushed.
    """

    start: Ms = 0.0
    end: Ms = 0.0
    interval: str = ""
    open: Dec = Decimal("0")
    close: Dec = Decimal("0")
    high: Dec = Decimal("0")
    low: Dec = Decimal("0")
    volume: Dec = Decimal("0")
    turnover: Dec = Decimal("0")
    confirm: bool = False
    timestamp: Ms = 0.0

    def to_kline(self, ticker: UniversalTicker) -> Kline:
        """The candle in shared form, still in Bybit's interval spelling.

        The interval is translated a layer up, in
        :class:`~mftik.exchange.bybit.public.BybitPublicClient`, so this model
        stays a faithful reading of the wire.
        """
        return Kline(
            universal_ticker=str(ticker),
            interval=self.interval,
            open_time=self.start,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            quote_volume=self.turnover,
            closed=self.confirm,
        )


# --- call replies ----------------------------------------------------------


class BybitOrderAck(BybitMessage):
    """The reply to ``order.create`` / ``order.cancel`` / ``order.amend``.

    Two ids and nothing else — no status, no quantity, no price. Bybit's order
    endpoints acknowledge receipt and say what happened on the ``order`` topic,
    which is why the connector reports the acked order as ``PENDING_NEW``
    rather than inventing a state the venue did not report.
    """

    order_id: str = Field(default="", alias="orderId")
    order_link_id: str = Field(default="", alias="orderLinkId")

    @property
    def client_order_id(self) -> str | None:
        return self.order_link_id or None


def kline_from_row(
    row: list[Any], ticker: UniversalTicker, interval: str
) -> Kline:
    """One row of ``GET /v5/market/kline`` — a positional array, not an object.

    Bybit's column order is OHLC and then the two volumes, with no second
    timestamp in between::

        [0] window start, ms    [4] close
        [1] open                [5] volume, base
        [2] high                [6] turnover, quote
        [3] low

    Every row from this endpoint is reported as closed, including the last —
    which is the window in progress, because the endpoint answers newest first
    and says nothing about which is which. The caller drops or keeps that row
    knowing the interval; guessing from a timestamp would be wrong exactly at
    the boundary that matters.
    """
    if len(row) < 7:
        raise ValueError(
            f"kline row for {ticker} {interval} has {len(row)} columns, "
            f"expected at least 7: {row!r}"
        )
    return Kline(
        universal_ticker=str(ticker),
        interval=interval,
        open_time=float(row[0]) / 1000.0,
        open=Decimal(str(row[1])),
        high=Decimal(str(row[2])),
        low=Decimal(str(row[3])),
        close=Decimal(str(row[4])),
        volume=Decimal(str(row[5])),
        quote_volume=Decimal(str(row[6])),
        closed=True,
    )


def order_book_from_result(
    result: dict[str, Any], ticker: UniversalTicker
) -> OrderBook:
    """``GET /v5/market/orderbook`` — a whole book, dated by the venue."""
    return OrderBook(
        universal_ticker=str(ticker),
        bids=_levels(result.get("b")),
        asks=_levels(result.get("a")),
        ts=float(result.get("ts", 0) or 0) / 1000.0,
    )


__all__ = [
    "EXEC_TYPE_TRADE",
    "BybitExecution",
    "BybitKline",
    "BybitLiquidation",
    "BybitMessage",
    "BybitOrderAck",
    "BybitOrderBook",
    "BybitOrderUpdate",
    "BybitPosition",
    "BybitPublicTrade",
    "BybitTicker",
    "BybitWallet",
    "BybitWalletCoin",
    "Dec",
    "Ms",
    "OptDec",
    "VenueSide",
    "kline_from_row",
    "order_book_from_result",
    "status_of",
    "type_of",
]
