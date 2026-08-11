"""Binance futures wire models — market streams, user data, and call replies.

These mirror Binance's wire format field-for-field, single-letter keys and all,
and keep its ``BTCUSDT`` spelling. Where a shared ``mft.exchange`` model is a
faithful fit there is a ``to_*`` converter; where futures has no equivalent
(mark price, funding, depth diffs) there deliberately is none, because the
shared models have nowhere to put those without inventing a reading.

Separate from :mod:`mft.exchange.binance.spot.models` on purpose, though the
primitives underneath are shared. Five payload differences are the reason, and
each one is a bug if it is assumed away:

* **``@ticker`` carries no quote.** The futures 24h ticker has no ``b``/``a``
  fields at all, so a :class:`~mft.exchange.models.Ticker` cannot be built from
  it alone — :meth:`BinanceFutureTicker.to_ticker` is *given* the quote, which
  the caller reads off ``@bookTicker``.
* **Average price is a field.** Futures publishes ``ap`` on both the order
  update and the order reply, so nothing here divides one total by another.
* **Depth pushes carry their symbol.** Partial depth on futures is a
  ``depthUpdate`` with ``s``, ``U``, ``u`` and ``pu`` — the same shape as the
  diff stream, and it means the opposite thing: a snapshot rather than a delta.
* **Fills are ``ORDER_TRADE_UPDATE`` with ``o.x == "TRADE"``**, whose ``l``/
  ``L`` are that execution alone while ``z`` is the order's running total.
  Using ``z`` where ``l`` belongs double-counts every partial.
* **A liquidation's ``S`` is the closing order's side, not the position's.**
  A long being liquidated is closed by a ``SELL``, and
  :class:`~mft.exchange.models.Liquidation` states the *position's* side — so
  :meth:`BinanceFutureLiquidation.to_liquidation` inverts it.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import Field

from mft.exchange.binance.models import (
    BinanceMessage,
    VenueSide,
    kline_from_row,
    levels,
    secs,
    side_of,
)
from mft.exchange.models import (
    AggTrade,
    Balance,
    BestQuote,
    BookLevel,
    Fill,
    Instrument,
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
from mft.exchange.oms import Position
from mft.exchange.tickers import UniversalTicker

#: Futures order status → ours. ``EXPIRED`` is a time-in-force that ran out or
#: an order the venue killed and ``EXPIRED_IN_MATCH`` a self-trade prevention
#: kill; both end an order without filling it, which is what CANCELED means
#: here. ``NEW_INSURANCE`` and ``NEW_ADL`` are the insurance fund and
#: auto-deleveraging taking the other side of a liquidation — they name a
#: *resting* order, so they read as NEW.
_STATUS: dict[str, OrderStatus] = {
    "NEW": OrderStatus.NEW,
    "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
    "FILLED": OrderStatus.FILLED,
    "CANCELED": OrderStatus.CANCELED,
    "REJECTED": OrderStatus.REJECTED,
    "EXPIRED": OrderStatus.CANCELED,
    "EXPIRED_IN_MATCH": OrderStatus.CANCELED,
    "NEW_INSURANCE": OrderStatus.NEW,
    "NEW_ADL": OrderStatus.NEW,
}

#: Futures order type → ours. The conditional types have no shared equivalent
#: and are read as the leg they turn into once triggered; ``LIQUIDATION`` is
#: the venue closing a position at market, which is what it becomes.
_TYPE: dict[str, OrderType] = {
    "MARKET": OrderType.MARKET,
    "LIMIT": OrderType.LIMIT,
    "STOP": OrderType.LIMIT,
    "STOP_MARKET": OrderType.MARKET,
    "TAKE_PROFIT": OrderType.LIMIT,
    "TAKE_PROFIT_MARKET": OrderType.MARKET,
    "TRAILING_STOP_MARKET": OrderType.MARKET,
    "LIQUIDATION": OrderType.MARKET,
}

#: What ``positionSide`` is on an account that is not in hedge mode. The only
#: value this adapter places orders under — see
#: :class:`~mft.exchange.binance.future.private.BinanceFuturePrivateClient`.
BOTH = "BOTH"


def status_of(value: str | None) -> OrderStatus:
    """Futures' ``status`` / ``X``, or UNKNOWN for one we have no name for.

    UNKNOWN rather than a guess: an unrecognised status means the venue told us
    something this platform cannot interpret, and
    :attr:`~mft.exchange.models.OrderStatus.UNKNOWN` is exactly the state that
    says "ask again" instead of inventing a lifecycle event.
    """
    return _STATUS.get((value or "").upper(), OrderStatus.UNKNOWN)


def type_of(value: str | None) -> OrderType:
    return _TYPE.get((value or "").upper(), OrderType.LIMIT)


def _opt(value: Decimal | None) -> Decimal | None:
    """A number the venue writes as ``0`` where it means "none"."""
    return value if value else None


# --- market streams --------------------------------------------------------


class BinanceFutureAggTrade(BinanceMessage):
    """``<symbol>@aggTrade`` — the tape, same-price fills coalesced.

    The only tape futures publishes: there is no ``@trade`` stream on this
    market. So ``a`` — the aggregate id — is the only trade id a futures feed
    can report, and a :class:`~mft.exchange.models.Trade` built here carries
    one that names a group of matches rather than a match.
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
        return secs(self.trade_time or self.event_time)

    def to_trade(self, ticker: UniversalTicker) -> Trade:
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


class BinanceFutureMarkPrice(BinanceMessage):
    """``<symbol>@markPrice`` — mark, index, funding rate, next funding time.

    A perpetual's own vocabulary, with no spot equivalent and no shared model:
    the mark price is what margin and liquidations are computed against and is
    deliberately not the last traded price, and the funding rate is a payment
    schedule rather than a quote. Converting either into a
    :class:`~mft.exchange.models.Ticker` would state something the venue did
    not, so this model is read in its own terms or not at all.
    """

    e: str = "markPriceUpdate"
    event_time: int = Field(default=0, alias="E")
    s: str
    mark_price: Decimal = Field(alias="p")
    index_price: Decimal | None = Field(default=None, alias="i")
    settle_price: Decimal | None = Field(default=None, alias="P")
    funding_rate: Decimal | None = Field(default=None, alias="r")
    next_funding_time: int = Field(default=0, alias="T")

    @property
    def symbol(self) -> str:
        return self.s

    @property
    def ts(self) -> float:
        return secs(self.event_time)


class BinanceFutureKlineWindow(BinanceMessage):
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


class BinanceFutureKlineEvent(BinanceMessage):
    """``<symbol>@kline_<interval>`` — the envelope around one candle."""

    e: str = "kline"
    event_time: int = Field(default=0, alias="E")
    s: str
    k: BinanceFutureKlineWindow

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
        :class:`~mft.exchange.binance.future.public.BinanceFuturePublicClient`,
        so that this model stays a faithful reading of the wire.
        """
        return Kline(
            universal_ticker=str(ticker),
            interval=self.k.i,
            # ``t`` is the window's open time, in milliseconds.
            open_time=secs(self.k.t),
            open=self.k.o,
            high=self.k.h,
            low=self.k.low,
            close=self.k.c,
            volume=self.k.v,
            quote_volume=self.k.quote_volume,
            closed=self.k.x,
        )


class BinanceFutureTicker(BinanceMessage):
    """``<symbol>@ticker`` — rolling 24h stats, and **no quote**.

    The one payload difference most likely to be assumed away: spot's ticker
    carries ``b``/``B``/``a``/``A``, and this one does not carry them at all.
    There is no bid and no ask to read, so :meth:`to_ticker` takes them as
    arguments rather than inventing them from ``c`` — a
    :class:`~mft.exchange.models.Ticker` whose bid, ask and last were the same
    number would read as a crossed-then-flat book to anything comparing
    venues, which is exactly what a cross-venue strategy does with it.
    """

    e: str = "24hrTicker"
    event_time: int = Field(default=0, alias="E")
    s: str
    price_change: Decimal | None = Field(default=None, alias="p")
    change_percent: Decimal | None = Field(default=None, alias="P")
    weighted_avg: Decimal | None = Field(default=None, alias="w")
    last: Decimal = Field(alias="c")
    last_qty: Decimal | None = Field(default=None, alias="Q")
    o: Decimal | None = None
    h: Decimal | None = None
    low: Decimal | None = Field(default=None, alias="l")
    v: Decimal | None = None
    q: Decimal | None = None

    @property
    def symbol(self) -> str:
        return self.s

    @property
    def ts(self) -> float:
        return secs(self.event_time)

    def to_ticker(
        self, ticker: UniversalTicker, *, bid: Decimal, ask: Decimal
    ) -> Ticker:
        """The 24h stats plus a quote the caller read off ``@bookTicker``."""
        return Ticker(
            universal_ticker=str(ticker),
            bid=bid,
            ask=ask,
            last=self.last,
            ts=self.ts,
        )


class BinanceFutureBookTicker(BinanceMessage):
    """``<symbol>@bookTicker`` — best bid/ask on every change.

    Unlike spot's, this one is dated: ``T`` is the transaction time and ``E``
    the event time, so nothing downstream has to stamp arrival.
    """

    e: str = "bookTicker"
    u: int = 0
    s: str
    bid: Decimal = Field(alias="b")
    bid_size: Decimal = Field(alias="B")
    ask: Decimal = Field(alias="a")
    ask_size: Decimal = Field(alias="A")
    transact_time: int = Field(default=0, alias="T")
    event_time: int = Field(default=0, alias="E")

    @property
    def symbol(self) -> str:
        return self.s

    @property
    def ts(self) -> float:
        return secs(self.transact_time or self.event_time)

    def to_best_quote(self, ticker: UniversalTicker) -> BestQuote:
        return BestQuote(
            universal_ticker=str(ticker),
            bid=self.bid,
            bid_qty=self.bid_size,
            ask=self.ask,
            ask_qty=self.ask_size,
            ts=self.ts,
        )


class BinanceFutureDepth(BinanceMessage):
    """The ``depth`` call reply — a whole book, as the WebSocket API answers it.

    Carries no symbol, only ``lastUpdateId`` and the two sides, so
    :meth:`to_order_book` is told which instrument it is. It *is* dated, unlike
    spot's: ``T`` is when the book was taken.
    """

    last_update_id: int = Field(default=0, alias="lastUpdateId")
    event_time: int = Field(default=0, alias="E")
    transact_time: int = Field(default=0, alias="T")
    bids: list[Any] = Field(default_factory=list)
    asks: list[Any] = Field(default_factory=list)

    def to_order_book(
        self, ticker: UniversalTicker, *, ts: float | None = None
    ) -> OrderBook:
        stamp = ts if ts is not None else secs(self.transact_time or self.event_time)
        fields: dict[str, Any] = {} if not stamp else {"ts": stamp}
        return OrderBook(
            universal_ticker=str(ticker),
            bids=levels(self.bids),
            asks=levels(self.asks),
            **fields,
        )


class BinanceFutureDepthUpdate(BinanceMessage):
    """``<symbol>@depth<levels>`` and ``<symbol>@depth`` — one shape, two meanings.

    Futures pushes both the capped snapshot and the diff stream as a
    ``depthUpdate`` with the same fields, and only the stream it arrived on
    says which it is:

    * from ``@depth<levels>`` the sides are the top of the book **as it now
      stands** — a snapshot, which is what :meth:`to_order_book` reads;
    * from ``@depth`` they are **changes**, where a zero quantity deletes a
      level rather than setting it to nothing — so applying one as a book
      would empty half of it.

    ``pu`` is the previous message's ``u`` on this stream, which makes a gap
    detectable without a snapshot to compare against; :meth:`follows` is that
    check.
    """

    e: str = "depthUpdate"
    event_time: int = Field(default=0, alias="E")
    transact_time: int = Field(default=0, alias="T")
    s: str
    first_id: int = Field(default=0, alias="U")
    last_id: int = Field(default=0, alias="u")
    prev_id: int = Field(default=0, alias="pu")
    bids: list[Any] = Field(default_factory=list, alias="b")
    asks: list[Any] = Field(default_factory=list, alias="a")

    @property
    def symbol(self) -> str:
        return self.s

    @property
    def ts(self) -> float:
        return secs(self.transact_time or self.event_time)

    def bid_levels(self) -> list[BookLevel]:
        return levels(self.bids)

    def ask_levels(self) -> list[BookLevel]:
        return levels(self.asks)

    def follows(self, last_applied_id: int) -> bool:
        """Whether this message continues a stream already at ``last_applied_id``.

        Futures' own rule, and stricter than spot's: consecutive diffs satisfy
        ``pu == u`` of the previous one, so a gap is visible immediately rather
        than only against a snapshot's ``lastUpdateId``.
        """
        return self.prev_id == last_applied_id

    def to_order_book(
        self, ticker: UniversalTicker, *, ts: float | None = None
    ) -> OrderBook:
        """The message read as a whole book — only valid off ``@depth<levels>``."""
        return OrderBook(
            universal_ticker=str(ticker),
            bids=levels(self.bids),
            asks=levels(self.asks),
            ts=ts if ts is not None else self.ts,
        )


class BinanceFutureLiquidationOrder(BinanceMessage):
    """The ``o`` block of a ``@forceOrder`` push — the closing order itself."""

    s: str
    side: VenueSide = Field(alias="S")
    order_type: str = Field(default="LIMIT", alias="o")
    time_in_force: str = Field(default="", alias="f")
    q: Decimal = Decimal("0")
    p: Decimal = Decimal("0")
    avg_price: Decimal = Field(default=Decimal("0"), alias="ap")
    order_status: str = Field(default="", alias="X")
    last_qty: Decimal = Field(default=Decimal("0"), alias="l")
    filled_qty: Decimal = Field(default=Decimal("0"), alias="z")
    trade_time: int = Field(default=0, alias="T")


class BinanceFutureLiquidation(BinanceMessage):
    """``<symbol>@forceOrder`` — one public forced liquidation.

    A **sample**, not the whole flow: Binance pushes the largest liquidation
    per symbol per second and drops the rest, so counting these counts what
    survived that filter.
    """

    e: str = "forceOrder"
    event_time: int = Field(default=0, alias="E")
    o: BinanceFutureLiquidationOrder

    @property
    def symbol(self) -> str:
        return self.o.s

    @property
    def ts(self) -> float:
        return secs(self.o.trade_time or self.event_time)

    def to_liquidation(self, ticker: UniversalTicker) -> Liquidation:
        """The event as :class:`~mft.exchange.models.Liquidation` — side flipped.

        Binance reports the **closing order's** side: a long that ran out of
        margin is closed by a ``SELL``. The shared model states the liquidated
        *position's* side, so the two are opposites and reading ``S`` straight
        through would report every liquidation as its own mirror image.

        ``ap`` where the order filled, ``p`` where it has not yet: the average
        fill price is what the position was actually closed at, and the order
        price is only the limit the venue posted it at.
        """
        return Liquidation(
            universal_ticker=str(ticker),
            price=self.o.avg_price or self.o.p,
            qty=self.o.q,
            side=Side.BUY if self.o.side is Side.SELL else Side.SELL,
            ts=self.ts,
        )


# --- user data stream ------------------------------------------------------


class BinanceFutureOrderUpdate(BinanceMessage):
    """The ``o`` block of ``ORDER_TRADE_UPDATE`` — futures' only order event.

    ``x`` (execution type) says what happened and ``X`` (order status) where
    the order now stands; only the pair is enough. A partial fill is
    ``x=TRADE`` with ``X=PARTIALLY_FILLED``, while a cancel that raced a fill is
    ``x=CANCELED`` with ``X=CANCELED`` and a non-zero ``z``.
    """

    s: str
    client_order_id: str = Field(default="", alias="c")
    side: VenueSide = Field(alias="S")
    order_type: str = Field(default="LIMIT", alias="o")
    time_in_force: str = Field(default="", alias="f")
    q: Decimal = Decimal("0")
    p: Decimal = Decimal("0")
    average_price: Decimal = Field(default=Decimal("0"), alias="ap")
    stop_price: Decimal = Field(default=Decimal("0"), alias="sp")
    exec_type: str = Field(default="", alias="x")
    order_status: str = Field(default="", alias="X")
    order_id: int = Field(default=0, alias="i")
    last_qty: Decimal = Field(default=Decimal("0"), alias="l")
    filled_qty_total: Decimal = Field(default=Decimal("0"), alias="z")
    last_price: Decimal = Field(default=Decimal("0"), alias="L")
    commission_asset: str | None = Field(default=None, alias="N")
    commission: Decimal = Field(default=Decimal("0"), alias="n")
    trade_time: int = Field(default=0, alias="T")
    trade_id: int = Field(default=-1, alias="t")
    is_maker: bool = Field(default=False, alias="m")
    reduce_only: bool = Field(default=False, alias="R")
    position_side: str = Field(default=BOTH, alias="ps")
    realized_pnl: Decimal = Field(default=Decimal("0"), alias="rp")

    @property
    def symbol(self) -> str:
        return self.s

    @property
    def status(self) -> OrderStatus:
        return status_of(self.order_status)

    @property
    def type(self) -> OrderType:
        return type_of(self.order_type)

    @property
    def is_fill(self) -> bool:
        """Whether this update carries an execution, not just a state change."""
        return self.exec_type.upper() == "TRADE" and self.last_qty > 0


class BinanceFutureOrderTradeUpdate(BinanceMessage):
    """``ORDER_TRADE_UPDATE`` — the envelope around one order event."""

    e: str = "ORDER_TRADE_UPDATE"
    event_time: int = Field(default=0, alias="E")
    transact_time: int = Field(default=0, alias="T")
    o: BinanceFutureOrderUpdate

    @property
    def symbol(self) -> str:
        return self.o.s

    @property
    def client_order_id(self) -> str | None:
        """The id we gave the order, on every update about it.

        Futures echoes it in ``c`` throughout an order's life — including on a
        cancel, unlike spot, which moves it to ``C`` and puts the cancel
        request's own id in ``c``. Binance also *writes* into this field:
        a liquidation carries ``autoclose-<ts>`` and an ADL close carries
        ``adl_autoclose``, neither of which is an id we issued.
        """
        return self.o.client_order_id or None

    @property
    def ts(self) -> float:
        return secs(self.o.trade_time or self.transact_time or self.event_time)

    @property
    def is_fill(self) -> bool:
        return self.o.is_fill

    def to_order(self, ticker: UniversalTicker) -> Order:
        return Order(
            universal_ticker=str(ticker),
            order_id=str(self.o.order_id),
            client_order_id=self.client_order_id,
            side=self.o.side,
            type=self.o.type,
            status=self.o.status,
            qty=self.o.q,
            # A market order reports price 0; None reads as "no limit price",
            # which is what it is.
            price=_opt(self.o.p),
            filled_qty=self.o.filled_qty_total,
            avg_price=_opt(self.o.average_price),
            ts=self.ts,
        )

    def to_fill(self, ticker: UniversalTicker) -> Fill:
        """This update's own execution — ``l``/``L``, never the running totals.

        Only meaningful when :attr:`is_fill`; callers filter first.
        """
        return Fill(
            universal_ticker=str(ticker),
            fill_id=str(self.o.trade_id),
            order_id=str(self.o.order_id),
            client_order_id=self.client_order_id,
            side=self.o.side,
            price=self.o.last_price,
            qty=self.o.last_qty,
            fee=self.o.commission,
            fee_asset=self.o.commission_asset or "",
            ts=self.ts,
        )


class BinanceFutureWalletBalance(BinanceMessage):
    """One asset inside an ``ACCOUNT_UPDATE``'s ``B`` array.

    ``wb`` is the wallet balance and ``cw`` the part of it not locked into an
    isolated position, so the difference is margin this account cannot spend
    on something else. ``bc`` is the *change* that caused the event and is not
    part of the balance — it is why, not what.
    """

    a: str
    wallet_balance: Decimal = Field(default=Decimal("0"), alias="wb")
    cross_wallet_balance: Decimal = Field(default=Decimal("0"), alias="cw")
    balance_change: Decimal = Field(default=Decimal("0"), alias="bc")

    def to_balance(self) -> Balance:
        """The asset as a shared :class:`~mft.exchange.models.Balance`.

        ``free`` is the cross wallet balance and ``locked`` the remainder,
        which is what an isolated position is holding. Not the same reading as
        spot's, where the venue publishes the two numbers directly — here the
        second one has to be derived, and it is derived rather than left at
        zero because a session that reported isolated margin as spendable
        would size its next order against money it cannot use.
        """
        locked = self.wallet_balance - self.cross_wallet_balance
        return Balance(
            asset=self.a,
            free=self.cross_wallet_balance,
            locked=locked if locked > 0 else Decimal("0"),
        )


class BinanceFuturePositionRow(BinanceMessage):
    """One position inside an ``ACCOUNT_UPDATE``'s ``P`` array.

    ``pa`` is signed — negative is short — which is the same convention
    :class:`~mft.exchange.oms.Position` states, so nothing has to combine a
    size with a direction here.
    """

    s: str
    position_amount: Decimal = Field(default=Decimal("0"), alias="pa")
    entry_price: Decimal = Field(default=Decimal("0"), alias="ep")
    break_even_price: Decimal = Field(default=Decimal("0"), alias="bep")
    accumulated_realized: Decimal = Field(default=Decimal("0"), alias="cr")
    unrealized_pnl: Decimal = Field(default=Decimal("0"), alias="up")
    margin_type: str = Field(default="", alias="mt")
    isolated_wallet: Decimal = Field(default=Decimal("0"), alias="iw")
    position_side: str = Field(default=BOTH, alias="ps")

    @property
    def symbol(self) -> str:
        return self.s

    def to_position(self, ticker: UniversalTicker) -> Position:
        return Position(
            universal_ticker=str(ticker),
            qty=self.position_amount,
            entry_price=_opt(self.entry_price),
            unrealised_pnl=self.unrealized_pnl,
        )


class BinanceFutureAccountUpdate(BinanceMessage):
    """``ACCOUNT_UPDATE`` — the balances and positions one event moved.

    A snapshot of *what changed*, not of the account: assets and positions
    untouched by the event are absent, and each row that is present can be
    taken as that asset's or that instrument's new truth. ``m`` says what
    caused it — ``ORDER``, ``FUNDING_FEE``, ``DEPOSIT`` and so on.
    """

    e: str = "ACCOUNT_UPDATE"
    event_time: int = Field(default=0, alias="E")
    transact_time: int = Field(default=0, alias="T")
    a: dict[str, Any] = Field(default_factory=dict)

    @property
    def reason(self) -> str:
        return str(self.a.get("m") or "")

    @property
    def ts(self) -> float:
        return secs(self.transact_time or self.event_time)

    def to_balances(self) -> list[Balance]:
        return [
            BinanceFutureWalletBalance.model_validate(row).to_balance()
            for row in self.a.get("B") or []
        ]

    def position_rows(self) -> list[BinanceFuturePositionRow]:
        """The event's position rows, still in Binance's spelling.

        Not converted here because a :class:`~mft.exchange.oms.Position` needs
        the universal ticker, and only the symbol plane maps to it — which
        lives a layer up, in the connector.
        """
        return [
            BinanceFuturePositionRow.model_validate(row)
            for row in self.a.get("P") or []
        ]


class BinanceListenKeyExpired(BinanceMessage):
    """``listenKeyExpired`` — this socket has stopped carrying anything.

    Not an error and not a disconnect: the connection stays open and simply
    goes quiet, which is why it has to be acted on rather than logged. See
    :class:`~mft.exchange.binance.future.user.BinanceFutureUserStream`.
    """

    e: str = "listenKeyExpired"
    event_time: int = Field(default=0, alias="E")
    listen_key: str = Field(default="", alias="listenKey")


# --- call replies ----------------------------------------------------------


class BinanceFutureOrderAck(BinanceMessage):
    """The reply to ``order.place`` / ``order.cancel`` / ``order.status``.

    Futures answers with the order's totals and — unlike spot — with
    ``avgPrice`` already computed, so nothing here divides ``cumQuote`` by
    ``executedQty``.
    """

    symbol: str = ""
    order_id: int = Field(default=0, alias="orderId")
    client_order_id: str = Field(default="", alias="clientOrderId")
    price: Decimal = Decimal("0")
    average_price: Decimal = Field(default=Decimal("0"), alias="avgPrice")
    orig_qty: Decimal = Field(default=Decimal("0"), alias="origQty")
    executed_qty: Decimal = Field(default=Decimal("0"), alias="executedQty")
    cum_quote: Decimal = Field(default=Decimal("0"), alias="cumQuote")
    status: str = ""
    time_in_force: str = Field(default="", alias="timeInForce")
    type: str = "LIMIT"
    orig_type: str = Field(default="", alias="origType")
    side: VenueSide = Side.BUY
    position_side: str = Field(default=BOTH, alias="positionSide")
    reduce_only: bool = Field(default=False, alias="reduceOnly")
    close_position: bool = Field(default=False, alias="closePosition")
    time: int = 0
    update_time: int = Field(default=0, alias="updateTime")

    @property
    def order_status(self) -> OrderStatus:
        return status_of(self.status)

    @property
    def ts(self) -> float:
        return secs(self.update_time or self.time)

    def to_order(self, ticker: UniversalTicker) -> Order:
        return Order(
            universal_ticker=str(ticker),
            order_id=str(self.order_id),
            client_order_id=self.client_order_id or None,
            side=self.side,
            # ``origType`` where it is published: Binance rewrites ``type`` to
            # the leg a triggered conditional order turned into, and the order
            # a caller placed is the one it asked about.
            type=type_of(self.orig_type or self.type),
            status=self.order_status,
            qty=self.orig_qty,
            price=_opt(self.price),
            filled_qty=self.executed_qty,
            avg_price=_opt(self.average_price),
            ts=self.ts,
        )


class BinanceFutureBalance(BinanceMessage):
    """One row of ``v2/account.balance``.

    ``balance`` is the wallet balance and ``availableBalance`` what is left to
    open something new with; the difference is margin and unrealised loss the
    account is already carrying.
    """

    account_alias: str = Field(default="", alias="accountAlias")
    asset: str = ""
    balance: Decimal = Decimal("0")
    cross_wallet_balance: Decimal = Field(
        default=Decimal("0"), alias="crossWalletBalance"
    )
    cross_unrealized: Decimal = Field(default=Decimal("0"), alias="crossUnPnl")
    available_balance: Decimal = Field(default=Decimal("0"), alias="availableBalance")
    max_withdraw: Decimal = Field(default=Decimal("0"), alias="maxWithdrawAmount")
    margin_available: bool = Field(default=True, alias="marginAvailable")

    def to_balance(self) -> Balance:
        """``free`` is what can still be committed; ``locked`` is the rest.

        A futures wallet has no free/locked split of its own — margin is not
        held per order, it is held against the position — so the split is
        derived from what the venue says is available. Reporting the whole
        wallet balance as free would let a session size an order against
        money already posted as margin.
        """
        locked = self.balance - self.available_balance
        return Balance(
            asset=self.asset,
            free=self.available_balance,
            locked=locked if locked > 0 else Decimal("0"),
        )


class BinanceFuturePosition(BinanceMessage):
    """One row of ``v2/account.position``.

    ``positionAmt`` is signed, so it is the shared model's ``qty`` unchanged.
    """

    symbol: str = ""
    position_side: str = Field(default=BOTH, alias="positionSide")
    position_amount: Decimal = Field(default=Decimal("0"), alias="positionAmt")
    entry_price: Decimal = Field(default=Decimal("0"), alias="entryPrice")
    break_even_price: Decimal = Field(default=Decimal("0"), alias="breakEvenPrice")
    mark_price: Decimal = Field(default=Decimal("0"), alias="markPrice")
    unrealized_profit: Decimal = Field(default=Decimal("0"), alias="unRealizedProfit")
    liquidation_price: Decimal = Field(default=Decimal("0"), alias="liquidationPrice")
    leverage: Decimal | None = None
    margin_asset: str = Field(default="", alias="marginAsset")
    update_time: int = Field(default=0, alias="updateTime")

    def to_position(self, ticker: UniversalTicker) -> Position:
        return Position(
            universal_ticker=str(ticker),
            qty=self.position_amount,
            entry_price=_opt(self.entry_price),
            unrealised_pnl=self.unrealized_profit,
        )


class BinanceFutureSymbolConfig(BinanceMessage):
    """One row of ``GET /fapi/v1/symbolConfig`` — per-symbol account settings.

    Unlike ``positionRisk`` v3 (only symbols with a position or resting order),
    this answers for a flat book, which is what leverage lookup needs before
    the first order.
    """

    symbol: str = ""
    margin_type: str = Field(default="", alias="marginType")
    is_auto_add_margin: bool = Field(default=False, alias="isAutoAddMargin")
    leverage: Decimal | None = None
    max_notional_value: str = Field(default="", alias="maxNotionalValue")


class BinanceFutureBookQuote(BinanceMessage):
    """The ``ticker.book`` reply — top of book with sizes, dated by Binance."""

    symbol: str = ""
    bid: Decimal = Field(default=Decimal("0"), alias="bidPrice")
    bid_qty: Decimal = Field(default=Decimal("0"), alias="bidQty")
    ask: Decimal = Field(default=Decimal("0"), alias="askPrice")
    ask_qty: Decimal = Field(default=Decimal("0"), alias="askQty")
    time: int = 0

    @property
    def quoted(self) -> bool:
        """Whether both sides have something resting.

        A zero side is an empty book, not a price of zero, and a quote with a
        hole in it is not something a caller can act on.
        """
        return bool(self.bid and self.ask and self.bid_qty and self.ask_qty)

    def to_best_quote(self, ticker: UniversalTicker) -> BestQuote:
        return BestQuote(
            universal_ticker=str(ticker),
            bid=self.bid,
            bid_qty=self.bid_qty,
            ask=self.ask,
            ask_qty=self.ask_qty,
            ts=secs(self.time),
        )


class BinanceFuturePrice(BinanceMessage):
    """The ``ticker.price`` reply — the last traded price, and nothing else."""

    symbol: str = ""
    price: Decimal = Decimal("0")
    time: int = 0

    @property
    def ts(self) -> float:
        return secs(self.time)


def instrument_from_row(row: dict[str, Any]) -> Instrument:
    """One ``exchangeInfo`` symbol, with its steps pulled out of ``filters``.

    Binance keeps the steps in a list of typed filter objects rather than as
    fields, and publishes ``0`` for a step it does not enforce — a zero step is
    dropped rather than stored, because a zero here would divide.

    Futures spells the notional floor ``notional`` inside a ``MIN_NOTIONAL``
    filter, where spot spells it ``minNotional``; reading spot's key here
    returns nothing and the floor silently disappears.
    """
    filters = {
        str(f.get("filterType", "")): f for f in row.get("filters", []) or []
    }
    price = filters.get("PRICE_FILTER", {})
    lot = filters.get("LOT_SIZE", {})
    notional = filters.get("MIN_NOTIONAL", {})

    fields: dict[str, Any] = {
        "symbol": str(row.get("symbol", "")),
        "base": str(row.get("baseAsset", "")),
        "quote": str(row.get("quoteAsset", "")),
        "min_qty": _dec_or_none(lot.get("minQty")),
        "min_notional": _dec_or_none(notional.get("notional")),
    }
    tick = _dec_or_none(price.get("tickSize"))
    if tick is not None:
        fields["tick_size"] = tick
    step = _dec_or_none(lot.get("stepSize"))
    if step is not None:
        fields["lot_size"] = step
    return Instrument(**fields)


def _dec_or_none(value: Any) -> Decimal | None:
    """``None`` where Binance publishes no bound, so the filter reads as absent."""
    if value is None or value == "":
        return None
    parsed = Decimal(str(value))
    return parsed if parsed > 0 else None


__all__ = [
    "BOTH",
    "BinanceFutureAccountUpdate",
    "BinanceFutureAggTrade",
    "BinanceFutureBalance",
    "BinanceFutureBookQuote",
    "BinanceFutureBookTicker",
    "BinanceFutureDepth",
    "BinanceFutureDepthUpdate",
    "BinanceFutureKlineEvent",
    "BinanceFutureKlineWindow",
    "BinanceFutureLiquidation",
    "BinanceFutureLiquidationOrder",
    "BinanceFutureMarkPrice",
    "BinanceFutureOrderAck",
    "BinanceFutureOrderTradeUpdate",
    "BinanceFutureOrderUpdate",
    "BinanceFuturePosition",
    "BinanceFuturePositionRow",
    "BinanceFuturePrice",
    "BinanceFutureSymbolConfig",
    "BinanceFutureTicker",
    "BinanceFutureWalletBalance",
    "BinanceListenKeyExpired",
    "instrument_from_row",
    "kline_from_row",
    "status_of",
    "type_of",
]
