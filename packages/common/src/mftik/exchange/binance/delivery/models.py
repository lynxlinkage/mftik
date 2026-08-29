"""Binance COIN-M wire models — public reads, market-stream pushes, trading replies.

These mirror Binance's wire format field-for-field and keep its
``BTCUSD_PERP`` spelling. Shared primitives live in
:mod:`mftik.exchange.binance.models`; nothing here is imported from
:mod:`mftik.exchange.binance.future`. USD-M models assume linear quantity.
dapi sizes the book and the tape in **contracts**, and a kline's two volume
columns mean each other's thing.

Quantity units, once, so nothing above this module has to remember them:

* **Book, tape, liquidations.** ``q`` / level size stay in contracts. The
  listed ``qty_step`` is a contract step. Multiplying by ``contractSize``
  (USD per contract) would invent a dollar notional, not a base quantity.
* **Klines.** REST ``[5]`` / WS ``k.v`` count contracts; REST ``[7]`` /
  WS ``k.q`` is base. :meth:`BinanceDeliveryKlineEvent.to_kline` takes
  ``quote_per_contract`` and swaps them the same way
  :func:`~mftik.exchange.binance.models.kline_from_row` does.

Two other payload facts match USD-M and are just as easy to get backwards:

* **``@ticker`` carries no quote.** :meth:`BinanceDeliveryTicker.to_ticker`
  is given bid/ask from ``@bookTicker``.
* **A liquidation's ``S`` is the closing order's side**, not the
  position's. :meth:`BinanceDeliveryLiquidation.to_liquidation` inverts it.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import Field

from mftik.exchange.binance.models import (
    BinanceMessage,
    VenueSide,
    levels,
    secs,
    side_of,
)
from mftik.exchange.models import (
    AggTrade,
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
from mftik.exchange.tickers import UniversalTicker

#: dapi order status → ours. ``EXPIRED`` / ``EXPIRED_IN_MATCH`` end an
#: order without filling it. ``NEW_INSURANCE`` / ``NEW_ADL`` name a
#: resting insurance or ADL order.
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

BOTH = "BOTH"


def status_of(value: str | None) -> OrderStatus:
    """dapi ``status``, or UNKNOWN for one we have no name for."""
    return _STATUS.get((value or "").upper(), OrderStatus.UNKNOWN)


def type_of(value: str | None) -> OrderType:
    return _TYPE.get((value or "").upper(), OrderType.LIMIT)


def _opt(value: Decimal | None) -> Decimal | None:
    """A number the venue writes as ``0`` where it means "none"."""
    return value if value else None


class BinanceDeliveryDepth(BinanceMessage):
    """The ``depth`` reply — a whole book, no symbol on the body."""

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


class BinanceDeliveryAggTrade(BinanceMessage):
    """``<symbol>@aggTrade`` — the tape, same-price fills coalesced.

    The only tape this market publishes. ``a`` is an aggregate id, and
    ``q`` is a contract count — left as the venue wrote it.
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


class BinanceDeliveryMarkPrice(BinanceMessage):
    """``<symbol>@markPrice`` — mark, index, funding rate, next funding.

    Named for a later consumer. No shared model: converting a mark into a
    :class:`~mftik.exchange.models.Ticker` would state something the venue
    did not.
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


class BinanceDeliveryKlineWindow(BinanceMessage):
    """The ``k`` block of a kline push — the candle itself.

    On dapi, ``v`` counts contracts and ``q`` is base volume — the reverse
    of a linear bar. The swap happens in
    :meth:`BinanceDeliveryKlineEvent.to_kline`, not here.
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


class BinanceDeliveryKlineEvent(BinanceMessage):
    """``<symbol>@kline_<interval>`` — the envelope around one candle."""

    e: str = "kline"
    event_time: int = Field(default=0, alias="E")
    s: str
    k: BinanceDeliveryKlineWindow

    @property
    def symbol(self) -> str:
        return self.s

    @property
    def interval(self) -> str:
        """Binance's spelling of the window, echoed back from the subscribe."""
        return self.k.i

    def to_kline(
        self, ticker: UniversalTicker, *, quote_per_contract: Decimal
    ) -> Kline:
        """The candle in shared form, volumes read as coin-margined.

        ``quote_per_contract`` is the row's ``contractSize`` (USD per
        contract). Required so a linear read cannot land here by omitting
        the argument: ``k.v`` × that number is quote volume, and ``k.q`` is
        the base volume the shared model expects.
        """
        return Kline(
            universal_ticker=str(ticker),
            interval=self.k.i,
            open_time=secs(self.k.t),
            open=self.k.o,
            high=self.k.h,
            low=self.k.low,
            close=self.k.c,
            volume=self.k.quote_volume,
            quote_volume=self.k.v * quote_per_contract,
            closed=self.k.x,
        )


class BinanceDeliveryTicker(BinanceMessage):
    """``<symbol>@ticker`` — rolling 24h stats, and **no quote**.

    Same gap as USD-M: there is no ``b``/``a``. :meth:`to_ticker` takes
    them as arguments rather than inventing them from ``c``.
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


class BinanceDeliveryBookTicker(BinanceMessage):
    """``<symbol>@bookTicker`` — best bid/ask on every change.

    Sizes stay in contracts. ``T`` / ``E`` date the print.
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


class BinanceDeliveryDepthUpdate(BinanceMessage):
    """``<symbol>@depth<levels>`` — a capped-depth snapshot as a ``depthUpdate``.

    Off this stream the sides are the book as it now stands, not diffs.
    ``q`` on each level is a contract count. The payload carries its own
    symbol (and a ``ps`` pair this model ignores).
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

    def to_order_book(
        self, ticker: UniversalTicker, *, ts: float | None = None
    ) -> OrderBook:
        return OrderBook(
            universal_ticker=str(ticker),
            bids=levels(self.bids),
            asks=levels(self.asks),
            ts=ts if ts is not None else self.ts,
        )


class BinanceDeliveryLiquidationOrder(BinanceMessage):
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


class BinanceDeliveryLiquidation(BinanceMessage):
    """``<symbol>@forceOrder`` — one public forced liquidation.

    A sample, not the whole flow: Binance pushes the largest liquidation
    per symbol per second and drops the rest. ``o.q`` is contracts.
    """

    e: str = "forceOrder"
    event_time: int = Field(default=0, alias="E")
    o: BinanceDeliveryLiquidationOrder

    @property
    def symbol(self) -> str:
        return self.o.s

    @property
    def ts(self) -> float:
        return secs(self.o.trade_time or self.event_time)

    def to_liquidation(self, ticker: UniversalTicker) -> Liquidation:
        """The event as :class:`~mftik.exchange.models.Liquidation` — side flipped.

        Binance reports the **closing order's** side: a long that ran out of
        margin is closed by a ``SELL``. The shared model states the
        liquidated *position's* side.
        """
        return Liquidation(
            universal_ticker=str(ticker),
            price=self.o.avg_price or self.o.p,
            qty=self.o.q,
            side=Side.BUY if self.o.side is Side.SELL else Side.SELL,
            ts=self.ts,
        )


class BinanceDeliveryOrderAck(BinanceMessage):
    """The reply to ``order.place`` / ``order.cancel`` / ``order.status``.

    ``origQty`` / ``executedQty`` are contract counts. ``avgPrice`` is
    published; nothing here divides a quote total by a fill.
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
            type=type_of(self.orig_type or self.type),
            status=self.order_status,
            qty=self.orig_qty,
            price=_opt(self.price),
            filled_qty=self.executed_qty,
            avg_price=_opt(self.average_price),
            ts=self.ts,
        )


class BinanceDeliveryBalance(BinanceMessage):
    """One row of ``account.balance``.

    ``availableBalance`` is what can still open something; older dapi
    rows spell the same number ``withdrawAvailable``.
    """

    account_alias: str = Field(default="", alias="accountAlias")
    asset: str = ""
    balance: Decimal = Decimal("0")
    cross_wallet_balance: Decimal = Field(
        default=Decimal("0"), alias="crossWalletBalance"
    )
    cross_unrealized: Decimal = Field(default=Decimal("0"), alias="crossUnPnl")
    available_balance: Decimal | None = Field(default=None, alias="availableBalance")
    withdraw_available: Decimal | None = Field(default=None, alias="withdrawAvailable")
    max_withdraw: Decimal = Field(default=Decimal("0"), alias="maxWithdrawAmount")
    update_time: int = Field(default=0, alias="updateTime")

    @property
    def available(self) -> Decimal:
        if self.available_balance is not None:
            return self.available_balance
        if self.withdraw_available is not None:
            return self.withdraw_available
        return Decimal("0")

    def to_balance(self) -> Balance:
        """``free`` is what can still be committed; ``locked`` is the rest."""
        locked = self.balance - self.available
        return Balance(
            asset=self.asset,
            free=self.available,
            locked=locked if locked > 0 else Decimal("0"),
        )


class BinanceDeliveryPosition(BinanceMessage):
    """One row of ``account.position``.

    ``positionAmt`` is signed and already in contracts. Flat rows are
    kept: that is how the OMS learns to drop a position it was carrying.
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


class BinanceDeliveryOrderUpdate(BinanceMessage):
    """The ``o`` block of ``ORDER_TRADE_UPDATE``.

    ``q`` / ``l`` / ``z`` are contract counts. ``x`` is what happened and
    ``X`` is where the order now stands.
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
        return self.exec_type.upper() == "TRADE" and self.last_qty > 0


class BinanceDeliveryOrderTradeUpdate(BinanceMessage):
    """``ORDER_TRADE_UPDATE`` — the envelope around one order event."""

    e: str = "ORDER_TRADE_UPDATE"
    event_time: int = Field(default=0, alias="E")
    transact_time: int = Field(default=0, alias="T")
    o: BinanceDeliveryOrderUpdate

    @property
    def symbol(self) -> str:
        return self.o.s

    @property
    def client_order_id(self) -> str | None:
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
            price=_opt(self.o.p),
            filled_qty=self.o.filled_qty_total,
            avg_price=_opt(self.o.average_price),
            ts=self.ts,
        )

    def to_fill(self, ticker: UniversalTicker) -> Fill:
        """This update's own execution — ``l``/``L``, never the running totals."""
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


class BinanceDeliveryWalletBalance(BinanceMessage):
    """One asset inside an ``ACCOUNT_UPDATE``'s ``B`` array."""

    a: str
    wallet_balance: Decimal = Field(default=Decimal("0"), alias="wb")
    cross_wallet_balance: Decimal = Field(default=Decimal("0"), alias="cw")
    balance_change: Decimal = Field(default=Decimal("0"), alias="bc")

    def to_balance(self) -> Balance:
        locked = self.wallet_balance - self.cross_wallet_balance
        return Balance(
            asset=self.a,
            free=self.cross_wallet_balance,
            locked=locked if locked > 0 else Decimal("0"),
        )


class BinanceDeliveryPositionRow(BinanceMessage):
    """One position inside an ``ACCOUNT_UPDATE``'s ``P`` array.

    ``pa`` is signed and already in contracts.
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


class BinanceDeliveryAccountUpdate(BinanceMessage):
    """``ACCOUNT_UPDATE`` — the balances and positions one event moved."""

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
            BinanceDeliveryWalletBalance.model_validate(row).to_balance()
            for row in self.a.get("B") or []
        ]

    def position_rows(self) -> list[BinanceDeliveryPositionRow]:
        return [
            BinanceDeliveryPositionRow.model_validate(row)
            for row in self.a.get("P") or []
        ]


class BinanceDeliveryListenKeyExpired(BinanceMessage):
    """``listenKeyExpired`` — this socket has stopped carrying anything."""

    e: str = "listenKeyExpired"
    event_time: int = Field(default=0, alias="E")
    listen_key: str = Field(default="", alias="listenKey")


class BinanceDeliveryMyTrade(BinanceMessage):
    """One row of ``GET /dapi/v1/userTrades`` — an execution, after the fact.

    ``qty`` is a contract count. ``id`` here is the user stream's ``t``, so a
    backfilled fill and a streamed one collapse onto one record.

    ``realizedPnl`` is not carried onto the fill. Binance computes it against
    the account's position basis, not this session's.
    """

    symbol: str = ""
    trade_id: int = Field(default=-1, alias="id")
    order_id: int = Field(default=0, alias="orderId")
    pair: str = ""
    side: VenueSide = Side.BUY
    price: Decimal = Decimal("0")
    qty: Decimal = Decimal("0")
    realized_pnl: Decimal = Field(default=Decimal("0"), alias="realizedPnl")
    margin_asset: str = Field(default="", alias="marginAsset")
    base_qty: Decimal = Field(default=Decimal("0"), alias="baseQty")
    commission: Decimal = Decimal("0")
    commission_asset: str = Field(default="", alias="commissionAsset")
    position_side: str = Field(default=BOTH, alias="positionSide")
    buyer: bool = False
    maker: bool = False
    time: int = 0

    def to_fill(self, ticker: UniversalTicker) -> Fill:
        """This execution as the shared model, ``client_order_id`` unset."""
        return Fill(
            universal_ticker=str(ticker),
            fill_id=str(self.trade_id),
            order_id=str(self.order_id),
            client_order_id=None,
            side=self.side,
            price=self.price,
            qty=self.qty,
            fee=self.commission,
            fee_asset=self.commission_asset,
            ts=secs(self.time),
        )


__all__ = [
    "BOTH",
    "BinanceDeliveryAccountUpdate",
    "BinanceDeliveryAggTrade",
    "BinanceDeliveryBalance",
    "BinanceDeliveryBookTicker",
    "BinanceDeliveryDepth",
    "BinanceDeliveryDepthUpdate",
    "BinanceDeliveryKlineEvent",
    "BinanceDeliveryKlineWindow",
    "BinanceDeliveryListenKeyExpired",
    "BinanceDeliveryLiquidation",
    "BinanceDeliveryLiquidationOrder",
    "BinanceDeliveryMarkPrice",
    "BinanceDeliveryMyTrade",
    "BinanceDeliveryOrderAck",
    "BinanceDeliveryOrderTradeUpdate",
    "BinanceDeliveryOrderUpdate",
    "BinanceDeliveryPosition",
    "BinanceDeliveryPositionRow",
    "BinanceDeliveryTicker",
    "BinanceDeliveryWalletBalance",
    "status_of",
    "type_of",
]
