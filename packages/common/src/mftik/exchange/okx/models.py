"""OKX v5 wire models — account pushes, market pushes, and call replies.

These mirror OKX's wire format field-for-field and keep its ``BTC-USDT`` /
``BTC-USDT-SWAP`` spelling. Where a shared ``mftik.exchange`` model is a
faithful fit there is a ``to_*`` converter; where OKX has no equivalent (book
updates) there deliberately is none, because flattening those loses the
sequencing that makes them usable.

Readings worth knowing before trusting the converters:

* **Everything numeric is a string, and "not applicable" is the empty
  string.** ``avgPx`` is ``""`` on an order that has not traded, so every
  number here goes through a validator that reads ``""`` as unset rather
  than raising.
* **Fills are their own channel.** The ``orders`` push carries a running
  ``accFillSz``; the fee for *this* match is on ``fills``.
* **Sides are lowercase ``buy``/``sell``.** No maker flag to invert.
* **A SWAP ``pos`` is signed in net mode** and unsigned in hedge mode, with
  the direction in ``posSide``. :meth:`OkxPosition.signed_size` hides that.
* **Spot market buys size in quote unless ``tgtCcy`` says otherwise.** The
  same trap as Bybit's ``marketUnit``.
* **A refusal can arrive as a cancellation.** OKX does not reject a
  post-only order that would take liquidity; it accepts it, kills it, and
  says so only in ``cancelSource``. See :data:`CANCEL_REFUSALS`.
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
    "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
    "FILLED": OrderStatus.FILLED,
    "CANCELED": OrderStatus.CANCELED,
    "MMP_CANCELED": OrderStatus.CANCELED,
    "ORDER_FAILED": OrderStatus.REJECTED,
    "FAILED": OrderStatus.REJECTED,
    "EFFECTIVE": OrderStatus.NEW,
}

_TYPE: dict[str, OrderType] = {
    "MARKET": OrderType.MARKET,
    "LIMIT": OrderType.LIMIT,
    "POST_ONLY": OrderType.LIMIT,
    "FOK": OrderType.LIMIT,
    "IOC": OrderType.LIMIT,
    "OPTIMAL_LIMIT_IOC": OrderType.MARKET,
    "MMP": OrderType.LIMIT,
    "MMP_AND_POST_ONLY": OrderType.LIMIT,
}

#: ``cancelSource`` values where OKX *refused* the order, and the words its
#: docs give them. Public, unlike its neighbours, because ``mftik_td.errors``
#: has to hold a matching entry for every line of it.
#:
#: OKX has no reject for a post-only that would take liquidity. It accepts the
#: request, kills the order, and pushes ``state=canceled`` — the same row, the
#: same terminal state, as a cancel the strategy asked for itself. This integer
#: is the only thing that separates them, so without reading it a refusal is
#: invisible: no reject reaches a strategy and nothing says the order was
#: turned down.
#:
#: **Only refusals belong here**, because everything in this table becomes a
#: reject. Every other source ends an order the venue had already accepted —
#: ``1`` is the user's own cancel, ``20`` is cancel-all-after, ``13`` and ``14``
#: are FOK and IOC doing exactly what those instructions ask, ``32`` is
#: self-trade prevention, which is a cancel on every venue here — and reading
#: any of them as a refusal would invent a rejection that never happened.
#:
#: The values are OKX's own wording, carried because the wire is not: the push
#: has the number and no text at all (``cancelSourceReason`` exists on the REST
#: order, never on the ``orders`` channel), and ``31`` alone is nothing a human
#: reads in a log. ``mftik_td.errors`` keys its OKX table on these words, so a
#: source added here needs an entry there or its reject lands as an unmapped
#: code.
CANCEL_REFUSALS: dict[str, str] = {
    "31": "The post-only order will take liquidity in taker orders",
}

_CATEGORY_OF: dict[str, Category] = {
    "SPOT": Category.SPOT,
    "MARGIN": Category.SPOT,
    "SWAP": Category.PERP,
    "FUTURES": Category.FUTURE,
    "OPTION": Category.OPTION,
}


def category_of(value: str | None, default: Category) -> Category:
    """The market a row came from, or ``default`` where it names none."""
    return _CATEGORY_OF.get((value or "").upper(), default)


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


def contracts_to_base(size: Decimal, contract_size: Decimal) -> Decimal:
    return size * contract_size


def base_to_contracts(qty: Decimal, contract_size: Decimal) -> Decimal:
    if contract_size <= 0:
        raise ValueError(f"contract_size must be positive, got {contract_size}")
    return qty / contract_size


def _in_base(size: Decimal, contract_size: Decimal | None) -> Decimal:
    if contract_size is None:
        return size
    return contracts_to_base(size, contract_size)


def _levels(
    rows: Any, contract_size: Decimal | None = None
) -> list[BookLevel]:
    out: list[BookLevel] = []
    for row in rows or []:
        if len(row) < 2:
            continue
        try:
            out.append(
                BookLevel(
                    price=Decimal(str(row[0])),
                    qty=_in_base(Decimal(str(row[1])), contract_size),
                )
            )
        except (InvalidOperation, ValueError):
            continue
    return out


class OkxMessage(BaseModel):
    """Base for wire models: tolerant of new fields, immutable once parsed."""

    model_config = ConfigDict(frozen=True, populate_by_name=True, extra="ignore")


# --- private channels ------------------------------------------------------


class OkxOrderUpdate(OkxMessage):
    """One row of the ``orders`` channel — and of the pending/history REST."""

    inst_type: str = Field(default="", alias="instType")
    inst_id: str = Field(default="", alias="instId")
    ord_id: str = Field(default="", alias="ordId")
    cl_ord_id: str = Field(default="", alias="clOrdId")
    side: VenueSide = Side.BUY
    ord_type: str = Field(default="limit", alias="ordType")
    state: str = ""
    cancel_source: str = Field(default="", alias="cancelSource")
    px: Dec = Decimal("0")
    sz: Dec = Decimal("0")
    tgt_ccy: str = Field(default="", alias="tgtCcy")
    acc_fill_sz: Dec = Field(default=Decimal("0"), alias="accFillSz")
    fill_sz: Dec = Field(default=Decimal("0"), alias="fillSz")
    avg_px_raw: OptDec = Field(default=None, alias="avgPx")
    fee: Dec = Decimal("0")
    fee_ccy: str = Field(default="", alias="feeCcy")
    reduce_only: str = Field(default="", alias="reduceOnly")
    td_mode: str = Field(default="", alias="tdMode")
    c_time: Ms = Field(default=0.0, alias="cTime")
    u_time: Ms = Field(default=0.0, alias="uTime")

    @property
    def symbol(self) -> str:
        return self.inst_id

    @property
    def client_order_id(self) -> str | None:
        return self.cl_ord_id or None

    @property
    def status(self) -> OrderStatus:
        """The shared lifecycle state, reading a refusal as one.

        A cancellation OKX made because it refused the order is a reject
        everywhere else on this platform — Bybit says ``Rejected``, Binance and
        Gate raise — so it is one here too. Left as ``CANCELED`` it would be
        indistinguishable from the strategy's own cancel, which is the whole
        complaint: the same hook, the same terminal state, nothing to branch
        on.
        """
        status = status_of(self.state)
        if status is OrderStatus.CANCELED and self.refusal:
            return OrderStatus.REJECTED
        return status

    @property
    def refusal(self) -> str:
        """Why OKX refused this order, or ``""`` where it did not.

        The words are OKX's, looked up from the ``cancelSource`` it sent; the
        number rides along in front of them because it is what the wire
        actually carried and what OKX's own docs are indexed by. See
        :data:`CANCEL_REFUSALS` for which sources count as a refusal.

        **An order that traded is never a refusal**, whatever the source says.
        A reject supersedes the order update rather than accompanying it, so
        reading a partial fill as one would lose the fill: the strategy would
        hear that the order was turned down and never that some of it went
        through.
        """
        if self.acc_fill_sz > 0:
            return ""
        source = self.cancel_source.strip()
        words = CANCEL_REFUSALS.get(source)
        return f"cancelSource={source} {words}" if words else ""

    @property
    def type(self) -> OrderType:
        return type_of(self.ord_type)

    @property
    def quote_sized(self) -> bool:
        return self.tgt_ccy.lower() == "quote_ccy"

    @property
    def avg_price(self) -> Decimal | None:
        if self.avg_px_raw is not None and self.avg_px_raw > 0:
            return self.avg_px_raw
        return None

    @property
    def ts(self) -> float:
        return self.u_time or self.c_time

    def to_order(
        self, ticker: UniversalTicker, *, contract_size: Decimal | None = None
    ) -> Order:
        filled = _in_base(self.acc_fill_sz, contract_size)
        qty = filled if self.quote_sized else _in_base(self.sz, contract_size)
        return Order(
            universal_ticker=str(ticker),
            order_id=self.ord_id,
            client_order_id=self.client_order_id,
            side=self.side,
            type=self.type,
            status=self.status,
            qty=qty,
            quote_qty=self.sz if self.quote_sized else None,
            price=self.px or None,
            filled_qty=filled,
            avg_price=self.avg_price,
            # OKX refuses on this channel rather than at the call, and says
            # which refusal it was in ``cancelSource`` and nowhere else.
            reject_reason=self.refusal,
            ts=self.ts,
        )


class OkxFill(OkxMessage):
    """One row of the ``fills`` channel — and of ``GET /api/v5/trade/fills``."""

    inst_type: str = Field(default="", alias="instType")
    inst_id: str = Field(default="", alias="instId")
    trade_id: str = Field(default="", alias="tradeId")
    ord_id: str = Field(default="", alias="ordId")
    cl_ord_id: str = Field(default="", alias="clOrdId")
    side: VenueSide = Side.BUY
    fill_px: Dec = Field(default=Decimal("0"), alias="fillPx")
    fill_sz: Dec = Field(default=Decimal("0"), alias="fillSz")
    fill_fee: Dec = Field(default=Decimal("0"), alias="fillFee")
    fill_fee_ccy: str = Field(default="", alias="fillFeeCcy")
    exec_type: str = Field(default="", alias="execType")
    bill_id: str = Field(default="", alias="billId")
    ts: Ms = 0.0

    @property
    def symbol(self) -> str:
        return self.inst_id

    @property
    def client_order_id(self) -> str | None:
        return self.cl_ord_id or None

    @property
    def is_fill(self) -> bool:
        return self.fill_sz > 0

    def to_fill(
        self, ticker: UniversalTicker, *, contract_size: Decimal | None = None
    ) -> Fill:
        fee = self.fill_fee
        if fee < 0:
            fee = -fee
        return Fill(
            universal_ticker=str(ticker),
            fill_id=self.trade_id,
            order_id=self.ord_id,
            client_order_id=self.client_order_id,
            side=self.side,
            price=self.fill_px,
            qty=_in_base(self.fill_sz, contract_size),
            fee=fee,
            fee_asset=self.fill_fee_ccy,
            ts=self.ts,
        )


class OkxBalanceDetail(OkxMessage):
    """One coin inside an ``account`` push or a balance reply."""

    ccy: str = ""
    eq: Dec = Decimal("0")
    cash_bal: Dec = Field(default=Decimal("0"), alias="cashBal")
    avail_bal: OptDec = Field(default=None, alias="availBal")
    avail_eq: OptDec = Field(default=None, alias="availEq")
    frozen_bal: Dec = Field(default=Decimal("0"), alias="frozenBal")

    def to_balance(self) -> Balance:
        """Spendable from ``availEq`` (UTA) falling back to ``availBal``."""
        spendable = self.avail_eq
        if spendable is None:
            spendable = self.avail_bal
        if spendable is None:
            spendable = self.cash_bal - self.frozen_bal
        if spendable < 0:
            spendable = Decimal("0")
        return Balance(asset=self.ccy, free=spendable, locked=self.frozen_bal)


class OkxAccount(OkxMessage):
    """One row of the ``account`` channel — a whole wallet, not a delta."""

    details: list[OkxBalanceDetail] = Field(default_factory=list)

    def to_balances(self) -> list[Balance]:
        return [row.to_balance() for row in self.details if row.ccy]


class OkxPosition(OkxMessage):
    """One row of the ``positions`` channel — and of ``GET .../positions``."""

    inst_type: str = Field(default="", alias="instType")
    inst_id: str = Field(default="", alias="instId")
    pos: Dec = Decimal("0")
    pos_side: str = Field(default="", alias="posSide")
    avg_px: OptDec = Field(default=None, alias="avgPx")
    upl: Dec = Decimal("0")
    lever: OptDec = None
    u_time: Ms = Field(default=0.0, alias="uTime")

    @property
    def symbol(self) -> str:
        return self.inst_id

    @property
    def signed_size(self) -> Decimal:
        """Size with direction: negative when short, zero when flat.

        Net mode already signs ``pos``. Hedge mode reports an unsigned size
        and puts the direction in ``posSide``.
        """
        side = self.pos_side.lower()
        if side == "short":
            return -abs(self.pos)
        if side == "long":
            return abs(self.pos)
        return self.pos

    def to_position(
        self, ticker: UniversalTicker, *, contract_size: Decimal | None = None
    ) -> Position:
        return Position(
            universal_ticker=str(ticker),
            qty=_in_base(self.signed_size, contract_size),
            entry_price=self.avg_px,
            unrealised_pnl=self.upl,
        )


class OkxLeverage(OkxMessage):
    """One row of ``GET /api/v5/account/leverage-info``."""

    inst_id: str = Field(default="", alias="instId")
    mgn_mode: str = Field(default="", alias="mgnMode")
    pos_side: str = Field(default="", alias="posSide")
    lever: OptDec = None


# --- public channels -------------------------------------------------------


class OkxPublicTrade(OkxMessage):
    """One row of ``trades`` — the tape. ``side`` is the aggressor."""

    inst_id: str = Field(default="", alias="instId")
    trade_id: str = Field(default="", alias="tradeId")
    px: Dec = Decimal("0")
    sz: Dec = Decimal("0")
    side: VenueSide = Side.BUY
    ts: Ms = 0.0

    @property
    def symbol(self) -> str:
        return self.inst_id

    def to_trade(
        self, ticker: UniversalTicker, *, contract_size: Decimal | None = None
    ) -> Trade:
        return Trade(
            universal_ticker=str(ticker),
            trade_id=self.trade_id,
            price=self.px,
            qty=_in_base(self.sz, contract_size),
            side=self.side,
            ts=self.ts,
        )


class OkxLiquidationDetail(OkxMessage):
    bk_px: OptDec = Field(default=None, alias="bkPx")
    sz: Dec = Decimal("0")
    side: VenueSide = Side.BUY
    pos_side: str = Field(default="", alias="posSide")
    ts: Ms = 0.0


class OkxLiquidation(OkxMessage):
    """One ``liquidation-orders`` row — a forced close on the contract books."""

    inst_type: str = Field(default="", alias="instType")
    inst_id: str = Field(default="", alias="instId")
    details: list[OkxLiquidationDetail] = Field(default_factory=list)

    @property
    def symbol(self) -> str:
        return self.inst_id

    def to_liquidations(
        self, ticker: UniversalTicker, *, contract_size: Decimal | None = None
    ) -> list[Liquidation]:
        out: list[Liquidation] = []
        for row in self.details:
            if row.sz <= 0:
                continue
            out.append(
                Liquidation(
                    universal_ticker=str(ticker),
                    price=row.bk_px or Decimal("0"),
                    qty=_in_base(row.sz, contract_size),
                    side=row.side,
                    ts=row.ts,
                )
            )
        return out


class OkxFundingRate(OkxMessage):
    """``funding-rate`` — the still-moving prediction for the next settlement.

    ``fundingTime`` / ``nextFundingTime`` / ``nextFundingRate`` stay on the
    wire. The shared model is ``rate`` + ``ts`` only.
    """

    inst_type: str = Field(default="", alias="instType")
    inst_id: str = Field(default="", alias="instId")
    funding_rate: OptDec = Field(default=None, alias="fundingRate")
    next_funding_rate: OptDec = Field(default=None, alias="nextFundingRate")
    funding_time: Ms = Field(default=0.0, alias="fundingTime")
    next_funding_time: Ms = Field(default=0.0, alias="nextFundingTime")
    ts: Ms = 0.0

    @property
    def symbol(self) -> str:
        return self.inst_id

    def to_funding_rate(self, ticker: UniversalTicker) -> FundingRate | None:
        """The still-moving prediction, when this push named one.

        A push without ``ts`` leaves the stamp off so the shared print takes
        its local receive default, rather than reading as the epoch.
        """
        if self.funding_rate is None:
            return None
        fields: dict[str, Any] = {} if self.ts <= 0 else {"ts": self.ts}
        return FundingRate(
            universal_ticker=str(ticker),
            rate=self.funding_rate,
            **fields,
        )


class OkxOpenInterest(OkxMessage):
    """``open-interest`` — current size, REST and the live channel.

    ``oi`` is contracts; ``oiCcy`` is already base. Notionals stay on
    the wire. The shared print is one ``qty`` in base.
    """

    inst_type: str = Field(default="", alias="instType")
    inst_id: str = Field(default="", alias="instId")
    oi: OptDec = None
    oi_ccy: OptDec = Field(default=None, alias="oiCcy")
    ts: Ms = 0.0

    @property
    def symbol(self) -> str:
        return self.inst_id

    def to_open_interest(
        self,
        ticker: UniversalTicker,
        *,
        contract_size: Decimal | None = None,
    ) -> OpenInterest | None:
        """Base size, preferring ``oiCcy``. Falls back to ``oi * ctVal``.

        A row with neither field, or only contracts and no multiplier,
        is not a print. A missing ``ts`` leaves the stamp off so the
        shared model takes its local receive default.
        """
        qty = self.oi_ccy
        if qty is None:
            if self.oi is None or contract_size is None or contract_size <= 0:
                return None
            qty = self.oi * contract_size
        fields: dict[str, Any] = {} if self.ts <= 0 else {"ts": self.ts}
        return OpenInterest(
            universal_ticker=str(ticker),
            qty=qty,
            **fields,
        )


class OkxTicker(OkxMessage):
    """``tickers``, and one row of ``GET /api/v5/market/ticker``."""

    inst_type: str = Field(default="", alias="instType")
    inst_id: str = Field(default="", alias="instId")
    last: OptDec = None
    bid_px: OptDec = Field(default=None, alias="bidPx")
    bid_sz: OptDec = Field(default=None, alias="bidSz")
    ask_px: OptDec = Field(default=None, alias="askPx")
    ask_sz: OptDec = Field(default=None, alias="askSz")
    high_24h: OptDec = Field(default=None, alias="high24h")
    low_24h: OptDec = Field(default=None, alias="low24h")
    vol_24h: OptDec = Field(default=None, alias="vol24h")
    ts: Ms = 0.0

    @property
    def symbol(self) -> str:
        return self.inst_id

    @property
    def quoted(self) -> bool:
        if self.last is not None:
            return True
        return self.bid_px is not None and self.ask_px is not None

    def to_ticker(self, ticker: UniversalTicker, *, ts: float = 0.0) -> Ticker:
        last = self.last or Decimal("0")
        bid = self.bid_px if self.bid_px else last
        ask = self.ask_px if self.ask_px else last
        fields: dict[str, Any] = {} if ts <= 0 else {"ts": ts}
        if not fields and self.ts:
            fields = {"ts": self.ts}
        return Ticker(
            universal_ticker=str(ticker), bid=bid, ask=ask, last=last, **fields
        )

    def to_best_quote(
        self,
        ticker: UniversalTicker,
        *,
        ts: float = 0.0,
        contract_size: Decimal | None = None,
    ) -> BestQuote | None:
        if not self.bid_px or not self.ask_px or not self.bid_sz or not self.ask_sz:
            return None
        fields: dict[str, Any] = {} if ts <= 0 else {"ts": ts}
        if not fields and self.ts:
            fields = {"ts": self.ts}
        return BestQuote(
            universal_ticker=str(ticker),
            bid=self.bid_px,
            bid_qty=_in_base(self.bid_sz, contract_size),
            ask=self.ask_px,
            ask_qty=_in_base(self.ask_sz, contract_size),
            **fields,
        )


class OkxOrderBook(OkxMessage):
    """One ``books`` / ``bbo-tbt`` payload — a snapshot **or** an update.

    ``seqId`` / ``prevSeqId`` is the gap detector: an update whose
    ``prevSeqId`` is not the last ``seqId`` we applied means a push was
    missed. Folding lives in :class:`~mftik.exchange.okx.feed.OkxBook`.
    """

    inst_id: str = Field(default="", alias="instId")
    asks: list[Any] = Field(default_factory=list)
    bids: list[Any] = Field(default_factory=list)
    ts: Ms = 0.0
    seq_id: int = Field(default=0, alias="seqId")
    prev_seq_id: int = Field(default=-1, alias="prevSeqId")
    checksum: int = 0

    @property
    def symbol(self) -> str:
        return self.inst_id

    def bid_levels(
        self, contract_size: Decimal | None = None
    ) -> list[BookLevel]:
        return _levels(self.bids, contract_size)

    def ask_levels(
        self, contract_size: Decimal | None = None
    ) -> list[BookLevel]:
        return _levels(self.asks, contract_size)

    def to_order_book(
        self,
        ticker: UniversalTicker,
        *,
        ts: float = 0.0,
        contract_size: Decimal | None = None,
    ) -> OrderBook:
        fields: dict[str, Any] = {} if ts <= 0 else {"ts": ts}
        if not fields and self.ts:
            fields = {"ts": self.ts}
        return OrderBook(
            universal_ticker=str(ticker),
            bids=self.bid_levels(contract_size),
            asks=self.ask_levels(contract_size),
            **fields,
        )

    def to_best_quote(
        self,
        ticker: UniversalTicker,
        *,
        ts: float = 0.0,
        contract_size: Decimal | None = None,
    ) -> BestQuote | None:
        bids = self.bid_levels(contract_size)
        asks = self.ask_levels(contract_size)
        if not bids or not asks:
            return None
        fields: dict[str, Any] = {} if ts <= 0 else {"ts": ts}
        if not fields and self.ts:
            fields = {"ts": self.ts}
        return BestQuote(
            universal_ticker=str(ticker),
            bid=bids[0].price,
            bid_qty=bids[0].qty,
            ask=asks[0].price,
            ask_qty=asks[0].qty,
            **fields,
        )


# --- call replies ----------------------------------------------------------


class OkxOrderAck(OkxMessage):
    """The reply to ``POST /trade/order`` and ``/trade/cancel-order``.

    Two ids, an ``sCode``, and nothing about state. The ``orders`` channel
    says what became of it, which is why the connector reports ``PENDING_NEW``
    rather than inventing a status the venue did not report.
    """

    ord_id: str = Field(default="", alias="ordId")
    cl_ord_id: str = Field(default="", alias="clOrdId")
    s_code: str = Field(default="0", alias="sCode")
    s_msg: str = Field(default="", alias="sMsg")
    ts: str = ""

    @property
    def client_order_id(self) -> str | None:
        return self.cl_ord_id or None

    @property
    def ok(self) -> bool:
        return self.s_code in ("", "0")


def kline_from_row(
    row: list[Any],
    ticker: UniversalTicker,
    interval: str,
    *,
    contract_size: Decimal | None = None,
) -> Kline:
    """One row of ``GET /api/v5/market/candles`` — a positional array.

        [0] window start, ms    [4] close
        [1] open                [5] volume (base on spot, contracts on SWAP)
        [2] high                [6] volume, quote on spot / base on SWAP
        [3] low                 [7] volume, quote on SWAP
                                [8] confirm (``0`` in progress, ``1`` closed)

    ``contract_size`` is how the SWAP row is read: ``[5]`` is converted to
    base and ``[7]`` is the quote volume. Spot passes none and keeps
    ``[5]`` / ``[6]``.
    """
    if len(row) < 7:
        raise ValueError(
            f"kline row for {ticker} {interval} has {len(row)} columns, "
            f"expected at least 7: {row!r}"
        )
    closed = True
    if len(row) > 8:
        closed = str(row[8]) == "1"
    volume = Decimal(str(row[5]))
    quote_volume = Decimal(str(row[6]))
    if contract_size is not None:
        volume = contracts_to_base(volume, contract_size)
        if len(row) > 7 and row[7] not in (None, ""):
            quote_volume = Decimal(str(row[7]))
    return Kline(
        universal_ticker=str(ticker),
        interval=interval,
        open_time=float(row[0]) / 1000.0,
        open=Decimal(str(row[1])),
        high=Decimal(str(row[2])),
        low=Decimal(str(row[3])),
        close=Decimal(str(row[4])),
        volume=volume,
        quote_volume=quote_volume,
        closed=closed,
    )


def order_book_from_result(
    result: dict[str, Any],
    ticker: UniversalTicker,
    *,
    contract_size: Decimal | None = None,
) -> OrderBook:
    """``GET /api/v5/market/books`` — a whole book, dated by the venue."""
    return OrderBook(
        universal_ticker=str(ticker),
        bids=_levels(result.get("bids"), contract_size),
        asks=_levels(result.get("asks"), contract_size),
        ts=float(result.get("ts", 0) or 0) / 1000.0,
    )


__all__ = [
    "CANCEL_REFUSALS",
    "Dec",
    "Ms",
    "OkxAccount",
    "OkxBalanceDetail",
    "OkxFill",
    "OkxFundingRate",
    "OkxLeverage",
    "OkxLiquidation",
    "OkxLiquidationDetail",
    "OkxOpenInterest",
    "OkxMessage",
    "OkxOrderAck",
    "OkxOrderBook",
    "OkxOrderUpdate",
    "OkxPosition",
    "OkxPublicTrade",
    "OkxTicker",
    "OptDec",
    "VenueSide",
    "base_to_contracts",
    "category_of",
    "contracts_to_base",
    "kline_from_row",
    "order_book_from_result",
    "status_of",
    "type_of",
]
