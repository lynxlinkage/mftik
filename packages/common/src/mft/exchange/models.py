"""Exchange domain models shared by all venue adapters."""

from __future__ import annotations

import time
import uuid
from decimal import Decimal
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


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


class TimeInForce(StrEnum):
    """How long an order stays live, and whether it may take liquidity.

    Post-only is a time-in-force here because that is the one place every
    adapter can read it from. Venues disagree on where it lives — Gate spells
    it as the ``poc`` time-in-force, others expose a ``MakerOnly`` /
    ``MakerOrder`` flag beside the type — so the translation belongs in each
    adapter rather than in every strategy that wants to rest passively.

    ``POST_ONLY`` is a promise about *not crossing*, not about duration: an
    order that would take is refused by the venue rather than repriced or
    filled. A strategy asking for it must be ready for that refusal.
    """

    #: Rest until filled or cancelled.
    GTC = "gtc"
    #: Fill what crosses now, cancel the rest.
    IOC = "ioc"
    #: Fill in full now, or not at all.
    FOK = "fok"
    #: Rest as maker; refused outright if it would cross the book.
    POST_ONLY = "post_only"


class OrderStatus(StrEnum):
    """One order's position in its lifecycle.

    Two of these are transient states the *local* side owns, and they are the
    reason this enum is not just a mirror of what the venue reports:

    ``PENDING_NEW`` — handed to the gateway, no venue answer yet. Nothing can
    be cancelled here (there is no venue order id), and a second submit for
    the same intent would be a duplicate.

    ``PENDING_CANCEL`` — a cancel went out, the venue has not confirmed. The
    order may still be resting and may still trade, so risk has to keep
    counting its exposure while refusing to treat it as cancellable capacity.

    ``UNKNOWN`` is the honest answer when the connection drops mid-flight:
    the order may or may not exist at the venue, and only a recovery query
    can say which.
    """

    PENDING_NEW = "pending_new"
    NEW = "new"
    PARTIALLY_FILLED = "partially_filled"
    PENDING_CANCEL = "pending_cancel"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"
    UNKNOWN = "unknown"


#: No transitions out — the order is done and its reservations are released.
TERMINAL_STATUSES: frozenset[OrderStatus] = frozenset(
    {OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED}
)

#: In flight locally: we are waiting on the venue to answer.
PENDING_STATUSES: frozenset[OrderStatus] = frozenset(
    {OrderStatus.PENDING_NEW, OrderStatus.PENDING_CANCEL}
)

#: Known to be resting at the venue and able to trade.
WORKING_STATUSES: frozenset[OrderStatus] = frozenset(
    {OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED}
)

#: Holding exposure: anything not finished, including the states we are unsure
#: about. Reservations stay in place for all of these.
OPEN_STATUSES: frozenset[OrderStatus] = (
    PENDING_STATUSES | WORKING_STATUSES | {OrderStatus.UNKNOWN}
)

_S = OrderStatus

#: Legal successor states. Beyond the venue-driven path this also allows the
#: two shortcuts a real venue takes but a diagram usually omits: an order that
#: fills completely on arrival never passes through PARTIALLY_FILLED, and a
#: recovery query can find an UNKNOWN order still working rather than finished.
_TRANSITIONS: dict[OrderStatus, frozenset[OrderStatus]] = {
    _S.PENDING_NEW: frozenset(
        {_S.NEW, _S.PARTIALLY_FILLED, _S.FILLED, _S.REJECTED, _S.UNKNOWN}
    ),
    _S.NEW: frozenset(
        {
            _S.PARTIALLY_FILLED,
            _S.FILLED,
            _S.PENDING_CANCEL,
            _S.CANCELED,
            _S.REJECTED,
            _S.UNKNOWN,
        }
    ),
    _S.PARTIALLY_FILLED: frozenset(
        {
            _S.PARTIALLY_FILLED,
            _S.FILLED,
            _S.PENDING_CANCEL,
            _S.CANCELED,
            _S.UNKNOWN,
        }
    ),
    # A cancel can lose the race: the order fills, or the venue refuses the
    # cancel and the order goes back to resting.
    _S.PENDING_CANCEL: frozenset(
        {
            _S.CANCELED,
            _S.FILLED,
            _S.PARTIALLY_FILLED,
            _S.NEW,
            _S.UNKNOWN,
        }
    ),
    _S.UNKNOWN: frozenset(
        {
            _S.NEW,
            _S.PARTIALLY_FILLED,
            _S.FILLED,
            _S.CANCELED,
            _S.REJECTED,
        }
    ),
    _S.FILLED: frozenset(),
    _S.CANCELED: frozenset(),
    _S.REJECTED: frozenset(),
}


def is_terminal(status: OrderStatus) -> bool:
    return status in TERMINAL_STATUSES


def is_pending(status: OrderStatus) -> bool:
    """Waiting on the venue to answer a submit or a cancel."""
    return status in PENDING_STATUSES


def is_working(status: OrderStatus) -> bool:
    """Confirmed resting at the venue and able to trade."""
    return status in WORKING_STATUSES


def is_open(status: OrderStatus) -> bool:
    """Not finished — still holding exposure and reservations."""
    return status in OPEN_STATUSES


def can_transition(current: OrderStatus, nxt: OrderStatus) -> bool:
    """Whether ``current → nxt`` is a move this lifecycle allows."""
    return nxt in _TRANSITIONS[current]


def next_statuses(current: OrderStatus) -> frozenset[OrderStatus]:
    return _TRANSITIONS[current]


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


class AggTrade(Trade):
    """Several matches at one price, reported as one print.

    A venue that publishes this coalesces every fill a single aggressing order
    took at a single price into one message. The tape's information is all
    still here — same price, same total quantity, same taker side — at a
    fraction of the message rate, which is why a strategy that only reads
    price and size should prefer this feed to ``trade``.

    What it adds is the part that *cannot* be recovered from the raw tape
    without counting: ``first_trade_id``/``last_trade_id`` bound the matches
    this print consumed, so :attr:`match_count` says whether 10 BTC lifted one
    resting order or swept forty. Those are different events for anything
    reading order flow, and a plain :class:`Trade` has nowhere to say which.

    A subclass rather than a sibling because it genuinely is one — every field
    a ``Trade`` promises means the same thing here — so a strategy that does
    not care about the aggregation can pass one wherever a ``Trade`` goes.
    ``trade_id`` is the aggregate's own id, not any constituent match's.

    Not every venue has the concept: Binance publishes ``@aggTrade``, Gate has
    no equivalent, and a venue without one simply serves no ``aggtrade`` feed.
    """

    model_config = ConfigDict(frozen=True)

    #: Venue id of the first match folded into this print.
    first_trade_id: str = ""
    #: Venue id of the last. Equal to ``first_trade_id`` for a single match.
    last_trade_id: str = ""

    @property
    def match_count(self) -> int:
        """How many matches this print folded together, or 0 if unreported.

        Venues number trades sequentially per instrument, so the count is the
        span of the id range. Zero means the venue sent no range rather than
        that nothing traded — the quantity says what traded.
        """
        if not self.first_trade_id or not self.last_trade_id:
            return 0
        try:
            return int(self.last_trade_id) - int(self.first_trade_id) + 1
        except ValueError:
            # A venue whose trade ids are not integers cannot be counted this
            # way; the ids still identify the range for anyone who can.
            return 0


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
    """One asset's balance, as the venue reports it plus what we reserved.

    ``free`` and ``locked`` are the venue's numbers. ``prelock`` is ours: funds
    committed to an order that has been sent but not yet acknowledged, so the
    venue still counts them as free. Without it, two orders placed in the same
    round-trip would each see the full balance and together overspend it.

    ``available`` is therefore what a strategy may actually still commit.
    """

    model_config = ConfigDict(frozen=True)

    asset: str
    free: Decimal
    locked: Decimal = Decimal("0")
    prelock: Decimal = Decimal("0")

    @property
    def total(self) -> Decimal:
        return self.free + self.locked

    @property
    def available(self) -> Decimal:
        """Spendable right now — venue-free minus our own reservations.

        Clamped at zero: a venue snapshot can land before the fill that
        justified a prelock, which would otherwise read as negative money.
        """
        spendable = self.free - self.prelock
        return spendable if spendable > 0 else Decimal("0")

    @property
    def held(self) -> Decimal:
        """Everything committed: the venue's hold plus ours.

        Deliberately not called ``locked`` — that name is the venue's own
        number, and a property of the same name would shadow the field it is
        built from.
        """
        return self.locked + self.prelock


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
    ``account`` / ``iceberg`` / ``stp_act``, for instance. Each adapter reads
    the keys it understands and ignores the rest, so a request stays portable
    even when it carries hints for one venue.

    ``tif`` is *not* one of those: it is a common field precisely because
    every venue has the concept under some spelling. A raw
    ``params["time_in_force"]`` still wins where an adapter supports it, for
    venue-only values this enum has no name for.
    """

    model_config = ConfigDict(frozen=True)

    symbol: str
    side: Side
    type: OrderType
    qty: Decimal
    price: Decimal | None = None
    client_order_id: str | None = None
    tif: TimeInForce | None = None
    params: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _post_only_must_rest(self) -> PlaceOrderRequest:
        # A market order exists to take; post-only exists to never take. No
        # venue can honour both, so catch it here rather than let each
        # adapter discover it as a rejection.
        if self.tif is TimeInForce.POST_ONLY and self.type is not OrderType.LIMIT:
            raise ValueError(
                f"{TimeInForce.POST_ONLY} requires a limit order, "
                f"got {self.type}"
            )
        return self


def market_order(
    *,
    symbol: str,
    side: Side,
    qty: Decimal,
    client_order_id: str | None = None,
    **extra: Any,
) -> PlaceOrderRequest:
    """A market :class:`PlaceOrderRequest`, spelled once.

    A builder rather than a method on a venue client. Filling in ``type`` is
    the same job at every venue, so it is nobody's implementation — and a
    connector that had to carry it would be carrying a shared interface's worth
    of convenience it never chose.
    """
    return PlaceOrderRequest(
        symbol=symbol,
        side=side,
        type=OrderType.MARKET,
        qty=qty,
        client_order_id=client_order_id,
        **extra,
    )


def limit_order(
    *,
    symbol: str,
    side: Side,
    qty: Decimal,
    price: Decimal,
    client_order_id: str | None = None,
    **extra: Any,
) -> PlaceOrderRequest:
    """A limit :class:`PlaceOrderRequest` — see :func:`market_order`."""
    return PlaceOrderRequest(
        symbol=symbol,
        side=side,
        type=OrderType.LIMIT,
        qty=qty,
        price=price,
        client_order_id=client_order_id,
        **extra,
    )
