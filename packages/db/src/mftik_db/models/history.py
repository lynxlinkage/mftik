"""Trading history — the accounting record, kept apart from the control plane.

The session tables next door say what is *running*. These say what *happened*,
and they are the only place it survives: TD's OMS drops an order the moment it
goes terminal, and a fill is announced on a pub/sub channel that keeps nothing.
Neither is a defect — an order book is not an archive — but PnL cannot be
derived from state that has already been discarded.

Four tables, and the split between them is the point:

* ``orders`` — one row per order, carrying **who placed it**. Written when the
  order is submitted, which is the only moment both the ``client_order_id`` and
  the ``session_id`` are in the same hand. Everything downstream reads
  attribution from here rather than decoding it.
* ``fills`` — one row per execution, append-mostly.
* ``cash_flows`` — money that moved for reasons no fill reports: funding,
  transfers, rebates. Account-level, deliberately not attributed to a session;
  see :class:`CashFlowRow`.
* ``backfill_cursors`` — how far the venue's own history has been re-read, per
  stream. This is what separates a settled record from a provisional one.

**Two tiers of confidence.** A row written from the live stream is timely and
may be incomplete — a fee that settles later, a fill lost while TD was down. A
row confirmed by re-reading the venue's history is authoritative. Which tier a
row is in is not stored on it: it is derived, ``ts <= confirmed_through_ts`` for
its stream. Storing it would mean rewriting a batch of rows every time the
watermark moves, when the watermark is one row.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Float,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from mftik_db.models.base import Base

# SQLite only autoincrements INTEGER PRIMARY KEY; Postgres keeps BIGINT.
_PK = BigInteger().with_variant(Integer, "sqlite")

#: Venue quantities need more headroom than float; 18dp matches the venues.
#: Same figure the symbol plane stores its filters at.
AMOUNT = Numeric(38, 18)


class Attribution:
    """How an order's ``session_id`` was arrived at.

    Recorded rather than inferred at read time, because the three cases are not
    equally trustworthy and a reader that cannot tell them apart will average
    them together.
    """

    #: Written at submit, from the request itself. The only authoritative one.
    DIRECT = "direct"
    #: Recovered later by decoding the ``cid_slot`` and matching a session's
    #: lifetime. Sound for history predating this table, a guess after a slot
    #: has wrapped and been reused.
    INFERRED = "inferred"
    #: Not ours. Placed by hand, by another tool, or before the account was
    #: attached. ``session_id`` is null and must stay that way.
    EXTERNAL = "external"


class Source:
    """Which tier a row's *current values* came from."""

    STREAM = "stream"
    BACKFILL = "backfill"


class Stream:
    """A venue history walk with its own cursor.

    Separate because they paginate independently — trades resume from a trade
    id, orders from an order id, cash flows from the account's own sequence —
    and because they settle at different rates, so one lagging does not have to
    hold back the confidence of the others.
    """

    TRADES = "trades"
    ORDERS = "orders"
    CASH_FLOWS = "cash_flows"


class OrderRow(Base):
    """One order, in whatever state it last reached."""

    __tablename__ = "orders"
    __table_args__ = (
        UniqueConstraint("api_id", "order_key", name="uq_orders_api_order_key"),
        Index("ix_orders_session_ts", "session_id", "ts"),
        Index("ix_orders_api_ts", "api_id", "ts"),
        Index("ix_orders_ticker_ts", "universal_ticker", "ts"),
    )

    id: Mapped[int] = mapped_column(_PK, primary_key=True, autoincrement=True)
    api_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    #: The ``client_order_id`` when there is one, else the venue's order id —
    #: the same rule ``mftik_td.oms.oms.order_key`` keys the live book by, so a
    #: row can be found from either side by one expression. An order we minted
    #: is addressable from before the venue has given it an id at all, which is
    #: why this cannot simply be the venue's.
    order_key: Mapped[str] = mapped_column(String(64), nullable=False)
    client_order_id: Mapped[str | None] = mapped_column(
        String(64), nullable=True, index=True
    )
    venue_order_id: Mapped[str | None] = mapped_column(String(64), nullable=True)

    #: Null for anything not ours. See :class:`Attribution`.
    session_id: Mapped[str | None] = mapped_column(
        String(64), nullable=True, index=True
    )
    #: Denormalized off ``sts_sessions`` so listing a period's orders does not
    #: join for a label.
    strategy: Mapped[str | None] = mapped_column(String(128), nullable=True)
    #: The 16-bit slot packed into ``client_order_id``, decoded once on the way
    #: in. Kept so a later pass can re-derive attribution without re-parsing,
    #: and so a slot collision is visible rather than silent.
    cid_slot: Mapped[int | None] = mapped_column(Integer, nullable=True)
    attribution: Mapped[str] = mapped_column(
        String(8), nullable=False, default=Attribution.DIRECT
    )

    universal_ticker: Mapped[str] = mapped_column(String(64), nullable=False)
    side: Mapped[str] = mapped_column(String(8), nullable=False)
    order_type: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    qty: Mapped[object] = mapped_column(AMOUNT, nullable=False)
    price: Mapped[object | None] = mapped_column(AMOUNT, nullable=True)
    filled_qty: Mapped[object] = mapped_column(AMOUNT, nullable=False, default=0)
    avg_price: Mapped[object | None] = mapped_column(AMOUNT, nullable=True)

    reject_code: Mapped[str | None] = mapped_column(String(32), nullable=True)
    reject_reason: Mapped[str | None] = mapped_column(String(256), nullable=True)

    #: When we asked. Null on an order discovered by backfill, which is exactly
    #: what distinguishes one — we were never there for its submit.
    submitted_at: Mapped[float | None] = mapped_column(Float(), nullable=True)
    #: Its last state change, and what every listing orders by.
    ts: Mapped[float] = mapped_column(Float(), nullable=False)
    source: Mapped[str] = mapped_column(
        String(8), nullable=False, default=Source.STREAM
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class FillRow(Base):
    """One execution.

    ``fill_id`` is the venue's own trade id, and the uniqueness key carries the
    instrument with it: on Binance a trade id is per-symbol, so two books would
    collide on the number alone.

    ``session_id`` is denormalized off the order rather than joined, because
    every PnL query scans fills by session and the join would be on the hottest
    path in the system. It is also not always derivable at write time: Bybit
    and Gate put our link id on the trade row, but neither Binance market does,
    so a backfilled Binance fill is attributed from ``orders`` afterwards — and
    must never be overwritten with the null a later pass would bring.
    """

    __tablename__ = "fills"
    __table_args__ = (
        UniqueConstraint(
            "api_id", "universal_ticker", "fill_id", name="uq_fills_api_ticker_fill"
        ),
        Index("ix_fills_session_ts_id", "session_id", "ts", "id"),
        Index("ix_fills_api_ts", "api_id", "ts"),
        Index("ix_fills_ticker_ts", "universal_ticker", "ts"),
    )

    id: Mapped[int] = mapped_column(_PK, primary_key=True, autoincrement=True)
    api_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    fill_id: Mapped[str] = mapped_column(String(64), nullable=False)
    universal_ticker: Mapped[str] = mapped_column(String(64), nullable=False)

    venue_order_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    client_order_id: Mapped[str | None] = mapped_column(
        String(64), nullable=True, index=True
    )
    session_id: Mapped[str | None] = mapped_column(
        String(64), nullable=True, index=True
    )

    side: Mapped[str] = mapped_column(String(8), nullable=False)
    price: Mapped[object] = mapped_column(AMOUNT, nullable=False)
    qty: Mapped[object] = mapped_column(AMOUNT, nullable=False)
    fee: Mapped[object] = mapped_column(AMOUNT, nullable=False, default=0)
    #: Native, never converted on the way in: a conversion done here would
    #: freeze one exchange rate into the record permanently.
    fee_asset: Mapped[str] = mapped_column(String(32), nullable=False, default="")
    #: What the venue itself booked, where it says. Perp venues do; spot has no
    #: such concept and leaves this null, its PnL being derived by cost
    #: matching instead.
    realized_pnl: Mapped[object | None] = mapped_column(AMOUNT, nullable=True)
    is_maker: Mapped[bool | None] = mapped_column(Boolean(), nullable=True)

    ts: Mapped[float] = mapped_column(Float(), nullable=False)
    source: Mapped[str] = mapped_column(
        String(8), nullable=False, default=Source.STREAM
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class CashFlowRow(Base):
    """Money that moved without a fill to explain it.

    Funding, wallet transfers, rebates, insurance clears — one endpoint's worth
    of them per venue (Binance ``/fapi/v1/income``, Bybit's transaction log),
    normalized onto one ``kind`` vocabulary the way order statuses already are.

    **Deliberately has no ``session_id``.** Funding is charged on the account's
    *net* position while sessions hold *gross* ones, so two sessions hedging
    each other pay nothing between them and any per-session split would invent
    numbers that do not sum back. Session PnL is therefore trading PnL and
    excludes this table; account PnL includes it. Transfers are the reason this
    matters beyond funding: without them, no PnL figure can be checked against
    the balance it should explain, because a deposit looks exactly like profit.
    """

    __tablename__ = "cash_flows"
    __table_args__ = (
        UniqueConstraint(
            "api_id", "venue_id", "kind", name="uq_cash_flows_api_venue_kind"
        ),
        Index("ix_cash_flows_api_ts", "api_id", "ts"),
    )

    id: Mapped[int] = mapped_column(_PK, primary_key=True, autoincrement=True)
    api_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    #: The venue's own id for the movement. Paired with ``kind`` because at
    #: least one venue reuses a transaction id across the legs of one transfer.
    venue_id: Mapped[str] = mapped_column(String(64), nullable=False)
    #: Ours, not the venue's spelling — ``funding``, ``transfer``, ``rebate``…
    kind: Mapped[str] = mapped_column(String(24), nullable=False, index=True)
    #: Funding names an instrument; a wallet transfer does not.
    universal_ticker: Mapped[str | None] = mapped_column(String(64), nullable=True)
    asset: Mapped[str] = mapped_column(String(32), nullable=False)
    #: Signed. Paying funding is negative, receiving it is positive.
    amount: Mapped[object] = mapped_column(AMOUNT, nullable=False)
    ts: Mapped[float] = mapped_column(Float(), nullable=False)
    source: Mapped[str] = mapped_column(
        String(8), nullable=False, default=Source.BACKFILL
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class BackfillCursorRow(Base):
    """How far one venue history walk has been confirmed.

    Keyed by ``(api_id, stream, scope)``. ``scope`` is the instrument for the
    per-symbol walks — most venues require a symbol on trade and order history
    — and empty for the account-wide ones, which cash flows are.

    ``confirmed_through_ts`` is the settlement line: at or before it, the venue
    has been re-read and agrees; after it, the record is whatever the live
    stream managed to catch. It is never advanced to *now* — a venue's history
    endpoint can lag its own stream, and confirming a window that later gains a
    row would make the guarantee a lie.
    """

    __tablename__ = "backfill_cursors"

    api_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    stream: Mapped[str] = mapped_column(String(16), primary_key=True)
    #: Universal ticker, or ``""`` for a walk that covers the whole account.
    scope: Mapped[str] = mapped_column(String(64), primary_key=True, default="")
    #: Where the next page resumes: a trade id, an order id, whatever the
    #: venue paginates by. Opaque here on purpose — only the adapter that wrote
    #: it knows how to read it.
    #:
    #: Unbounded text, because a length limit is a guess about a value this
    #: column has promised not to understand. Bybit's is a URL-encoded pair of
    #: order ids and timestamps and runs past a hundred characters; Postgres
    #: raises on a too-long value rather than truncating, and the raise would
    #: land in the middle of a walk.
    last_id: Mapped[str | None] = mapped_column(Text(), nullable=True)
    confirmed_through_ts: Mapped[float] = mapped_column(
        Float(), nullable=False, default=0.0
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


__all__ = [
    "AMOUNT",
    "Attribution",
    "BackfillCursorRow",
    "CashFlowRow",
    "FillRow",
    "OrderRow",
    "Source",
    "Stream",
]
