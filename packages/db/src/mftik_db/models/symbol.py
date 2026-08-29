"""Symbol plane tables — the golden record for venue instruments.

``symbol_ticker`` holds the identity of an instrument: what it is and how the
venue spells it. ``symbol_filter`` holds its trading restrictions, one row per
restriction, because venues expose different sets of them and a fixed column
list would either lose data or be mostly NULL.

Rows are written only by the ``sym`` service, which refreshes them from each
venue's instrument-info endpoint. Everything else reads.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from mftik_db.models.base import Base

#: Venue quantities need more headroom than float; 18dp matches the venues.
QUANTITY = Numeric(38, 18)


class SymbolCategory(StrEnum):
    """Instrument class. Determines which optional ticker columns apply.

    Mirrors ``mftik.exchange.tickers.Category`` — spelling included, since these
    values are the middle part of a stored universal ticker. This package does
    not depend on ``mftik``, so the two are kept in step by a test rather
    than by an import.
    """

    SPOT = "Spot"
    PERP = "Perp"
    INVERSE = "Inverse"
    FUTURE = "Future"
    OPTION = "Option"


class FilterName(StrEnum):
    """Filters seen so far. Not exhaustive — the column is a free string.

    A venue may publish restrictions no other venue has; ``symbol_filter``
    takes them as-is rather than forcing them into a shared shape.
    """

    PRICE_TICK = "price_tick"
    QTY_STEP = "qty_step"
    MIN_QTY = "min_qty"
    MAX_QTY = "max_qty"
    MIN_NOTIONAL = "min_notional"
    MAX_NOTIONAL = "max_notional"
    MIN_PRICE = "min_price"
    MAX_PRICE = "max_price"


class SymbolTicker(Base):
    """One tradeable instrument, anywhere.

    Identity is the universal ticker — ``Gate_Spot_BTCUSDT`` — which already
    carries venue, category and canonical symbol. They are not also stored as
    columns: three fields that must agree with a fourth is three chances for
    them not to, and the parse that recovers them is a string split.

    Filtering by venue or by venue+category is a prefix match, which
    ``ix_symbol_ticker_prefix`` serves — hence ``text_pattern_ops`` on
    PostgreSQL, without which the default collation makes ``LIKE 'Gate\\_%'``
    a sequential scan over every listed instrument.
    """

    __tablename__ = "symbol_ticker"
    __table_args__ = (
        UniqueConstraint("universal_ticker", name="uq_symbol_ticker_identity"),
        Index(
            "ix_symbol_ticker_prefix",
            "universal_ticker",
            postgresql_ops={"universal_ticker": "text_pattern_ops"},
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    #: ``<Venue>_<Category>_<SYMBOL>`` — see ``mftik.exchange.tickers``.
    universal_ticker: Mapped[str] = mapped_column(String(96))

    base: Mapped[str] = mapped_column(String(32))
    quote: Mapped[str] = mapped_column(String(32))
    #: The venue's own spelling (``BTC_USDT``) — what goes on the wire.
    exch_ticker: Mapped[str] = mapped_column(String(64))

    # Contract-only; NULL for spot.
    contract_size: Mapped[Decimal | None] = mapped_column(QUANTITY, nullable=True)
    settlement_asset: Mapped[str | None] = mapped_column(
        String(32), nullable=True
    )
    #: Delivery contracts only; NULL for spot and perps.
    expiry: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    #: False once the venue stops listing it. Rows are never deleted — orders
    #: and sessions reference instruments that may since have been delisted.
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    filters = relationship(
        "SymbolFilter",
        back_populates="ticker",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class SymbolFilter(Base):
    """One trading restriction on one instrument.

    ``value`` is nullable because venues do publish a filter with no bound
    (Gate's ``min_quote_amount`` can be null) — that is different from not
    publishing the filter at all, and the distinction matters to a strategy
    deciding whether it needs to check.
    """

    __tablename__ = "symbol_filter"
    __table_args__ = (
        UniqueConstraint("ticker_id", "name", name="uq_symbol_filter_name"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ticker_id: Mapped[int] = mapped_column(
        ForeignKey("symbol_ticker.id", ondelete="CASCADE"), index=True
    )
    name: Mapped[str] = mapped_column(String(32))
    value: Mapped[Decimal | None] = mapped_column(QUANTITY, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    ticker = relationship("SymbolTicker", back_populates="filters")
