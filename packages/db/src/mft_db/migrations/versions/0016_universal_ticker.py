"""universal tickers — Venue_Category_SYMBOL replaces venue/symbol/category

Folding the category into the venue name (``gate_spot``) only describes a
classic-account exchange, where each market has its own credential and its own
endpoints. A unified account — Bybit — signs once for spot and perps both, so
the category is a property of the instrument, not of the venue. See
``mft.exchange.tickers``.

This is a clean break. Venue names become CamelCase (``gate_spot`` → ``Gate``,
``paper`` → ``Paper``), categories become capitalized (``spot`` → ``Spot``),
``symbol_ticker`` collapses its three identity columns into one, and md feed
keys turn inside out (``paper.orderbook.BTCUSDT`` → ``orderbook.Paper_Spot_
BTCUSDT``). Every persisted spelling is rewritten here; nothing accepts the
old form afterwards.

Revision ID: 0016_universal_ticker
Revises: 0015_sts_restart
Create Date: 2026-08-08

"""

import json
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0016_universal_ticker"
down_revision: Union[str, Sequence[str], None] = "0015_sts_restart"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

#: Old venue name → new. The reverse is unambiguous, so ``downgrade`` inverts
#: it rather than keeping a second table in step.
VENUES = {"gate_spot": "Gate", "paper": "Paper"}
CATEGORIES = {"spot": "Spot", "perp": "Perp", "future": "Future", "option": "Option"}


def upgrade() -> None:
    bind = op.get_bind()

    # --- symbol_ticker: three identity columns become one ------------------
    op.add_column(
        "symbol_ticker",
        sa.Column("universal_ticker", sa.String(length=96), nullable=True),
    )
    for old, new in VENUES.items():
        for old_cat, new_cat in CATEGORIES.items():
            bind.execute(
                sa.text(
                    "UPDATE symbol_ticker "
                    "SET universal_ticker = :prefix || symbol "
                    "WHERE venue = :old AND category = :old_cat"
                ),
                {"prefix": f"{new}_{new_cat}_", "old": old, "old_cat": old_cat},
            )
    # Anything the table above did not cover is a venue this build has never
    # heard of. Deleting it is safe and dropping it is not: the column is about
    # to go NOT NULL, and the rows are a refreshable cache of venue listings.
    bind.execute(
        sa.text(
            "DELETE FROM symbol_filter WHERE ticker_id IN "
            "(SELECT id FROM symbol_ticker WHERE universal_ticker IS NULL)"
        )
    )
    bind.execute(sa.text("DELETE FROM symbol_ticker WHERE universal_ticker IS NULL"))

    op.drop_constraint(
        "uq_symbol_ticker_identity", "symbol_ticker", type_="unique"
    )
    op.drop_index("ix_symbol_ticker_venue", table_name="symbol_ticker")
    op.drop_index("ix_symbol_ticker_symbol", table_name="symbol_ticker")
    op.drop_column("symbol_ticker", "venue")
    op.drop_column("symbol_ticker", "symbol")
    op.drop_column("symbol_ticker", "category")
    op.alter_column(
        "symbol_ticker",
        "universal_ticker",
        existing_type=sa.String(length=96),
        nullable=False,
    )
    op.create_unique_constraint(
        "uq_symbol_ticker_identity", "symbol_ticker", ["universal_ticker"]
    )
    # text_pattern_ops so `LIKE 'Gate\_%'` is an index range scan. Under the
    # default collation PostgreSQL cannot use a plain btree for a prefix LIKE,
    # and listing one venue would scan every instrument on every venue.
    op.create_index(
        "ix_symbol_ticker_prefix",
        "symbol_ticker",
        ["universal_ticker"],
        postgresql_ops={"universal_ticker": "text_pattern_ops"},
    )

    # --- venue names wherever else they are stored -------------------------
    for old, new in VENUES.items():
        bind.execute(
            sa.text("UPDATE apis SET venue = :new WHERE venue = :old"),
            {"new": new, "old": old},
        )
        bind.execute(
            sa.text("UPDATE md_sessions SET venue = :new WHERE venue = :old"),
            {"new": new, "old": old},
        )

    _rewrite_md_ids(bind, _feed_to_universal)


def downgrade() -> None:
    bind = op.get_bind()
    reverse = {new: old for old, new in VENUES.items()}
    reverse_cat = {new: old for old, new in CATEGORIES.items()}

    op.add_column(
        "symbol_ticker", sa.Column("venue", sa.String(length=64), nullable=True)
    )
    op.add_column(
        "symbol_ticker", sa.Column("symbol", sa.String(length=64), nullable=True)
    )
    op.add_column(
        "symbol_ticker",
        sa.Column("category", sa.String(length=16), server_default="spot", nullable=True),
    )
    rows = bind.execute(
        sa.text("SELECT id, universal_ticker FROM symbol_ticker")
    ).fetchall()
    for row_id, ticker in rows:
        parts = (ticker or "").split("_")
        if len(parts) != 3:
            continue
        venue, category, symbol = parts
        bind.execute(
            sa.text(
                "UPDATE symbol_ticker SET venue = :venue, category = :category, "
                "symbol = :symbol WHERE id = :id"
            ),
            {
                "venue": reverse.get(venue, venue),
                "category": reverse_cat.get(category, category.lower()),
                "symbol": symbol,
                "id": row_id,
            },
        )
    bind.execute(
        sa.text(
            "DELETE FROM symbol_filter WHERE ticker_id IN "
            "(SELECT id FROM symbol_ticker WHERE venue IS NULL)"
        )
    )
    bind.execute(sa.text("DELETE FROM symbol_ticker WHERE venue IS NULL"))

    op.drop_index("ix_symbol_ticker_prefix", table_name="symbol_ticker")
    op.drop_constraint(
        "uq_symbol_ticker_identity", "symbol_ticker", type_="unique"
    )
    op.drop_column("symbol_ticker", "universal_ticker")
    for column in ("venue", "symbol", "category"):
        op.alter_column("symbol_ticker", column, nullable=False)
    op.create_index("ix_symbol_ticker_venue", "symbol_ticker", ["venue"])
    op.create_index("ix_symbol_ticker_symbol", "symbol_ticker", ["symbol"])
    op.create_unique_constraint(
        "uq_symbol_ticker_identity",
        "symbol_ticker",
        ["venue", "symbol", "category"],
    )

    for new, old in reverse.items():
        bind.execute(
            sa.text("UPDATE apis SET venue = :old WHERE venue = :new"),
            {"new": new, "old": old},
        )
        bind.execute(
            sa.text("UPDATE md_sessions SET venue = :old WHERE venue = :new"),
            {"new": new, "old": old},
        )

    _rewrite_md_ids(bind, _feed_to_legacy)


def _rewrite_md_ids(bind, convert) -> None:
    """Rewrite every feed key in ``sts_sessions.md_ids``, element by element.

    The column is JSON, so this reads and writes whole documents rather than
    doing it in SQL — there are as many rows as there have been deployments,
    and a broken feed key silently subscribes to nothing.
    """
    # Keyed by ``session_id``: this table's primary key is the session id, not
    # a surrogate ``id``.
    rows = bind.execute(
        sa.text("SELECT session_id, md_ids FROM sts_sessions")
    ).fetchall()
    for session_id, md_ids in rows:
        feeds = md_ids if isinstance(md_ids, list) else json.loads(md_ids or "[]")
        if not feeds:
            continue
        rewritten = [convert(str(feed)) for feed in feeds]
        if rewritten == feeds:
            continue
        bind.execute(
            sa.text(
                "UPDATE sts_sessions SET md_ids = :md_ids "
                "WHERE session_id = :session_id"
            ),
            {"md_ids": json.dumps(rewritten), "session_id": session_id},
        )


def _feed_to_universal(feed: str) -> str:
    """``paper.orderbook.BTCUSDT`` → ``orderbook.Paper_Spot_BTCUSDT``.

    A feed key that does not have three parts is left as it is: rejecting it
    would fail the migration over a row whose session finished months ago, and
    the parser downstream will refuse it just as clearly if it is ever reused.
    """
    parts = feed.split(".", 2)
    if len(parts) != 3:
        return feed
    venue, topic, symbol = parts
    return f"{topic}.{VENUES.get(venue, venue)}_Spot_{symbol.upper()}"


def _feed_to_legacy(feed: str) -> str:
    """``orderbook.Paper_Spot_BTCUSDT`` → ``paper.orderbook.BTCUSDT``."""
    topic, separator, rest = feed.partition(".")
    parts = rest.split("_")
    if not separator or len(parts) != 3:
        return feed
    reverse = {new: old for old, new in VENUES.items()}
    venue, _category, symbol = parts
    return f"{reverse.get(venue, venue)}.{topic}.{symbol}"
