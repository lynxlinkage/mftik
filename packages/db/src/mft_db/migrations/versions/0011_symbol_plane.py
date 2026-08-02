"""symbol_ticker / symbol_filter — the symbol plane

Revision ID: 0011_symbol_plane
Revises: 0010_account_name
Create Date: 2026-08-02

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0011_symbol_plane"
down_revision: Union[str, Sequence[str], None] = "0010_account_name"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

QUANTITY = sa.Numeric(38, 18)


def upgrade() -> None:
    op.create_table(
        "symbol_ticker",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("venue", sa.String(length=64), nullable=False),
        sa.Column("symbol", sa.String(length=64), nullable=False),
        sa.Column(
            "category",
            sa.String(length=16),
            server_default="spot",
            nullable=False,
        ),
        sa.Column("base", sa.String(length=32), nullable=False),
        sa.Column("quote", sa.String(length=32), nullable=False),
        sa.Column("exch_ticker", sa.String(length=64), nullable=False),
        sa.Column("contract_size", QUANTITY, nullable=True),
        sa.Column("settlement_asset", sa.String(length=32), nullable=True),
        sa.Column("expiry", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "is_active",
            sa.Boolean(),
            server_default=sa.text("true"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "venue", "symbol", "category", name="uq_symbol_ticker_identity"
        ),
    )
    op.create_index("ix_symbol_ticker_venue", "symbol_ticker", ["venue"])
    op.create_index("ix_symbol_ticker_symbol", "symbol_ticker", ["symbol"])
    op.create_index("ix_symbol_ticker_is_active", "symbol_ticker", ["is_active"])

    op.create_table(
        "symbol_filter",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("ticker_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=32), nullable=False),
        # Nullable: a venue can publish a filter with no bound, which differs
        # from not publishing it at all.
        sa.Column("value", QUANTITY, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["ticker_id"], ["symbol_ticker.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ticker_id", "name", name="uq_symbol_filter_name"),
    )
    op.create_index("ix_symbol_filter_ticker_id", "symbol_filter", ["ticker_id"])


def downgrade() -> None:
    op.drop_index("ix_symbol_filter_ticker_id", table_name="symbol_filter")
    op.drop_table("symbol_filter")
    op.drop_index("ix_symbol_ticker_is_active", table_name="symbol_ticker")
    op.drop_index("ix_symbol_ticker_symbol", table_name="symbol_ticker")
    op.drop_index("ix_symbol_ticker_venue", table_name="symbol_ticker")
    op.drop_table("symbol_ticker")
