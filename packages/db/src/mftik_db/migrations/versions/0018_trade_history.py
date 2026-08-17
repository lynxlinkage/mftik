"""orders / fills / cash_flows / backfill_cursors — the accounting record

Revision ID: 0018_trade_history
Revises: 0017_strategy_yaml
Create Date: 2026-08-12

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0018_trade_history"
down_revision: Union[str, Sequence[str], None] = "0017_strategy_yaml"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

#: Venue quantities need more headroom than float; matches the symbol plane.
AMOUNT = sa.Numeric(38, 18)


def upgrade() -> None:
    op.create_table(
        "orders",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("api_id", sa.Integer(), nullable=False),
        sa.Column("order_key", sa.String(length=64), nullable=False),
        sa.Column("client_order_id", sa.String(length=64), nullable=True),
        sa.Column("venue_order_id", sa.String(length=64), nullable=True),
        sa.Column("session_id", sa.String(length=64), nullable=True),
        sa.Column("strategy", sa.String(length=128), nullable=True),
        sa.Column("cid_slot", sa.Integer(), nullable=True),
        sa.Column("attribution", sa.String(length=8), nullable=False),
        sa.Column("universal_ticker", sa.String(length=64), nullable=False),
        sa.Column("side", sa.String(length=8), nullable=False),
        sa.Column("order_type", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("qty", AMOUNT, nullable=False),
        sa.Column("price", AMOUNT, nullable=True),
        sa.Column("filled_qty", AMOUNT, nullable=False),
        sa.Column("avg_price", AMOUNT, nullable=True),
        sa.Column("reject_code", sa.String(length=32), nullable=True),
        sa.Column("reject_reason", sa.String(length=256), nullable=True),
        sa.Column("submitted_at", sa.Float(), nullable=True),
        sa.Column("ts", sa.Float(), nullable=False),
        sa.Column("source", sa.String(length=8), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("api_id", "order_key", name="uq_orders_api_order_key"),
    )
    op.create_index("ix_orders_api_id", "orders", ["api_id"])
    op.create_index("ix_orders_client_order_id", "orders", ["client_order_id"])
    op.create_index("ix_orders_session_id", "orders", ["session_id"])
    op.create_index("ix_orders_status", "orders", ["status"])
    op.create_index("ix_orders_session_ts", "orders", ["session_id", "ts"])
    op.create_index("ix_orders_api_ts", "orders", ["api_id", "ts"])
    op.create_index("ix_orders_ticker_ts", "orders", ["universal_ticker", "ts"])

    op.create_table(
        "fills",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("api_id", sa.Integer(), nullable=False),
        sa.Column("fill_id", sa.String(length=64), nullable=False),
        sa.Column("universal_ticker", sa.String(length=64), nullable=False),
        sa.Column("venue_order_id", sa.String(length=64), nullable=True),
        sa.Column("client_order_id", sa.String(length=64), nullable=True),
        sa.Column("session_id", sa.String(length=64), nullable=True),
        sa.Column("side", sa.String(length=8), nullable=False),
        sa.Column("price", AMOUNT, nullable=False),
        sa.Column("qty", AMOUNT, nullable=False),
        sa.Column("fee", AMOUNT, nullable=False),
        sa.Column("fee_asset", sa.String(length=32), nullable=False),
        sa.Column("realized_pnl", AMOUNT, nullable=True),
        sa.Column("is_maker", sa.Boolean(), nullable=True),
        sa.Column("ts", sa.Float(), nullable=False),
        sa.Column("source", sa.String(length=8), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        # The ticker is part of the key on purpose: a Binance trade id is
        # per-symbol, so two books would collide on the number alone.
        sa.UniqueConstraint(
            "api_id", "universal_ticker", "fill_id", name="uq_fills_api_ticker_fill"
        ),
    )
    op.create_index("ix_fills_api_id", "fills", ["api_id"])
    op.create_index("ix_fills_client_order_id", "fills", ["client_order_id"])
    op.create_index("ix_fills_session_id", "fills", ["session_id"])
    op.create_index("ix_fills_session_ts_id", "fills", ["session_id", "ts", "id"])
    op.create_index("ix_fills_api_ts", "fills", ["api_id", "ts"])
    op.create_index("ix_fills_ticker_ts", "fills", ["universal_ticker", "ts"])

    op.create_table(
        "cash_flows",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("api_id", sa.Integer(), nullable=False),
        sa.Column("venue_id", sa.String(length=64), nullable=False),
        sa.Column("kind", sa.String(length=24), nullable=False),
        sa.Column("universal_ticker", sa.String(length=64), nullable=True),
        sa.Column("asset", sa.String(length=32), nullable=False),
        sa.Column("amount", AMOUNT, nullable=False),
        sa.Column("ts", sa.Float(), nullable=False),
        sa.Column("source", sa.String(length=8), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        # ``kind`` joins the key because at least one venue reuses a
        # transaction id across the legs of a single transfer.
        sa.UniqueConstraint(
            "api_id", "venue_id", "kind", name="uq_cash_flows_api_venue_kind"
        ),
    )
    op.create_index("ix_cash_flows_api_id", "cash_flows", ["api_id"])
    op.create_index("ix_cash_flows_kind", "cash_flows", ["kind"])
    op.create_index("ix_cash_flows_api_ts", "cash_flows", ["api_id", "ts"])

    op.create_table(
        "backfill_cursors",
        sa.Column("api_id", sa.Integer(), nullable=False),
        sa.Column("stream", sa.String(length=16), nullable=False),
        sa.Column("scope", sa.String(length=64), nullable=False),
        # Text, not a bounded string: this holds whatever a venue paginates
        # by and the column has promised not to understand it. Bybit's is a
        # URL-encoded pair of order ids and timestamps, well past 64.
        sa.Column("last_id", sa.Text(), nullable=True),
        sa.Column("confirmed_through_ts", sa.Float(), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("api_id", "stream", "scope"),
    )


def downgrade() -> None:
    op.drop_table("backfill_cursors")

    op.drop_index("ix_cash_flows_api_ts", table_name="cash_flows")
    op.drop_index("ix_cash_flows_kind", table_name="cash_flows")
    op.drop_index("ix_cash_flows_api_id", table_name="cash_flows")
    op.drop_table("cash_flows")

    op.drop_index("ix_fills_ticker_ts", table_name="fills")
    op.drop_index("ix_fills_api_ts", table_name="fills")
    op.drop_index("ix_fills_session_ts_id", table_name="fills")
    op.drop_index("ix_fills_session_id", table_name="fills")
    op.drop_index("ix_fills_client_order_id", table_name="fills")
    op.drop_index("ix_fills_api_id", table_name="fills")
    op.drop_table("fills")

    op.drop_index("ix_orders_ticker_ts", table_name="orders")
    op.drop_index("ix_orders_api_ts", table_name="orders")
    op.drop_index("ix_orders_session_ts", table_name="orders")
    op.drop_index("ix_orders_status", table_name="orders")
    op.drop_index("ix_orders_session_id", table_name="orders")
    op.drop_index("ix_orders_client_order_id", table_name="orders")
    op.drop_index("ix_orders_api_id", table_name="orders")
    op.drop_table("orders")
