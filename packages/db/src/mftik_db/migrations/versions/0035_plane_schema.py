"""Spec/Status columns, intents, standing subscriptions, selector state

Revision ID: 0035_plane_schema
Revises: 0034_strategy_type_key
Create Date: 2026-10-01

Additive only. Drops — ``rebuild_count``, ``st_facts``, and the day
``md_sessions`` / ``td_sessions`` stop being written — stay with B10-01.
``strategy_digest`` and ``env_generation`` stay with IF-16 (F39). No
registry table and no artifact table (F40): those live on host disk.

``restart`` widens from 8 to 16 so ``on_failure`` fits (F11). Existing
values are left alone, including a historical ``always``. The server
default becomes ``never``, which is what a new row means when nobody
sets the column. ``restart_count`` is a new counter at 0. It is not a
rename of ``rebuild_count`` and it does not copy that column: rebuild
counted a mechanism RM-01 already removed.

``sts_sessions.generation`` backfills to 1 (§8.1: a new spec starts
there; a row already stored is generation 1 of itself).
``observed_generation`` and ``worker_incarnation`` stay null.
``apis.cancel_on_disconnect`` backfills to false (F37).

Sqlite has no ``ALTER COLUMN`` for a type change, so the widen goes
through batch mode there. Postgres uses a plain ``ALTER`` — batch mode
on a table this wide rewrites it, and the rewrite is what used to make
``alembic check`` disagree about constraint names.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0035_plane_schema"
down_revision: Union[str, Sequence[str], None] = "0034_strategy_type_key"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _widen_restart(to_length: int, default: str, from_length: int) -> None:
    kwargs = {
        "existing_type": sa.String(length=from_length),
        "type_": sa.String(length=to_length),
        "existing_nullable": False,
        "server_default": default,
    }
    if op.get_bind().dialect.name == "sqlite":
        with op.batch_alter_table("sts_sessions") as batch:
            batch.alter_column("restart", **kwargs)
    else:
        op.alter_column("sts_sessions", "restart", **kwargs)


def _drop_sts_columns() -> None:
    columns = (
        "generation",
        "observed_generation",
        "worker_incarnation",
        "conditions",
        "restart_count",
    )
    if op.get_bind().dialect.name == "sqlite":
        with op.batch_alter_table("sts_sessions") as batch:
            for name in columns:
                batch.drop_column(name)
        return
    for name in columns:
        op.drop_column("sts_sessions", name)


def upgrade() -> None:
    op.add_column(
        "sts_sessions",
        sa.Column("generation", sa.Integer(), nullable=False, server_default="1"),
    )
    op.add_column(
        "sts_sessions",
        sa.Column("observed_generation", sa.Integer(), nullable=True),
    )
    op.add_column(
        "sts_sessions",
        sa.Column("worker_incarnation", sa.Integer(), nullable=True),
    )
    op.add_column(
        "sts_sessions",
        sa.Column(
            "conditions", sa.JSON(), nullable=False, server_default="{}"
        ),
    )
    op.add_column(
        "sts_sessions",
        sa.Column(
            "restart_count", sa.Integer(), nullable=False, server_default="0"
        ),
    )
    _widen_restart(to_length=16, default="never", from_length=8)

    op.add_column(
        "apis",
        sa.Column(
            "cancel_on_disconnect",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )

    op.create_table(
        "md_intents",
        sa.Column("session_id", sa.String(length=64), nullable=False),
        sa.Column("instance", sa.String(length=64), nullable=False),
        sa.Column("feeds", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("atoms", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column(
            "generation", sa.Integer(), nullable=False, server_default="1"
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("released_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("session_id", "instance"),
    )
    op.create_index(
        "ix_md_intents_instance", "md_intents", ["instance"], unique=False
    )

    op.create_table(
        "td_intents",
        sa.Column("session_id", sa.String(length=64), nullable=False),
        sa.Column("api_id", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("released_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("session_id", "api_id"),
    )
    op.create_index(
        "ix_td_intents_api_id", "td_intents", ["api_id"], unique=False
    )

    op.create_table(
        "md_standing_subscriptions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("instance", sa.String(length=64), nullable=False),
        sa.Column("declaration", sa.JSON(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_md_standing_subscriptions_instance",
        "md_standing_subscriptions",
        ["instance"],
        unique=False,
    )

    op.create_table(
        "md_selector_state",
        sa.Column("spec_hash", sa.String(length=128), nullable=False),
        sa.Column("universe", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("epoch", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("center", sa.JSON(), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("spec_hash"),
    )


def downgrade() -> None:
    op.drop_table("md_selector_state")

    op.drop_index(
        "ix_md_standing_subscriptions_instance",
        table_name="md_standing_subscriptions",
    )
    op.drop_table("md_standing_subscriptions")

    op.drop_index("ix_td_intents_api_id", table_name="td_intents")
    op.drop_table("td_intents")

    op.drop_index("ix_md_intents_instance", table_name="md_intents")
    op.drop_table("md_intents")

    op.drop_column("apis", "cancel_on_disconnect")
    _widen_restart(to_length=8, default="always", from_length=16)
    _drop_sts_columns()
