"""strategies table for strategy.yml deploys

Revision ID: 0008_strategies
Revises: 0007_md_venue
Create Date: 2026-08-01

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0008_strategies"
down_revision: Union[str, Sequence[str], None] = "0007_md_venue"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "strategies",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("type", sa.String(length=128), nullable=False),
        sa.Column("config", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("created_by", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("sts_session", sa.String(length=64), nullable=False),
        sa.ForeignKeyConstraint(
            ["created_by"],
            ["users.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["sts_session"],
            ["sts_sessions.session_id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("sts_session", name="uq_strategies_sts_session"),
    )
    op.create_index("ix_strategies_type", "strategies", ["type"], unique=False)
    op.create_index(
        "ix_strategies_created_by", "strategies", ["created_by"], unique=False
    )


def downgrade() -> None:
    op.drop_index("ix_strategies_created_by", table_name="strategies")
    op.drop_index("ix_strategies_type", table_name="strategies")
    op.drop_table("strategies")
