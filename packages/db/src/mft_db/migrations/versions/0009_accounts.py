"""accounts table — 1-1 with apis

Revision ID: 0009_accounts
Revises: 0008_strategies
Create Date: 2026-08-01

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0009_accounts"
down_revision: Union[str, Sequence[str], None] = "0008_strategies"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "accounts",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("api_id", sa.Integer(), nullable=False),
        sa.Column("created_by", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["api_id"], ["apis.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["created_by"], ["users.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("api_id", name="uq_accounts_api_id"),
    )
    op.create_index(
        "ix_accounts_created_by", "accounts", ["created_by"], unique=False
    )

    # Backfill one account per existing api (name = venue/api_key).
    op.execute(
        sa.text(
            """
            INSERT INTO accounts (name, api_id, created_by, created_at)
            SELECT
                a.venue || '/' || a.api_key,
                a.id,
                a.owner_id,
                a.created_at
            FROM apis a
            WHERE NOT EXISTS (
                SELECT 1 FROM accounts c WHERE c.api_id = a.id
            )
            """
        )
    )


def downgrade() -> None:
    op.drop_index("ix_accounts_created_by", table_name="accounts")
    op.drop_table("accounts")
