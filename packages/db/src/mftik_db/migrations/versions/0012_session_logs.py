"""session_logs — persisted STS/TD/MD log lines

Revision ID: 0012_session_logs
Revises: 0011_symbol_plane
Create Date: 2026-08-03

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0012_session_logs"
down_revision: Union[str, Sequence[str], None] = "0011_symbol_plane"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "session_logs",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("envelope_id", sa.String(length=64), nullable=False),
        sa.Column("domain", sa.String(length=8), nullable=False),
        sa.Column("stream_id", sa.String(length=128), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("level", sa.String(length=16), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("ts", sa.Float(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("envelope_id", name="uq_session_logs_envelope_id"),
    )
    op.create_index(
        "ix_session_logs_domain_stream_ts_id",
        "session_logs",
        ["domain", "stream_id", "ts", "id"],
    )


def downgrade() -> None:
    op.drop_index("ix_session_logs_domain_stream_ts_id", table_name="session_logs")
    op.drop_table("session_logs")
