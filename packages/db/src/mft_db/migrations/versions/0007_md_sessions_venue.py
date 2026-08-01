"""recreate md_sessions as (venue, session_id) attaches — history pruned

Revision ID: 0007_md_venue
Revises: 0006_session_deploy
Create Date: 2026-08-01

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0007_md_venue"
down_revision: Union[str, Sequence[str], None] = "0006_session_deploy"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_table("md_sessions")
    op.create_table(
        "md_sessions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("venue", sa.String(length=64), nullable=False),
        sa.Column("session_id", sa.String(length=64), nullable=False),
        sa.Column("created_by", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "status",
            sa.String(length=16),
            nullable=False,
            server_default="live",
        ),
        sa.ForeignKeyConstraint(
            ["created_by"], ["users.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "venue", "session_id", name="uq_md_sessions_venue_session"
        ),
    )
    op.create_index(
        op.f("ix_md_sessions_venue"), "md_sessions", ["venue"], unique=False
    )
    op.create_index(
        op.f("ix_md_sessions_session_id"),
        "md_sessions",
        ["session_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_md_sessions_created_by"),
        "md_sessions",
        ["created_by"],
        unique=False,
    )
    op.create_index(
        op.f("ix_md_sessions_status"), "md_sessions", ["status"], unique=False
    )


def downgrade() -> None:
    op.drop_table("md_sessions")
    op.create_table(
        "md_sessions",
        sa.Column("session_id", sa.String(length=64), nullable=False),
        sa.Column("created_by", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "status",
            sa.String(length=16),
            nullable=False,
            server_default="live",
        ),
        sa.ForeignKeyConstraint(
            ["created_by"], ["users.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("session_id"),
    )
    op.create_index(
        op.f("ix_md_sessions_created_by"),
        "md_sessions",
        ["created_by"],
        unique=False,
    )
    op.create_index(
        op.f("ix_md_sessions_status"), "md_sessions", ["status"], unique=False
    )
