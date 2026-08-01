"""sts deploy fields + td_sessions (session_id, api_id) unique

Revision ID: 0006_session_deploy
Revises: 0005_split_sessions
Create Date: 2026-08-01

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0006_session_deploy"
down_revision: Union[str, Sequence[str], None] = "0005_split_sessions"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "sts_sessions",
        sa.Column("td_api_ids", sa.JSON(), nullable=False, server_default="[]"),
    )
    op.add_column(
        "sts_sessions",
        sa.Column("md_ids", sa.JSON(), nullable=False, server_default="[]"),
    )
    op.add_column(
        "sts_sessions",
        sa.Column("st_paras", sa.JSON(), nullable=False, server_default="{}"),
    )

    # Rebuild td_sessions: old PK was session_id alone; now multi-api per session.
    # Create under a temp name so PG index/constraint names from 0005 do not collide
    # (ALTER TABLE RENAME keeps index names on the old relation).
    op.create_table(
        "td_sessions_new",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("session_id", sa.String(length=64), nullable=False),
        sa.Column("api_id", sa.Integer(), nullable=False),
        sa.Column("created_by", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "session_id", "api_id", name="uq_td_sessions_session_api"
        ),
    )
    op.execute(
        """
        INSERT INTO td_sessions_new
            (session_id, api_id, created_by, created_at, finished_at, status)
        SELECT
            session_id,
            api_id,
            created_by,
            created_at,
            finished_at,
            status
        FROM td_sessions
        """
    )
    op.drop_table("td_sessions")
    op.rename_table("td_sessions_new", "td_sessions")
    op.create_index(
        op.f("ix_td_sessions_session_id"), "td_sessions", ["session_id"], unique=False
    )
    op.create_index(
        op.f("ix_td_sessions_api_id"), "td_sessions", ["api_id"], unique=False
    )
    op.create_index(
        op.f("ix_td_sessions_created_by"), "td_sessions", ["created_by"], unique=False
    )
    op.create_index(
        op.f("ix_td_sessions_status"), "td_sessions", ["status"], unique=False
    )


def downgrade() -> None:
    op.rename_table("td_sessions", "td_sessions_new")
    op.create_table(
        "td_sessions",
        sa.Column("session_id", sa.String(length=64), nullable=False),
        sa.Column("created_by", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("api_id", sa.Integer(), nullable=False),
        sa.Column("sts_session_id", sa.String(length=64), nullable=False),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("session_id"),
    )
    op.execute(
        """
        INSERT INTO td_sessions
            (session_id, created_by, created_at, finished_at, status,
             api_id, sts_session_id)
        SELECT
            session_id || ':' || api_id::text,
            created_by,
            created_at,
            finished_at,
            status,
            api_id,
            session_id
        FROM td_sessions_new
        """
    )
    op.drop_table("td_sessions_new")

    op.drop_column("sts_sessions", "st_paras")
    op.drop_column("sts_sessions", "md_ids")
    op.drop_column("sts_sessions", "td_api_ids")
