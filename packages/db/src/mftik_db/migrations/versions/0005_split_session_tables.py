"""split sessions into sts_sessions / td_sessions / md_sessions

Revision ID: 0005_split_sessions
Revises: 0004_sessions
Create Date: 2026-08-01

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0005_split_sessions"
down_revision: Union[str, Sequence[str], None] = "0004_sessions"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "sts_sessions",
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
        sa.Column("strategy", sa.String(length=128), nullable=True),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("session_id"),
    )
    op.create_index(
        op.f("ix_sts_sessions_created_by"), "sts_sessions", ["created_by"], unique=False
    )
    op.create_index(
        op.f("ix_sts_sessions_status"), "sts_sessions", ["status"], unique=False
    )

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
    op.create_index(
        op.f("ix_td_sessions_created_by"), "td_sessions", ["created_by"], unique=False
    )
    op.create_index(
        op.f("ix_td_sessions_status"), "td_sessions", ["status"], unique=False
    )
    op.create_index(op.f("ix_td_sessions_api_id"), "td_sessions", ["api_id"], unique=False)
    op.create_index(
        op.f("ix_td_sessions_sts_session_id"),
        "td_sessions",
        ["sts_session_id"],
        unique=False,
    )

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
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("session_id"),
    )
    op.create_index(
        op.f("ix_md_sessions_created_by"), "md_sessions", ["created_by"], unique=False
    )
    op.create_index(
        op.f("ix_md_sessions_status"), "md_sessions", ["status"], unique=False
    )

    # Move existing unified rows into domain tables (best-effort).
    conn = op.get_bind()
    conn.execute(
        sa.text(
            """
            INSERT INTO sts_sessions
                (session_id, created_by, created_at, finished_at, status, strategy)
            SELECT session_id, created_by, created_at, finished_at, status, NULL
            FROM sessions
            WHERE domain = 'sts'
            """
        )
    )
    conn.execute(
        sa.text(
            """
            INSERT INTO td_sessions
                (session_id, created_by, created_at, finished_at, status,
                 api_id, sts_session_id)
            SELECT session_id, created_by, created_at, finished_at, status,
                   0, ''
            FROM sessions
            WHERE domain = 'td'
            """
        )
    )
    conn.execute(
        sa.text(
            """
            INSERT INTO md_sessions
                (session_id, created_by, created_at, finished_at, status)
            SELECT session_id, created_by, created_at, finished_at, status
            FROM sessions
            WHERE domain = 'md'
            """
        )
    )

    op.drop_index(op.f("ix_sessions_status"), table_name="sessions")
    op.drop_index(op.f("ix_sessions_created_by"), table_name="sessions")
    op.drop_index(op.f("ix_sessions_domain"), table_name="sessions")
    op.drop_table("sessions")


def downgrade() -> None:
    op.create_table(
        "sessions",
        sa.Column("session_id", sa.String(length=64), nullable=False),
        sa.Column("domain", sa.String(length=16), nullable=False),
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
        sa.PrimaryKeyConstraint("session_id"),
    )
    op.create_index(op.f("ix_sessions_domain"), "sessions", ["domain"], unique=False)
    op.create_index(
        op.f("ix_sessions_created_by"), "sessions", ["created_by"], unique=False
    )
    op.create_index(op.f("ix_sessions_status"), "sessions", ["status"], unique=False)

    conn = op.get_bind()
    conn.execute(
        sa.text(
            """
            INSERT INTO sessions
                (session_id, domain, created_by, created_at, finished_at, status)
            SELECT session_id, 'sts', created_by, created_at, finished_at, status
            FROM sts_sessions
            """
        )
    )
    conn.execute(
        sa.text(
            """
            INSERT INTO sessions
                (session_id, domain, created_by, created_at, finished_at, status)
            SELECT session_id, 'td', created_by, created_at, finished_at, status
            FROM td_sessions
            """
        )
    )
    conn.execute(
        sa.text(
            """
            INSERT INTO sessions
                (session_id, domain, created_by, created_at, finished_at, status)
            SELECT session_id, 'md', created_by, created_at, finished_at, status
            FROM md_sessions
            """
        )
    )

    op.drop_index(op.f("ix_md_sessions_status"), table_name="md_sessions")
    op.drop_index(op.f("ix_md_sessions_created_by"), table_name="md_sessions")
    op.drop_table("md_sessions")

    op.drop_index(op.f("ix_td_sessions_sts_session_id"), table_name="td_sessions")
    op.drop_index(op.f("ix_td_sessions_api_id"), table_name="td_sessions")
    op.drop_index(op.f("ix_td_sessions_status"), table_name="td_sessions")
    op.drop_index(op.f("ix_td_sessions_created_by"), table_name="td_sessions")
    op.drop_table("td_sessions")

    op.drop_index(op.f("ix_sts_sessions_status"), table_name="sts_sessions")
    op.drop_index(op.f("ix_sts_sessions_created_by"), table_name="sts_sessions")
    op.drop_table("sts_sessions")
