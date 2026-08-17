"""sessions table

Revision ID: 0004_sessions
Revises: 0003_apis_venue
Create Date: 2026-08-01

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0004_sessions"
down_revision: Union[str, Sequence[str], None] = "0003_apis_venue"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
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


def downgrade() -> None:
    op.drop_index(op.f("ix_sessions_status"), table_name="sessions")
    op.drop_index(op.f("ix_sessions_created_by"), table_name="sessions")
    op.drop_index(op.f("ix_sessions_domain"), table_name="sessions")
    op.drop_table("sessions")
