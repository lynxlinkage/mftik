"""Fold strategies into sts_sessions

Revision ID: 0024_fold_strategies
Revises: 0023_auth_identities
Create Date: 2026-08-18

``strategies`` was a 1:1 sidecar of ``sts_sessions``, written in a second
transaction after deploy succeeded. Attach failures therefore left a session
with no strategy row, and the strategies list — driven by that table — hid
them. Folding the two unique columns onto the session removes the class of
orphan rather than catching it.

``type`` and ``yaml_text`` move. ``config``, ``created_by``, ``created_at``
and ``sts_session`` were duplicates of session columns and drop with the
table. The surrogate ``strategies.id`` cannot be reconstructed: nothing else
records it. A downgrade recreates the table and refills from the session
rows, but the ids will be new, so links built to
``/sts/strategies/{id}/yaml`` will not come back pointing at the same deploys.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0024_fold_strategies"
down_revision: Union[str, Sequence[str], None] = "0023_auth_identities"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "sts_sessions", sa.Column("type", sa.String(length=128), nullable=True)
    )
    op.add_column("sts_sessions", sa.Column("yaml_text", sa.Text(), nullable=True))
    op.create_index("ix_sts_sessions_type", "sts_sessions", ["type"])

    conn = op.get_bind()
    # Refuse rather than guess. Dropping ``config`` is only safe while it
    # says the same thing as st_paras, and this is the last moment anyone
    # can find out that it did not.
    drifted = conn.execute(
        sa.text(
            """
            SELECT count(*) FROM strategies st
            JOIN sts_sessions s ON s.session_id = st.sts_session
            WHERE st.config::jsonb IS DISTINCT FROM s.st_paras::jsonb
            """
        )
    ).scalar_one()
    if drifted:
        raise RuntimeError(
            f"{drifted} strategies.config rows disagree with sts_sessions."
            "st_paras — resolve before dropping the column"
        )

    conn.execute(
        sa.text(
            """
            UPDATE sts_sessions AS s
               SET "type" = st."type", yaml_text = st.yaml_text
              FROM strategies AS st
             WHERE st.sts_session = s.session_id
            """
        )
    )

    # Two production rows were stopped by attach-rollback but recorded as a
    # clean operator stop. The code path that wrote that is fixed alongside
    # this revision; this rewrites the rows it already produced.
    conn.execute(
        sa.text(
            """
            UPDATE sts_sessions
               SET status = 'failed',
                   reason = 'attach failed — rolled back during deploy'
             WHERE session_id IN (
                 '95c1aeeab64641508499a1e7bd3828bf',
                 '20d3877f124b4ad5b678b4e967055cd7'
             )
               AND status = 'done'
            """
        )
    )

    op.drop_index("ix_strategies_sts_session", table_name="strategies")
    op.drop_index("ix_strategies_created_by", table_name="strategies")
    op.drop_index("ix_strategies_type", table_name="strategies")
    op.drop_table("strategies")


def downgrade() -> None:
    op.create_table(
        "strategies",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("type", sa.String(length=128), nullable=False),
        sa.Column("yaml_text", sa.Text(), nullable=True),
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
    )
    op.create_index("ix_strategies_type", "strategies", ["type"], unique=False)
    op.create_index(
        "ix_strategies_created_by", "strategies", ["created_by"], unique=False
    )
    op.create_index(
        "ix_strategies_sts_session", "strategies", ["sts_session"], unique=True
    )

    op.get_bind().execute(
        sa.text(
            """
            INSERT INTO strategies (
                "type", yaml_text, config, created_by, created_at, sts_session
            )
            SELECT s."type", s.yaml_text, s.st_paras,
                   s.created_by, s.created_at, s.session_id
              FROM sts_sessions s
             WHERE s."type" IS NOT NULL
            """
        )
    )

    op.drop_index("ix_sts_sessions_type", table_name="sts_sessions")
    op.drop_column("sts_sessions", "yaml_text")
    op.drop_column("sts_sessions", "type")
