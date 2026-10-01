"""sts_sessions.abort_target — a create abort that outlives the API process

Revision ID: 0035_sts_abort_target
Revises: 0034_strategy_type_key
Create Date: 2026-10-01

A create whose reply missed the API timeout is killed by a retry loop in
that process. The loop is gone when the process is. The target stays on
the live row so the next boot sends ``abort_start`` again. Null everywhere
else, including every row that already ended.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0035_sts_abort_target"
down_revision: str | Sequence[str] | None = "0034_strategy_type_key"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "sts_sessions",
        sa.Column("abort_target", sa.String(length=64), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("sts_sessions", "abort_target")
