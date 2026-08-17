"""sts_sessions.reason — why a session ended (carries the failed reason)

Revision ID: 0013_sts_reason
Revises: 0012_session_logs
Create Date: 2026-08-04

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0013_sts_reason"
down_revision: Union[str, Sequence[str], None] = "0012_session_logs"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Nullable on purpose: live sessions and natural exits have no reason, and
    # every row that predates the ``failed`` status ended without one.
    op.add_column(
        "sts_sessions",
        sa.Column("reason", sa.String(length=256), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("sts_sessions", "reason")
