"""sts_sessions.restart / rebuild_count — whether a run wants to come back

Revision ID: 0015_sts_restart
Revises: 0014_sts_rebuild
Create Date: 2026-08-05

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0015_sts_restart"
down_revision: Union[str, Sequence[str], None] = "0014_sts_rebuild"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Existing rows default to always, which changes nothing for them: they
    # are all finished, and a rebuild would refuse them on age anyway.
    op.add_column(
        "sts_sessions",
        sa.Column(
            "restart",
            sa.String(length=8),
            nullable=False,
            server_default="always",
        ),
    )
    op.add_column(
        "sts_sessions",
        sa.Column(
            "rebuild_count", sa.Integer(), nullable=False, server_default="0"
        ),
    )


def downgrade() -> None:
    op.drop_column("sts_sessions", "rebuild_count")
    op.drop_column("sts_sessions", "restart")
