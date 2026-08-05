"""sts_sessions.cid_slot / st_facts — what a rebuilt session needs to keep

Revision ID: 0014_sts_rebuild
Revises: 0013_sts_reason
Create Date: 2026-08-05

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0014_sts_rebuild"
down_revision: Union[str, Sequence[str], None] = "0013_sts_reason"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Nullable: rows written before this have no recorded slot, and there is
    # no way to recover one — the allocation was process-local. Those sessions
    # simply cannot be rebuilt, which is correct, since they are all finished.
    op.add_column(
        "sts_sessions",
        sa.Column("cid_slot", sa.Integer(), nullable=True),
    )
    op.add_column(
        "sts_sessions",
        sa.Column("st_facts", sa.JSON(), nullable=False, server_default="{}"),
    )


def downgrade() -> None:
    op.drop_column("sts_sessions", "st_facts")
    op.drop_column("sts_sessions", "cid_slot")
