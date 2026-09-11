"""Global cid_slot sequence for new STS sessions.

Revision ID: 0032_cid_slot_seq
Revises: 0031_plane_instances
Create Date: 2026-09-11

``sts_sessions.cid_slot`` is already the durable home. Allocation used to
go through a JetStream KV counter so two STS processes could not mint the
same slot. This table is that counter next to the row it lands on.

One row, incremented on each create, then ``% 65536``. Rebuild keeps
reading the session row and never comes here.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0032_cid_slot_seq"
down_revision: Union[str, Sequence[str], None] = "0031_plane_instances"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "cid_slot_seq",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("value", sa.Integer(), nullable=False, server_default="0"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.execute(sa.text("INSERT INTO cid_slot_seq (id, value) VALUES (1, 0)"))


def downgrade() -> None:
    op.drop_table("cid_slot_seq")
