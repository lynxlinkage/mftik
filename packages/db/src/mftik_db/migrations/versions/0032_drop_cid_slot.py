"""Drop cid_slot — v1 client_order_id packs the session id itself.

Revision ID: 0032_drop_cid_slot
Revises: 0031_plane_instances
Create Date: 2026-09-11

``sts_sessions.cid_slot`` and ``orders.cid_slot`` were the 16-bit hash
fingerprint used to own fills. v1 encodes the six-hex session id in the
client_order_id, so the columns are leftover.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0032_drop_cid_slot"
down_revision: Union[str, Sequence[str], None] = "0031_plane_instances"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_column("sts_sessions", "cid_slot")
    op.drop_column("orders", "cid_slot")


def downgrade() -> None:
    op.add_column(
        "sts_sessions",
        sa.Column("cid_slot", sa.Integer(), nullable=True),
    )
    op.add_column(
        "orders",
        sa.Column("cid_slot", sa.Integer(), nullable=True),
    )
