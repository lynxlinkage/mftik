"""add venue to apis

Revision ID: 0003_apis_venue
Revises: 0002_apis_audits
Create Date: 2026-08-01

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0003_apis_venue"
down_revision: Union[str, Sequence[str], None] = "0002_apis_audits"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("apis", sa.Column("venue", sa.String(length=64), nullable=False))
    op.create_index(op.f("ix_apis_venue"), "apis", ["venue"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_apis_venue"), table_name="apis")
    op.drop_column("apis", "venue")
