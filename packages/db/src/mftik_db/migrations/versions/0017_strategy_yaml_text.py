"""strategies.yaml_text — keep the document the operator actually submitted

Revision ID: 0017_strategy_yaml
Revises: 0016_universal_ticker
Create Date: 2026-08-09

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0017_strategy_yaml"
down_revision: Union[str, Sequence[str], None] = "0016_universal_ticker"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Nullable, and left null for existing rows on purpose: their original
    # text was never kept, and back-filling a reconstruction here would make
    # a derived document indistinguishable from one a person wrote. Null is
    # the honest record of "not stored", and the read path falls back to
    # reconstructing it.
    op.add_column(
        "strategies",
        sa.Column("yaml_text", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("strategies", "yaml_text")
