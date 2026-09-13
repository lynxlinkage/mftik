"""symbol_ticker strike and option_type — option identity as columns.

Revision ID: 0033_option_strike
Revises: 0032_drop_cid_slot
Create Date: 2026-09-13

The option's strike and C/P already live in ``universal_ticker``
(``Deribit_Option_BTCUSD-260913-70000-C``). They are also attributes
of the instrument, the way ``expiry`` is, so they get their own columns
rather than being recoverable only by splitting the ticker.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0033_option_strike"
down_revision: Union[str, Sequence[str], None] = "0032_drop_cid_slot"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

QUANTITY = sa.Numeric(38, 18)


def upgrade() -> None:
    op.add_column(
        "symbol_ticker",
        sa.Column("strike", QUANTITY, nullable=True),
    )
    op.add_column(
        "symbol_ticker",
        sa.Column("option_type", sa.String(length=1), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("symbol_ticker", "option_type")
    op.drop_column("symbol_ticker", "strike")
