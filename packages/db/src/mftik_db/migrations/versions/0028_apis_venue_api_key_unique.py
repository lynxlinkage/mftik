"""apis uniqueness is (venue, api_key), not the key string alone

Revision ID: 0028_apis_venue_key
Revises: 0027_sts_td_mapping
Create Date: 2026-08-31

Binance issues one key for spot, USD-M and COIN-M. Those are three
venues here — different hosts and wallets — so the same key string has
to be storeable once per venue. The old unique index on ``api_key``
refused the second plane. A non-unique index stays so a key can still
be found without a scan.
"""

from typing import Sequence, Union

from alembic import op

revision: str = "0028_apis_venue_key"
down_revision: Union[str, Sequence[str], None] = "0027_sts_td_mapping"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_index("ix_apis_api_key", table_name="apis")
    op.create_index("ix_apis_api_key", "apis", ["api_key"], unique=False)
    op.create_unique_constraint(
        "uq_apis_venue_api_key", "apis", ["venue", "api_key"]
    )


def downgrade() -> None:
    op.drop_constraint("uq_apis_venue_api_key", "apis", type_="unique")
    op.drop_index("ix_apis_api_key", table_name="apis")
    op.create_index("ix_apis_api_key", "apis", ["api_key"], unique=True)
