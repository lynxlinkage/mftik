"""apis uniqueness is (venue, api_key), not the key string alone

Revision ID: 0028_apis_venue_key
Revises: 0027_sts_td_mapping
Create Date: 2026-08-31

Binance issues one key for spot, USD-M and COIN-M. Those are three
venues here — different hosts and wallets — so the same key string has
to be storeable once per venue. The old unique index on ``api_key``
refused the second plane. A non-unique index stays so a key can still
be found without a scan.

The constraint compares ``venue`` exactly, so any row left in a
non-canonical spelling by older code would slip past it. Canonicalize
those first — the registry spellings as of this revision, frozen here the
way 0016 froze its own mapping.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0028_apis_venue_key"
down_revision: Union[str, Sequence[str], None] = "0027_sts_td_mapping"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Snapshot of mftik.exchange.venues.names() at this revision. A migration
# must not drift with the registry, so it is copied rather than imported —
# and packages/db does not depend on packages/common.
CANONICAL_VENUES = (
    "Binance",
    "BinanceDelivery",
    "BinanceFuture",
    "Bybit",
    "Gate",
    "GateFutures",
    "Okx",
    "Paper",
)


def upgrade() -> None:
    # Runs before the old unique index on api_key is dropped, so no two rows
    # share a key yet and folding a spelling cannot collide. The NOT EXISTS
    # guard keeps that true for a database that somehow already has both.
    bind = op.get_bind()
    for canonical in CANONICAL_VENUES:
        bind.execute(
            sa.text(
                "UPDATE apis AS a SET venue = :canonical "
                "WHERE lower(a.venue) = lower(:canonical) "
                "AND a.venue <> :canonical "
                "AND NOT EXISTS ("
                "  SELECT 1 FROM apis AS b "
                "  WHERE b.venue = :canonical AND b.api_key = a.api_key"
                ")"
            ),
            {"canonical": canonical},
        )

    op.drop_index("ix_apis_api_key", table_name="apis")
    op.create_index("ix_apis_api_key", "apis", ["api_key"], unique=False)
    op.create_unique_constraint(
        "uq_apis_venue_api_key", "apis", ["venue", "api_key"]
    )


def downgrade() -> None:
    op.drop_constraint("uq_apis_venue_api_key", "apis", type_="unique")
    op.drop_index("ix_apis_api_key", table_name="apis")
    op.create_index("ix_apis_api_key", "apis", ["api_key"], unique=True)
