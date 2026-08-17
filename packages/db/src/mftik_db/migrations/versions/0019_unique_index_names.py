"""accounts / strategies uniqueness — say it the way the models do

Revision ID: 0019_unique_names
Revises: 0018_trade_history
Create Date: 2026-08-14

Three columns are declared ``unique=True, index=True`` on the model, which
SQLAlchemy renders as a unique index named ``ix_<table>_<column>``. The
migrations that created these tables wrote the uniqueness by hand instead —
two as named ``UniqueConstraint``s and one as an index under a ``uq_`` name.
Both enforce the same rule, and Postgres backs a unique constraint with an
index regardless, so nothing about what the database permits changes here.

What changes is that ``alembic check`` passes. Until now the models and the
migrations disagreed on paper, so the check could not be put in CI, and every
future disagreement — including one that *is* a real difference — had nothing
watching for it. Both tables are configuration-sized; the rewrite is instant.
"""

from typing import Sequence, Union

from alembic import op

revision: str = "0019_unique_names"
down_revision: Union[str, Sequence[str], None] = "0018_trade_history"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Postgres runs migrations in one transaction, so there is no moment here
    # in which the column is unenforced.
    op.drop_constraint("uq_accounts_api_id", "accounts", type_="unique")
    op.create_index("ix_accounts_api_id", "accounts", ["api_id"], unique=True)

    op.drop_index("uq_accounts_name", table_name="accounts")
    op.create_index("ix_accounts_name", "accounts", ["name"], unique=True)

    op.drop_constraint("uq_strategies_sts_session", "strategies", type_="unique")
    op.create_index(
        "ix_strategies_sts_session", "strategies", ["sts_session"], unique=True
    )


def downgrade() -> None:
    op.drop_index("ix_strategies_sts_session", table_name="strategies")
    op.create_unique_constraint(
        "uq_strategies_sts_session", "strategies", ["sts_session"]
    )

    op.drop_index("ix_accounts_name", table_name="accounts")
    op.create_index("uq_accounts_name", "accounts", ["name"], unique=True)

    op.drop_index("ix_accounts_api_id", table_name="accounts")
    op.create_unique_constraint("uq_accounts_api_id", "accounts", ["api_id"])
