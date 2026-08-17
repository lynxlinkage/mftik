"""accounts.name unique — strategy.yml td references by name

Revision ID: 0010_account_name
Revises: 0009_accounts
Create Date: 2026-08-02

"""

from typing import Sequence, Union

from alembic import op

revision: str = "0010_account_name"
down_revision: Union[str, Sequence[str], None] = "0009_accounts"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "uq_accounts_name", "accounts", ["name"], unique=True
    )


def downgrade() -> None:
    op.drop_index("uq_accounts_name", table_name="accounts")
