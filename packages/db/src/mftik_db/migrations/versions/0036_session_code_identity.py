"""sts_sessions pins strategy_digest and env_generation

Revision ID: 0036_session_code_identity
Revises: 0035_plane_schema
Create Date: 2026-10-02

Additive only (F39, IF-16). Both columns are nullable: a built-in
strategy has no digest, and every row written before this revision has
neither pin. Nothing is backfilled. Drops stay with B10-01.

``strategy_digest`` is ``sha256:`` plus 64 hex characters, 71 in all.
``env_generation`` is the extras generation, not ``sts_sessions.generation``.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0036_session_code_identity"
down_revision: Union[str, Sequence[str], None] = "0035_plane_schema"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "sts_sessions",
        sa.Column("strategy_digest", sa.String(length=71), nullable=True),
    )
    op.add_column(
        "sts_sessions",
        sa.Column("env_generation", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    if op.get_bind().dialect.name == "sqlite":
        with op.batch_alter_table("sts_sessions") as batch:
            batch.drop_column("env_generation")
            batch.drop_column("strategy_digest")
        return
    op.drop_column("sts_sessions", "env_generation")
    op.drop_column("sts_sessions", "strategy_digest")
