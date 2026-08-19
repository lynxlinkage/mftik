"""Record which proof wrote an audit line

Revision ID: 0025_audit_via
Revises: 0024_fold_strategies
Create Date: 2026-08-19

``audits.user_id`` is the Owner. An instance has one, so the column does
not tell an operator who acted — a Discord session and a CI key both
write the same id. ``via`` is the string ``Principal.via`` already is
(``password``, a provider name, ``key:{name}``, ``disabled``). ``key_id``
and ``key_kind`` snapshot a machine credential so the trail can say
which key and which kind without joining a row that may later be gone.

Nullable because the table is append-only and already has rows. Those
stay blank; the page renders an em dash. See docs/AuditIdentity.md.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0025_audit_via"
down_revision: Union[str, Sequence[str], None] = "0024_fold_strategies"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ``key:{name}``: the name is 64, the prefix is 4. 128 leaves room
    # rather than clipping the rows this column exists to keep.
    op.add_column("audits", sa.Column("via", sa.String(length=128), nullable=True))
    op.add_column("audits", sa.Column("key_id", sa.Integer(), nullable=True))
    op.add_column(
        "audits", sa.Column("key_kind", sa.String(length=16), nullable=True)
    )
    op.create_foreign_key(
        "fk_audits_key_id_auth_keys",
        "audits",
        "auth_keys",
        ["key_id"],
        ["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint("fk_audits_key_id_auth_keys", "audits", type_="foreignkey")
    op.drop_column("audits", "key_kind")
    op.drop_column("audits", "key_id")
    op.drop_column("audits", "via")
