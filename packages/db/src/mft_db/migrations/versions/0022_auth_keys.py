"""Machine credentials: the keys the Owner issues to scripts and to peers

Revision ID: 0022_auth_keys
Revises: 0021_auth_owner
Create Date: 2026-08-16

One table for both kinds. An API key and a registry key differ in what they
are allowed to do, not in how they are stored or checked, so ``kind`` carries
the difference and the resolution path stays a single indexed lookup. Two
tables would have meant two lookups on every bearer token to answer the same
question.

The secret is never here. ``key_hash`` is a SHA-256 of the whole token and
``prefix`` is the first characters of its random part — unique, indexed, and
the reason verifying a key does not scan every hash on the table. A plain
digest rather than argon2 on purpose: this is 256 bits of ``secrets`` output,
not something a person chose, so there is no dictionary to slow down and no
reason to put ~50ms in front of every request a CI job makes.

``revoked_at`` rather than a delete. Audit lines name keys by id, and deleting
the row turns those into dangling numbers; a revoked key is also history worth
showing, since "this key stopped working" and "this key never existed" are
different things to whoever is reading the list.

``scopes`` is written at mint time from ``kind`` rather than derived on use.
Widening what a kind may do should not silently widen every key already in the
field — that is a decision to make per key, by issuing a new one.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0022_auth_keys"
down_revision: Union[str, Sequence[str], None] = "0021_auth_owner"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "auth_keys",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("kind", sa.String(length=16), nullable=False),
        sa.Column("name", sa.String(length=64), nullable=False),
        sa.Column("prefix", sa.String(length=16), nullable=False),
        sa.Column("key_hash", sa.Text(), nullable=False),
        sa.Column("scopes", sa.JSON(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_auth_keys_user_id", "auth_keys", ["user_id"])
    op.create_index("ix_auth_keys_prefix", "auth_keys", ["prefix"], unique=True)


def downgrade() -> None:
    op.drop_index("ix_auth_keys_prefix", table_name="auth_keys")
    op.drop_index("ix_auth_keys_user_id", table_name="auth_keys")
    op.drop_table("auth_keys")
