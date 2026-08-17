"""Give the Owner a way to log in, and somewhere to keep the seat

Revision ID: 0021_auth_owner
Revises: 0020_macd_dollar
Create Date: 2026-08-16

The API has never authenticated anyone; ``MFTIK_DEFAULT_USER_ID`` names the
Owner and the gate lives in Traefik. This is the first half of moving that
gate into the app — the columns a username/password login needs, and the
table a browser session lives in. See docs/Auth.md.

**Why the new user columns are nullable.** There is already a ``users`` row
on every deployment and on every local stack: ``seed`` creates one so that
``owner_id`` and ``created_by`` have something to point at, and local compose
runs it before the API container starts. That row has no username and no
password, and it cannot be given one here — a password hash is not something
a migration can invent. So ``ADD COLUMN username NOT NULL`` would fail
outright on a populated table, and inventing a placeholder would be worse: it
would make the row look credentialed to any check that asks whether setup has
run.

Nullable is therefore load-bearing, not laxity. ``password_hash IS NULL`` is
precisely the state ``GET /auth/status`` reports as ``setup_required``, and
``POST /auth/setup`` fills these two columns in on the existing row rather
than inserting a second Owner. Uniqueness is unaffected: Postgres does not
count NULLs against a unique index, so the one real username is still unique.

``email`` drops ``NOT NULL`` in the same move. It was the login identifier by
default — the only unique human-readable column on the table — and it stops
being one here. Addresses change hands and get recycled; a Discord snowflake
or a Google ``sub`` does not, which is what a linked identity will key on
later. Email stays only as display, and only when a provider volunteers it.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0021_auth_owner"
down_revision: Union[str, Sequence[str], None] = "0020_macd_dollar"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("users", sa.Column("username", sa.String(length=64), nullable=True))
    op.create_index("ix_users_username", "users", ["username"], unique=True)
    op.add_column("users", sa.Column("password_hash", sa.Text(), nullable=True))
    op.alter_column(
        "users",
        "email",
        existing_type=sa.String(length=320),
        nullable=True,
    )

    op.create_table(
        "auth_sessions",
        # SHA-256 of the cookie, not the cookie: a dump of this table is not a
        # set of live credentials, and nothing ever needs to read one back.
        sa.Column("id", sa.String(length=64), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("via", sa.String(length=32), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "last_seen_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("user_agent", sa.String(length=256), nullable=True),
        sa.Column("ip", sa.String(length=64), nullable=True),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_auth_sessions_user_id", "auth_sessions", ["user_id"])
    # Expiry is read on every authenticated request and swept in bulk.
    op.create_index("ix_auth_sessions_expires_at", "auth_sessions", ["expires_at"])


def downgrade() -> None:
    op.drop_index("ix_auth_sessions_expires_at", table_name="auth_sessions")
    op.drop_index("ix_auth_sessions_user_id", table_name="auth_sessions")
    op.drop_table("auth_sessions")

    # Going back needs an address for anyone who no longer has one. Rows the
    # Owner never had an email for take a synthetic one rather than blocking
    # the downgrade; they were display-only either way.
    op.execute(
        "UPDATE users SET email = 'user-' || id || '@mftik.invalid' "
        "WHERE email IS NULL"
    )
    op.alter_column(
        "users",
        "email",
        existing_type=sa.String(length=320),
        nullable=False,
    )
    op.drop_column("users", "password_hash")
    op.drop_index("ix_users_username", table_name="users")
    op.drop_column("users", "username")
