"""Linked identities, and the record a callback is answered from

Revision ID: 0023_auth_identities
Revises: 0022_auth_keys
Create Date: 2026-08-16

Two tables, and the reason for the second is the interesting one.

``auth_identities`` is keyed on ``(provider, subject)`` — the provider's own
stable id. Not email: addresses change hands and Workspace domains recycle
them, so keying on one means whoever inherits an address inherits the node. A
Discord snowflake and a Google ``sub`` are never reused, which is the property
an identity key actually needs. Email is carried for display and matched on
never; there is no path here that turns "same address" into "same person".

Nothing about an OAuth callback creates a ``users`` row. Rows here are only
written by a Connect performed from a live session, so an unknown account
arriving at the callback is refused rather than welcomed — that is the whole
of what keeps a single-tenant instance single-tenant.

``auth_oauth_states`` exists because ``state`` on the wire must be an
unguessable nonce and nothing else. Whether a callback is a login or a link,
which provider it belongs to, and which session started it are read back from
here rather than from the query string. A readable ``state=connect`` would be
an account takeover: anyone could walk the Owner's browser into a callback
carrying their own code, have their account linked, and log in as the Owner
from then on. The row is deleted when it is read, so a replayed callback finds
nothing, and it expires in minutes because a flow nobody finished is litter.

``session_id`` cascades from ``auth_sessions``: logging out mid-flow should
abandon the link, not leave a record that could complete against whatever
session comes next.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0023_auth_identities"
down_revision: Union[str, Sequence[str], None] = "0022_auth_keys"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "auth_identities",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("subject", sa.String(length=128), nullable=False),
        sa.Column("label", sa.String(length=128), nullable=True),
        sa.Column("email", sa.String(length=320), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("provider", "subject", name="uq_identity"),
    )
    op.create_index("ix_auth_identities_user_id", "auth_identities", ["user_id"])

    op.create_table(
        "auth_oauth_states",
        sa.Column("state", sa.String(length=64), nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("mode", sa.String(length=16), nullable=False),
        sa.Column("verifier", sa.String(length=128), nullable=False),
        sa.Column("session_id", sa.String(length=64), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["session_id"], ["auth_sessions.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("state"),
    )
    op.create_index(
        "ix_auth_oauth_states_expires_at", "auth_oauth_states", ["expires_at"]
    )


def downgrade() -> None:
    op.drop_index("ix_auth_oauth_states_expires_at", table_name="auth_oauth_states")
    op.drop_table("auth_oauth_states")
    op.drop_index("ix_auth_identities_user_id", table_name="auth_identities")
    op.drop_table("auth_identities")
