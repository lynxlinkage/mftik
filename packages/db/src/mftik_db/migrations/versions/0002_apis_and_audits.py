"""apis and audits tables

Revision ID: 0002_apis_audits
Revises: 0001_init
Create Date: 2026-08-01

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0002_apis_audits"
down_revision: Union[str, Sequence[str], None] = "0001_init"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "apis",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("owner_id", sa.Integer(), nullable=False),
        sa.Column("api_key", sa.String(length=256), nullable=False),
        sa.Column("api_secret", sa.Text(), nullable=False),
        sa.Column("type", sa.String(length=32), nullable=False),
        sa.Column("passphrase", sa.String(length=256), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["owner_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_apis_owner_id"), "apis", ["owner_id"], unique=False)
    op.create_index(op.f("ix_apis_api_key"), "apis", ["api_key"], unique=True)

    op.create_table(
        "audits",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("operation", sa.String(length=128), nullable=False),
        sa.Column("result", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_audits_user_id"), "audits", ["user_id"], unique=False)
    op.create_index(
        op.f("ix_audits_created_at"), "audits", ["created_at"], unique=False
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_audits_created_at"), table_name="audits")
    op.drop_index(op.f("ix_audits_user_id"), table_name="audits")
    op.drop_table("audits")
    op.drop_index(op.f("ix_apis_api_key"), table_name="apis")
    op.drop_index(op.f("ix_apis_owner_id"), table_name="apis")
    op.drop_table("apis")
