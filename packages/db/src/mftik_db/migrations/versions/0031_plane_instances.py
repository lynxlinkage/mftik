"""Plane instances: the instances table, and the columns that name one

Revision ID: 0031_plane_instances
Revises: 0030_dated_hyphen
Create Date: 2026-09-06

A node can run several processes of one plane — ``td-jp-1``, ``md-jp-2``,
``sts-tw`` — and the control plane has to address them by name. This lands the
table those names live in and the columns that point at it.

The order below is forced by the foreign key, not by taste. ``apis.instance_id``
is ``NOT NULL``, so the row it points at has to exist first: create the table,
seed it, add the column nullable, backfill, then tighten. Doing it any other
way fails on the first database that already has an ``apis`` row.

``created_by`` is nullable and the seeded rows leave it null. This revision runs
before ``seed`` has created the Owner — ``migrate`` waits only on Postgres — so
on an empty database there is no ``users`` row to attribute them to, and a
``NOT NULL`` here would fail the upgrade on precisely the deployment that has
never been upgraded. Null is also true: nobody created those three.

The seeded names are the plane names because ``MFTIK_INSTANCE`` defaults to the
plane name. An existing single-process deployment therefore comes up already
matching three declared rows and needs no configuration at all.

Three rows, not five. ``sym`` and ``paper`` are not instanced.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0031_plane_instances"
down_revision: Union[str, Sequence[str], None] = "0030_dated_hyphen"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

#: The default instance of each instanced plane. Named for the plane so a
#: deployment that never sets ``MFTIK_INSTANCE`` is already declared.
_DEFAULTS = ("td", "md", "sts")


def upgrade() -> None:
    op.create_table(
        "instances",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(length=64), nullable=False),
        sa.Column("domain", sa.String(length=16), nullable=False),
        sa.Column("region", sa.String(length=64), nullable=True),
        sa.Column(
            "enabled",
            sa.Boolean(),
            server_default=sa.text("true"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("created_by", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["created_by"], ["users.id"], ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    # A unique *index*, not a UniqueConstraint beside a plain one. The model
    # says ``unique=True, index=True``, which SQLAlchemy renders exactly this
    # way — and 0019 already went through the tables that disagreed, for the
    # reason its docstring gives: until the two agree on paper, ``alembic
    # check`` cannot be in CI, and every later disagreement has nothing
    # watching for it.
    op.create_index("ix_instances_name", "instances", ["name"], unique=True)
    op.create_index(
        "ix_instances_domain", "instances", ["domain"], unique=False
    )
    op.create_index(
        "ix_instances_created_by", "instances", ["created_by"], unique=False
    )

    for name in _DEFAULTS:
        op.execute(
            sa.text(
                "INSERT INTO instances (name, domain, enabled) "
                "VALUES (:name, :domain, true)"
            ).bindparams(name=name, domain=name)
        )

    # Nullable first: existing rows have no value yet and the column is about
    # to be NOT NULL.
    op.add_column("apis", sa.Column("instance_id", sa.Integer(), nullable=True))
    op.execute(
        sa.text(
            "UPDATE apis SET instance_id = "
            "(SELECT id FROM instances WHERE name = 'td') "
            "WHERE instance_id IS NULL"
        )
    )
    op.alter_column("apis", "instance_id", nullable=False)
    op.create_index("ix_apis_instance_id", "apis", ["instance_id"], unique=False)
    op.create_foreign_key(
        "fk_apis_instance_id",
        "apis",
        "instances",
        ["instance_id"],
        ["id"],
        ondelete="RESTRICT",
    )

    # md_sessions: an attach row now names the MD that holds it, and the
    # uniqueness that used to be (venue, session) has to admit two instances
    # serving one venue for one session.
    op.add_column(
        "md_sessions",
        sa.Column(
            "instance",
            sa.String(length=64),
            server_default="md",
            nullable=False,
        ),
    )
    op.alter_column("md_sessions", "instance", server_default=None)
    op.drop_constraint(
        "uq_md_sessions_venue_session", "md_sessions", type_="unique"
    )
    op.create_unique_constraint(
        "uq_md_sessions_instance_venue_session",
        "md_sessions",
        ["instance", "venue", "session_id"],
    )
    op.create_index(
        "ix_md_sessions_instance", "md_sessions", ["instance"], unique=False
    )

    # sts_sessions: which STS was asked to run this. Nullable — a row written
    # before this revision was not pinned, and an unpinned deploy still is not.
    op.add_column(
        "sts_sessions",
        sa.Column("instance", sa.String(length=64), nullable=True),
    )
    op.create_index(
        "ix_sts_sessions_instance", "sts_sessions", ["instance"], unique=False
    )


def downgrade() -> None:
    op.drop_index("ix_sts_sessions_instance", table_name="sts_sessions")
    op.drop_column("sts_sessions", "instance")

    op.drop_index("ix_md_sessions_instance", table_name="md_sessions")
    op.drop_constraint(
        "uq_md_sessions_instance_venue_session", "md_sessions", type_="unique"
    )
    op.create_unique_constraint(
        "uq_md_sessions_venue_session", "md_sessions", ["venue", "session_id"]
    )
    op.drop_column("md_sessions", "instance")

    op.drop_constraint("fk_apis_instance_id", "apis", type_="foreignkey")
    op.drop_index("ix_apis_instance_id", table_name="apis")
    op.drop_column("apis", "instance_id")

    op.drop_index("ix_instances_created_by", table_name="instances")
    op.drop_index("ix_instances_domain", table_name="instances")
    op.drop_index("ix_instances_name", table_name="instances")
    op.drop_table("instances")
