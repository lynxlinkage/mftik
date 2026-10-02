"""Drop sts_sessions.rebuild_count and sts_sessions.st_facts

Revision ID: 0037_drop_rebuild_facts
Revises: 0036_session_code_identity
Create Date: 2026-10-02

B10-01. IF-14 already added ``restart_count`` as its own counter at 0
and did not copy ``rebuild_count`` (rebuild counted a mechanism RM-01
removed). This revision drops ``rebuild_count``. It does not rename
that column and it does not copy the values anywhere. It also drops
``st_facts`` (F36). No archive table. No row in any table is rewritten:
``restart`` stays whatever it already said, including a historical
``always``, and statuses are left alone.

Downgrade puts both columns back with the types and server defaults
``main`` declares (``JSON NOT NULL '{}'``, ``INTEGER NOT NULL 0``), so
``main``'s ORM can read ``sts_sessions`` again. The data does not come
back: every row reads ``{}`` and ``0``.

Sqlite uses batch mode for the drop and the re-add. Postgres uses a
plain ``ALTER``. Same split as ``0035_plane_schema``: batch mode on
Postgres used to make ``alembic check`` disagree about constraint names.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0037_drop_rebuild_facts"
down_revision: Union[str, Sequence[str], None] = "0036_session_code_identity"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_DROPPED = ("rebuild_count", "st_facts")


def _restored_columns() -> tuple[sa.Column, sa.Column]:
    # ``main`` at 0014 / 0015. A downgrade that used different types would
    # leave a table ``main``'s models cannot select.
    return (
        sa.Column(
            "rebuild_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "st_facts",
            sa.JSON(),
            nullable=False,
            server_default="{}",
        ),
    )


def upgrade() -> None:
    if op.get_bind().dialect.name == "sqlite":
        with op.batch_alter_table("sts_sessions") as batch:
            for name in _DROPPED:
                batch.drop_column(name)
        return
    for name in _DROPPED:
        op.drop_column("sts_sessions", name)


def downgrade() -> None:
    # The values that were in these columns are gone. Existing rows are
    # filled from the server defaults: ``rebuild_count`` 0, ``st_facts``
    # ``{}``. Nothing here reads a side file or restores a previous value.
    columns = _restored_columns()
    if op.get_bind().dialect.name == "sqlite":
        with op.batch_alter_table("sts_sessions") as batch:
            for column in columns:
                batch.add_column(column)
        return
    for column in columns:
        op.add_column("sts_sessions", column)
