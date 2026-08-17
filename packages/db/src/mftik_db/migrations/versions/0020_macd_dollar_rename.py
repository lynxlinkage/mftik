"""macd_volume → macd_dollar in the rows that name it

Revision ID: 0020_macd_dollar
Revises: 0019_unique_names
Create Date: 2026-08-15

``1b6a2bf`` renamed the strategy and said no session row referenced it. That
was true of the branch and false of the deployment: a session had been
deployed under the old name, so its row went on naming a strategy no build
would answer to again. Every STS boot then logged a stack trace for it while
scanning for interrupted sessions to rebuild.

Renaming code the database stores by name is a migration, not an edit. The
alternative — teaching the registry the old name as an alias — would have put
the misleading name back in service, and the rename exists precisely because
someone reading "volume bars" sizes the threshold in BTC and is wrong by five
orders of magnitude.

Two columns carry it. ``sts_sessions.strategy`` holds the short name that
``Strategy.name`` gives, and ``strategies.type`` holds the class name from a
deploy's ``sts.type``. Both spellings changed, and ``resolve`` accepts either
form, so both are rewritten wherever they appear.

This renames a record of what ran. It does not make that session rebuildable:
``MacdDollarBars.rebuildable`` is False, so the scan will now skip it saying
so, which is the answer it should have been giving all along.
"""

from typing import Sequence, Union

from alembic import op

revision: str = "0020_macd_dollar"
down_revision: Union[str, Sequence[str], None] = "0019_unique_names"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

#: (table, column, old, new) — every place a strategy is stored by name.
_RENAMES = (
    ("sts_sessions", "strategy", "macd_volume", "macd_dollar"),
    ("sts_sessions", "strategy", "MacdVolumeBars", "MacdDollarBars"),
    ("strategies", "type", "MacdVolumeBars", "MacdDollarBars"),
    ("strategies", "type", "macd_volume", "macd_dollar"),
)


def _rename(pairs: Sequence[tuple[str, str, str, str]]) -> None:
    for table, column, old, new in pairs:
        op.execute(
            f"UPDATE {table} SET {column} = '{new}' WHERE {column} = '{old}'"
        )


def upgrade() -> None:
    _rename(_RENAMES)


def downgrade() -> None:
    _rename([(t, c, new, old) for t, c, old, new in _RENAMES])
