"""sts_sessions.type is the only strategy identity

Revision ID: 0034_strategy_type_key
Revises: 0033_option_strike
Create Date: 2026-09-28

``strategy`` held ``Strategy.name``. ``type`` held the qualified key, except
where a rename wrote the short name into ``type`` as well (``macd_dollar``).
One column remains. A live or interrupted row that still cannot be named by
a bundled class or a qualified key stops the migration: those sessions would
be rebuilt, and a guess is worse than refusing to start. A finished row that
cannot be named keeps ``type`` null. Both columns null — a deploy that died
before the document was recorded — is left alone.

Downgrade puts ``strategy`` back and copies ``type`` into it. That is the
qualified key, not the short name this column used to store.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0034_strategy_type_key"
down_revision: Union[str, Sequence[str], None] = "0033_option_strike"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

#: Short name a bundled strategy used to be stored under, and its class name.
_SHORT_TO_CLASS = {
    "noop": "NoopStrategy",
    "chase": "ChaseOrder",
    "oco": "OneCancelOther",
    "cross_arb": "CrossArb",
    "twap": "TwapStrategy",
    "tape_keeper": "TapeKeeper",
    "macd_dollar": "MacdDollarBars",
}
_CLASS_NAMES = frozenset(_SHORT_TO_CLASS.values())
_REBUILDABLE = frozenset({"live", "interrupted"})


def _canonical(value: str | None) -> str | None:
    """A qualified key or bundled class name. An unmatched token is None."""
    if value is None:
        return None
    mapped = _SHORT_TO_CLASS.get(value)
    if mapped is not None:
        return mapped
    if "::" in value or value in _CLASS_NAMES:
        return value
    return None


def upgrade() -> None:
    conn = op.get_bind()
    rows = conn.execute(
        sa.text(
            "SELECT session_id, status, strategy, type FROM sts_sessions"
        )
    ).fetchall()
    blocked: list[str] = []
    updates: list[tuple[str | None, str]] = []
    for session_id, status, strategy, type_name in rows:
        new_type, refuse = _resolved(status, strategy, type_name)
        if refuse:
            blocked.append(str(session_id))
            continue
        if new_type != type_name:
            updates.append((new_type, str(session_id)))
    if blocked:
        listed = ", ".join(blocked)
        raise RuntimeError(
            "live or interrupted sessions have no qualified strategy type: "
            f"{listed}"
        )
    for new_type, session_id in updates:
        conn.execute(
            sa.text(
                "UPDATE sts_sessions SET type = :type "
                "WHERE session_id = :session_id"
            ),
            {"type": new_type, "session_id": session_id},
        )
    # Batch so sqlite, which the test suite migrates against, can drop the
    # column. Postgres runs the same operation as a plain DROP COLUMN.
    with op.batch_alter_table("sts_sessions") as batch:
        batch.drop_column("strategy")


def _resolved(
    status: str, strategy: str | None, type_name: str | None
) -> tuple[str | None, bool]:
    """``(type to store, refuse this row)``.

    A value already in ``type`` wins. An unmatched token there is not copied
    forward: ``type`` stays a qualified key, or null.
    """
    if type_name is not None:
        canonical = _canonical(type_name)
        if canonical is None:
            if status in _REBUILDABLE:
                return None, True
            return None, False
        return canonical, False
    if strategy is None:
        return None, False
    canonical = _canonical(strategy)
    if canonical is None:
        if status in _REBUILDABLE:
            return None, True
        return None, False
    return canonical, False


def downgrade() -> None:
    op.add_column(
        "sts_sessions",
        sa.Column("strategy", sa.String(length=128), nullable=True),
    )
    op.execute("UPDATE sts_sessions SET strategy = type")
