"""sts_sessions.type is the only strategy identity

Revision ID: 0034_strategy_type_key
Revises: 0033_option_strike
Create Date: 2026-09-28

``strategy`` held ``Strategy.name``. ``type`` held the qualified key, except
where a rename wrote the short name into ``type`` as well (``macd_dollar``).
One key column remains. A live or interrupted row that still cannot be named
by a bundled class or a qualified key stops the migration: those sessions
would be rebuilt, and a guess is worse than refusing to start. Both columns
null — a deploy that died before the document was recorded — is left alone.

A finished row that cannot be named keeps ``type`` null and has the token it
was carrying written to ``legacy_strategy``. That column is not a key and
nothing resolves a strategy from it: it exists so that dropping ``strategy``
does not leave a completed session labelled with nothing. Putting the short
name in ``type`` instead would have made every listing and every alert
selector treat ``tiny`` as a registry key that resolves to nothing.

Downgrade puts ``strategy`` back, fills it from ``type`` where there is one
and from ``legacy_strategy`` where there is not, and drops
``legacy_strategy``. Every row that had a value before the upgrade has one
again — the qualified key rather than the short name, for the rows the
upgrade could name.
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
    updates: list[tuple[str | None, str | None, str]] = []
    for session_id, status, strategy, type_name in rows:
        new_type, legacy, refuse = _resolved(status, strategy, type_name)
        if refuse:
            blocked.append(str(session_id))
            continue
        if new_type != type_name or legacy is not None:
            updates.append((new_type, legacy, str(session_id)))
    if blocked:
        # Before any DDL: a refused batch should leave the schema exactly as
        # it was, without relying on the transaction to take a column back.
        listed = ", ".join(blocked)
        raise RuntimeError(
            "live or interrupted sessions have no qualified strategy type: "
            f"{listed}"
        )
    op.add_column(
        "sts_sessions",
        sa.Column("legacy_strategy", sa.String(length=128), nullable=True),
    )
    for new_type, legacy, session_id in updates:
        conn.execute(
            sa.text(
                "UPDATE sts_sessions SET type = :type, "
                "legacy_strategy = :legacy WHERE session_id = :session_id"
            ),
            {
                "type": new_type,
                "legacy": legacy,
                "session_id": session_id,
            },
        )
    # Batch so sqlite, which the test suite migrates against, can drop the
    # column. Postgres runs the same operation as a plain DROP COLUMN.
    with op.batch_alter_table("sts_sessions") as batch:
        batch.drop_column("strategy")


def _resolved(
    status: str, strategy: str | None, type_name: str | None
) -> tuple[str | None, str | None, bool]:
    """``(type to store, label to keep, refuse this row)``.

    A value already in ``type`` wins. An unmatched token is not copied
    forward into ``type`` — that column stays a qualified key, or null — and
    is kept in ``legacy_strategy`` instead, where nothing resolves it. The
    label is the ``strategy`` short name when there is one, because that is
    the column about to be dropped; an unmatched ``type`` token is kept when
    it is the only thing the row has.
    """
    if type_name is not None:
        canonical = _canonical(type_name)
        if canonical is None:
            if status in _REBUILDABLE:
                return None, None, True
            return None, strategy or type_name, False
        return canonical, None, False
    if strategy is None:
        return None, None, False
    canonical = _canonical(strategy)
    if canonical is None:
        if status in _REBUILDABLE:
            return None, None, True
        return None, strategy, False
    return canonical, None, False


def downgrade() -> None:
    op.add_column(
        "sts_sessions",
        sa.Column("strategy", sa.String(length=128), nullable=True),
    )
    # COALESCE, not two statements: a row the upgrade could name has its key
    # in ``type``, and a row it could not has its old short name in
    # ``legacy_strategy``. Every row that arrived with a value leaves with
    # one.
    op.execute(
        "UPDATE sts_sessions SET strategy = COALESCE(type, legacy_strategy)"
    )
    with op.batch_alter_table("sts_sessions") as batch:
        batch.drop_column("legacy_strategy")
