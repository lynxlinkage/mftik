"""Dated future symbols grow a hyphen: BTCUSDT250926 → BTCUSDT-250926

Revision ID: 0030_dated_hyphen
Revises: 0029_binance_um_cm
Create Date: 2026-09-05

The stored symbol used to glue ``YYMMDD`` onto the pair so a quarterly
could not collide with its perpetual. That spelling cannot grow a strike
or a call/put flag, so the platform form is now ``PAIR-YYMMDD`` (and
later ``PAIR-YYMMDD-STRIKE-C``). Code no longer accepts the glued form
on the wire or in a column, so every stored ticker has to move with it.

Only ``Future`` and ``Option`` tickers are rewritten. A spot or perp
symbol that happens to end in six digits is a different instrument.
"""

from __future__ import annotations

import json
import re
from typing import Any, Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0030_dated_hyphen"
down_revision: Union[str, Sequence[str], None] = "0029_binance_um_cm"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

#: ``Venue_Future_PAIRYYMMDD`` / ``Venue_Option_PAIRYYMMDD``. The lookbehind
#: keeps a feed key prefix (``aggtrade.``) out of the venue; the lookahead
#: refuses to touch an already-hyphenated option
#: (``PAIR-YYMMDD-STRIKE-C``).
#:
#: The venue half stays ASCII because ``_VENUE_RE`` says venue names are.
#: The pair half does not: ``_check_symbol`` deliberately admits the CJK
#: meme tokens Gate lists, and a row this skipped would survive the
#: upgrade in a spelling ``UniversalTicker.parse`` then refuses — which
#: takes out every symbol read for that venue, not just the one row. No
#: such instrument is listed on a dated book today; matching the grammar
#: the tree enforces is cheaper than depending on that.
_PAIR = r"[^\W_]+"
#: Not followed by another symbol character or a hyphen — same rule as the
#: pair, so a longer digit run and a hyphenated option both stay untouched.
_TAIL = r"(?![^\W_]|-)"
_HEAD = r"(?P<head>(?<![A-Za-z0-9])[A-Z][A-Za-z0-9]*_(?:Future|Option)_)"

_GLUED = re.compile(_HEAD + rf"(?P<pair>{_PAIR})(?P<date>\d{{6}})" + _TAIL)
_HYPHENATED = re.compile(
    _HEAD + rf"(?P<pair>{_PAIR})-(?P<date>\d{{6}})" + _TAIL
)

#: Cheap, portable superset of "this value could hold a dated ticker",
#: used to keep the row loop off every order and fill ever recorded. No
#: ``_`` in the pattern and so no ``ESCAPE`` clause: ``_`` is a LIKE
#: wildcard, the dialects spell the escape differently, and the regex is
#: what actually decides. A superset is all this has to be.
_DATED_LIKE = ("%Future%", "%Option%")

#: Columns that hold a universal ticker, or a feed key that embeds one.
_TICKER_COLUMNS = (
    ("symbol_ticker", ("id",), "universal_ticker"),
    ("orders", ("id",), "universal_ticker"),
    ("fills", ("id",), "universal_ticker"),
    ("cash_flows", ("id",), "universal_ticker"),
    ("backfill_cursors", ("api_id", "stream", "scope"), "scope"),
)


def rewrite_text(value: str, *, reverse: bool = False) -> str:
    """Insert or remove the hyphen before a dated ticker's ``YYMMDD``."""
    if reverse:
        return _HYPHENATED.sub(r"\g<head>\g<pair>\g<date>", value)
    return _GLUED.sub(r"\g<head>\g<pair>-\g<date>", value)


def rewrite_json(value: Any, *, reverse: bool = False) -> Any:
    """Walk a JSON document and rewrite dated tickers in keys and values."""
    if isinstance(value, str):
        return rewrite_text(value, reverse=reverse)
    if isinstance(value, list):
        return [rewrite_json(item, reverse=reverse) for item in value]
    if isinstance(value, dict):
        out = {}
        for key, item in value.items():
            if isinstance(key, str):
                key = rewrite_text(key, reverse=reverse)
            out[key] = rewrite_json(item, reverse=reverse)
        return out
    return value


def upgrade() -> None:
    _apply(reverse=False)


def downgrade() -> None:
    _apply(reverse=True)


def _apply(*, reverse: bool) -> None:
    bind = op.get_bind()
    for table, pk, column in _TICKER_COLUMNS:
        _rewrite_column(bind, table, pk, column, reverse=reverse)
    _rewrite_session_text(bind, reverse=reverse)
    _rewrite_session_json(bind, "md_ids", reverse=reverse)
    _rewrite_session_json(bind, "st_paras", reverse=reverse)
    _rewrite_session_json(bind, "st_facts", reverse=reverse)


def _rewrite_column(
    bind: sa.Connection,
    table: str,
    pk: Sequence[str],
    column: str,
    *,
    reverse: bool,
) -> None:
    # Only the dated rows are read. ``orders`` and ``fills`` grow one row
    # per order state change and per execution, and the rows this rewrites
    # are the handful of Binance dated futures — so selecting the table
    # whole would pull a production history into memory to change almost
    # none of it. 0029 was set-based for the same reason; the rewrite here
    # is a regex rather than a ``replace()``, so the predicate moves to the
    # SELECT instead.
    cols = ", ".join((*pk, column))
    like = " OR ".join(
        f"{column} LIKE :pat{i}" for i in range(len(_DATED_LIKE))
    )
    rows = bind.execute(
        sa.text(f"SELECT {cols} FROM {table} WHERE {like}"),
        {f"pat{i}": pattern for i, pattern in enumerate(_DATED_LIKE)},
    ).fetchall()
    for row in rows:
        value = row[-1]
        if not isinstance(value, str) or not value:
            continue
        rewritten = rewrite_text(value, reverse=reverse)
        if rewritten == value:
            continue
        where = " AND ".join(f"{name} = :{name}" for name in pk)
        params = {name: row[i] for i, name in enumerate(pk)}
        params["new_value"] = rewritten
        bind.execute(
            sa.text(f"UPDATE {table} SET {column} = :new_value WHERE {where}"),
            params,
        )


def _rewrite_session_text(bind: sa.Connection, *, reverse: bool) -> None:
    rows = bind.execute(
        sa.text("SELECT session_id, yaml_text FROM sts_sessions")
    ).fetchall()
    for session_id, yaml_text in rows:
        if not isinstance(yaml_text, str) or not yaml_text:
            continue
        rewritten = rewrite_text(yaml_text, reverse=reverse)
        if rewritten == yaml_text:
            continue
        bind.execute(
            sa.text(
                "UPDATE sts_sessions SET yaml_text = :yaml_text "
                "WHERE session_id = :session_id"
            ),
            {"yaml_text": rewritten, "session_id": session_id},
        )


def _rewrite_session_json(
    bind: sa.Connection, column: str, *, reverse: bool
) -> None:
    rows = bind.execute(
        sa.text(f"SELECT session_id, {column} FROM sts_sessions")
    ).fetchall()
    for session_id, raw in rows:
        loaded = _load_json(raw)
        if not loaded:
            continue
        rewritten = rewrite_json(loaded, reverse=reverse)
        if rewritten == loaded:
            continue
        bind.execute(
            sa.text(
                f"UPDATE sts_sessions SET {column} = :value "
                f"WHERE session_id = :session_id"
            ),
            {"value": json.dumps(rewritten), "session_id": session_id},
        )


def _load_json(raw: Any) -> Any:
    if raw is None or isinstance(raw, (dict, list)):
        return raw
    return json.loads(raw)
