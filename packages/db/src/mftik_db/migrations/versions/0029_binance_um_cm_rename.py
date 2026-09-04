"""BinanceFuture → BinanceUM, BinanceDelivery → BinanceCM

Revision ID: 0029_binance_um_cm
Revises: 0028_apis_venue_key
Create Date: 2026-09-04

The registry names changed so the ticker says which Binance margin
plane an instrument is on. Code no longer accepts the old spellings,
so every stored venue and every stored ticker has to move with it.
Teaching ``venues.get`` the old names as aliases would put them back
in service; the rename exists so they are not.

0028's ``CANONICAL_VENUES`` snapshot stays frozen at the spellings it
shipped. This revision is the fold.
"""

from __future__ import annotations

import json
from typing import Any, Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0029_binance_um_cm"
down_revision: Union[str, Sequence[str], None] = "0028_apis_venue_key"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

#: Old registry name → new. Tickers, feed keys and YAML mention the same
#: strings as a prefix, so one table drives every rewrite.
_RENAMES = (
    ("BinanceFuture", "BinanceUM"),
    ("BinanceDelivery", "BinanceCM"),
)

#: Columns that hold a venue name and nothing else.
_VENUE_COLUMNS = (
    ("apis", "venue"),
    ("md_sessions", "venue"),
    ("alert_sources", "selector"),
)

#: Columns that hold a universal ticker, or empty for an account-wide walk.
_TICKER_COLUMNS = (
    ("symbol_ticker", "universal_ticker"),
    ("orders", "universal_ticker"),
    ("fills", "universal_ticker"),
    ("cash_flows", "universal_ticker"),
    ("backfill_cursors", "scope"),
)


def rewrite_text(value: str, pairs: Sequence[tuple[str, str]] = _RENAMES) -> str:
    """Replace every old venue spelling inside a stored string."""
    for old, new in pairs:
        value = value.replace(old, new)
    return value


def rewrite_json(value: Any, pairs: Sequence[tuple[str, str]] = _RENAMES) -> Any:
    """Walk a JSON document and rewrite venue spellings in keys and values."""
    if isinstance(value, str):
        return rewrite_text(value, pairs)
    if isinstance(value, list):
        return [rewrite_json(item, pairs) for item in value]
    if isinstance(value, dict):
        out = {}
        for key, item in value.items():
            if isinstance(key, str):
                key = rewrite_text(key, pairs)
            out[key] = rewrite_json(item, pairs)
        return out
    return value


def _pairs(*, reverse: bool) -> Sequence[tuple[str, str]]:
    return tuple((new, old) if reverse else (old, new) for old, new in _RENAMES)


def _load_json(raw: Any) -> Any:
    if raw is None or isinstance(raw, (dict, list)):
        return raw
    return json.loads(raw)


def upgrade() -> None:
    _apply(_pairs(reverse=False))


def downgrade() -> None:
    _apply(_pairs(reverse=True))


def _apply(pairs: Sequence[tuple[str, str]]) -> None:
    bind = op.get_bind()
    for table, column in _VENUE_COLUMNS:
        for old, new in pairs:
            bind.execute(
                sa.text(
                    f"UPDATE {table} SET {column} = :new "
                    f"WHERE lower({column}) = lower(:old)"
                ),
                {"new": new, "old": old},
            )
    for table, column in _TICKER_COLUMNS:
        for old, new in pairs:
            bind.execute(
                sa.text(
                    f"UPDATE {table} SET {column} = replace({column}, :old, :new) "
                    f"WHERE {column} LIKE :pat"
                ),
                {"old": old, "new": new, "pat": f"{old}%"},
            )
    _rewrite_session_text(bind, pairs)
    _rewrite_session_json(bind, "md_ids", pairs)
    _rewrite_session_json(bind, "st_paras", pairs)
    _rewrite_session_json(bind, "st_facts", pairs)


def _rewrite_session_text(
    bind: sa.Connection, pairs: Sequence[tuple[str, str]]
) -> None:
    rows = bind.execute(
        sa.text("SELECT session_id, yaml_text FROM sts_sessions")
    ).fetchall()
    for session_id, yaml_text in rows:
        if not yaml_text:
            continue
        rewritten = rewrite_text(yaml_text, pairs)
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
    bind: sa.Connection, column: str, pairs: Sequence[tuple[str, str]]
) -> None:
    rows = bind.execute(
        sa.text(f"SELECT session_id, {column} FROM sts_sessions")
    ).fetchall()
    for session_id, raw in rows:
        loaded = _load_json(raw)
        if not loaded:
            continue
        rewritten = rewrite_json(loaded, pairs)
        if rewritten == loaded:
            continue
        bind.execute(
            sa.text(
                f"UPDATE sts_sessions SET {column} = :value "
                f"WHERE session_id = :session_id"
            ),
            {"value": json.dumps(rewritten), "session_id": session_id},
        )
