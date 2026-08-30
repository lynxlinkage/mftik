"""sts_sessions.td_api_ids → td mapping

Revision ID: 0027_sts_td_mapping
Revises: 0026_alerts
Create Date: 2026-08-30

The attach identity is account name → {api_id, settings}. The old JSON
list of api ids cannot carry a name, so rebuild had nothing honest to
read. Old rows become ``account-<id>`` keys so the board still has ids
to show.

CrossArb now requires ``quote_account`` / ``hedge_account`` in
``st_paras``. Rows that survived from before those keys existed get
the first / second mapping key so ``on_initialized`` can rebuild.
"""

from __future__ import annotations

import json
from typing import Any, Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0027_sts_td_mapping"
down_revision: Union[str, Sequence[str], None] = "0026_alerts"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def td_mapping_from_ids(ids: Any) -> dict[str, dict[str, int]]:
    """Old ``td_api_ids`` list → name → {api_id}."""
    if isinstance(ids, str):
        ids = json.loads(ids)
    return {
        f"account-{int(api_id)}": {"api_id": int(api_id)}
        for api_id in (ids or [])
    }


def backfill_cross_arb_st_paras(
    paras: Any, td_keys: list[str]
) -> dict[str, Any] | None:
    """Set quote/hedge account names from the first two ``td`` keys.

    Returns the updated mapping, or ``None`` when there is nothing to
    write (already populated, or fewer than two attached accounts).
    """
    if isinstance(paras, str):
        paras = json.loads(paras)
    out = dict(paras or {})
    if len(td_keys) < 2:
        return None
    if out.get("quote_account") and out.get("hedge_account"):
        return None
    out.setdefault("quote_account", td_keys[0])
    out.setdefault("hedge_account", td_keys[1])
    return out


def _json_load(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def upgrade() -> None:
    op.add_column(
        "sts_sessions",
        sa.Column("td", sa.JSON(), nullable=False, server_default="{}"),
    )
    conn = op.get_bind()
    rows = conn.execute(
        sa.text(
            "SELECT session_id, type, td_api_ids, st_paras FROM sts_sessions"
        )
    ).mappings()
    for row in rows:
        mapping = td_mapping_from_ids(row["td_api_ids"])
        params: dict[str, Any] = {
            "td": json.dumps(mapping),
            "sid": row["session_id"],
        }
        sql = "UPDATE sts_sessions SET td = :td"
        if row["type"] == "CrossArb":
            paras = backfill_cross_arb_st_paras(
                row["st_paras"], list(mapping)
            )
            if paras is not None:
                sql += ", st_paras = :paras"
                params["paras"] = json.dumps(paras)
        sql += " WHERE session_id = :sid"
        conn.execute(sa.text(sql), params)
    op.drop_column("sts_sessions", "td_api_ids")


def downgrade() -> None:
    op.add_column(
        "sts_sessions",
        sa.Column(
            "td_api_ids",
            sa.JSON(),
            nullable=False,
            server_default="[]",
        ),
    )
    conn = op.get_bind()
    rows = conn.execute(
        sa.text("SELECT session_id, td FROM sts_sessions")
    ).mappings()
    for row in rows:
        raw = _json_load(row["td"])
        ids = []
        for value in (raw or {}).values():
            if isinstance(value, dict):
                ids.append(int(value["api_id"]))
            else:
                ids.append(int(value))
        conn.execute(
            sa.text(
                "UPDATE sts_sessions SET td_api_ids = :ids "
                "WHERE session_id = :sid"
            ),
            {"ids": json.dumps(ids), "sid": row["session_id"]},
        )
    op.drop_column("sts_sessions", "td")
