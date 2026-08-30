"""sts_sessions.td_api_ids → td mapping

Revision ID: 0027_sts_td_mapping
Revises: 0026_alerts
Create Date: 2026-08-30

The attach identity is account name → {api_id, settings}. The old JSON
list of api ids cannot carry a name, so rebuild had nothing honest to
read. Old rows become ``account-<id>`` keys so the board still has ids
to show.
"""

from __future__ import annotations

import json
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0027_sts_td_mapping"
down_revision: Union[str, Sequence[str], None] = "0026_alerts"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "sts_sessions",
        sa.Column("td", sa.JSON(), nullable=False, server_default="{}"),
    )
    conn = op.get_bind()
    rows = conn.execute(
        sa.text("SELECT session_id, td_api_ids FROM sts_sessions")
    ).mappings()
    for row in rows:
        ids = row["td_api_ids"]
        if isinstance(ids, str):
            ids = json.loads(ids)
        mapping = {
            f"account-{int(api_id)}": {"api_id": int(api_id)}
            for api_id in (ids or [])
        }
        conn.execute(
            sa.text(
                "UPDATE sts_sessions SET td = :td WHERE session_id = :sid"
            ),
            {"td": json.dumps(mapping), "sid": row["session_id"]},
        )
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
        raw = row["td"]
        if isinstance(raw, str):
            raw = json.loads(raw)
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
