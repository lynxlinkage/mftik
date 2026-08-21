"""Alert graph: sources, matchers, alerts, two joins, deliveries

Revision ID: 0026_alerts
Revises: 0025_audit_via
Create Date: 2026-08-21

Six tables. Two join tables with real foreign keys so Source → Matcher
→ Alert is the only representable path, and deleting a node cascades
its wires. There is no ``alert_edges`` table: a polymorphic
``(from_kind, from_id)`` pair cannot carry an FK.

``alerts.created_by`` (and the same column on sources and matchers) is
the Owner, the same shape as ``sts_sessions.created_by``. Deleting an
Alert deletes its deliveries — the fire log goes with the webhook.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0026_alerts"
down_revision: Union[str, Sequence[str], None] = "0025_audit_via"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "alert_sources",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("created_by", sa.Integer(), nullable=False),
        sa.Column("domain", sa.String(length=8), nullable=False),
        sa.Column("selector", sa.String(length=128), nullable=False),
        sa.ForeignKeyConstraint(
            ["created_by"], ["users.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "domain", "selector", name="uq_alert_sources_domain_selector"
        ),
    )
    op.create_index(
        "ix_alert_sources_created_by", "alert_sources", ["created_by"]
    )
    op.create_index("ix_alert_sources_domain", "alert_sources", ["domain"])

    op.create_table(
        "alert_matchers",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("created_by", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("kind", sa.String(length=16), nullable=False),
        sa.Column("spec", sa.JSON(), nullable=False),
        sa.ForeignKeyConstraint(
            ["created_by"], ["users.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", name="uq_alert_matchers_name"),
    )
    op.create_index(
        "ix_alert_matchers_created_by", "alert_matchers", ["created_by"]
    )

    op.create_table(
        "alerts",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("created_by", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column(
            "kind",
            sa.String(length=32),
            server_default="discord_webhook",
            nullable=False,
        ),
        sa.Column("webhook_url", sa.Text(), nullable=False),
        sa.Column(
            "enabled",
            sa.Boolean(),
            server_default=sa.text("true"),
            nullable=False,
        ),
        sa.Column(
            "flush_interval_s",
            sa.Integer(),
            server_default="30",
            nullable=False,
        ),
        sa.Column(
            "max_events_in_payload",
            sa.Integer(),
            server_default="15",
            nullable=False,
        ),
        sa.Column(
            "max_buffer_events",
            sa.Integer(),
            server_default="200",
            nullable=False,
        ),
        sa.Column(
            "dedupe",
            sa.Boolean(),
            server_default=sa.text("true"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["created_by"], ["users.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", name="uq_alerts_name"),
    )
    op.create_index("ix_alerts_created_by", "alerts", ["created_by"])

    op.create_table(
        "alert_source_matcher",
        sa.Column("source_id", sa.Integer(), nullable=False),
        sa.Column("matcher_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["source_id"], ["alert_sources.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["matcher_id"], ["alert_matchers.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("source_id", "matcher_id"),
    )

    op.create_table(
        "alert_matcher_alert",
        sa.Column("matcher_id", sa.Integer(), nullable=False),
        sa.Column("alert_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["matcher_id"], ["alert_matchers.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["alert_id"], ["alerts.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("matcher_id", "alert_id"),
    )

    op.create_table(
        "alert_deliveries",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("alert_id", sa.Integer(), nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("event_count", sa.Integer(), nullable=False),
        sa.Column(
            "dropped_count", sa.Integer(), server_default="0", nullable=False
        ),
        sa.Column("http_status", sa.Integer(), nullable=True),
        sa.Column("error", sa.String(length=256), nullable=True),
        sa.Column(
            "ts",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["alert_id"], ["alerts.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_alert_deliveries_alert_id", "alert_deliveries", ["alert_id"]
    )


def downgrade() -> None:
    op.drop_index("ix_alert_deliveries_alert_id", table_name="alert_deliveries")
    op.drop_table("alert_deliveries")
    op.drop_table("alert_matcher_alert")
    op.drop_table("alert_source_matcher")
    op.drop_index("ix_alerts_created_by", table_name="alerts")
    op.drop_table("alerts")
    op.drop_index("ix_alert_matchers_created_by", table_name="alert_matchers")
    op.drop_table("alert_matchers")
    op.drop_index("ix_alert_sources_domain", table_name="alert_sources")
    op.drop_index("ix_alert_sources_created_by", table_name="alert_sources")
    op.drop_table("alert_sources")
