"""Alert graph: Source → Matcher → Alert, plus the fire log.

Two join tables, not a polymorphic edge table. ``(from_kind, from_id)``
cannot carry a foreign key, so a cascade would have been application
code and an orphan a forgotten path. Matcher → matcher is
unrepresentable rather than merely refused.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from mftik_db.models.base import Base


class AlertSourceDomain(StrEnum):
    STS = "sts"
    TD = "td"
    MD = "md"


class AlertMatcherKind(StrEnum):
    LEVEL = "level"
    REGEX = "regex"
    EXTRACT = "extract"


class AlertKind(StrEnum):
    DISCORD_WEBHOOK = "discord_webhook"


class AlertSource(Base):
    """A subscription: which live log stream a Matcher listens to."""

    __tablename__ = "alert_sources"
    __table_args__ = (
        UniqueConstraint(
            "domain", "selector", name="uq_alert_sources_domain_selector"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_by: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        index=True,
    )
    domain: Mapped[str] = mapped_column(String(8), index=True)
    #: ``*``, a qualified type (``private::Tiny``), a TD ``api_id``, or a
    #: venue name. A hex session id is legal and never matches.
    selector: Mapped[str] = mapped_column(String(128))

    creator = relationship("User", back_populates="alert_sources")


class AlertMatcher(Base):
    """A judgement: level, regex, or extract."""

    __tablename__ = "alert_matchers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_by: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        index=True,
    )
    name: Mapped[str] = mapped_column(String(128), unique=True)
    kind: Mapped[str] = mapped_column(String(16))
    spec: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)

    creator = relationship("User", back_populates="alert_matchers")


class Alert(Base):
    """A Discord webhook sink plus the fire policy."""

    __tablename__ = "alerts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_by: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        index=True,
    )
    name: Mapped[str] = mapped_column(String(128), unique=True)
    kind: Mapped[str] = mapped_column(
        String(32), default=AlertKind.DISCORD_WEBHOOK.value
    )
    webhook_url: Mapped[str] = mapped_column(Text)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    flush_interval_s: Mapped[int] = mapped_column(Integer, default=30)
    max_events_in_payload: Mapped[int] = mapped_column(Integer, default=15)
    max_buffer_events: Mapped[int] = mapped_column(Integer, default=200)
    dedupe: Mapped[bool] = mapped_column(Boolean, default=True)

    creator = relationship("User", back_populates="alerts")
    deliveries = relationship(
        "AlertDelivery",
        back_populates="alert",
        cascade="all, delete-orphan",
    )


class AlertSourceMatcher(Base):
    """Source → Matcher. Composite PK, cascade both ways."""

    __tablename__ = "alert_source_matcher"

    source_id: Mapped[int] = mapped_column(
        ForeignKey("alert_sources.id", ondelete="CASCADE"),
        primary_key=True,
    )
    matcher_id: Mapped[int] = mapped_column(
        ForeignKey("alert_matchers.id", ondelete="CASCADE"),
        primary_key=True,
    )


class AlertMatcherAlert(Base):
    """Matcher → Alert. Composite PK, cascade both ways."""

    __tablename__ = "alert_matcher_alert"

    matcher_id: Mapped[int] = mapped_column(
        ForeignKey("alert_matchers.id", ondelete="CASCADE"),
        primary_key=True,
    )
    alert_id: Mapped[int] = mapped_column(
        ForeignKey("alerts.id", ondelete="CASCADE"),
        primary_key=True,
    )


class AlertDelivery(Base):
    """One quiesce-window POST. Deleted with the Alert."""

    __tablename__ = "alert_deliveries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    alert_id: Mapped[int] = mapped_column(
        ForeignKey("alerts.id", ondelete="CASCADE"),
        index=True,
    )
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    event_count: Mapped[int] = mapped_column(Integer)
    dropped_count: Mapped[int] = mapped_column(Integer, default=0)
    http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    #: Short failure. Never the webhook URL.
    error: Mapped[str | None] = mapped_column(String(256), nullable=True)
    ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    alert = relationship("Alert", back_populates="deliveries")
