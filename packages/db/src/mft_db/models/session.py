"""Per-domain control-plane session tables (sts / td / md)."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import JSON, DateTime, ForeignKey, Integer, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from mft_db.models.base import Base


class SessionDomain(StrEnum):
    """Logical domain labels used by protocol / stats."""

    STS = "sts"
    TD = "td"
    MD = "md"


class SessionStatus(StrEnum):
    LIVE = "live"
    DONE = "done"


class StsSessionRow(Base):
    """STS strategy session record."""

    __tablename__ = "sts_sessions"

    session_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    created_by: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    status: Mapped[str] = mapped_column(
        String(16),
        default=SessionStatus.LIVE.value,
        index=True,
    )
    strategy: Mapped[str | None] = mapped_column(String(128), nullable=True)
    td_api_ids: Mapped[list[Any]] = mapped_column(JSON, default=list)
    md_ids: Mapped[list[Any]] = mapped_column(JSON, default=list)
    st_paras: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)

    creator = relationship("User", back_populates="sts_sessions")


class TdSessionRow(Base):
    """TD trading attach record — one row per (session_id, api_id)."""

    __tablename__ = "td_sessions"
    __table_args__ = (
        UniqueConstraint("session_id", "api_id", name="uq_td_sessions_session_api"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    session_id: Mapped[str] = mapped_column(String(64), index=True)
    api_id: Mapped[int] = mapped_column(Integer, index=True)
    created_by: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    status: Mapped[str] = mapped_column(
        String(16),
        default=SessionStatus.LIVE.value,
        index=True,
    )

    creator = relationship("User", back_populates="td_sessions")


class MdSessionRow(Base):
    """MD market-data session record (placeholder until MD sessions exist)."""

    __tablename__ = "md_sessions"

    session_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    created_by: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    status: Mapped[str] = mapped_column(
        String(16),
        default=SessionStatus.LIVE.value,
        index=True,
    )

    creator = relationship("User", back_populates="md_sessions")
