"""Deployed strategy.yml records — join to sts_sessions for runtime status."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import JSON, DateTime, ForeignKey, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from mft_db.models.base import Base


class StrategyRow(Base):
    """One row per strategy.yml deploy (type + config snapshot)."""

    __tablename__ = "strategies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    type: Mapped[str] = mapped_column(String(128), index=True)
    #: The strategy.yml exactly as submitted — the only record of what a person
    #: wrote, comments and all. Everything else here and on the session row is
    #: derived from it at deploy time and never edited afterwards, so the two
    #: cannot drift. Null for deploys made before this column existed; those
    #: can only be reconstructed.
    yaml_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    #: Parsed ``sts:`` block. Derived from :attr:`yaml_text`; kept structured
    #: because that is the form the runtime is handed.
    config: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_by: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    sts_session: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("sts_sessions.session_id", ondelete="CASCADE"),
        unique=True,
        index=True,
    )

    creator = relationship("User", back_populates="strategies")
    session = relationship("StsSessionRow", back_populates="strategy_row")
