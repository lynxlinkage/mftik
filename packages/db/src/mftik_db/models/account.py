"""Trading accounts — 1-1 with venue API credentials."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from mftik_db.models.base import Base


class Account(Base):
    """One trading account bound to exactly one :class:`Api` credential."""

    __tablename__ = "accounts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    api_id: Mapped[int] = mapped_column(
        ForeignKey("apis.id", ondelete="CASCADE"),
        unique=True,
        index=True,
    )
    created_by: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    api = relationship("Api", back_populates="account", uselist=False)
    creator = relationship("User", back_populates="accounts")
