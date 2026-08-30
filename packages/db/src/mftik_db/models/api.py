from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from sqlalchemy import DateTime, ForeignKey, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from mftik_db.models.base import Base


class ApiType(StrEnum):
    """Venue API credential algorithm / auth style."""

    HMAC = "HMAC"
    ED25519 = "ED25519"


class Api(Base):
    """Exchange / venue API credential owned by a user.

    Uniqueness is ``(venue, api_key)``, not the key string alone. Binance
    issues one key for spot, USD-M and COIN-M; each plane is its own venue
    and needs its own row, so the same key must be allowed on more than one.
    """

    __tablename__ = "apis"
    __table_args__ = (
        UniqueConstraint("venue", "api_key", name="uq_apis_venue_api_key"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    owner_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        index=True,
    )
    venue: Mapped[str] = mapped_column(String(64), index=True)
    api_key: Mapped[str] = mapped_column(String(256), index=True)
    api_secret: Mapped[str] = mapped_column(Text())
    type: Mapped[str] = mapped_column(String(32), default=ApiType.HMAC.value)
    passphrase: Mapped[str | None] = mapped_column(String(256), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    owner = relationship("User", back_populates="apis")
    account = relationship(
        "Account",
        back_populates="api",
        uselist=False,
        cascade="all, delete-orphan",
        single_parent=True,
    )
