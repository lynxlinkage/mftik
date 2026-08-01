from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from mft_db.models.base import Base


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    email: Mapped[str] = mapped_column(String(320), unique=True, index=True)
    display_name: Mapped[str] = mapped_column(String(128), default="")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    apis = relationship("Api", back_populates="owner", cascade="all, delete-orphan")
    audits = relationship("Audit", back_populates="user", cascade="all, delete-orphan")
    sts_sessions = relationship(
        "StsSessionRow", back_populates="creator", cascade="all, delete-orphan"
    )
    td_sessions = relationship(
        "TdSessionRow", back_populates="creator", cascade="all, delete-orphan"
    )
    md_sessions = relationship(
        "MdSessionRow", back_populates="creator", cascade="all, delete-orphan"
    )
