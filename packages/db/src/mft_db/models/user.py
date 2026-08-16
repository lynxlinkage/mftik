from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from mft_db.models.base import Base


class User(Base):
    """The Owner. An instance has one, and auth exists to prove you are it."""

    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    #: Login identifier. Nullable because the row can predate its credentials
    #: — ``seed`` creates the Owner so foreign keys resolve, and setup is what
    #: gives it a way to log in. Unique still holds for the one real username;
    #: Postgres does not count NULLs against a unique index.
    username: Mapped[str | None] = mapped_column(
        String(64), unique=True, index=True, nullable=True
    )
    #: argon2id. NULL means "setup has not run", which is what
    #: ``GET /auth/status`` reports as ``setup_required``.
    password_hash: Mapped[str | None] = mapped_column(Text(), nullable=True)
    #: Display only, and optional — never a join key, never how an OAuth
    #: identity is matched to this row. Addresses change hands; subjects do not.
    email: Mapped[str | None] = mapped_column(
        String(320), unique=True, index=True, nullable=True
    )
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
    strategies = relationship(
        "StrategyRow", back_populates="creator", cascade="all, delete-orphan"
    )
    accounts = relationship(
        "Account", back_populates="creator", cascade="all, delete-orphan"
    )
    auth_sessions = relationship(
        "AuthSession", back_populates="user", cascade="all, delete-orphan"
    )
    auth_keys = relationship(
        "AuthKey", back_populates="user", cascade="all, delete-orphan"
    )
