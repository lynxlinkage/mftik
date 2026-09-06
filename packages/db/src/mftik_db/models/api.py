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

    ``venue`` holds the canonical registry spelling — writers resolve it
    through ``venues`` first, and 0028 folded the older rows — because the
    constraint compares it exactly while venue identity is otherwise
    case-insensitive. :meth:`ApiRepository.get_by_venue_and_api_key` matches
    case-blind so a stray spelling is still caught before a write.
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
    #: Which TD instance may use this credential. ``NOT NULL``: a credential is
    #: bound to a region as a matter of fact, and TD has no anycast subject to
    #: fall through to — there is no "unassigned credential" state. A foreign
    #: key rather than a name string, because a typo in a free-text column is a
    #: credential that silently never attaches.
    #:
    #: ``RESTRICT``: retiring an instance a credential still points at is
    #: refused, not cascaded. See ``docs/Instances.md``.
    instance_id: Mapped[int] = mapped_column(
        # Named so ``create_all`` and migration 0031 build the same constraint.
        # Left to autogenerate they differ, and a schema built one way cannot
        # be migrated by code written for the other.
        ForeignKey(
            "instances.id",
            ondelete="RESTRICT",
            name="fk_apis_instance_id",
        ),
        index=True,
    )
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
    instance = relationship("Instance", uselist=False)
    account = relationship(
        "Account",
        back_populates="api",
        uselist=False,
        cascade="all, delete-orphan",
        single_parent=True,
    )
