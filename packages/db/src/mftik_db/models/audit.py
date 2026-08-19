from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from mftik_db.models.base import Base


class Audit(Base):
    """Append-only audit log entry."""

    __tablename__ = "audits"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        index=True,
    )
    #: How the request was proved. Same vocabulary as ``Principal.via``.
    #: Null on rows written before this column existed.
    via: Mapped[str | None] = mapped_column(String(64), nullable=True)
    #: The machine credential that acted, if one did. SET NULL rather than
    #: CASCADE: deleting a key must not take the trail with it.
    key_id: Mapped[int | None] = mapped_column(
        ForeignKey("auth_keys.id", ondelete="SET NULL"),
        nullable=True,
    )
    #: ``api`` or ``registry``, snapshotted so the display does not join.
    key_kind: Mapped[str | None] = mapped_column(String(16), nullable=True)
    operation: Mapped[str] = mapped_column(String(128))
    result: Mapped[str] = mapped_column(Text())
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        index=True,
    )

    user = relationship("User", back_populates="audits")
