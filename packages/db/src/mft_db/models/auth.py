"""Login sessions.

Opaque rows rather than JWTs: logging out, and later a single-seat rule, are
a delete here instead of a revocation list the token format cannot express.

``session`` already means a running strategy or trading session everywhere
else in this schema (``sts_sessions``, ``td_sessions``, ``md_sessions``), and
``mft_db.session`` is the SQLAlchemy one. This table is the browser kind, and
the ``auth_`` prefix is the only thing keeping them apart — keep it.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from mft_db.models.base import Base


class AuthSession(Base):
    __tablename__ = "auth_sessions"

    #: SHA-256 of the cookie, never the cookie. A database dump is then not a
    #: set of live credentials, which is the same reason API keys are stored
    #: hashed — the difference is only that nobody ever reads this one back.
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        index=True,
    )
    #: Which identity proved it: ``password`` today, a provider name later.
    #: Carried into audits so the trail can tell logins apart.
    via: Mapped[str] = mapped_column(String(32), default="password")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    #: Slides on use, but not on every request — see ``TOUCH_INTERVAL``.
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    #: The sliding idle deadline. The absolute cap is derived from
    #: ``created_at`` instead, so a session cannot be renewed forever.
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    user_agent: Mapped[str | None] = mapped_column(String(256), nullable=True)
    ip: Mapped[str | None] = mapped_column(String(64), nullable=True)

    user = relationship("User", back_populates="auth_sessions")
