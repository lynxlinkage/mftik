"""Per-domain control-plane session tables (sts / td / md)."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    JSON,
    DateTime,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from mftik_db.models.base import Base


class SessionDomain(StrEnum):
    """Logical domain labels used by protocol / stats."""

    STS = "sts"
    TD = "td"
    MD = "md"


class SessionStatus(StrEnum):
    LIVE = "live"
    #: The session reached its own end — the strategy decided it was finished.
    DONE = "done"
    #: Terminal, but not a natural end — the session stopped because something
    #: went wrong. Only ``sts_sessions`` records this; td/md rows follow their
    #: owning strategy session and stay live/done.
    FAILED = "failed"
    #: Cut short by STS going down, with nothing wrong with the strategy
    #: itself. Kept apart from ``done`` because it answers a question no other
    #: status can: *this one did not choose to stop*, so it is the set a
    #: rebuild-on-restart would draw from. Recording it is all this does —
    #: rebuilding is per-strategy work that does not exist yet.
    INTERRUPTED = "interrupted"
    #: An operator looked at a failed or interrupted session and marked it
    #: seen. The original reason stays; rebuild does not draw from this.
    ACK = "ack"

    @classmethod
    def terminal(cls) -> frozenset[str]:
        """Statuses a session never leaves.

        Exists so "has it ended" is asked in one place rather than spelled as
        ``!= live`` or, worse, ``== done`` — the latter silently skips every
        status added later, which is how ``failed`` sessions first went
        missing from the dashboard.
        """
        return frozenset(
            {
                cls.DONE.value,
                cls.FAILED.value,
                cls.INTERRUPTED.value,
                cls.ACK.value,
            }
        )


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
    #: Why the session ended. Carries the exit reason for ``failed``; left
    #: null for a session that is still live or ended naturally.
    reason: Mapped[str | None] = mapped_column(String(256), nullable=True)
    strategy: Mapped[str | None] = mapped_column(String(128), nullable=True)
    #: The 16-bit slot packed into every ``client_order_id`` this session
    #: mints. Persisted so a rebuilt session can keep it: ``Strategy.owns()``
    #: matches orders by slot, so a new one would leave the strategy unable to
    #: recognise the orders it placed before the restart. Null for rows
    #: written before this was recorded.
    cid_slot: Mapped[int | None] = mapped_column(Integer, nullable=True)
    #: ``always`` | ``never`` — whether this run asked to be restored after an
    #: STS restart. A property of the deploy, not of the strategy class or of
    #: whoever configured the process.
    restart: Mapped[str] = mapped_column(String(8), default="always")
    #: How many times a rebuild has been attempted. Counted before the attempt
    #: rather than after it, so a rebuild that takes the process down with it
    #: still counts — that is the loop the cap exists to break.
    rebuild_count: Mapped[int] = mapped_column(Integer, default=0)
    td_api_ids: Mapped[list[Any]] = mapped_column(JSON, default=list)
    md_ids: Mapped[list[Any]] = mapped_column(JSON, default=list)
    st_paras: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    #: What ``Strategy.remember()`` wrote — facts established while running
    #: that cannot be re-derived from ``st_paras`` or from TD reconciliation,
    #: like the price a chase anchored its slippage guard on. Kept apart from
    #: ``st_paras`` so configuration and runtime facts do not blur together.
    st_facts: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)

    creator = relationship("User", back_populates="sts_sessions")
    strategy_row = relationship(
        "StrategyRow",
        back_populates="session",
        uselist=False,
    )


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
    """MD attach record — one row per (venue, STS session_id)."""

    __tablename__ = "md_sessions"
    __table_args__ = (
        UniqueConstraint(
            "venue", "session_id", name="uq_md_sessions_venue_session"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    venue: Mapped[str] = mapped_column(String(64), index=True)
    session_id: Mapped[str] = mapped_column(String(64), index=True)
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
