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
    Text,
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
    #: Qualified registry key — ``CrossArb``, ``private::Tiny``,
    #: ``node1::Tiny``. Not the same fact as :attr:`strategy`, which holds
    #: the short ``Strategy.name``. ``list_live_for_origin`` prefix-matches
    #: this on ``{origin}::`` to refuse deleting a registry entry a live
    #: session is using. Null when a deploy failed before the document was
    #: recorded.
    type: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    #: The strategy.yml exactly as submitted — the only record of what a
    #: person wrote, comments and all. Null for deploys that never got that
    #: far.
    yaml_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    #: The 16-bit slot packed into every ``client_order_id`` this session
    #: mints. Persisted so a rebuilt session can keep it: ``Strategy.owns()``
    #: matches orders by slot, so a new one would leave the strategy unable to
    #: recognise the orders it placed before the restart. Null for rows
    #: written before this was recorded.
    cid_slot: Mapped[int | None] = mapped_column(Integer, nullable=True)
    #: Which STS was asked to run this. Nullable: a row written before
    #: instances existed was not pinned, and an unpinned deploy still is not.
    #: The rebuild scan filters on it, so a run pinned to ``sts-tw`` comes back
    #: on ``sts-tw`` or does not come back — a session pinned to an instance
    #: nobody runs stays ``interrupted`` and waits for a person rather than
    #: silently moving. A plain string, not a foreign key: this is history, and
    #: retiring an instance must not break the record of what it did.
    instance: Mapped[str | None] = mapped_column(
        String(64), nullable=True, index=True
    )
    #: ``always`` | ``never`` — whether this run asked to be restored after an
    #: STS restart. A property of the deploy, not of the strategy class or of
    #: whoever configured the process.
    restart: Mapped[str] = mapped_column(String(8), default="always")
    #: How many times a rebuild has been attempted. Counted before the attempt
    #: rather than after it, so a rebuild that takes the process down with it
    #: still counts — that is the loop the cap exists to break.
    rebuild_count: Mapped[int] = mapped_column(Integer, default=0)
    #: Account name → ``{api_id, settings}``. The attach list the UI still
    #: calls ``td_api_ids`` is derived from this.
    td: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    #: Instance name → feed keys, with ``"*"`` for feeds the deploy did not
    #: pin. Read it with ``md_feeds_of`` / ``load_md`` and never by iterating:
    #: a mapping iterated yields its keys, which is how the strategy list came
    #: to render every session's feeds as the single string ``"*"``.
    #:
    #: ``dict | list`` because a row written before instances holds the list,
    #: and the readers take either. The column keeps its name: it is what the
    #: board and the API still call this field, and renaming a JSON column
    #: costs a migration to say nothing new.
    md_ids: Mapped[dict[str, Any] | list[Any]] = mapped_column(
        JSON, default=dict
    )
    st_paras: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    #: What ``Strategy.remember()`` wrote — facts established while running
    #: that cannot be re-derived from ``st_paras`` or from TD reconciliation,
    #: like the price a chase anchored its slippage guard on. Kept apart from
    #: ``st_paras`` so configuration and runtime facts do not blur together.
    st_facts: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)

    creator = relationship("User", back_populates="sts_sessions")

    @property
    def td_api_ids(self) -> list[int]:
        """Derived attach ids, in mapping insertion order."""
        refs = self.td or {}
        out: list[int] = []
        for value in refs.values():
            if isinstance(value, dict):
                out.append(int(value["api_id"]))
            else:
                out.append(int(getattr(value, "api_id", value)))
        return out


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
        # Instance leads because a session's feeds may be split across MDs, and
        # two instances serving one venue for one session is the arrangement
        # this exists to allow rather than an accident to refuse. A genuine
        # duplicate — the same instance twice — is still refused.
        UniqueConstraint(
            "instance",
            "venue",
            "session_id",
            name="uq_md_sessions_instance_venue_session",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    #: Which MD holds this attach. A plain string rather than a foreign key:
    #: it is history, it records the name as it was at the time, and retiring
    #: an instance must not break the rows describing what it did — the same
    #: reason :attr:`venue` is a string.
    instance: Mapped[str] = mapped_column(
        String(64), default="md", index=True
    )
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
