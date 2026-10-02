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
    """STS strategy session record, Spec and Status on one row (P2, §3.3).

    The API is the only writer of Spec: the deploy, ``restart``, and
    ``generation``. The STS controller's Supervisor is the only writer of
    Status: phase (``status``, ``reason``), ``observed_generation``,
    ``worker_incarnation``, ``conditions``, ``restart_count``. Readers —
    API, UI, CLI — do not write those. ``strategy_digest`` and
    ``env_generation`` are Spec too (F39): the API writes them at start,
    and the controller only reads them.
    """

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
    #: Qualified registry key — ``CrossArb``, ``private::Tiny``,
    #: ``node1::Tiny``. The only strategy identity on the row.
    #: ``list_live_for_origin`` prefix-matches this on ``{origin}::`` to
    #: refuse deleting a registry entry a live session is using. Null when
    #: a deploy failed before the document was recorded, or when a finished
    #: row predates the key and its short name did not map to a bundled class.
    type: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    #: The short ``Strategy.name`` a finished row was deployed under, kept by
    #: ``0034_strategy_type_key`` for the rows it could not name. History, not
    #: identity: nothing resolves a strategy from this, nothing routes on it,
    #: and no new row ever writes it. It exists because the alternative was to
    #: drop the column that held it and leave those sessions labelled with
    #: nothing at all — a deploy is not a reason for history to forget what
    #: ran. Null everywhere else, which is everywhere that ``type`` answers.
    legacy_strategy: Mapped[str | None] = mapped_column(
        String(128), nullable=True
    )
    #: The strategy.yml exactly as submitted — the only record of what a
    #: person wrote, comments and all. Null for deploys that never got that
    #: far.
    yaml_text: Mapped[str | None] = mapped_column(Text, nullable=True)
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
    #: ``never`` | ``on_failure`` (F11). Default ``never``: a crash ends the
    #: session. ``on_failure`` may start a fresh run from ``on_start``, and
    #: only for an A-class crash inside the document's ``max_restarts`` /
    #: ``restart_window_s`` — those limits are not columns. Width 16 because
    #: ``on_failure`` does not fit in the original 8. A row from before F11
    #: may still say ``always``, which is no longer a policy.
    restart: Mapped[str] = mapped_column(String(16), default="never")
    #: Spec generation (P2, §8.1). The API writes ``1`` at start and bumps
    #: it when the spec changes. The controller does not. Existing rows are
    #: backfilled to ``1``: the stored spec is generation 1 of itself.
    #: Not the extras pin: that is :attr:`env_generation`.
    generation: Mapped[int] = mapped_column(
        Integer, default=1, server_default="1", nullable=False
    )
    #: Spec (F39). The strategy-tree digest pinned at start,
    #: ``sha256:`` plus 64 hex characters. Null for a built-in strategy,
    #: whose code is the platform release, and for every row written
    #: before 0036. The API writes it. The controller reads it and does
    #: not update it; a rehang uses this value, not the registry index.
    strategy_digest: Mapped[str | None] = mapped_column(String(71), nullable=True)
    #: Spec (F39). The extras generation (``env/gen-{N}``) pinned at start.
    #: Null when the session has no extras pin, and on rows from before
    #: 0036. Not :attr:`generation`.
    env_generation: Mapped[int | None] = mapped_column(Integer, nullable=True)
    #: Status. The generation the STS controller has reconciled to (P2's
    #: observedGeneration). Null until it records one — not ``0``, which
    #: would claim a generation that was never written.
    observed_generation: Mapped[int | None] = mapped_column(Integer, nullable=True)
    #: Status. The incarnation the controller assigned to the worker
    #: (§4.3, §5.2). Null until a worker exists. The controller is the only
    #: writer (§3.3); a restart uses the previous value plus one.
    worker_incarnation: Mapped[int | None] = mapped_column(Integer, nullable=True)
    #: Status. Readiness and start progress — ``MdReady``, ``TdReady``, and
    #: the lines the board shows (§5.2, §5.6). The STS controller's
    #: Supervisor is the only writer (§3.3). An empty object is "nothing
    #: reported yet"; the document shape inside the object belongs to the
    #: controller, not to this column.
    conditions: Mapped[dict[str, Any]] = mapped_column(
        JSON, default=dict, server_default="{}", nullable=False
    )
    #: Status. How many F11 restarts this session has used. The Supervisor
    #: writes it (§5.2). Starts at ``0``. ``0037_drop_rebuild_facts`` dropped
    #: the old ``rebuild_count`` column without copying it: that counter
    #: belonged to a mechanism RM-01 already removed, and this one counts a
    #: different event.
    restart_count: Mapped[int] = mapped_column(
        Integer, default=0, server_default="0", nullable=False
    )
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
    """TD trading attach record — one row per (session_id, api_id).

    Read-only history from B10-01 (F38, §8.4). The table stays so a cutover
    can still list what was attached before intents existed. Nothing in the
    application inserts or updates a row; ``TdSessionRepository`` only reads.
    """

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
    """MD attach record — one row per (venue, STS session_id).

    Read-only history from B10-01 (F38, §8.4), same as :class:`TdSessionRow`.
    ``MdSessionRepository`` only reads.
    """

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
