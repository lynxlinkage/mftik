"""Control-plane tables added for the process planes (§8.4).

The DB is the store. It is not the authority for any of these rows (§3.3):
each table names, on the class, the one writer that is. Nothing here is a
registry, a code version, or an artifact — those stay on the host disk
(F39, F40) and out of this schema.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import JSON, DateTime, Index, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from mftik_db.models.base import Base


class MdIntent(Base):
    """One session's MD demand on one MD instance (§8.4, F38).

    Authority is shared by the writers in §3.3 — the API at start, the STS
    controller when it heals, the session worker through ``md.intent.patch``
    — and the MD controller is the reader. ``instance`` is that MD, the same
    word as ``md_sessions.instance``: ``td_intents`` keys the same idea by
    ``api_id``, and this table keys it by the MD the feeds are pinned to.
    The STS owner ``(sts_instance, session_id)`` (§8.2) is not a column.
    ``session_id`` is the session's primary key, and ``sts_sessions.instance``
    is the STS instance.

    Identity is ``(session_id, instance)``. F38 keeps the row when the
    session ends and sets :attr:`released_at`; it does not insert a second
    row, and a later ``put`` clears ``released_at`` on this one. ``generation``
    is the intent's own generation, bumped when ``feeds`` change. It is not
    the MD controller's ``(controller_epoch, seq)`` (F18), which stays in
    memory.

    ``feeds`` is the declaration for this MD (feed keys and ``select:``
    blocks). ``atoms`` is the ``{feed: [atom_id]}`` map MD fills in when it
    accepts the intent (§6.1). ``{}`` means nothing has been resolved.
    """

    __tablename__ = "md_intents"
    __table_args__ = (Index("ix_md_intents_instance", "instance"),)

    session_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    instance: Mapped[str] = mapped_column(String(64), primary_key=True)
    feeds: Mapped[list[Any]] = mapped_column(JSON, default=list, nullable=False)
    atoms: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    generation: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    released_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class TdIntent(Base):
    """One session's claim on one account (§8.4, F38).

    The API and the STS controller write it; the TD controller reads it
    (§3.3). ``api_id`` is the account, not a foreign key — the same choice
    ``td_sessions`` makes, so a retired credential does not take the history
    with it. There is no ``instance`` column: the TD instance is
    ``apis.instance_id``, and the STS owner is ``sts_sessions.instance``
    joined through ``session_id`` (§8.2).

    Identity is ``(session_id, api_id)``. Ending the session sets
    :attr:`released_at` and leaves the row (F38). Refcount for the trading
    layer is the count of rows for that ``api_id`` whose ``released_at``
    is null (F35); this table does not store the count.
    """

    __tablename__ = "td_intents"
    __table_args__ = (Index("ix_td_intents_api_id", "api_id"),)

    session_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    api_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    released_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class MdStandingSubscription(Base):
    """MD demand that no session owns (§6.2, §3.3).

    Authority is the config file. This row is the copy the MD controller
    reads when it rebuilds desired, including after a restart (§6.2). It
    is not an intent: no ``session_id``, no ``released_at`` (F38 is session
    history). ``tape_keeper`` is what this replaces, and B8-05 is what
    starts writing it.

    ``instance`` is the MD the declaration is pinned to. ``declaration``
    is that instance's ``md:`` fragment — feed keys and ``select:`` blocks,
    the list §6.4 already uses — stored as JSON so this table does not
    invent a second document shape. One row per fragment, surrogate
    ``id``, because §8.4 names the table and not a natural key: two
    fragments on one MD both have to fit, so ``instance`` is not unique.
    """

    __tablename__ = "md_standing_subscriptions"
    __table_args__ = (
        Index("ix_md_standing_subscriptions_instance", "instance"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    instance: Mapped[str] = mapped_column(String(64), nullable=False)
    declaration: Mapped[list[Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class MdSelectorState(Base):
    """The previous selector answer, keyed by spec hash (§6.4, §8.4, F33).

    The MD controller is the only writer (§3.3). It reads the row back as
    ``prev`` so a restart continues the universe instead of recentering.
    Owners that share a spec hash share this one row.

    Columns are the tuple §8.4 lists and nothing else. ``universe`` and
    ``center`` are JSON because the plan names them and not their document:
    B9 decides what a selected contract looks like, and what option-chain
    centering stores versus a rolling future's current. ``epoch`` counts
    universe changes. ``center`` null means this spec has never been
    centered.
    """

    __tablename__ = "md_selector_state"

    spec_hash: Mapped[str] = mapped_column(String(128), primary_key=True)
    universe: Mapped[list[Any]] = mapped_column(JSON, default=list, nullable=False)
    epoch: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    center: Mapped[Any | None] = mapped_column(JSON, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
