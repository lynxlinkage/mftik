"""Declared plane instances — what should be running, named."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from mftik_db.models.base import Base


class Instance(Base):
    """One declared process of a plane — ``td-jp-1``, ``md-jp-2``, ``sts-tw``.

    A row here says an instance *should* exist. Whether it does is a separate
    question, answered by probing ``{plane}.{name}`` and never by this table.
    The pair is the point: a declared row that does not answer reads *down*,
    and this row is the only thing that remembers it should be there at all.
    Without it an instance that dies simply vanishes from the dashboard.

    ``name`` is immutable, and there is no rename. A process learns its name
    from ``MFTIK_INSTANCE`` in a compose file on the host, which the API has
    never read and cannot write, so a row renamed here would not reach the
    process that answers to it. That is the opposite of :class:`Account`, whose
    name *is* renameable precisely because it is a lookup label resolved to an
    ``api_id`` at deploy — an instance name is the address itself. See
    ``docs/Instances.md``.
    """

    __tablename__ = "instances"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    #: ``MFTIK_INSTANCE``. Unique across planes rather than per plane: a ``td``
    #: and an ``md`` both called ``jp-1`` would be two rows a person has to
    #: read the domain column to tell apart, on a page whose job is to be
    #: unambiguous at a glance.
    name: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    #: ``td`` / ``md`` / ``sts`` — see :class:`SessionDomain`. Not ``sym`` or
    #: ``paper``: neither is instanced. SYM is off the hot path behind
    #: ``SymbolClient``'s cache, and one shared book is the whole point of
    #: paper.
    domain: Mapped[str] = mapped_column(String(16), index=True)
    #: Operator label, and the axis unnamed STS sessions are placed on.
    #: A credential's TD instance has a region; the unique enabled STS in
    #: that region is who rebuilds a null row and who receives an unnamed
    #: create. Editing a TD's region moves where those sessions come back.
    #: Declared rather than reported because a process put in the wrong
    #: datacentre would report whatever its environment says rather than
    #: where it is.
    region: Mapped[str | None] = mapped_column(String(64), nullable=True)
    #: Retire an instance without deleting the rows that reference it. Drains
    #: rather than evicts: new deploys refuse to name it, sessions already
    #: attached keep running.
    enabled: Mapped[bool] = mapped_column(
        Boolean, default=True, nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    #: Nullable, unlike every other operator-created table. The migration that
    #: creates this table also seeds ``td`` / ``md`` / ``sts``, and it runs
    #: before ``seed`` has created the Owner — so there is no ``users`` row to
    #: point at on an empty database, and a ``NOT NULL`` here would fail the
    #: upgrade on exactly the deployment that has never been upgraded before.
    #: Null is also the honest value: nobody created those three.
    #:
    #: ``SET NULL`` rather than ``CASCADE``. Deleting a user must not delete
    #: the infrastructure they happened to declare.
    created_by: Mapped[int | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
