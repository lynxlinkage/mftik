"""Login sessions, and the machine credentials the Owner issues.

Opaque rows rather than JWTs: logging out, and later a single-seat rule, are
a delete here instead of a revocation list the token format cannot express.

``session`` already means a running strategy or trading session everywhere
else in this schema (``sts_sessions``, ``td_sessions``, ``md_sessions``), and
``mftik_db.session`` is the SQLAlchemy one. This table is the browser kind, and
the ``auth_`` prefix is the only thing keeping them apart — keep it.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from sqlalchemy import (
    JSON,
    DateTime,
    ForeignKey,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from mftik_db.models.base import Base


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


class KeyKind(StrEnum):
    """Who a key is for. It decides the wire prefix and the scopes."""

    #: The Owner's own scripts and CI. Acts as the Owner everywhere except
    #: the routes that change who the Owner is or mint more keys.
    API = "api"
    #: Another MFTIK node. Reads this node's published strategies and nothing
    #: else — it does not stand for a person at all.
    REGISTRY = "registry"


class AuthKey(Base):
    """A bearer credential, stored the way a password would be.

    The secret is returned once, by the call that mints it, and never again:
    what lives here is a SHA-256 and a short prefix. That prefix is not
    decoration — it is how a presented token finds its row in one indexed
    lookup instead of a scan over every hash.
    """

    __tablename__ = "auth_keys"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        index=True,
    )
    kind: Mapped[str] = mapped_column(String(16))
    #: What the Owner called it. The only way to tell two keys apart in a UI
    #: that can never show either of them.
    name: Mapped[str] = mapped_column(String(64))
    #: First characters of the random part, unique so a lookup is exact.
    prefix: Mapped[str] = mapped_column(String(16), unique=True, index=True)
    key_hash: Mapped[str] = mapped_column(Text())
    #: Resolved from ``kind`` at mint time and stored, so a key keeps the
    #: powers it was issued with even if the mapping is widened later.
    scopes: Mapped[list[str]] = mapped_column(JSON, default=list)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    #: Throttled the same way as a session's — see ``TOUCH_INTERVAL``.
    last_used_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    #: Revoking is a timestamp, not a delete: the audit trail refers to keys
    #: by id, and a deleted row turns those lines into dangling numbers.
    revoked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    user = relationship("User", back_populates="auth_keys")


class AuthIdentity(Base):
    """A way of proving you are the Owner that some other service vouches for.

    Keyed on ``(provider, subject)`` — the provider's own stable id, never an
    email. Addresses change hands and get recycled; a Discord snowflake and a
    Google ``sub`` do not, and keying on the mutable one would mean whoever
    inherits an address inherits the node.

    A row here is only ever created by a Connect performed from a live
    session. Nothing about an OAuth callback can create a ``users`` row, which
    is what keeps a single-tenant instance single-tenant.
    """

    __tablename__ = "auth_identities"
    __table_args__ = (UniqueConstraint("provider", "subject", name="uq_identity"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        index=True,
    )
    provider: Mapped[str] = mapped_column(String(32))
    subject: Mapped[str] = mapped_column(String(128))
    #: What to show so the Owner can see *which* account is attached. A bare
    #: "connected" does not let anyone notice the wrong one is.
    label: Mapped[str | None] = mapped_column(String(128), nullable=True)
    #: Taken from the profile for display only. Never matched on.
    email: Mapped[str | None] = mapped_column(String(320), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    user = relationship("User", back_populates="auth_identities")


class AuthOAuthState(Base):
    """Everything a callback needs, kept where the browser cannot edit it.

    ``state`` on the wire is an unguessable nonce and nothing else. Whether a
    callback is a login or a link, which provider it belongs to, and which
    session started it are all read back from here.

    A readable ``state=connect`` would be an account takeover: anyone could
    walk the Owner's browser into a callback carrying *their* code and have
    their account linked to the Owner, then log in with it forever after.
    """

    __tablename__ = "auth_oauth_states"

    state: Mapped[str] = mapped_column(String(64), primary_key=True)
    provider: Mapped[str] = mapped_column(String(32))
    #: ``login`` or ``connect``. The authority on which this is.
    mode: Mapped[str] = mapped_column(String(16))
    #: PKCE. Belt and braces over a confidential client and this nonce, but
    #: cheap, and the thing OAuth 2.1 asks every client for.
    verifier: Mapped[str] = mapped_column(String(128))
    #: Set for a connect: the session that started it, which the callback must
    #: be presented by. Without this, a link started in one browser could be
    #: completed in another.
    session_id: Mapped[str | None] = mapped_column(
        ForeignKey("auth_sessions.id", ondelete="CASCADE"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    #: Minutes, not hours. A flow nobody completed is litter.
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
