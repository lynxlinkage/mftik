from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Float, Index, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from mftik_db.models.base import Base

# SQLite only autoincrements INTEGER PRIMARY KEY; Postgres keeps BIGINT.
_PK = BigInteger().with_variant(Integer, "sqlite")


class SessionLog(Base):
    """Append-only persisted session log line (STS / TD / MD)."""

    __tablename__ = "session_logs"
    __table_args__ = (
        Index(
            "ix_session_logs_domain_stream_ts_id",
            "domain",
            "stream_id",
            "ts",
            "id",
        ),
    )

    id: Mapped[int] = mapped_column(_PK, primary_key=True, autoincrement=True)
    envelope_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    domain: Mapped[str] = mapped_column(String(8), nullable=False)
    stream_id: Mapped[str] = mapped_column(String(128), nullable=False)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    level: Mapped[str] = mapped_column(String(16), nullable=False)
    message: Mapped[str] = mapped_column(Text(), nullable=False)
    ts: Mapped[float] = mapped_column(Float(), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
