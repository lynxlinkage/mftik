"""Generic message envelope — the wire wrapper for all broker messages.

Usage::

    env = Envelope[Heartbeat].wrap(
        Heartbeat(status="ok"),
        type="heartbeat",
        source="md",
    )
    raw = env.to_json()
    restored = Envelope[Heartbeat].from_json(raw)
"""

from __future__ import annotations

import time
import uuid
from typing import Any, Generic, Self, TypeVar

from pydantic import BaseModel, ConfigDict, Field

PayloadT = TypeVar("PayloadT")


class Envelope(BaseModel, Generic[PayloadT]):
    """Typed wire envelope: metadata + payload of type ``PayloadT``."""

    model_config = ConfigDict(frozen=True)

    id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    type: str
    source: str
    session_id: str | None = None
    reply_to: str | None = None
    ts: float = Field(default_factory=time.time)
    payload: PayloadT

    @classmethod
    def wrap(
        cls,
        payload: PayloadT,
        *,
        type: str,
        source: str,
        session_id: str | None = None,
        reply_to: str | None = None,
    ) -> Self:
        """Build an envelope around a payload (infers ``Envelope[PayloadT]``)."""
        return cls(
            type=type,
            source=source,
            session_id=session_id,
            reply_to=reply_to,
            payload=payload,
        )

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, data: str | bytes) -> Self:
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        return cls.model_validate_json(data)


# Untyped wire form used when the payload schema is not known yet.
# Classic assignment (not `type X = ...`) so classmethods stay available.
UntypedEnvelope = Envelope[dict[str, Any]]
