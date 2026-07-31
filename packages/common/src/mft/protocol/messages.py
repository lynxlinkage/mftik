"""Concrete payload models carried inside ``Envelope[T]``."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from mft.protocol.envelope import Envelope


class Heartbeat(BaseModel):
    """Periodic liveness signal from a domain process."""

    model_config = ConfigDict(frozen=True)

    status: str = "ok"


class Log(BaseModel):
    """Session log line streamed to the UI over WebSocket."""

    model_config = ConfigDict(frozen=True, extra="allow")

    level: str = "info"
    message: str


HeartbeatEnvelope = Envelope[Heartbeat]
LogEnvelope = Envelope[Log]
