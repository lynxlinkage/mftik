from __future__ import annotations

import time
import uuid
from typing import Any

from pydantic import BaseModel, Field


class MessageEnvelope(BaseModel):
    """Common wire format for all MFT broker messages."""

    id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    type: str
    source: str
    session_id: str | None = None
    ts: float = Field(default_factory=time.time)
    payload: dict[str, Any] = Field(default_factory=dict)

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, data: str | bytes) -> MessageEnvelope:
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        return cls.model_validate_json(data)
