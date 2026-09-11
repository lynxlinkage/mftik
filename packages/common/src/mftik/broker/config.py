from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class BrokerConfig:
    """How this node talks to the bus.

    One config, read once per process. Every plane reads it the same way, so a
    reader asking "what is this node configured to do" gets one answer in one
    place.
    """

    #: NATS. The store every plane talks through.
    nats_url: str = "nats://localhost:4222"

    #: First segment of every subject this node owns. Two nodes share one
    #: server without reading each other's traffic.
    key_prefix: str = "mft"
    request_timeout: float = 5.0

    @classmethod
    def from_env(cls) -> BrokerConfig:
        return cls(
            nats_url=os.getenv("NATS_URL", "nats://localhost:4222"),
            key_prefix=os.getenv("BROKER_KEY_PREFIX", "mft"),
            request_timeout=float(os.getenv("BROKER_REQUEST_TIMEOUT", "5")),
        )
