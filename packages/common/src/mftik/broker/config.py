from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class BrokerConfig:
    """Redis broker connection and IPC defaults."""

    redis_url: str = "redis://localhost:6379/0"
    key_prefix: str = "mft"
    request_timeout: float = 5.0
    reply_ttl_seconds: int = 60
    #: How many log lines ``publish_log`` keeps per topic for late WS
    #: subscribers. Older lines are trimmed; live pub/sub is unaffected.
    log_buffer_maxlen: int = 100
    #: How old a pooled connection may be before it is pinged on checkout.
    #: Must stay under the Redis server's ``timeout`` (300s in production) —
    #: the point is to retire a connection the server has already closed
    #: before a caller borrows it and fails on it.
    health_check_interval: int = 30

    @classmethod
    def from_env(cls) -> BrokerConfig:
        return cls(
            redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            key_prefix=os.getenv("BROKER_KEY_PREFIX", "mft"),
            request_timeout=float(os.getenv("BROKER_REQUEST_TIMEOUT", "5")),
            reply_ttl_seconds=int(os.getenv("BROKER_REPLY_TTL", "60")),
            log_buffer_maxlen=max(
                1, int(os.getenv("BROKER_LOG_BUFFER_MAXLEN", "100"))
            ),
            health_check_interval=int(
                os.getenv("BROKER_HEALTH_CHECK_INTERVAL", "30")
            ),
        )
