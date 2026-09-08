from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class BrokerConfig:
    """How this node talks to the broker's store.

    One config, read once per process. Every plane reads it the same way, so a
    reader asking "what is this node configured to do" gets one answer in one
    place.
    """

    #: NATS. The store every plane talks through.
    nats_url: str = "nats://localhost:4222"

    #: First segment of every name this node owns in the store. It prefixes
    #: each subject and names the streams and buckets, which is what lets two
    #: nodes share one server without reading each other's traffic.
    key_prefix: str = "mft"
    request_timeout: float = 5.0
    reply_ttl_seconds: int = 60
    #: How many log lines ``publish_log`` keeps per topic for late WS
    #: subscribers. Older lines are trimmed; live fan-out is unaffected.
    log_buffer_maxlen: int = 100
    #: How long an idle consumer survives before the server reaps it. Every
    #: ``subscribe`` is a consumer of its own and every served subject has one,
    #: and the subjects are per-session and per-account, so the count follows
    #: the fleet rather than the code. This is what stops a node that has
    #: churned a thousand sessions from carrying a thousand consumers for the
    #: rest of its life.
    #:
    #: Comfortably above :attr:`request_timeout`, because a consumer reaped
    #: while a caller is still waiting on it costs that caller its answer.
    consumer_idle_seconds: float = 300.0

    @classmethod
    def from_env(cls) -> BrokerConfig:
        return cls(
            nats_url=os.getenv("NATS_URL", "nats://localhost:4222"),
            key_prefix=os.getenv("BROKER_KEY_PREFIX", "mft"),
            request_timeout=float(os.getenv("BROKER_REQUEST_TIMEOUT", "5")),
            reply_ttl_seconds=int(os.getenv("BROKER_REPLY_TTL", "60")),
            log_buffer_maxlen=max(1, int(os.getenv("BROKER_LOG_BUFFER_MAXLEN", "100"))),
            consumer_idle_seconds=float(
                os.getenv("BROKER_CONSUMER_IDLE_SECONDS", "300")
            ),
        )
