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
    #: the point is to find a connection the server has already closed before
    #: a caller borrows it and fails on it. Finding it is all this does; what
    #: replaces it is :attr:`command_retries`.
    health_check_interval: int = 30
    #: How many times one command may be retried on a ConnectionError. This is
    #: what makes the health check above useful: the ping on a dead connection
    #: raises, and with no retry that exception surfaces at whatever borrowed
    #: it — which has already failed sessions that had nothing wrong with them.
    #: Backoff is 50ms doubling to a 500ms cap, so three retries add at most
    #: ~350ms before giving up for real.
    command_retries: int = 3
    #: How long one ``BLPOP`` parks before the loop around it looks up.
    #:
    #: It is a poll granularity, not a latency: a request or a reply that
    #: arrives wakes the pop immediately. What it bounds is how long a loop
    #: takes to notice something *other* than an element — its stop event,
    #: or a deadline that has passed. A serving loop cannot be cancelled out
    #: of a blocking pop without leaving the unread reply on the pooled
    #: connection, so shutting one down means waiting out at most one of
    #: these (see ``SessionManager._destroy_account``).
    #:
    #: A second in production, where nothing is waiting on a domain's
    #: shutdown. Tests drive whole session lifecycles per test and pay it on
    #: every teardown, so the test broker sets it far lower.
    serve_poll_seconds: float = 1.0

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
            command_retries=max(
                0, int(os.getenv("BROKER_COMMAND_RETRIES", "3"))
            ),
            serve_poll_seconds=float(
                os.getenv("BROKER_SERVE_POLL_SECONDS", "1")
            ),
        )
