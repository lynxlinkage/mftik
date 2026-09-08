from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class BrokerConfig:
    """Which store the broker talks to, and the IPC defaults on top of it.

    One config for every transport rather than one per transport. The fields
    that belong to a single store are marked as such below and ignored by the
    others, which is a smaller price than a config hierarchy: ``from_env`` is
    read once per process and every plane reads it the same way, so a reader
    asking "what is this node configured to do" gets one answer in one place.
    """

    #: Which transport to build. See :mod:`mftik.broker.transport` for the
    #: registered names. ``nats`` is the default and the one a node deploys on;
    #: ``redis`` is still complete and still tested, and is what a rollback
    #: selects without a code change.
    transport: str = "nats"

    #: NATS. Used when :attr:`transport` is ``nats``.
    nats_url: str = "nats://localhost:4222"

    #: Redis. Used when :attr:`transport` is ``redis``.
    redis_url: str = "redis://localhost:6379/0"

    #: First segment of every name this node owns in the store. Under Redis it
    #: prefixes each key; under NATS it prefixes each subject and names the
    #: streams and buckets. Either way it is what lets two nodes share one
    #: server without reading each other's traffic.
    key_prefix: str = "mft"
    request_timeout: float = 5.0
    reply_ttl_seconds: int = 60
    #: How many log lines ``publish_log`` keeps per topic for late WS
    #: subscribers. Older lines are trimmed; live fan-out is unaffected.
    log_buffer_maxlen: int = 100
    #: Redis only. How old a pooled connection may be before it is pinged on
    #: checkout. Must stay under the Redis server's ``timeout`` (300s in
    #: production) — the point is to find a connection the server has already
    #: closed before a caller borrows it and fails on it. Finding it is all
    #: this does; what replaces it is :attr:`command_retries`.
    health_check_interval: int = 30
    #: Redis only. How many times one command may be retried on a
    #: ConnectionError. This is what makes the health check above useful: the
    #: ping on a dead connection raises, and with no retry that exception
    #: surfaces at whatever borrowed it — which has already failed sessions
    #: that had nothing wrong with them. Backoff is 50ms doubling to a 500ms
    #: cap, so three retries add at most ~350ms before giving up for real.
    command_retries: int = 3
    #: Redis only. How long one ``BLPOP`` parks before the loop around it looks
    #: up.
    #:
    #: It is a poll granularity, not a latency: a request or a reply that
    #: arrives wakes the pop immediately. What it bounds is how long a loop
    #: takes to notice something *other* than an element — its stop event, or a
    #: deadline that has passed. A serving loop cannot be cancelled out of a
    #: blocking pop without leaving the unread reply on the pooled connection,
    #: so shutting one down means waiting out at most one of these.
    #:
    #: A second in production, where nothing is waiting on a domain's
    #: shutdown. Tests drive whole session lifecycles per test and pay it on
    #: every teardown, so the test broker sets it far lower.
    #:
    #: NATS has no equivalent and needs none: a subscription is cancellable, so
    #: a serve loop stops when it is told to rather than at the end of a lap.
    serve_poll_seconds: float = 1.0
    #: NATS only. How long an idle consumer survives before the server reaps
    #: it. Every ``subscribe`` is a consumer of its own and every served subject
    #: has one, and the subjects are per-session and per-account, so the count
    #: follows the fleet rather than the code. This is what stops a node that
    #: has churned a thousand sessions from carrying a thousand consumers for
    #: the rest of its life.
    #:
    #: Comfortably above :attr:`request_timeout`, because a consumer reaped
    #: while a caller is still waiting on it costs that caller its answer.
    consumer_idle_seconds: float = 300.0

    @classmethod
    def from_env(cls) -> BrokerConfig:
        return cls(
            transport=os.getenv("BROKER_TRANSPORT", "nats").strip().lower(),
            nats_url=os.getenv("NATS_URL", "nats://localhost:4222"),
            redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            key_prefix=os.getenv("BROKER_KEY_PREFIX", "mft"),
            request_timeout=float(os.getenv("BROKER_REQUEST_TIMEOUT", "5")),
            reply_ttl_seconds=int(os.getenv("BROKER_REPLY_TTL", "60")),
            log_buffer_maxlen=max(1, int(os.getenv("BROKER_LOG_BUFFER_MAXLEN", "100"))),
            health_check_interval=int(os.getenv("BROKER_HEALTH_CHECK_INTERVAL", "30")),
            command_retries=max(0, int(os.getenv("BROKER_COMMAND_RETRIES", "3"))),
            serve_poll_seconds=float(os.getenv("BROKER_SERVE_POLL_SECONDS", "1")),
            consumer_idle_seconds=float(
                os.getenv("BROKER_CONSUMER_IDLE_SECONDS", "300")
            ),
        )
