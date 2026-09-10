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
    #: How many log lines :meth:`Broker.fetch_log_buffer` returns per topic
    #: for late WS subscribers. The stream holds a larger ring; this is the
    #: replay window. Live fan-out is unaffected.
    log_buffer_maxlen: int = 100
    #: How many JetStream replicas a KV bucket should keep. NATS defaults to
    #: one, which puts the whole bucket on a single server — a 503 on that
    #: server's JS API is then a failed ledger read. Production sets three
    #: and pins them to the JP cluster (``NATS_KV_REPLICAS``,
    #: ``NATS_KV_PLACEMENT_CLUSTER``). Tests stay at one: a single-node
    #: server cannot place three.
    kv_replicas: int = 1
    kv_placement_cluster: str = ""

    @classmethod
    def from_env(cls) -> BrokerConfig:
        return cls(
            nats_url=os.getenv("NATS_URL", "nats://localhost:4222"),
            key_prefix=os.getenv("BROKER_KEY_PREFIX", "mft"),
            request_timeout=float(os.getenv("BROKER_REQUEST_TIMEOUT", "5")),
            log_buffer_maxlen=max(1, int(os.getenv("BROKER_LOG_BUFFER_MAXLEN", "100"))),
            kv_replicas=max(1, int(os.getenv("NATS_KV_REPLICAS", "1"))),
            kv_placement_cluster=os.getenv("NATS_KV_PLACEMENT_CLUSTER", ""),
        )
