"""How the broker gets its store.

A factory, not a registry. There is one transport, and :func:`build` is the
seam :class:`~mftik.broker.client.Broker` goes through, so a second
implementation — if one is ever justified again — is still an entry here
rather than a rewrite of the client.
"""

from __future__ import annotations

from mftik.broker.config import BrokerConfig
from mftik.broker.transport.base import LEASE_ANONYMOUS, BrokerTransport
from mftik.broker.transport.nats import NatsTransport


def build(config: BrokerConfig) -> BrokerTransport:
    """The transport this node runs."""
    return NatsTransport(config)


__all__ = [
    "LEASE_ANONYMOUS",
    "BrokerTransport",
    "NatsTransport",
    "build",
]
