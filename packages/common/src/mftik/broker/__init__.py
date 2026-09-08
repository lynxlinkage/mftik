"""The broker — fan-out, request-reply, shared state and fenced session links.

One vocabulary, one store. ``docs/Broker.md`` is what a plane may say and
what NATS does to answer it. The seven patterns are in
``docs/BrokerPatterns.md``.
"""

from mftik.broker.client import LEASE_ANONYMOUS, Broker, BrokerClient
from mftik.broker.config import BrokerConfig
from mftik.broker.errors import (
    BrokerError,
    BrokerNotConnectedError,
    RequestTimeoutError,
)
from mftik.broker.link import LeasedSessionLink
from mftik.broker.request import IncomingRequest
from mftik.broker.state import StateProjection
from mftik.broker.transport import BrokerTransport

__all__ = [
    "LEASE_ANONYMOUS",
    "Broker",
    "BrokerClient",
    "BrokerConfig",
    "BrokerError",
    "BrokerNotConnectedError",
    "BrokerTransport",
    "IncomingRequest",
    "LeasedSessionLink",
    "RequestTimeoutError",
    "StateProjection",
]
