"""The registered transports, and how one is chosen.

A dict of name to class and a lookup, following
:mod:`mftik.exchange.venues`: every implementation is named in one place, the
invariants are checked at import so a broken registry fails on the way in
rather than at the first call, and adding one is an entry here plus a module
beside it.

Selection is by name out of :class:`~mftik.broker.config.BrokerConfig`, which
reads ``BROKER_TRANSPORT`` from the environment. That is deliberately not an
import-time decision: the API process builds several brokers over its life —
one per WebSocket bridge, one per background worker — and they must all agree
without any of them re-reading a module global.
"""

from __future__ import annotations

from collections.abc import Callable

from mftik.broker.config import BrokerConfig
from mftik.broker.transport.base import LEASE_ANONYMOUS, BrokerTransport
from mftik.broker.transport.nats import NatsTransport
from mftik.broker.transport.redis import RedisTransport

#: Every transport a node may be configured with, by the name
#: ``BROKER_TRANSPORT`` takes.
TRANSPORTS: dict[str, Callable[[BrokerConfig], BrokerTransport]] = {
    "nats": NatsTransport,
    "redis": RedisTransport,
}

#: What a node runs on unless it is told otherwise. Matches
#: :attr:`BrokerConfig.transport`, and the two are checked against each other
#: below so they cannot drift apart silently.
DEFAULT = "nats"


def names() -> tuple[str, ...]:
    """Every registered name, sorted, for an error message or a test id."""
    return tuple(sorted(TRANSPORTS))


def build(config: BrokerConfig) -> BrokerTransport:
    """The transport ``config`` names.

    An unknown name raises rather than falling back to the default. A node
    started with ``BROKER_TRANSPORT=natss`` has been told something specific
    and quietly running on the other store is the worst available answer: half
    a fleet would be talking to a server the other half is not.
    """
    try:
        factory = TRANSPORTS[config.transport]
    except KeyError:
        raise ValueError(
            f"unknown broker transport {config.transport!r}; "
            f"BROKER_TRANSPORT must be one of {', '.join(names())}"
        ) from None
    return factory(config)


def check_registry() -> None:
    """Fail on a registry that could not answer a call. Runs at import."""
    for name, factory in TRANSPORTS.items():
        assert name == name.lower(), f"transport {name!r} is not lower-case"
        assert isinstance(factory, type) and issubclass(factory, BrokerTransport), (
            f"transport {name!r} does not implement BrokerTransport"
        )
    assert DEFAULT in TRANSPORTS, f"the default transport {DEFAULT!r} is not registered"
    assert BrokerConfig().transport == DEFAULT, (
        "BrokerConfig's default transport and this module's disagree: "
        f"{BrokerConfig().transport!r} vs {DEFAULT!r}"
    )


check_registry()

__all__ = [
    "DEFAULT",
    "LEASE_ANONYMOUS",
    "TRANSPORTS",
    "BrokerTransport",
    "NatsTransport",
    "RedisTransport",
    "build",
    "names",
]
