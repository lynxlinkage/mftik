"""Shared MFTIK library — protocol, broker, runtime, exchange, strategy.

:mod:`mftik.strategy` is what a strategy is written against, and it is here
rather than in the STS app so it installs beside a strategy on a developer's
machine. Nothing in it needs a database or a running node.
"""

from mftik.runtime import (
    configure_logging,
    run_heartbeat_service,
    run_until_stopped,
)

__all__ = [
    "configure_logging",
    "run_heartbeat_service",
    "run_until_stopped",
]
