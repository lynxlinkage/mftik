"""Shared MFTIK library — protocol, broker, runtime, exchange, strategy.

:mod:`mftik.strategy` is what a strategy is written against, and it is here
rather than in the STS app so it installs beside a strategy on a developer's
machine. Nothing in it needs a database or a running node.
"""

from mftik.health import serve_health
from mftik.instance import (
    INSTANCE_ENV,
    INSTANCED_PLANES,
    ROLE_ENV,
    STANDBY_PLANES,
    Role,
    control_subjects,
    instance_name,
    instance_role,
    validate_instance_name,
)
from mftik.runtime import (
    configure_logging,
    run_heartbeat_service,
    run_until_stopped,
)

__all__ = [
    "INSTANCED_PLANES",
    "INSTANCE_ENV",
    "ROLE_ENV",
    "STANDBY_PLANES",
    "Role",
    "configure_logging",
    "control_subjects",
    "instance_name",
    "instance_role",
    "validate_instance_name",
    "serve_health",
    "run_heartbeat_service",
    "run_until_stopped",
]
