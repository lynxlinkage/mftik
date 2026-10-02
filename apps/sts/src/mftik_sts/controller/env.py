"""Environment the session worker is spawned with.

``WorkerSpec.env`` is the child's whole environment. The shim adds
``MFTIK_STATUS_FD`` itself. Anything not listed here is not forwarded:
database URLs and secrets stay in the controller (F10, B5-09). No new
variables are introduced. The names are the ones the worker's existing
code already reads.
"""

from __future__ import annotations

import os

#: Bus, interpreter, and the directories the worker already consults.
#: Absent keys are omitted. ``MFTIK_DATA`` is the registry root
#: (:data:`mftik.registry.store.DATA_ENV`).
_FORWARDED_ENV = (
    "NATS_URL",
    "BROKER_KEY_PREFIX",
    "BROKER_REQUEST_TIMEOUT",
    "LOG_LEVEL",
    "PYTHONPATH",
    "SSL_CERT_DIR",
    "SSL_CERT_FILE",
    "STS_EVENTLOG_DIR",
    "STS_EVENTLOG_QUEUE",
    "STS_EVENTLOG_MAX_BYTES",
    "STS_EVENTLOG_BACKUPS",
    "STS_ARTIFACT_DIR",
    "MFTIK_DATA",
)


def forwarded_env() -> dict[str, str]:
    """The subset of this process's environment the session worker runs with."""
    return {key: os.environ[key] for key in _FORWARDED_ENV if os.environ.get(key)}
