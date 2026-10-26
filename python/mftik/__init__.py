"""STS Python SDK."""

from mftik._mftik import Context, ContextError, Poster
from mftik._version import package_version
from mftik.strategy import Strategy

__all__ = [
    "Context",
    "ContextError",
    "Poster",
    "Strategy",
    "run",
]

__version__ = package_version()


def __getattr__(name: str):
    # ``python -m mftik.run`` imports this package first. Importing ``mftik.run``
    # here would leave it in ``sys.modules`` before that entrypoint executes.
    if name == "run":
        from mftik.run import run

        return run
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
