"""The installed distribution version, without importing the entrypoint."""

import importlib.metadata

FALLBACK = "0.12.0"


def package_version() -> str:
    """The installed mftik distribution version."""
    try:
        return importlib.metadata.version("mftik")
    except importlib.metadata.PackageNotFoundError:
        return FALLBACK
