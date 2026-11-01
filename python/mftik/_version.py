"""The installed distribution version, without importing the entrypoint."""

import importlib.metadata

# scripts/release-check versions keeps this equal to the workspace version.
FALLBACK = "1.3.0"


def package_version() -> str:
    """The installed mftik distribution version."""
    try:
        return importlib.metadata.version("mftik")
    except importlib.metadata.PackageNotFoundError:
        return FALLBACK
