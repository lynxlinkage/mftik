"""Failures while adding a strategy to a local registry."""

from __future__ import annotations


class RegistryError(ValueError):
    """The files cannot be recorded as a strategy. ``code`` is the HTTP shape."""

    code: str = "registry_error"

    def __init__(self, message: str) -> None:
        super().__init__(message)


class RegistryConflict(RegistryError):
    """A strategy of that name is already in that origin."""

    code = "registry_conflict"
