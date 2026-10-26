"""Errors from ``python -m mftik.pack`` that are safe to print."""


class PackError(Exception):
    """The pack or the push cannot continue. The message names no credential."""
