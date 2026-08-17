"""Registry wire version — refuse a remote whose protocol we cannot speak."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from mftik.registry.errors import RegistryError

PROTOCOL = "mftik.registry"
PROTOCOL_VERSION = 1
PROTOCOL_MIN = 1


def _installed_version() -> str:
    """What this node tells a peer it is running.

    Read from the installed distribution rather than written here. A second
    copy of a version number is a copy that goes stale on the release that
    forgets it, and this one is stated to other people's nodes — a handshake
    that misreports itself is worse than one that says nothing.
    """
    try:
        return version("mftik")
    except PackageNotFoundError:  # pragma: no cover - source tree, not a wheel
        return "0.0.0"


MFTIK_VERSION = _installed_version()


def handshake_info(*, mftik_version: str = MFTIK_VERSION) -> dict[str, str | int]:
    return {
        "protocol": PROTOCOL,
        "protocol_version": PROTOCOL_VERSION,
        "protocol_min": PROTOCOL_MIN,
        "mftik_version": mftik_version,
    }


def check_handshake(info: object) -> None:
    if not isinstance(info, dict):
        raise RegistryError("remote /registry/v1/info is not an object")
    if info.get("protocol") != PROTOCOL:
        raise RegistryError("remote is not an mftik registry")
    try:
        their_ver = int(info["protocol_version"])
        their_min = int(info["protocol_min"])
    except (KeyError, TypeError, ValueError) as exc:
        raise RegistryError("remote handshake is missing protocol versions") from exc
    if PROTOCOL_VERSION < their_min or their_ver < PROTOCOL_MIN:
        raise RegistryError(
            f"incompatible registry protocol (local "
            f"{PROTOCOL_VERSION}/{PROTOCOL_MIN}, remote {their_ver}/{their_min}); "
            "upgrade mft"
        )
