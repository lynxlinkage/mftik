"""Registry wire version — refuse a remote whose protocol we cannot speak."""

from __future__ import annotations

from mftik.registry.errors import RegistryError

PROTOCOL = "mft.registry"
PROTOCOL_VERSION = 1
PROTOCOL_MIN = 1
MFT_VERSION = "0.1.0"


def handshake_info(*, mft_version: str = MFT_VERSION) -> dict[str, str | int]:
    return {
        "protocol": PROTOCOL,
        "protocol_version": PROTOCOL_VERSION,
        "protocol_min": PROTOCOL_MIN,
        "mft_version": mft_version,
    }


def check_handshake(info: object) -> None:
    if not isinstance(info, dict):
        raise RegistryError("remote /registry/v1/info is not an object")
    if info.get("protocol") != PROTOCOL:
        raise RegistryError("remote is not an mft registry")
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
