"""Registry wire version — refuse a remote whose protocol we cannot speak."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

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


def handshake_info(
    *,
    mftik_version: str = MFTIK_VERSION,
    data_dir: str | Path | None = None,
    pins: bool = True,
) -> dict[str, Any]:
    """Versions, plus applied extras this interpreter can actually import.

    ``source`` is not published — where this node got a package is its own
    bookkeeping. A stamp whose python/platform do not match is reported as
    empty extras so a peer does not copy a promise this process cannot keep.

    ``pins`` is the exact ``version`` / ``dist``. The names are not secret
    (connect compares them) but the pins are: an anonymous ``/info`` keeps
    the keys and drops the values.
    """
    from mftik.environment import NodeEnv

    env = NodeEnv(data_dir) if data_dir is not None else NodeEnv.from_env()
    stamp = env.read_stamp()
    extras: dict[str, dict[str, str]] = {}
    generation = 0
    if stamp.generation > 0 and stamp.matches_runtime():
        if pins:
            extras = {
                name: {"version": rec.version, "dist": rec.dist}
                for name, rec in stamp.packages.items()
            }
        else:
            extras = {name: {} for name in stamp.packages}
        generation = stamp.generation
    return {
        "protocol": PROTOCOL,
        "protocol_version": PROTOCOL_VERSION,
        "protocol_min": PROTOCOL_MIN,
        "mftik_version": mftik_version,
        "extras": extras,
        "env_generation": generation,
    }


def extra_names(info: object) -> frozenset[str]:
    """Import names a handshake advertises. Keys only — value shape is ignored."""
    if not isinstance(info, dict):
        return frozenset()
    extras = info.get("extras")
    if not isinstance(extras, dict):
        return frozenset()
    return frozenset(str(name) for name in extras)


def check_remote_extras(info: object, local_names: frozenset[str]) -> None:
    """Refuse a new connect whose extras this node does not have.

    Version and dist are ignored. A pin difference is a ``diff`` warning,
    not a connect refusal — that is ENV-8's job, per tree, at deploy.
    """
    missing = sorted(extra_names(info) - local_names)
    if missing:
        raise RegistryError(
            "remote extras not on this node: " + ", ".join(missing)
        )


def extra_version(item: object) -> str:
    if isinstance(item, str):
        return item
    if isinstance(item, dict):
        return str(item.get("version") or "")
    return ""


def extras_version_warnings(remote: object, local: object) -> tuple[str, ...]:
    if not isinstance(remote, dict) or not isinstance(local, dict):
        return ()
    theirs = remote.get("extras") if isinstance(remote.get("extras"), dict) else {}
    ours = local.get("extras") if isinstance(local.get("extras"), dict) else {}
    if not isinstance(theirs, dict) or not isinstance(ours, dict):
        return ()
    warnings: list[str] = []
    for name in sorted(set(theirs) & set(ours)):
        remote_ver = extra_version(theirs[name])
        local_ver = extra_version(ours[name])
        if remote_ver and local_ver and remote_ver != local_ver:
            warnings.append(f"{name}: remote {remote_ver}, local {local_ver}")
    return tuple(warnings)


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
