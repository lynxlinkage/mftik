"""The extras this STS process believes it has.

Read at boot and on every registry reload. Deploy and ``/info`` use this
copy, not a fresh open of ``applied.json``: one process must not answer two
different extras questions a second apart because a PUT landed between them.

``current`` is a symlink already on ``sys.path``, so a committed generation
is physically importable the moment the API retargets it. What stays stale
until :func:`refresh` runs is the negative import cache, ``_REGISTRY``, and
this in-memory stamp.
"""

from __future__ import annotations

import importlib
import logging
import sys
import sysconfig
from pathlib import Path

from mftik.environment import EnvStamp, NodeEnv
from mftik.registry import RegistryStore
from mftik.registry.qualify import qualify

from mftik_sts.impl import _BUILTIN_KEYS, load_local_registry

logger = logging.getLogger(__name__)

_ABI_MISMATCH = "env overlay ABI mismatch"

_stamp = EnvStamp.empty()
_sys_path_entry: str | None = None
RUNTIME_EMPTY = "runtime-empty"


def current_stamp() -> EnvStamp:
    return _stamp


def extras_names() -> frozenset[str]:
    if not _stamp.matches_runtime():
        return frozenset()
    return _stamp.names()


class IncompatibleEnvironment(Exception):
    """This node does not have the extras a stored tree declared."""

    def __init__(self, type_name: str, missing: tuple[str, ...]) -> None:
        self.type_name = type_name
        self.missing = missing
        extra = ", ".join(missing) if missing else "applied extras"
        super().__init__(
            f"{type_name} requires {extra} which this node does not have"
        )


def ensure_deployable(
    type_name: str | None,
    store: RegistryStore | None = None,
) -> None:
    """Refuse a registry tree whose ``requires`` this process cannot honour.

    Bundled strategies skip the check. A type the store does not have is left
    to ``resolve`` as ``unknown_strategy``. Deploy reads the in-memory stamp,
    not the volume, so two creates a second apart cannot disagree.
    """
    if not type_name or type_name in _BUILTIN_KEYS:
        return
    store = store or RegistryStore.from_env()
    rec = _record_for(store, type_name)
    if rec is None:
        return
    applied = extras_names()
    missing = tuple(name for name in rec.requires if name not in applied)
    if missing:
        raise IncompatibleEnvironment(type_name, missing)


def _record_for(store: RegistryStore, type_name: str):
    for rec in store.list_all():
        if qualify(rec.origin, rec.type) == type_name:
            return rec
    for rec in store.list_all():
        if rec.type == type_name or rec.name == type_name:
            return rec
    return None


def attach_overlay(data_dir: str | Path | None = None) -> EnvStamp:
    """Put ``env/current`` on ``sys.path`` and adopt the stamp into memory.

    The path string is the symlink itself, not its target, so a later
    retarget needs no second insert. A stamp whose ``python`` / ``platform``
    do not match this interpreter is kept in memory for ABI reporting, but
    extras are treated as empty and ``sys.path`` points at a fresh empty
    directory so this process cannot import the mismatched wheels.
    """
    global _stamp
    env = NodeEnv(data_dir) if data_dir is not None else NodeEnv.from_env()
    current = env.ensure_current()
    stamp = env.read_stamp()
    if stamp.generation > 0 and not stamp.matches_runtime():
        logger.warning(
            "%s python=%s/%s platform=%s/%s — extras treated as empty",
            _ABI_MISMATCH,
            f"{stamp.python[0]}.{stamp.python[1]}",
            f"{sys.version_info[0]}.{sys.version_info[1]}",
            stamp.platform,
            sysconfig.get_platform(),
        )
        safe = env.root / RUNTIME_EMPTY / "site-packages"
        safe.mkdir(parents=True, exist_ok=True)
        _set_sys_path(safe)
    else:
        _set_sys_path(current)
    importlib.invalidate_caches()
    _stamp = stamp
    return _stamp


def refresh(
    store: RegistryStore | None = None,
    data_dir: str | Path | None = None,
) -> tuple[list[str], EnvStamp]:
    """Re-read the stamp, ensure the overlay path, invalidate, reload trees."""
    stamp = attach_overlay(data_dir)
    if store is None and data_dir is not None:
        store = RegistryStore(data_dir)
    loaded = load_local_registry(store)
    return loaded, stamp


def reset_for_tests() -> None:
    """Drop process state so overlay tests do not leak onto ``sys.path``."""
    global _stamp, _sys_path_entry
    if _sys_path_entry is not None:
        _remove_entry(_sys_path_entry)
    _sys_path_entry = None
    _stamp = EnvStamp.empty()
    for key in list(sys.modules):
        if key == "numpy" or key.startswith("numpy."):
            sys.modules.pop(key, None)
        if key.startswith("_mftik_reg_"):
            sys.modules.pop(key, None)
    importlib.invalidate_caches()


def _set_sys_path(path: Path) -> None:
    global _sys_path_entry
    entry = str(path)
    if _sys_path_entry is not None and _sys_path_entry != entry:
        _remove_entry(_sys_path_entry)
    if entry not in sys.path:
        sys.path.insert(0, entry)
    _sys_path_entry = entry


def _remove_entry(entry: str) -> None:
    sys.path[:] = [p for p in sys.path if p != entry]
