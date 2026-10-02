"""Write the digest-addressed replica from a registry or env sync.

The controller calls these. They do not import a strategy tree. A
``reload`` probes wanted keys in a child interpreter and reports the
rest of the index as loaded without opening it, so the API still sees
the full set.

Legacy ``<origin>/<name>/`` directories are copied into ``trees/``
before the batch is applied, so a name that this batch rebinds still
has its previous digest on disk. They are deleted only after a full
``retain`` rebuild copied every legacy tree and every pinned digest
resolves. A partial push, or one unreadable legacy tree, deletes none
of them.
"""

from __future__ import annotations

import logging
import shutil
from collections.abc import Collection
from pathlib import Path

from mftik.environment import EnvStamp, NodeEnv
from mftik.protocol import (
    StsEnvPackagePin,
    StsEnvSyncRequest,
    StsEnvSyncResult,
    StsRegistryReloadResult,
    StsRegistrySyncRequest,
    StsRegistrySyncResult,
    StsRegistryTreeOp,
)
from mftik.registry.digest import digest_files
from mftik.registry.errors import RegistryDigestMismatch, RegistryError
from mftik.registry.files import normalize_files, read_tree
from mftik.registry.qualify import qualify

from mftik_sts.hostdisk.probe import REASON_ABSENT, probe
from mftik_sts.hostdisk.replica import (
    INDEX_NAME,
    TREES_DIRNAME,
    TreeReplica,
    require_digest,
)

logger = logging.getLogger(__name__)

_SKIP_ROOT = frozenset({TREES_DIRNAME, INDEX_NAME, "remotes.toml"})
_OWN = ("public", "private")


def prepare_disk(data_dir: str | Path | None = None) -> bool:
    """Copy legacy trees into the digest layout. Do not delete them.

    Boot calls this before RPC so a catch-up that arrives next can
    resolve a pin that still lives only under ``<origin>/<name>/``.
    False means at least one legacy directory could not be copied.
    """
    return adopt_legacy(_replica(data_dir))


def adopt_legacy(replica: TreeReplica) -> bool:
    """Copy each legacy tree and bind its name. Leave the old directory.

    False when any candidate directory is not a strategy tree. The
    caller must not delete legacy directories in that case, including
    the ones this call did copy.
    """
    ok = True
    for origin, name, path in _legacy_trees(replica):
        try:
            _copy_legacy(replica, origin, name, path)
        except (RegistryError, OSError, ValueError) as exc:
            ok = False
            logger.warning(
                "legacy registry directory %s was not copied: %s", path, exc
            )
    return ok


def drop_legacy(replica: TreeReplica) -> tuple[Path, ...]:
    """Delete legacy ``<origin>/<name>/`` directories and log each path.

    Only a full rebuild that has already copied every legacy tree, and
    whose pins all resolve, may call this. A shared ``MFTIK_DATA``
    (the compose file mounts one volume for the API and STS) means
    these directories are also the API store.
    """
    removed: list[Path] = []
    for _origin, _name, path in _legacy_trees(replica):
        shutil.rmtree(path)
        logger.info("removed legacy registry directory %s", path)
        removed.append(path)
    return tuple(removed)


def apply_registry_sync(
    request: StsRegistrySyncRequest,
    *,
    keep_digests: Collection[str],
    data_dir: str | Path | None = None,
) -> StsRegistrySyncResult:
    """Adopt, apply, and on a full retain maybe drop the legacy layout.

    ``keep_digests`` is the pin set. GC keeps those and whatever the
    index still names. ``reload`` false returns ``loaded=[]`` and does
    not start a probe. ``reload`` true probes the keys this batch
    upserted plus ``explain``; every other index key is listed in
    ``loaded`` without an import.
    """
    if isinstance(keep_digests, str) or not isinstance(keep_digests, Collection):
        raise TypeError("keep_digests must be a collection of digests")
    replica = _replica(data_dir)
    adopt_ok = adopt_legacy(replica)
    skipped = _apply_ops(replica, request)
    if request.retain is not None:
        retain = set(request.retain)
        for name in list(replica.names()):
            if name not in retain:
                replica.unbind(name)
        if adopt_ok and _pins_present(replica, keep_digests):
            drop_legacy(replica)
    replica.gc(keep_digests)
    env = NodeEnv(replica.data_dir)
    generation = env.read_stamp().generation
    if not request.reload:
        return StsRegistrySyncResult(
            loaded=[], generation=generation, skipped=skipped
        )
    site = (
        generation
        if env.site_packages(generation).is_dir()
        else None
    )
    loaded = _probe_wanted(replica, request, skipped, site)
    return StsRegistrySyncResult(
        loaded=loaded, generation=generation, skipped=skipped
    )


def reload_index(data_dir: str | Path | None = None) -> StsRegistryReloadResult:
    """The index's names and the env stamp's generation. No import."""
    replica = _replica(data_dir)
    env = NodeEnv(replica.data_dir)
    return StsRegistryReloadResult(
        loaded=sorted(replica.names()),
        generation=env.read_stamp().generation,
    )


def apply_env_sync(
    request: StsEnvSyncRequest,
    *,
    keep_generations: Collection[int],
) -> StsEnvSyncResult:
    """Write the pin file, apply, then prune generations nothing pins.

    The pin file is written first so :meth:`NodeEnv.commit` cannot drop
    a generation a session still imports. This uses
    :func:`mftik_sts.rpc.env.apply_requested`, which honours
    ``installer_for_sync`` and does not call :func:`mftik_sts.runtime_env.refresh`.
    """
    from mftik_sts.hostdisk.pin import gc_env
    from mftik_sts.rpc.env import apply_requested

    env = NodeEnv.from_env()
    env.write_pinned_generations(keep_generations)
    apply_requested(request)
    gc_env(env, keep_generations)
    stamp = env.read_stamp()
    live = _overlay_live(env, stamp)
    packages = _packages(stamp) if live else {}
    loaded = sorted(TreeReplica(env.data_dir).names())
    return StsEnvSyncResult(
        loaded=loaded,
        generation=stamp.generation,
        packages=packages,
        overlay_live=live,
    )


def _replica(data_dir: str | Path | None) -> TreeReplica:
    if data_dir is None:
        return TreeReplica(NodeEnv.from_env().data_dir)
    return TreeReplica(data_dir)


def _legacy_trees(replica: TreeReplica) -> list[tuple[str, str, Path]]:
    registry = replica.registry_dir
    if not registry.is_dir():
        return []
    found: list[tuple[str, str, Path]] = []
    for origin in _OWN:
        found.extend(_child_trees(registry / origin, origin))
    pulled = registry / "pulled"
    if pulled.is_dir():
        for remote in sorted(pulled.iterdir(), key=lambda path: path.name):
            if remote.name.startswith(".") or not remote.is_dir():
                continue
            found.extend(_child_trees(remote, remote.name))
    return found


def _child_trees(root: Path, origin: str) -> list[tuple[str, str, Path]]:
    if not root.is_dir():
        return []
    found: list[tuple[str, str, Path]] = []
    for child in sorted(root.iterdir(), key=lambda path: path.name):
        if child.name.startswith(".") or child.name in _SKIP_ROOT:
            continue
        if not child.is_dir():
            continue
        found.append((origin, child.name, child))
    return found


def _copy_legacy(replica: TreeReplica, origin: str, name: str, path: Path) -> None:
    normalised = normalize_files(read_tree(path))
    digest = digest_files(normalised)
    replica.put(digest, normalised)
    replica.bind(qualify(origin, name), digest)


def _apply_ops(
    replica: TreeReplica, request: StsRegistrySyncRequest
) -> dict[str, str]:
    skipped: dict[str, str] = {}
    for op in request.trees:
        key = qualify(op.origin, op.name)
        if op.op == "delete":
            try:
                replica.unbind(key)
            except ValueError as exc:
                skipped[key] = f"refused: {exc}"
            continue
        _upsert(replica, op, key, skipped)
    return skipped


def _upsert(
    replica: TreeReplica,
    op: StsRegistryTreeOp,
    key: str,
    skipped: dict[str, str],
) -> None:
    try:
        normalised = normalize_files(op.files)
    except RegistryError as exc:
        skipped[key] = f"refused: {exc}"
        return
    actual = digest_files(normalised)
    if op.digest and actual != op.digest:
        skipped[key] = "digest mismatch"
        return
    try:
        replica.put(actual, normalised)
        replica.bind(key, actual)
    except RegistryDigestMismatch:
        skipped[key] = "digest mismatch"
    except RegistryError as exc:
        skipped[key] = f"refused: {exc}"
    except OSError as exc:
        skipped[key] = f"write error: {exc}"


def _pins_present(replica: TreeReplica, keep_digests: Collection[str]) -> bool:
    for digest in keep_digests:
        try:
            require_digest(digest)
        except ValueError:
            return False
        if replica.path_of(digest) is None:
            return False
    return True


def _probe_wanted(
    replica: TreeReplica,
    request: StsRegistrySyncRequest,
    skipped: dict[str, str],
    generation: int | None,
) -> list[str]:
    wanted: list[str] = []
    seen: set[str] = set()
    for op in request.trees:
        if op.op != "upsert":
            continue
        key = qualify(op.origin, op.name)
        if key not in seen:
            seen.add(key)
            wanted.append(key)
    for key in request.explain:
        if key not in seen:
            seen.add(key)
            wanted.append(key)
    loaded: list[str] = []
    for key in wanted:
        if key in skipped:
            continue
        digest = replica.current(key)
        if digest is None:
            skipped[key] = REASON_ABSENT
            continue
        result = probe(digest, generation, replica=replica)
        if result.status == "loaded":
            loaded.append(key)
        else:
            skipped[key] = result.reason or REASON_ABSENT
    for key in sorted(replica.names()):
        if key in skipped or key in loaded:
            continue
        loaded.append(key)
    return loaded


def _overlay_live(env: NodeEnv, stamp: EnvStamp) -> bool:
    if stamp.generation == 0:
        return True
    if not stamp.matches_runtime():
        return False
    return env.overlay_for(stamp) is not None


def _packages(stamp: EnvStamp) -> dict[str, StsEnvPackagePin]:
    return {
        name: StsEnvPackagePin(
            version=rec.version, dist=rec.dist, source=rec.source
        )
        for name, rec in stamp.packages.items()
    }
