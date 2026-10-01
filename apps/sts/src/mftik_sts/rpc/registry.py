"""Re-read the strategy registry without restarting STS, and copy trees onto it.

Adding a strategy writes files into the API's ``MFTIK_DATA``. An STS on
another host never sees that directory. ``sts.registry.sync`` carries the
tree and this process writes it into its own registry before the scan.
``sts.registry.reload`` stays a rescan of whatever is already on disk.
Until one of them runs, a deploy naming the new tree answers
``unknown_strategy``. ``refresh`` re-reads the env stamp, puts the overlay
on ``sys.path``, and ``load_local_registry`` decides what to skip.
"""

from __future__ import annotations

import logging
import time

from mftik.broker import IncomingRequest
from mftik.protocol import (
    STS_ERROR,
    STS_REGISTRY_GENERATION,
    STS_REGISTRY_LOADED,
    STS_REGISTRY_RELOAD,
    STS_REGISTRY_SYNC,
    RpcError,
    RpcErrorEnvelope,
    StsRegistryGenerationResult,
    StsRegistryGenerationResultEnvelope,
    StsRegistryLoadedResult,
    StsRegistryLoadedResultEnvelope,
    StsRegistryReloadResult,
    StsRegistryReloadResultEnvelope,
    StsRegistrySyncRequest,
    StsRegistrySyncResult,
    StsRegistrySyncResultEnvelope,
    StsRegistryTreeOp,
)
from mftik.registry import RegistryError, RegistryStore, qualify, split_qualified
from mftik.registry.errors import RegistryDigestMismatch
from mftik.registry.qualify import OWN_ORIGINS

from mftik_sts.impl import registered_keys, registry_skips
from mftik_sts.rpc.env import current_packages
from mftik_sts.runtime_env import current_stamp, overlay_is_live, refresh

logger = logging.getLogger(__name__)

SKIP_ABSENT = "not present on this registry disk"
SKIP_COLLISION = "name collision with a bundled strategy"
SKIP_DIGEST = "digest mismatch"

#: How many extra scans to spend when an upsert's directory is missing.
#: Two STS on one disk used to rename the live tree aside before the new
#: one was in place, and the peer's rescan landed in that gap and
#: unregistered the type. The swap no longer opens the gap; this covers
#: a kernel that still publishes with two renames.
_RESCAN_GAP_TRIES = 5
_RESCAN_GAP_S = 0.004


def _delete_tree(store: RegistryStore, op: StsRegistryTreeOp) -> None:
    """Remove one tree. Already gone is success, on either kind of origin."""
    if op.origin in OWN_ORIGINS:
        try:
            store.remove(op.name, origin=op.origin)
        except RegistryError:
            return
        return
    store.discard(op.name, origin=op.origin)


def _prune_to(store: RegistryStore, retain: set[str]) -> None:
    """Delete registry trees the manifest does not name.

    A missed delete leaves a ghost that every boot reloads. The retain list
    is the API store; anything else on this disk is that ghost.
    """
    for rec in store.list_all():
        key = qualify(rec.origin, rec.name)
        if key in retain:
            continue
        try:
            store.discard(rec.name, origin=rec.origin)
        except RegistryError as exc:
            logger.warning("registry prune refused %s: %s", key, exc)


def _apply_ops(store: RegistryStore, request: StsRegistrySyncRequest) -> dict[str, str]:
    """Write the batch. A tree that cannot be written is skipped, not fatal.

    One broken upsert or delete must not discard the rest of the batch.
    A digest that does not match is refused before the previous tree is
    replaced. A tree already at ``op.digest`` is left untouched so two
    processes sharing a volume do not rewrite the same bytes.
    """
    skipped: dict[str, str] = {}
    for op in request.trees:
        key = qualify(op.origin, op.name)
        if op.op == "delete":
            try:
                _delete_tree(store, op)
            except RegistryError as exc:
                skipped[key] = f"refused: {exc}"
            continue
        if op.digest:
            existing = _find(store, op.origin, op.name)
            if existing is not None and existing.digest == op.digest:
                continue
        try:
            store.add(
                op.files,
                replace=True,
                origin=op.origin,
                expect_digest=op.digest or None,
            )
        except RegistryDigestMismatch:
            skipped[key] = SKIP_DIGEST
            continue
        except RegistryError as exc:
            skipped[key] = f"refused: {exc}"
            continue
        except OSError as exc:
            skipped[key] = f"write error: {exc}"
            continue
    if request.retain is not None:
        _prune_to(store, set(request.retain))
    return skipped


def _find(store: RegistryStore, origin: str, name: str):
    for rec in store.list_all():
        if rec.origin == origin and rec.name == name:
            return rec
    return None


def _dir_missing(store: RegistryStore, key: str) -> bool:
    split = split_qualified(key)
    if split is None:
        return False
    origin, name = split
    return not store.has_tree(name, origin=origin)


def _scan_may_be_stale(store: RegistryStore, key: str) -> bool:
    """True when another look might load ``key``.

    A missing directory is the publish gap. A directory that is present
    but was not loaded and was not recorded as an import failure appeared
    after the scan started. An import error is stable and is not retried.
    """
    if split_qualified(key) is None:
        return False
    if _dir_missing(store, key):
        return True
    return key not in registry_skips()


def _refresh_for_sync(
    store: RegistryStore, wanted: list[str], skipped: dict[str, str]
) -> tuple[list[str], object]:
    """Rescan, and repeat while an upsert's directory is in the publish gap."""
    loaded, stamp = refresh(store, data_dir=store.data_dir)
    for _ in range(_RESCAN_GAP_TRIES):
        pending = [
            key
            for key in wanted
            if key not in loaded
            and key not in skipped
            and _scan_may_be_stale(store, key)
        ]
        if not pending:
            break
        if any(_dir_missing(store, key) for key in pending):
            time.sleep(_RESCAN_GAP_S)
        loaded, stamp = refresh(store, data_dir=store.data_dir)
    return loaded, stamp


def explain_skip(store: RegistryStore, key: str) -> str:
    """Why ``key`` is on the request and not in the scan's loaded list.

    The scan already imported the tree and recorded why it skipped. Importing
    again would run broken top-level code a second time. ``store`` is the
    disk that scan walked; a key it did not mention is simply not there.
    """
    del store
    return registry_skips().get(key, SKIP_ABSENT)


def apply_sync(
    store: RegistryStore, request: StsRegistrySyncRequest
) -> StsRegistrySyncResult:
    """Write ``request`` onto ``store`` and, on the last batch, rescan it."""
    skipped = _apply_ops(store, request)
    if request.reload:
        upserted = [
            qualify(op.origin, op.name)
            for op in request.trees
            if op.op == "upsert"
        ]
        wanted = [*request.explain, *upserted]
        loaded, stamp = _refresh_for_sync(store, wanted, skipped)
        for key in wanted:
            if key in loaded or key in skipped:
                continue
            skipped[key] = explain_skip(store, key)
    else:
        loaded = []
        stamp = current_stamp()
    return StsRegistrySyncResult(
        loaded=list(loaded),
        generation=stamp.generation,
        skipped=skipped,
    )


async def handle_registry_sync(
    req: IncomingRequest,
    *,
    instance: str | None = None,
) -> None:
    del instance
    request = StsRegistrySyncRequest.model_validate(req.envelope.payload)
    try:
        # Same constraint as reload: the scan imports modules and mutates
        # ``sys.modules``, so it stays on the event loop. The writes in front
        # of it are a handful of small source files.
        result = apply_sync(RegistryStore.from_env(), request)
    except Exception as exc:
        logger.exception("registry sync failed")
        await req.reply(
            RpcErrorEnvelope.wrap(
                RpcError(code="sync_failed", message=str(exc)),
                type=STS_ERROR,
                source="sts",
                session_id=req.envelope.session_id,
            )
        )
        return
    logger.info(
        "registry synced: %d strategy(ies), %d skipped",
        len(result.loaded),
        len(result.skipped),
    )
    await req.reply(
        StsRegistrySyncResultEnvelope.wrap(
            result,
            type=STS_REGISTRY_SYNC,
            source="sts",
            session_id=req.envelope.session_id,
        )
    )


async def handle_registry_loaded(
    req: IncomingRequest,
    *,
    instance: str | None = None,
) -> None:
    del instance
    await req.reply(
        StsRegistryLoadedResultEnvelope.wrap(
            StsRegistryLoadedResult(loaded=registered_keys()),
            type=STS_REGISTRY_LOADED,
            source="sts",
            session_id=req.envelope.session_id,
        )
    )


async def handle_registry_reload(
    req: IncomingRequest,
    *,
    instance: str | None = None,
) -> None:
    del instance
    try:
        # Synchronous, and deliberately not moved to a thread. It imports
        # Python modules, which mutates ``sys.modules`` — running that
        # alongside a session's own imports is not something to arrange
        # casually, and the scan is a directory walk over a handful of trees.
        loaded, stamp = refresh()
    except Exception as exc:
        # A scan that raises is a broken store, not a broken tree: individual
        # trees are already skipped one by one inside. Worth answering as an
        # error rather than as an empty list, which would read as "nothing is
        # loadable" and send the caller looking at their strategy.
        logger.exception("registry reload failed")
        await req.reply(
            RpcErrorEnvelope.wrap(
                RpcError(code="reload_failed", message=str(exc)),
                type=STS_ERROR,
                source="sts",
                session_id=req.envelope.session_id,
            )
        )
        return

    logger.info("registry reloaded: %d strategy(ies)", len(loaded))
    await req.reply(
        StsRegistryReloadResultEnvelope.wrap(
            StsRegistryReloadResult(loaded=loaded, generation=stamp.generation),
            type=STS_REGISTRY_RELOAD,
            source="sts",
            session_id=req.envelope.session_id,
        )
    )


async def handle_registry_generation(
    req: IncomingRequest,
    *,
    instance: str | None = None,
) -> None:
    del instance
    stamp = current_stamp()
    await req.reply(
        StsRegistryGenerationResultEnvelope.wrap(
            StsRegistryGenerationResult(
                generation=stamp.generation,
                packages=current_packages(),
                overlay_live=overlay_is_live(),
            ),
            type=STS_REGISTRY_GENERATION,
            source="sts",
            session_id=req.envelope.session_id,
        )
    )
