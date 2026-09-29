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
from pathlib import Path
from typing import TYPE_CHECKING

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
from mftik.registry import (
    RegistryError,
    RegistryStore,
    load_class,
    qualify,
    split_qualified,
)
from mftik.registry.qualify import OWN_ORIGINS
from mftik.strategy import Strategy

from mftik_sts.impl import _BUILTIN_KEYS, registered_keys
from mftik_sts.rpc.env import current_packages
from mftik_sts.runtime_env import current_stamp, overlay_is_live, refresh

if TYPE_CHECKING:
    from mftik_sts.session import SessionManager

logger = logging.getLogger(__name__)

SKIP_ABSENT = "not present on this registry disk"
SKIP_COLLISION = "name collision with a bundled strategy"
SKIP_DIGEST = "digest mismatch"


def _delete_tree(store: RegistryStore, op: StsRegistryTreeOp) -> None:
    """Remove one tree. Already gone is success, on either kind of origin."""
    if op.origin in OWN_ORIGINS:
        try:
            store.remove(op.name, origin=op.origin)
        except RegistryError:
            return
        return
    store.discard(op.name, origin=op.origin)


def _apply_ops(store: RegistryStore, request: StsRegistrySyncRequest) -> dict[str, str]:
    """Write the batch. A tree that cannot be written is skipped, not fatal.

    One broken upsert must not discard the rest of the batch, and must not
    look like "the disk is missing" — the caller needs the import error.
    """
    skipped: dict[str, str] = {}
    for op in request.trees:
        key = qualify(op.origin, op.name)
        if op.op == "delete":
            _delete_tree(store, op)
            continue
        try:
            added = store.add(op.files, replace=True, origin=op.origin)
        except RegistryError as exc:
            skipped[key] = f"import error: {exc}"
            continue
        except OSError as exc:
            skipped[key] = f"write error: {exc}"
            continue
        if op.digest and added.digest != op.digest:
            _delete_tree(
                store,
                StsRegistryTreeOp(op="delete", origin=op.origin, name=op.name),
            )
            skipped[key] = SKIP_DIGEST
    return skipped


def _find(store: RegistryStore, origin: str, name: str):
    for rec in store.list_all():
        if rec.origin == origin and rec.name == name:
            return rec
    return None


def explain_skip(store: RegistryStore, key: str) -> str:
    """Why ``key`` is on the request and not in the scan's loaded list."""
    split = split_qualified(key)
    if split is None:
        return SKIP_ABSENT
    origin, name = split
    rec = _find(store, origin, name)
    if rec is None:
        return SKIP_ABSENT
    try:
        cls = load_class(
            Path(rec.path),
            type_name=rec.type,
            source=rec.origin,
            name=rec.name,
            digest=rec.digest,
        )
    except Exception as exc:
        return f"import error: {exc}"
    if not isinstance(cls, type) or not issubclass(cls, Strategy):
        return f"import error: {rec.type} is not a Strategy"
    if cls.__name__ in _BUILTIN_KEYS:
        return SKIP_COLLISION
    return SKIP_ABSENT


def apply_sync(
    store: RegistryStore, request: StsRegistrySyncRequest
) -> StsRegistrySyncResult:
    """Write ``request`` onto ``store`` and, on the last batch, rescan it."""
    skipped = _apply_ops(store, request)
    if request.reload:
        loaded, stamp = refresh(store, data_dir=store.data_dir)
        upserted = [
            qualify(op.origin, op.name)
            for op in request.trees
            if op.op == "upsert"
        ]
        for key in [*request.explain, *upserted]:
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
    sessions: SessionManager | None = None,
) -> None:
    del sessions
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
    sessions: SessionManager | None = None,
) -> None:
    del sessions
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
    sessions: SessionManager | None = None,
) -> None:
    del sessions
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
    sessions: SessionManager | None = None,
) -> None:
    del sessions
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
