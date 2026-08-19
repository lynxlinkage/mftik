"""Strategy registry — publish on this node, pull from another.

``public/`` is what this node serves. ``private/`` stays here.
``pulled/{name}/`` is a copy of someone else's ``public/``; it is never
re-exported.
"""

from __future__ import annotations

import logging

import httpx
from fastapi import APIRouter, HTTPException, Query
from mftik.environment import NodeEnv
from mftik.protocol import (
    STS_REGISTRY_RELOAD,
    StsRegistryReloadRequest,
    StsRegistryReloadRequestEnvelope,
    StsRegistryReloadResult,
    Topics,
)
from mftik.registry import (
    AddedStrategy,
    MissingRemoteExtras,
    RegistryConflict,
    RegistryError,
    connect_remote,
    diff_remote,
    qualify,
    split_qualified,
)
from mftik.registry.protocol import handshake_info
from mftik.registry.qualify import PRIVATE_ORIGIN, PUBLIC_ORIGIN
from mftik_db.repositories import StsSessionRepository
from mftik_db.session import session_scope

from mftik_api.auth import ANONYMOUS, PrincipalDep
from mftik_api.broker_rpc import DomainRpcError, request_domain
from mftik_api.deps import BrokerDep, RegistryStoreDep
from mftik_api.schemas import (
    RegistryAddBody,
    RegistryAddOut,
    RegistryConnectOut,
    RegistryDiffOut,
    RegistryInfoOut,
    RegistryRemoteBody,
    RegistryRemoteDetailOut,
    RegistryRemoteOut,
    RegistryRemotesResponse,
    RegistryRemovedOut,
    RegistryStrategyDetailOut,
    RegistryStrategyListResponse,
    RegistryStrategyOut,
    RegistrySyncRow,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/registry/v1", tags=["registry"])


def _strategy_out(added: AddedStrategy) -> RegistryStrategyOut:
    return RegistryStrategyOut(
        name=added.name,
        type=added.type,
        digest=added.digest,
        requires_mftik=added.requires_mftik,
        requires=list(added.requires),
        origin=added.origin,
        files=list(added.files),
    )


@router.get("/info", response_model=RegistryInfoOut, response_model_exclude_none=True)
async def registry_info(principal: PrincipalDep = ANONYMOUS) -> RegistryInfoOut:
    """Wire version. A peer that cannot speak this refuses to connect.

    Extra *names* are public — ``connect_remote`` only compares those.
    Exact pins stay off the anonymous response; a registry key, API key,
    or session gets ``version`` and ``dist``.
    """
    return RegistryInfoOut.model_validate(
        handshake_info(pins=principal.authenticated)
    )


@router.get("/strategies", response_model=RegistryStrategyListResponse)
async def list_published(store: RegistryStoreDep) -> RegistryStrategyListResponse:
    """Strategies this node publishes. Private and pulled copies are not listed."""
    return RegistryStrategyListResponse(
        strategies=[_strategy_out(rec) for rec in store.list_public()]
    )


@router.get("/private", response_model=RegistryStrategyListResponse)
async def list_private(store: RegistryStoreDep) -> RegistryStrategyListResponse:
    """Strategies that stay on this node. Peers never see this list."""
    return RegistryStrategyListResponse(
        strategies=[_strategy_out(rec) for rec in store.list_private()]
    )


@router.get(
    "/strategies/{name}", response_model=RegistryStrategyDetailOut
)
async def get_published(
    name: str, store: RegistryStoreDep
) -> RegistryStrategyDetailOut:
    """One published tree, including source, so another node can copy it."""
    rec = store.get_public(name)
    if rec is None:
        raise HTTPException(status_code=404, detail=f"unknown strategy: {name}")
    try:
        contents = store.read_contents(rec)
    except RegistryError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RegistryStrategyDetailOut(
        **_strategy_out(rec).model_dump(), contents=contents
    )


def _applied_extras() -> dict[str, str]:
    stamp = NodeEnv.from_env().read_stamp()
    if not stamp.matches_runtime():
        return {}
    return {name: rec.version for name, rec in stamp.packages.items()}


@router.post("/add", response_model=RegistryAddOut)
async def add_strategy(
    body: RegistryAddBody,
    store: RegistryStoreDep,
    broker: BrokerDep,
) -> RegistryAddOut:
    """Copy a strategy's files into this node's public or private registry.

    Then tell STS to re-read the registry, and say whether it worked. Writing
    the files is only half of an add: STS imports the registry at boot, so
    until it re-scans, a deploy naming this strategy answers
    ``unknown_strategy`` — and a *replace* is worse, because the deploy
    succeeds and runs the code from before the edit.

    The reload not working does not undo the add. The files are on disk and
    the next STS restart will find them, so this answers 200 with ``loaded``
    false rather than a 5xx that would invite a retry of a write that already
    happened.
    """
    try:
        added = store.add(
            body.files,
            replace=body.replace,
            origin=body.origin,
            applied_extras=_applied_extras(),
        )
    except RegistryConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except RegistryError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    key = qualify(added.origin, added.type)
    keys, rpc_error = await _reload_sts(broker)
    if rpc_error is not None:
        return RegistryAddOut(
            **_strategy_out(added).model_dump(),
            loaded=False,
            load_error=(
                f"the strategy was stored, but STS did not reload ({rpc_error}). "
                "It will be picked up when STS next restarts."
            ),
        )
    if key in keys:
        return RegistryAddOut(**_strategy_out(added).model_dump(), loaded=True)
    # STS answered and did not list it, so the scan reached this tree and
    # rejected it. Its own log has the reason; what is knowable here is that
    # deploying will not work, which is the part the caller has to act on.
    logger.warning("STS reloaded but did not register %s", key)
    return RegistryAddOut(
        **_strategy_out(added).model_dump(),
        loaded=False,
        load_error=(
            f"the strategy was stored, but STS did not load it as {key!r} — "
            "check the STS log for the import error or name collision."
        ),
    )


async def _reload_sts(
    broker: BrokerDep,
) -> tuple[frozenset[str], str | None]:
    """Ask STS to re-scan the registry.

    Returns the qualified type keys it answers to afterwards, or an empty set
    and the reason it could not be asked. Deliberately not "did it work" —
    ``add`` wants a key to be present and ``delete`` wants one to be absent,
    and a helper that answered either of those directly would have to be read
    backwards by the other caller.
    """
    try:
        result = await request_domain(
            broker,
            Topics.STS,
            StsRegistryReloadRequestEnvelope.wrap(
                StsRegistryReloadRequest(),
                type=STS_REGISTRY_RELOAD,
                source="api",
            ),
            result_type=StsRegistryReloadResult,
            timeout=30.0,
        )
    except DomainRpcError as exc:
        logger.warning("STS registry reload failed: %s", exc.message)
        return frozenset(), exc.message
    return frozenset(result.loaded), None


@router.delete("/strategies/{name}", response_model=RegistryRemovedOut)
async def delete_strategy(
    name: str,
    store: RegistryStoreDep,
    broker: BrokerDep,
    origin: str = Query(
        ...,
        description="public or private — which of this node's own registries",
    ),
) -> RegistryRemovedOut:
    """Delete one of this node's own trees, then tell STS to forget it.

    ``origin`` is required rather than defaulted. ``public`` and ``private``
    can hold trees of the same name, one of which peers pull and one of which
    they never see, and a default would pick between them on a guess. A
    pulled copy is not deletable here at all — that is ``DELETE /remotes``.

    Refuses while a live session is running this strategy. The session holds
    its own instance and would survive the files going away, which is exactly
    what makes it worth refusing: an operator who deletes a strategy has
    decided it should not be running, and finding out that it still is
    belongs before the delete rather than after.
    """
    try:
        rec = _own_strategy(store, name, origin)
    except RegistryError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if rec is None:
        raise HTTPException(
            status_code=404, detail=f"no {origin} strategy named {name!r}"
        )

    key = qualify(origin, rec.type)
    async with session_scope() as db:
        rows = await StsSessionRepository(db).list_live_for_origin(origin)
    live = [row.session_id for row in rows if row.type == key]
    if live:
        raise HTTPException(
            status_code=409,
            detail=(
                f"cannot delete {key}: live sessions are running it. "
                f"Stop these first: {', '.join(live)}"
            ),
        )

    try:
        removed = store.remove(name, origin=origin)
    except RegistryError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    keys, rpc_error = await _reload_sts(broker)
    if rpc_error is not None:
        error = (
            f"the strategy was deleted, but STS did not reload ({rpc_error}). "
            f"It will go on answering to {key!r} until it restarts."
        )
    elif key in keys:
        # The files are gone and STS still lists the key. Nothing here can do
        # anything about that, and an operator who deleted a strategy needs to
        # know it is still deployable.
        logger.error("STS still answers to %s after it was deleted", key)
        error = (
            f"the strategy was deleted, but STS still answers to {key!r}. "
            "Restart STS."
        )
    else:
        error = None
    return RegistryRemovedOut(
        **_strategy_out(removed).model_dump(),
        unloaded=error is None,
        unload_error=error,
    )


def _own_strategy(
    store: RegistryStoreDep, name: str, origin: str
) -> AddedStrategy | None:
    if origin == PUBLIC_ORIGIN:
        return store.get_public(name)
    if origin == PRIVATE_ORIGIN:
        return store.get_private(name)
    raise RegistryError(
        f"origin must be {PUBLIC_ORIGIN!r} or {PRIVATE_ORIGIN!r}, got {origin!r} — "
        f"a pulled copy goes away with its remote (DELETE /registry/v1/remotes)"
    )


@router.get("/remotes", response_model=RegistryRemotesResponse)
async def list_remotes(store: RegistryStoreDep) -> RegistryRemotesResponse:
    return RegistryRemotesResponse(
        remotes=[
            RegistryRemoteOut(
                name=r.name,
                url=r.url,
                count=len(store.list_pulled_from(r.name)),
                authenticated=r.token is not None,
            )
            for r in store.list_remotes()
        ]
    )


@router.get("/remotes/{name}", response_model=RegistryRemoteDetailOut)
async def get_remote(
    name: str, store: RegistryStoreDep
) -> RegistryRemoteDetailOut:
    remote = store.get_remote(name)
    if remote is None:
        raise HTTPException(status_code=404, detail=f"unknown remote: {name}")
    pulled = store.list_pulled_from(name)
    return RegistryRemoteDetailOut(
        name=remote.name,
        url=remote.url,
        count=len(pulled),
        authenticated=remote.token is not None,
        strategies=[_strategy_out(rec) for rec in pulled],
    )


@router.get("/remotes/{name}/diff", response_model=RegistryDiffOut)
async def remote_diff(name: str, store: RegistryStoreDep) -> RegistryDiffOut:
    """Pulled copy versus what the peer currently publishes."""
    remote = store.get_remote(name)
    if remote is None:
        raise HTTPException(status_code=404, detail=f"unknown remote: {name}")
    result = await diff_remote(store, name=name)
    return RegistryDiffOut(
        name=result.name,
        url=result.url,
        count=len(result.rows),
        authenticated=remote.token is not None,
        reachable=result.reachable,
        error=result.error,
        strategies=[
            RegistrySyncRow(
                name=row.name,
                type=row.type,
                local_digest=row.local_digest,
                remote_digest=row.remote_digest,
                status=row.status,
            )
            for row in result.rows
        ],
        extras_warnings=list(result.extras_warnings),
    )


@router.post("/remotes", response_model=RegistryConnectOut)
async def connect(
    body: RegistryRemoteBody, store: RegistryStoreDep, broker: BrokerDep
) -> RegistryConnectOut:
    """Name a peer, check protocol, and pull everything it publishes.

    Then reload, for the same reason ``add`` does: pulling writes trees into
    the registry, and the process that runs them imported it at boot.
    ``loaded`` is which of the pulled strategies STS can now resolve — not
    necessarily all of them, since a pulled tree can collide with a bundled
    name or fail to import here.
    """
    try:
        result = await connect_remote(
            store, name=body.name, url=body.url, token=body.token
        )
    except MissingRemoteExtras as exc:
        # Structured, because the caller's next move depends on which names
        # and whether each is absent or merely unapproved. A client that had
        # to read this out of the sentence would break the first time the
        # sentence changed — which is exactly what happened.
        raise HTTPException(
            status_code=400,
            detail={
                "error": exc.code,
                "message": str(exc),
                "missing": exc.rows(),
            },
        ) from exc
    except RegistryError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502, detail=f"cannot reach remote: {exc}"
        ) from exc

    keys, rpc_error = await _reload_sts(broker)
    pulled_keys = {qualify(rec.origin, rec.type) for rec in result.pulled}
    return RegistryConnectOut(
        name=result.name,
        url=result.url,
        pulled=[_strategy_out(rec) for rec in result.pulled],
        loaded=sorted(pulled_keys & keys),
        load_error=(
            None
            if rpc_error is None
            else (
                f"the strategies were pulled, but STS did not reload "
                f"({rpc_error}). They become deployable when it restarts."
            )
        ),
    )


@router.delete("/remotes/{name}", response_model=RegistryRemoteOut)
async def disconnect_remote(
    name: str, store: RegistryStoreDep, broker: BrokerDep
) -> RegistryRemoteOut:
    """Drop the named peer and the copy pulled from it.

    Refuses while any live STS session still uses a strategy pulled from
    this peer — stop those sessions first.

    Reloads afterwards so STS stops resolving what it just lost. Unlike the
    other three, this one has nothing useful to put in the response: the
    remote is gone from this node either way, and a reload that could not be
    delivered leaves stale keys that the next restart clears. It goes to the
    log instead.
    """
    if store.get_remote(name) is None:
        raise HTTPException(status_code=404, detail=f"unknown remote: {name}")
    async with session_scope() as db:
        rows = await StsSessionRepository(db).list_live_for_origin(name)
    live: list[tuple[str, str]] = []
    for row in rows:
        split = split_qualified(row.type)
        if split is None or split[0] != name:
            continue
        live.append((split[1], row.session_id))
    if live:
        listed = ", ".join(
            f"{type_name} ({session_id})" for type_name, session_id in live
        )
        raise HTTPException(
            status_code=409,
            detail=(
                f"cannot disconnect {name}: live sessions still use its "
                f"strategies. Stop these first: {listed}"
            ),
        )
    remote = store.drop_remote(name)
    _, rpc_error = await _reload_sts(broker)
    if rpc_error is not None:
        logger.warning(
            "disconnected %s but STS did not reload (%s); it will go on "
            "resolving that remote's strategies until it restarts",
            name,
            rpc_error,
        )
    return RegistryRemoteOut(name=remote.name, url=remote.url, count=0)

