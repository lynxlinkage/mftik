"""Strategy registry — publish on this node, pull from another.

``public/`` is what this node serves. ``private/`` stays here.
``pulled/{name}/`` is a copy of someone else's ``public/``; it is never
re-exported.
"""

from __future__ import annotations

import logging

import httpx
from fastapi import APIRouter, HTTPException, Query
from mftik.environment import NodeEnv, unapproved_present
from mftik.protocol import StsRegistryTreeOp
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
from mftik_api.sts_fanout import StsFanoutResult, sync_registry

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/registry/v1", tags=["registry"])


def _upsert(rec: AddedStrategy, files: dict[str, str]) -> StsRegistryTreeOp:
    return StsRegistryTreeOp(
        op="upsert",
        origin=rec.origin,
        name=rec.name,
        digest=rec.digest,
        files=files,
    )


def _delete_op(origin: str, name: str) -> StsRegistryTreeOp:
    return StsRegistryTreeOp(op="delete", origin=origin, name=name)


def _sync_failure(fanout: StsFanoutResult, *, stored: str, restart: str) -> str:
    """Sentence for a fan-out that did not complete.

    ``stored`` is already true on the API disk (``the strategy was stored``).
    A payload that cannot fit in one broker message says so; anything else
    is a timeout or a census miss, which already names the instance.
    """
    if fanout.error is not None and "broker payload limit" in fanout.error:
        return (
            f"{stored}, but it exceeds the broker payload limit and was not "
            f"sent to STS ({fanout.error})."
        )
    return f"{stored}, but STS did not reload ({fanout.error}). {restart}"


_ABSENT_ON_STS = "not present on this registry disk"
_ABSENT_NAMED = "not present on its registry disk"


def _missing_on(fanout: StsFanoutResult, key: str) -> str:
    """Per-instance reason ``key`` did not take this write.

    A skip wins over ``loaded``. The rescan can still list the previous
    class when the new bytes were refused, and that is not "deployable".
    STS says "this registry disk" because it is speaking about itself. Once
    the instance name is in front of the clause, "its" is the sentence.
    """
    parts: list[str] = []
    labels = list(fanout.loaded_by) + [
        label for label in fanout.skipped if label not in fanout.loaded_by
    ]
    for label in labels:
        reason = fanout.skipped.get(label, {}).get(key)
        if reason is None and key in fanout.loaded_by.get(label, ()):
            continue
        if reason is None or reason == _ABSENT_ON_STS:
            reason = _ABSENT_NAMED
        parts.append(f"{label}: {reason}")
    if not parts:
        return "not present on its registry disk"
    return "; ".join(parts)


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


def _present_extras() -> dict[str, str]:
    """On the volume as somebody's dependency, and not approved.

    The store cannot read this — it reads neither Postgres nor the overlay —
    so the refusal only tells ``mftik push`` the difference between "not on
    this node" and "here at 1.26.4, approve it" if this hands it over.
    """
    env = NodeEnv.from_env()
    return unapproved_present(env, env.read_stamp())


@router.post("/add", response_model=RegistryAddOut)
async def add_strategy(
    body: RegistryAddBody,
    store: RegistryStoreDep,
    broker: BrokerDep,
) -> RegistryAddOut:
    """Copy a strategy's files into this node's public or private registry.

    Then send the same files to every enabled STS, which writes them onto
    its own registry and re-scans. The API disk and an STS disk are not the
    same volume. Until that copy lands, a deploy naming this strategy
    answers ``unknown_strategy`` — and a *replace* is worse, because the
    deploy succeeds and runs the code from before the edit.

    A sync that does not land does not undo the add. The files are on the
    API disk, so this answers 200 with ``loaded`` false rather than a 5xx
    that would invite a retry of a write that already happened. ``load_error``
    names the instance that does not have the tree, and why.
    """
    try:
        added = store.add(
            body.files,
            replace=body.replace,
            origin=body.origin,
            applied_extras=_applied_extras(),
            present_extras=_present_extras(),
        )
    except RegistryConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except RegistryError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    key = qualify(added.origin, added.type)
    fanout = await sync_registry(
        broker, [_upsert(added, store.read_contents(added))]
    )
    if fanout.error is not None:
        return RegistryAddOut(
            **_strategy_out(added).model_dump(),
            loaded=False,
            load_error=_sync_failure(
                fanout,
                stored="the strategy was stored",
                restart="It will be copied when that STS next starts.",
            ),
        )
    if key in fanout.loaded:
        return RegistryAddOut(**_strategy_out(added).model_dump(), loaded=True)
    logger.warning("STS synced but did not register %s", key)
    return RegistryAddOut(
        **_strategy_out(added).model_dump(),
        loaded=False,
        load_error=(
            f"the strategy was stored, but {_missing_on(fanout, key)}"
        ),
    )


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

    fanout = await sync_registry(broker, [_delete_op(origin, removed.name)])
    if fanout.error is not None:
        error = _sync_failure(
            fanout,
            stored="the strategy was deleted",
            restart=(
                "It will be removed from that STS when the process next starts."
            ),
        )
    else:
        # Absent from every process that answered. The intersection would
        # call this unloaded when only one STS had dropped the key.
        still = [
            label
            for label, keys in fanout.loaded_by.items()
            if key in keys
        ]
        if still:
            logger.error(
                "STS still answers to %s after it was deleted: %s",
                key,
                ", ".join(still),
            )
            error = (
                f"the strategy was deleted, but {', '.join(still)} still "
                f"answers to {key!r}. It will be removed when that STS "
                f"next starts."
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

    Then copy each pulled tree onto every STS and rescan, for the same
    reason ``add`` does. ``loaded`` is which of the pulled strategies every
    STS can now resolve — not necessarily all of them, since a pulled tree
    can collide with a bundled name or fail to import here.
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

    fanout = await sync_registry(
        broker, [_upsert(rec, store.read_contents(rec)) for rec in result.pulled]
    )
    pulled_keys = {qualify(rec.origin, rec.type) for rec in result.pulled}
    if fanout.error is not None:
        load_error = _sync_failure(
            fanout,
            stored="the strategies were pulled",
            restart="They are copied when that STS next starts.",
        )
    else:
        missing = sorted(pulled_keys - fanout.loaded)
        load_error = (
            None
            if not missing
            else "the strategies were pulled, but "
            + "; ".join(
                f"{key}: {_missing_on(fanout, key)}" for key in missing
            )
        )
    return RegistryConnectOut(
        name=result.name,
        url=result.url,
        pulled=[_strategy_out(rec) for rec in result.pulled],
        loaded=sorted(pulled_keys & fanout.loaded),
        load_error=load_error,
    )


@router.delete("/remotes/{name}", response_model=RegistryRemoteOut)
async def disconnect_remote(
    name: str, store: RegistryStoreDep, broker: BrokerDep
) -> RegistryRemoteOut:
    """Drop the named peer and the copy pulled from it.

    Refuses while any live STS session still uses a strategy pulled from
    this peer — stop those sessions first.

    Then deletes those trees on every STS. Unlike the other three, this one
    has nothing useful to put in the response: the remote is gone from this
    node either way, and a sync that could not be delivered leaves stale
    keys until that STS next starts and catches up. It goes to the log.
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
    pulled = list(store.list_pulled_from(name))
    remote = store.drop_remote(name)
    fanout = await sync_registry(
        broker, [_delete_op(name, rec.name) for rec in pulled]
    )
    rpc_error = fanout.error
    if rpc_error is not None:
        logger.warning(
            "disconnected %s but STS did not reload (%s); that STS drops "
            "the remote's strategies when it next starts",
            name,
            rpc_error,
        )
    return RegistryRemoteOut(name=remote.name, url=remote.url, count=0)

