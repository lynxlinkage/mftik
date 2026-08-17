"""Strategy registry — publish on this node, pull from another.

``public/`` is what this node serves. ``private/`` stays here.
``pulled/{name}/`` is a copy of someone else's ``public/``; it is never
re-exported.
"""

from __future__ import annotations

import httpx
from fastapi import APIRouter, HTTPException
from mftik.registry import (
    AddedStrategy,
    RegistryConflict,
    RegistryError,
    connect_remote,
    diff_remote,
    split_qualified,
)
from mftik.registry.protocol import handshake_info
from mftik_db.repositories import StrategyRepository
from mftik_db.session import session_scope

from mftik_api.deps import RegistryStoreDep
from mftik_api.schemas import (
    RegistryAddBody,
    RegistryConnectOut,
    RegistryDiffOut,
    RegistryInfoOut,
    RegistryRemoteBody,
    RegistryRemoteDetailOut,
    RegistryRemoteOut,
    RegistryRemotesResponse,
    RegistryStrategyDetailOut,
    RegistryStrategyListResponse,
    RegistryStrategyOut,
    RegistrySyncRow,
)

router = APIRouter(prefix="/registry/v1", tags=["registry"])


def _strategy_out(added: AddedStrategy) -> RegistryStrategyOut:
    return RegistryStrategyOut(
        name=added.name,
        type=added.type,
        digest=added.digest,
        requires_mftik=added.requires_mftik,
        origin=added.origin,
        files=list(added.files),
    )


@router.get("/info", response_model=RegistryInfoOut)
async def registry_info() -> RegistryInfoOut:
    """Wire version. A peer that cannot speak this refuses to connect."""
    return RegistryInfoOut.model_validate(handshake_info())


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


@router.post("/add", response_model=RegistryStrategyOut)
async def add_strategy(
    body: RegistryAddBody, store: RegistryStoreDep
) -> RegistryStrategyOut:
    """Copy a strategy's files into this node's public or private registry."""
    try:
        added = store.add(body.files, replace=body.replace, origin=body.origin)
    except RegistryConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except RegistryError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _strategy_out(added)


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
    )


@router.post("/remotes", response_model=RegistryConnectOut)
async def connect(
    body: RegistryRemoteBody, store: RegistryStoreDep
) -> RegistryConnectOut:
    """Name a peer, check protocol, and pull everything it publishes."""
    try:
        result = await connect_remote(
            store, name=body.name, url=body.url, token=body.token
        )
    except RegistryError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502, detail=f"cannot reach remote: {exc}"
        ) from exc
    return RegistryConnectOut(
        name=result.name,
        url=result.url,
        pulled=[_strategy_out(rec) for rec in result.pulled],
    )


@router.delete("/remotes/{name}", response_model=RegistryRemoteOut)
async def disconnect_remote(
    name: str, store: RegistryStoreDep
) -> RegistryRemoteOut:
    """Drop the named peer and the copy pulled from it.

    Refuses while any live STS session still uses a strategy pulled from
    this peer — stop those sessions first.
    """
    if store.get_remote(name) is None:
        raise HTTPException(status_code=404, detail=f"unknown remote: {name}")
    async with session_scope() as db:
        rows = await StrategyRepository(db).list_live_for_origin(name)
    live: list[tuple[str, str]] = []
    for row in rows:
        split = split_qualified(row.type)
        if split is None or split[0] != name:
            continue
        live.append((split[1], row.sts_session))
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
    return RegistryRemoteOut(name=remote.name, url=remote.url, count=0)

