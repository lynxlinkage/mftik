"""Owner-curated node extras — apply, then tell STS to see the generation."""

from __future__ import annotations

import logging
import sys

import httpx
from fastapi import APIRouter, HTTPException, Query
from mftik.envapply import (
    ApplyFailed,
    ApplyInProgress,
    ApplySpec,
    EnvironmentDisruptive,
    EnvironmentInvalid,
    EnvironmentMissing,
    Installer,
    disruptive_names,
    merge_packages,
    run_uv_installer,
)
from mftik.envimport import (
    ImportPreview,
    ImportRow,
    confirm_blockers,
    peer_source,
    preview_import,
)
from mftik.environment import (
    EnvironmentLocked,
    EnvStamp,
    NodeEnv,
    dependency_sources,
    normalize_dist,
    provided_imports,
    resolved_dists,
)
from mftik.protocol import (
    STS_REGISTRY_GENERATION,
    STS_REGISTRY_RELOAD,
    STS_SESSION_LIST,
    ListSessionsRequest,
    ListSessionsRequestEnvelope,
    ListSessionsResult,
    StsRegistryGenerationRequest,
    StsRegistryGenerationRequestEnvelope,
    StsRegistryGenerationResult,
    StsRegistryReloadRequest,
    StsRegistryReloadRequestEnvelope,
    StsRegistryReloadResult,
    Topics,
)
from mftik.registry import RegistryStore
from mftik.registry.errors import RegistryError
from mftik.registry.sync import fetch_handshake
from starlette.concurrency import run_in_threadpool

from mftik_api.audit_util import record_audit
from mftik_api.auth import ANONYMOUS, OwnerId, PrincipalDep
from mftik_api.auth.principal import Principal
from mftik_api.broker_rpc import DomainRpcError, request_domain
from mftik_api.deps import DEFAULT_USER_ID, BrokerDep, RegistryStoreDep
from mftik_api.schemas import (
    BrokenTreeOut,
    EnvImportRowOut,
    EnvInstalledOut,
    EnvironmentImportBody,
    EnvironmentImportOut,
    EnvironmentOut,
    EnvironmentPackageBody,
    EnvironmentPutBody,
    EnvPackageIn,
    EnvPackageOut,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/environment", tags=["environment"])

#: Tests replace this so apply never talks to an index.
installer_for_apply: Installer | None = None

#: Tests replace this so import never opens a real socket.
import_client: httpx.AsyncClient | None = None


def _installer() -> Installer:
    return installer_for_apply or run_uv_installer


def _env() -> NodeEnv:
    return NodeEnv.from_env()


def _spec(name: str, item: EnvPackageIn) -> ApplySpec:
    return ApplySpec(
        version=item.version,
        dist=item.dist or name,
        source=item.source,
    )


def _installed_out(env: NodeEnv, stamp: EnvStamp) -> list[EnvInstalledOut]:
    """What is on disk in the live generation, and what the stamp claims.

    A dependency the Owner never named is importable but not declarable: the
    deploy check reads the stamp, so a tree needing numpy is refused while
    numpy sits in the same directory. Listing them is what makes approving
    one possible — and approving is a no-op install at the version here, so
    this list is also where the right pin comes from.
    """
    approved = {normalize_dist(rec.dist) for rec in stamp.packages.values()}
    site_packages = env.site_packages(stamp.generation)
    live = resolved_dists(site_packages)
    sources = dependency_sources(site_packages)
    provides = provided_imports(site_packages)
    rows: list[EnvInstalledOut] = []
    for dist, version in sorted(live.items()):
        # Read off the wheel, never guessed from the distribution name:
        # python-dateutil provides ``dateutil``, and ``python_dateutil`` is a
        # valid identifier that imports nothing. More than one top-level name
        # is a choice, and not this code's to make.
        names = provides.get(dist, ())
        rows.append(
            EnvInstalledOut(
                dist=dist,
                version=version,
                approved=dist in approved,
                suggested_name=names[0] if len(names) == 1 else None,
                needed_by=list(sources.get(dist, ())),
            )
        )
    return rows


def _packages_out(stamp: EnvStamp) -> dict[str, EnvPackageOut]:
    return {
        name: EnvPackageOut(version=rec.version, dist=rec.dist, source=rec.source)
        for name, rec in stamp.packages.items()
    }


def _view(
    stamp: EnvStamp,
    *,
    restart_required: bool = False,
    loaded: bool = True,
    load_error: str | None = None,
    broken: list[BrokenTreeOut] | None = None,
) -> EnvironmentOut:
    runtime = EnvStamp.empty()
    return EnvironmentOut(
        generation=stamp.generation,
        python=list(stamp.python),
        platform=stamp.platform,
        bytes=stamp.nbytes,
        packages=_packages_out(stamp),
        installed=_installed_out(_env(), stamp),
        abi_ok=stamp.matches_runtime(),
        runtime_python=list(runtime.python),
        runtime_platform=runtime.platform,
        restart_required=restart_required,
        loaded=loaded,
        load_error=load_error,
        broken=broken or [],
    )


async def _live_session_ids(broker: BrokerDep) -> list[str]:
    result = await request_domain(
        broker,
        Topics.STS,
        ListSessionsRequestEnvelope.wrap(
            ListSessionsRequest(domain="sts", status="live"),
            type=STS_SESSION_LIST,
            source="api",
        ),
        result_type=ListSessionsResult,
    )
    return [row.session_id for row in result.sessions]


async def _require_no_live_sessions(broker: BrokerDep) -> None:
    try:
        live = await _live_session_ids(broker)
    except DomainRpcError as exc:
        raise HTTPException(
            status_code=409,
            detail=(
                "cannot change extras: STS did not answer the session list "
                f"({exc.message}). Refusing so a live session is not assumed absent."
            ),
        ) from exc
    if live:
        raise HTTPException(
            status_code=409,
            detail=(
                "cannot change extras: live sessions are running. "
                f"Stop these first: {', '.join(live)}"
            ),
        )


async def _reload_sts(
    broker: BrokerDep,
) -> tuple[frozenset[str], int | None, str | None]:
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
        return frozenset(), None, exc.message
    return frozenset(result.loaded), result.generation, None


async def _sts_generation(
    broker: BrokerDep,
) -> tuple[int | None, str | None]:
    try:
        result = await request_domain(
            broker,
            Topics.STS,
            StsRegistryGenerationRequestEnvelope.wrap(
                StsRegistryGenerationRequest(),
                type=STS_REGISTRY_GENERATION,
                source="api",
            ),
            result_type=StsRegistryGenerationResult,
        )
    except DomainRpcError as exc:
        logger.warning("STS env generation failed: %s", exc.message)
        return None, exc.message
    return result.generation, None


def _broken_trees(store: RegistryStore, removed: str) -> list[BrokenTreeOut]:
    return [
        BrokenTreeOut(
            name=rec.name,
            type=rec.type,
            origin=rec.origin,
            requires=list(rec.requires),
        )
        for rec in store.list_all()
        if removed in rec.requires
    ]


async def _apply_set(
    env: NodeEnv,
    broker: BrokerDep,
    *,
    replace: dict[str, ApplySpec] | None = None,
    upsert: dict[str, ApplySpec] | None = None,
    remove: frozenset[str] | None = None,
    force: bool,
    owner: int,
    principal: Principal,
    operation: str,
    broken: list[BrokenTreeOut] | None = None,
) -> EnvironmentOut:
    preview = merge_packages(
        env.read_stamp(),
        replace=replace,
        upsert=upsert,
        remove=remove or (),
        missing_ok=True,
    )
    preview_changed = disruptive_names(env.read_stamp(), preview)
    if preview_changed and not force:
        await _require_no_live_sessions(broker)
    pending = ApplyInProgress(
        env,
        replace,
        upsert=upsert,
        remove=remove,
        allow_disruptive=force or bool(preview_changed),
        installer=_installer(),
    )
    try:
        # ``uv`` is a blocking subprocess that can sit on an index for
        # minutes, and commit walks the new generation. Neither may run on
        # the event loop, or every other request on this node stalls with it.
        # ``__enter__`` cleans up after itself, so a failure here needs no
        # matching ``__exit__``. Merge happens inside ``__enter__``, after
        # the flock, so a second tab cannot overwrite the first's add.
        await run_in_threadpool(pending.__enter__)
    except EnvironmentInvalid as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except EnvironmentMissing as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except EnvironmentLocked as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except EnvironmentDisruptive as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ApplyFailed as exc:
        raise HTTPException(status_code=502, detail=exc.message) from exc
    try:
        # ``pending.disruptive``, not ``changed``: the installer has run by
        # now, so this is the first point that knows whether the resolver
        # moved a dependency nobody named. Adding scipy is not a change to
        # any stamped name and can still swap numpy under a live session.
        if pending.disruptive and not force:
            await _require_no_live_sessions(broker)
        result = await run_in_threadpool(pending.commit)
    except BaseException:
        await run_in_threadpool(pending.__exit__, *sys.exc_info())
        raise
    await run_in_threadpool(pending.__exit__, None, None, None)

    _keys, sts_generation, rpc_error = await _reload_sts(broker)
    if rpc_error is not None:
        loaded = False
        load_error = (
            f"the environment was applied, but STS did not reload ({rpc_error}). "
            "It will be picked up when STS next restarts."
        )
        restart_required = True
    else:
        loaded = True
        load_error = None
        restart_required = result.restart_required or (
            sts_generation is not None and sts_generation < result.stamp.generation
        )

    await record_audit(
        user_id=owner,
        operation=operation,
        result=(
            f"generation={result.stamp.generation} "
            f"packages={','.join(sorted(pending.packages)) or '(none)'}"
        ),
        principal=principal,
    )
    return _view(
        result.stamp,
        restart_required=restart_required,
        loaded=loaded,
        load_error=load_error,
        broken=broken,
    )


@router.get("", response_model=EnvironmentOut)
async def get_environment(broker: BrokerDep) -> EnvironmentOut:
    stamp = _env().read_stamp()
    sts_generation, rpc_error = await _sts_generation(broker)
    if rpc_error is not None:
        restart = stamp.generation > 0
    else:
        restart = (
            sts_generation is not None and sts_generation < stamp.generation
        )
    return _view(stamp, restart_required=restart)


@router.put("", response_model=EnvironmentOut)
async def put_environment(
    body: EnvironmentPutBody,
    broker: BrokerDep,
    force: bool = Query(False),
    owner: OwnerId = DEFAULT_USER_ID,
    principal: PrincipalDep = ANONYMOUS,
) -> EnvironmentOut:
    packages = {name: _spec(name, item) for name, item in body.packages.items()}
    return await _apply_set(
        _env(),
        broker,
        replace=packages,
        force=force or body.force,
        owner=owner,
        principal=principal,
        operation="environment.put",
    )


@router.post("/packages", response_model=EnvironmentOut)
async def upsert_package(
    body: EnvironmentPackageBody,
    broker: BrokerDep,
    force: bool = Query(False),
    owner: OwnerId = DEFAULT_USER_ID,
    principal: PrincipalDep = ANONYMOUS,
) -> EnvironmentOut:
    return await _apply_set(
        _env(),
        broker,
        upsert={
            body.name: ApplySpec(
                version=body.version,
                dist=body.dist or body.name,
                source=body.source,
            )
        },
        force=force or body.force,
        owner=owner,
        principal=principal,
        operation="environment.package.upsert",
    )


@router.delete("/packages/{name}", response_model=EnvironmentOut)
async def delete_package(
    name: str,
    broker: BrokerDep,
    store: RegistryStoreDep,
    force: bool = Query(False),
    owner: OwnerId = DEFAULT_USER_ID,
    principal: PrincipalDep = ANONYMOUS,
) -> EnvironmentOut:
    env = _env()
    if name not in env.read_stamp().packages:
        raise HTTPException(
            status_code=404, detail=f"no extra named {name!r} on this node"
        )
    broken = _broken_trees(store, name)
    return await _apply_set(
        env,
        broker,
        remove=frozenset({name}),
        force=force,
        owner=owner,
        principal=principal,
        operation="environment.package.delete",
        broken=broken,
    )


def _row_out(row: ImportRow) -> EnvImportRowOut:
    return EnvImportRowOut(
        name=row.name,
        version=row.version,
        dist=row.dist,
        status=row.status,
        guessed=row.guessed,
        pinned=row.pinned,
        local_version=row.local_version,
        local_dist=row.local_dist,
    )


def _preview_out(
    preview: ImportPreview, *, environment: EnvironmentOut | None = None
) -> EnvironmentImportOut:
    return EnvironmentImportOut(
        added=[_row_out(row) for row in preview.added],
        kept=[_row_out(row) for row in preview.kept],
        conflicts=[_row_out(row) for row in preview.conflicts],
        guessed=list(preview.guessed_names),
        unpinned=list(preview.unpinned_names),
        applied=environment is not None,
        environment=environment,
    )


def _resolve_peer(
    body: EnvironmentImportBody, store: RegistryStore
) -> tuple[str, str | None, str | None]:
    """Return ``(url, token, name)``. Does not write remotes."""
    if body.url:
        return body.url, body.token, body.name
    remote = store.get_remote(body.name or "")
    if remote is None:
        raise HTTPException(
            status_code=404,
            detail=f"unknown remote: {body.name}",
        )
    return remote.url, body.token or remote.token, remote.name


@router.post("/import", response_model=EnvironmentImportOut)
async def import_environment(
    body: EnvironmentImportBody,
    broker: BrokerDep,
    store: RegistryStoreDep,
    force: bool = Query(False),
    owner: OwnerId = DEFAULT_USER_ID,
    principal: PrincipalDep = ANONYMOUS,
) -> EnvironmentImportOut:
    """Diff a peer's extras against this stamp; confirm is what applies them."""
    url, token, name = _resolve_peer(body, store)
    try:
        info = await fetch_handshake(url, token=token, client=import_client)
    except RegistryError as exc:
        message = str(exc)
        code = 400 if "url must" in message else 502
        raise HTTPException(status_code=code, detail=message) from exc

    extras = info.get("extras") if isinstance(info, dict) else {}
    env = _env()
    stamp = env.read_stamp()
    preview = preview_import(stamp, extras, dist_overrides=body.dist)
    if not body.confirm:
        return _preview_out(preview)

    blockers = confirm_blockers(preview)
    if blockers:
        raise HTTPException(status_code=409, detail="; ".join(blockers))

    if not preview.added:
        return _preview_out(preview, environment=_view(stamp))

    source = peer_source(url, name)
    applied = await _apply_set(
        env,
        broker,
        upsert={
            row.name: ApplySpec(
                version=row.version, dist=row.dist, source=source
            )
            for row in preview.added
        },
        force=force or body.force,
        owner=owner,
        principal=principal,
        operation="environment.import",
    )
    return _preview_out(preview, environment=applied)
