"""Owner-curated node extras — apply, then tell STS to see the generation."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query
from mftik.envapply import (
    ApplyFailed,
    ApplyInProgress,
    ApplySpec,
    EnvironmentDisruptive,
    Installer,
    disruptive_names,
    run_uv_installer,
)
from mftik.environment import EnvironmentLocked, EnvStamp, NodeEnv
from mftik.protocol import (
    STS_REGISTRY_RELOAD,
    STS_SESSION_LIST,
    ListSessionsRequest,
    ListSessionsRequestEnvelope,
    ListSessionsResult,
    StsRegistryReloadRequest,
    StsRegistryReloadRequestEnvelope,
    StsRegistryReloadResult,
    Topics,
)
from mftik.registry import RegistryStore

from mftik_api.audit_util import record_audit
from mftik_api.auth import ANONYMOUS, OwnerId, PrincipalDep
from mftik_api.auth.principal import Principal
from mftik_api.broker_rpc import DomainRpcError, request_domain
from mftik_api.deps import DEFAULT_USER_ID, BrokerDep, RegistryStoreDep
from mftik_api.schemas import (
    BrokenTreeOut,
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


def _specs_from_stamp(stamp: EnvStamp) -> dict[str, ApplySpec]:
    return {
        name: ApplySpec(version=rec.version, dist=rec.dist, source=rec.source)
        for name, rec in stamp.packages.items()
    }


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
    packages: dict[str, ApplySpec],
    *,
    force: bool,
    owner: int,
    principal: Principal,
    operation: str,
    broken: list[BrokenTreeOut] | None = None,
) -> EnvironmentOut:
    changed = disruptive_names(env.read_stamp(), packages)
    if changed and not force:
        await _require_no_live_sessions(broker)
    try:
        with ApplyInProgress(
            env,
            packages,
            allow_disruptive=bool(changed),
            installer=_installer(),
        ) as pending:
            if changed and not force:
                await _require_no_live_sessions(broker)
            result = pending.commit()
    except EnvironmentLocked as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except EnvironmentDisruptive as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ApplyFailed as exc:
        raise HTTPException(status_code=502, detail=exc.message) from exc

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
            f"packages={','.join(sorted(packages)) or '(none)'}"
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
    _keys, sts_generation, rpc_error = await _reload_sts(broker)
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
        packages,
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
    env = _env()
    packages = _specs_from_stamp(env.read_stamp())
    packages[body.name] = ApplySpec(
        version=body.version,
        dist=body.dist or body.name,
        source=body.source,
    )
    return await _apply_set(
        env,
        broker,
        packages,
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
    stamp = env.read_stamp()
    if name not in stamp.packages:
        raise HTTPException(
            status_code=404, detail=f"no extra named {name!r} on this node"
        )
    packages = _specs_from_stamp(stamp)
    del packages[name]
    broken = _broken_trees(store, name)
    return await _apply_set(
        env,
        broker,
        packages,
        force=force,
        owner=owner,
        principal=principal,
        operation="environment.package.delete",
        broken=broken,
    )
