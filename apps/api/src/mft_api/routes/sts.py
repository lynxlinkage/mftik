"""STS HTTP facade — strategy.yml deploy + list/control."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException
from mft.protocol import (
    DEFAULT_STRATEGY_YML,
    STS_SESSION_LIST,
    STS_SESSION_PAUSE,
    STS_SESSION_RESUME,
    STS_SESSION_STOP,
    ListSessionsRequest,
    ListSessionsRequestEnvelope,
    ListSessionsResult,
    StrategySpec,
    StrategyStsSpec,
    StrategyYamlError,
    StsSessionControlRequest,
    StsSessionControlRequestEnvelope,
    StsSessionControlResult,
    Topics,
    dump_strategy_yml,
    parse_strategy_yml,
)
from mft_db.repositories import AccountRepository, StrategyRepository
from mft_db.session import session_scope

from mft_api.audit_util import record_audit
from mft_api.broker_rpc import DomainRpcError, request_domain
from mft_api.deps import DEFAULT_USER_ID, BrokerDep
from mft_api.orchestrate import deploy_strategy
from mft_api.schemas import (
    DeployResponse,
    SessionListResponse,
    SessionOut,
    StrategyDeployBody,
    StrategyListResponse,
    StrategyOut,
    StrategyTypesResponse,
    StrategyYamlResponse,
    StsControlResponse,
    TdAttachOut,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/sts", tags=["sts"])

# Class names accepted in strategy.yml ``sts.type`` (must match STS registry).
_KNOWN_STRATEGY_TYPES = ["NoopStrategy"]


@router.get("/template")
async def strategy_template() -> dict[str, str]:
    """Default strategy.yml snippet for the live editor."""
    return {"yaml": DEFAULT_STRATEGY_YML}


@router.get("/types", response_model=StrategyTypesResponse)
async def list_strategy_types() -> StrategyTypesResponse:
    return StrategyTypesResponse(types=list(_KNOWN_STRATEGY_TYPES))


@router.get("/strategies", response_model=StrategyListResponse)
async def list_strategies(
    broker: BrokerDep, limit: int = 100
) -> StrategyListResponse:
    """List deployed strategy.yml rows, joined to sts session status."""
    paused_by_session: dict[str, bool] = {}
    try:
        live = await request_domain(
            broker,
            Topics.STS,
            ListSessionsRequestEnvelope.wrap(
                ListSessionsRequest(domain="sts", status="live"),
                type=STS_SESSION_LIST,
                source="api",
            ),
            result_type=ListSessionsResult,
        )
        for s in live.sessions:
            if s.paused is not None:
                paused_by_session[s.session_id] = s.paused
    except DomainRpcError:
        logger.exception("failed to fetch live STS pause state for strategies list")

    async with session_scope() as db:
        rows = await StrategyRepository(db).list_with_session(limit=limit)

    out: list[StrategyOut] = []
    for row in rows:
        session = row.session
        out.append(
            StrategyOut(
                id=row.id,
                type=row.type,
                config=dict(row.config or {}),
                created_by=row.created_by,
                created_at=row.created_at.timestamp() if row.created_at else 0.0,
                sts_session=row.sts_session,
                status=session.status if session is not None else None,
                paused=paused_by_session.get(row.sts_session),
            )
        )
    return StrategyListResponse(strategies=out)


#: Stand-in for a ``td`` account whose credential has since been deleted. The
#: name is unrecoverable, so emit something that fails loudly at deploy rather
#: than something that silently deploys against fewer accounts.
def _deleted_td_placeholder(api_id: int) -> str:
    return f"<deleted api_id={api_id}>"


@router.get("/strategies/{strategy_id}/yaml", response_model=StrategyYamlResponse)
async def strategy_yaml(strategy_id: int) -> StrategyYamlResponse:
    """Rebuild a past deploy as strategy.yml.

    The submitted document is not stored, so this is a reconstruction from the
    spec that was persisted: ``strategies`` for the sts block and the session
    row for the td/md attach lists. It parses back to the same spec, but the
    original comments and formatting are gone.
    """
    async with session_scope() as db:
        row = await StrategyRepository(db).get_with_session(strategy_id)
        if row is None:
            raise HTTPException(
                status_code=404, detail=f"strategy not found: {strategy_id}"
            )

        session = row.session
        td_api_ids = [int(v) for v in (session.td_api_ids or [])] if session else []
        md_ids = [str(v) for v in (session.md_ids or [])] if session else []

        # strategy.yml names accounts; the session stores api ids. Map back.
        accounts = AccountRepository(db)
        td_names: list[str] = []
        unresolved: list[int] = []
        for api_id in td_api_ids:
            account = await accounts.get_by_api_id(api_id)
            if account is None:
                unresolved.append(api_id)
                td_names.append(_deleted_td_placeholder(api_id))
                continue
            td_names.append(account.name)

    spec = StrategySpec(
        td=td_names,
        md=md_ids,
        sts=StrategyStsSpec(type=row.type, config=dict(row.config or {})),
    )
    return StrategyYamlResponse(
        id=row.id,
        sts_session=row.sts_session,
        yaml=dump_strategy_yml(spec),
        unresolved_td=unresolved,
    )


@router.get("/sessions", response_model=SessionListResponse)
async def list_sessions(
    broker: BrokerDep, status: str | None = "live"
) -> SessionListResponse:
    try:
        result = await request_domain(
            broker,
            Topics.STS,
            ListSessionsRequestEnvelope.wrap(
                ListSessionsRequest(domain="sts", status=status),
                type=STS_SESSION_LIST,
                source="api",
            ),
            result_type=ListSessionsResult,
        )
    except DomainRpcError as exc:
        raise HTTPException(status_code=502, detail=exc.message) from exc
    return SessionListResponse(
        sessions=[SessionOut.model_validate(s.model_dump()) for s in result.sessions]
    )


@router.post("/sessions/{session_id}/pause", response_model=StsControlResponse)
async def pause_session(session_id: str, broker: BrokerDep) -> StsControlResponse:
    return await _control(broker, session_id, STS_SESSION_PAUSE, "sts.session.pause")


@router.post("/sessions/{session_id}/resume", response_model=StsControlResponse)
async def resume_session(session_id: str, broker: BrokerDep) -> StsControlResponse:
    return await _control(broker, session_id, STS_SESSION_RESUME, "sts.session.resume")


@router.post("/sessions/{session_id}/stop", response_model=StsControlResponse)
async def stop_session(session_id: str, broker: BrokerDep) -> StsControlResponse:
    return await _control(broker, session_id, STS_SESSION_STOP, "sts.session.stop")


@router.post("", response_model=DeployResponse)
@router.post("/", response_model=DeployResponse, include_in_schema=False)
async def deploy(body: StrategyDeployBody, broker: BrokerDep) -> DeployResponse:
    """Deploy a strategy.yml document; API orchestrates STS create + MD/TD attach."""
    try:
        spec = parse_strategy_yml(body.yaml)
    except StrategyYamlError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if spec.sts.type not in _KNOWN_STRATEGY_TYPES:
        raise HTTPException(
            status_code=404, detail=f"unknown strategy type: {spec.sts.type}"
        )

    created_by = body.created_by if body.created_by is not None else DEFAULT_USER_ID
    td_api_ids = await _resolve_td_names(list(spec.td))
    try:
        result = await deploy_strategy(
            broker,
            strategy_id=spec.sts.type,
            td=td_api_ids,
            md=list(spec.md),
            st_paras=dict(spec.sts.config),
            created_by=created_by,
            timeout=body.timeout,
        )
    except DomainRpcError as exc:
        code = 404 if exc.code in {"unknown_strategy", "not_found"} else 502
        if exc.code == "timeout":
            code = 504
        raise HTTPException(status_code=code, detail=exc.message) from exc

    session_id = result["session_id"]
    async with session_scope() as db:
        row = await StrategyRepository(db).create(
            type=spec.sts.type,
            config=dict(spec.sts.config),
            created_by=created_by,
            sts_session=session_id,
        )
        strategy_id = row.id

    await record_audit(
        user_id=created_by,
        operation="sts.deploy",
        result=(
            f"id={strategy_id} session_id={session_id} type={spec.sts.type} "
            f"td_names={list(spec.td)} "
            f"td={[a['api_id'] for a in result['td']]} md={result['md']}"
        ),
    )
    return DeployResponse(
        id=strategy_id,
        session_id=session_id,
        type=spec.sts.type,
        config=dict(spec.sts.config),
        td=[TdAttachOut(**a) for a in result["td"]],
        md=result["md"],
        status=result["status"],
    )


async def _resolve_td_names(names: list[str]) -> list[int]:
    """Map strategy.yml account names → api ids (order preserved)."""
    if not names:
        return []
    async with session_scope() as db:
        accounts = AccountRepository(db)
        api_ids: list[int] = []
        for name in names:
            account = await accounts.get_by_name(name)
            if account is None or account.api is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"unknown td account name: {name!r}",
                )
            api_ids.append(account.api_id)
        return api_ids


async def _control(
    broker: BrokerDep,
    session_id: str,
    type_name: str,
    audit_op: str,
) -> StsControlResponse:
    try:
        result = await request_domain(
            broker,
            Topics.STS,
            StsSessionControlRequestEnvelope.wrap(
                StsSessionControlRequest(session_id=session_id),
                type=type_name,
                source="api",
                session_id=session_id,
            ),
            result_type=StsSessionControlResult,
            timeout=10.0,
        )
    except DomainRpcError as exc:
        code = 404 if exc.code == "not_found" else 502
        raise HTTPException(status_code=code, detail=exc.message) from exc

    await record_audit(
        user_id=DEFAULT_USER_ID,
        operation=audit_op,
        result=f"session_id={result.session_id} status={result.status}",
    )
    return StsControlResponse.model_validate(result.model_dump())
