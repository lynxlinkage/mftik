"""STS HTTP facade — strategy.yml deploy + list/control."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException
from mft.protocol import (
    DEFAULT_STRATEGY_TYPE,
    RESTART_ALWAYS,
    STS_SESSION_LIST,
    STS_SESSION_PAUSE,
    STS_SESSION_RESUME,
    STS_SESSION_STOP,
    ListSessionsRequest,
    ListSessionsRequestEnvelope,
    ListSessionsResult,
    StrategySpec,
    StrategyYamlError,
    StsSessionControlRequest,
    StsSessionControlRequestEnvelope,
    StsSessionControlResult,
    Topics,
    all_templates,
    default_template,
    dump_strategy_yml,
    get_template,
    parse_strategy_yml,
    strategy_types,
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
    StrategyTemplateOut,
    StrategyTypesResponse,
    StrategyYamlResponse,
    StsControlResponse,
    TdAttachOut,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/sts", tags=["sts"])

@router.get("/template")
async def strategy_template() -> dict[str, str]:
    """Template for the default strategy — the editor's starting document."""
    return {"yaml": default_template().yaml}


@router.get("/types", response_model=StrategyTypesResponse)
async def list_strategy_types() -> StrategyTypesResponse:
    """Deployable strategies, with the template each one starts from."""
    return StrategyTypesResponse(
        types=strategy_types(),
        templates=[
            StrategyTemplateOut.model_validate(t.model_dump())
            for t in all_templates()
        ],
        default=DEFAULT_STRATEGY_TYPE,
    )


@router.get("/types/{strategy_type}/template", response_model=StrategyTemplateOut)
async def strategy_type_template(strategy_type: str) -> StrategyTemplateOut:
    """The starting document for one strategy type."""
    template = get_template(strategy_type)
    if template is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"unknown strategy type: {strategy_type}; "
                f"known: {', '.join(strategy_types())}"
            ),
        )
    return StrategyTemplateOut.model_validate(template.model_dump())


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
                reason=session.reason if session is not None else None,
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
    """The strategy.yml behind a past deploy.

    Served verbatim from what was submitted. The stored text is what a person
    wrote — comments, ordering and the account names as typed — and it is what
    everything else about the deploy was derived from, so it is the document
    to hand back.

    Deploys made before the text was kept have nothing to serve, and fall back
    to a reconstruction from the persisted spec (``reconstructed``). That
    document parses to the same spec but is not the same document: comments
    and formatting are gone, and ``td`` shows each account's *current* name
    rather than the one that was typed.
    """
    async with session_scope() as db:
        row = await StrategyRepository(db).get_with_session(strategy_id)
        if row is None:
            raise HTTPException(
                status_code=404, detail=f"strategy not found: {strategy_id}"
            )

        if row.yaml_text:
            return StrategyYamlResponse(
                id=row.id,
                type=row.type,
                sts_session=row.sts_session,
                yaml=row.yaml_text,
                reconstructed=False,
            )

        session = row.session
        td_api_ids = [int(v) for v in (session.td_api_ids or [])] if session else []
        md_ids = [str(v) for v in (session.md_ids or [])] if session else []
        restart = session.restart if session is not None else RESTART_ALWAYS

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
        td=td_names, md=md_ids, restart=restart, sts=dict(row.config or {})
    )
    return StrategyYamlResponse(
        id=row.id,
        type=row.type,
        sts_session=row.sts_session,
        yaml=dump_strategy_yml(spec),
        unresolved_td=unresolved,
        reconstructed=True,
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


@router.post("/deploy/{strategy_type}", response_model=DeployResponse)
async def deploy(
    strategy_type: str, body: StrategyDeployBody, broker: BrokerDep
) -> DeployResponse:
    """Deploy ``strategy_type`` with the td / md / sts document in ``body``.

    The type is in the path rather than the document because it decides what
    ``sts:`` is allowed to contain — keeping them together let a user edit one
    into disagreement with the other.
    """
    if get_template(strategy_type) is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"unknown strategy type: {strategy_type}; "
                f"known: {', '.join(strategy_types())}"
            ),
        )
    try:
        spec = parse_strategy_yml(body.yaml)
    except StrategyYamlError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    created_by = body.created_by if body.created_by is not None else DEFAULT_USER_ID
    td_api_ids = await _resolve_td_names(list(spec.td))
    try:
        result = await deploy_strategy(
            broker,
            strategy_id=strategy_type,
            td=td_api_ids,
            md=list(spec.md),
            st_paras=dict(spec.sts),
            created_by=created_by,
            timeout=body.timeout,
            restart=spec.restart,
        )
    except DomainRpcError as exc:
        code = 404 if exc.code in {"unknown_strategy", "not_found"} else 502
        if exc.code == "timeout":
            code = 504
        raise HTTPException(status_code=code, detail=str(exc)) from exc

    session_id = result["session_id"]
    async with session_scope() as db:
        row = await StrategyRepository(db).create(
            type=strategy_type,
            # Stored as submitted, not re-dumped from the spec: the point is
            # to keep the document the operator wrote, which round-tripping
            # through the parser would strip back down to its parsed shape.
            yaml_text=body.yaml,
            config=dict(spec.sts),
            created_by=created_by,
            sts_session=session_id,
        )
        strategy_id = row.id

    await record_audit(
        user_id=created_by,
        operation="sts.deploy",
        result=(
            f"id={strategy_id} session_id={session_id} type={strategy_type} "
            f"td_names={list(spec.td)} "
            f"td={[a['api_id'] for a in result['td']]} md={result['md']}"
        ),
    )
    return DeployResponse(
        id=strategy_id,
        session_id=session_id,
        type=strategy_type,
        config=dict(spec.sts),
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
