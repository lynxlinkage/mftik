"""STS HTTP facade — deploy + list/control."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from mft.protocol import (
    STS_SESSION_LIST,
    STS_SESSION_PAUSE,
    STS_SESSION_RESUME,
    STS_SESSION_STOP,
    ListSessionsRequest,
    ListSessionsRequestEnvelope,
    ListSessionsResult,
    StsSessionControlRequest,
    StsSessionControlRequestEnvelope,
    StsSessionControlResult,
    Topics,
)

from mft_api.audit_util import record_audit
from mft_api.broker_rpc import DomainRpcError, request_domain
from mft_api.deps import DEFAULT_USER_ID, BrokerDep
from mft_api.orchestrate import deploy_strategy
from mft_api.schemas import (
    DeployBody,
    DeployResponse,
    SessionListResponse,
    SessionOut,
    StrategiesResponse,
    StsControlResponse,
    TdAttachOut,
)

router = APIRouter(prefix="/sts", tags=["sts"])

_KNOWN_STRATEGIES = ["noop"]


@router.get("/strategies", response_model=StrategiesResponse)
async def list_strategies() -> StrategiesResponse:
    return StrategiesResponse(strategies=list(_KNOWN_STRATEGIES))


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


@router.post("/{strategy_id}", response_model=DeployResponse)
async def deploy(
    strategy_id: str, body: DeployBody, broker: BrokerDep
) -> DeployResponse:
    """Deploy a strategy; API orchestrates STS create + MD/TD attach."""
    if strategy_id not in _KNOWN_STRATEGIES:
        raise HTTPException(status_code=404, detail=f"unknown strategy: {strategy_id}")

    created_by = body.created_by if body.created_by is not None else DEFAULT_USER_ID
    try:
        result = await deploy_strategy(
            broker,
            strategy_id=strategy_id,
            td=list(body.td),
            md=list(body.md),
            st_paras=dict(body.st_paras),
            created_by=created_by,
            timeout=body.timeout,
        )
    except DomainRpcError as exc:
        code = 404 if exc.code in {"unknown_strategy", "not_found"} else 502
        if exc.code == "timeout":
            code = 504
        raise HTTPException(status_code=code, detail=exc.message) from exc

    await record_audit(
        user_id=created_by,
        operation="sts.deploy",
        result=(
            f"session_id={result['session_id']} strategy={result['strategy']} "
            f"td={[a['api_id'] for a in result['td']]} md={result['md']}"
        ),
    )
    return DeployResponse(
        session_id=result["session_id"],
        strategy=result["strategy"],
        td=[TdAttachOut(**a) for a in result["td"]],
        md=result["md"],
        status=result["status"],
    )


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
