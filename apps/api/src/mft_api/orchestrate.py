"""Deploy orchestrator — API sequences STS then TD (MD later)."""

from __future__ import annotations

import logging
from typing import Any
from uuid import uuid4

from mft.broker import Broker
from mft.protocol import (
    STS_SESSION_CREATE,
    STS_SESSION_STOP,
    TD_SESSION_ATTACH,
    StsCreateSessionRequest,
    StsCreateSessionRequestEnvelope,
    StsCreateSessionResult,
    StsSessionControlRequest,
    StsSessionControlRequestEnvelope,
    StsSessionControlResult,
    TdAttachRequest,
    TdAttachRequestEnvelope,
    TdAttachResult,
    Topics,
    publish_sts_log,
)

from mft_api.broker_rpc import DomainRpcError, request_domain

logger = logging.getLogger(__name__)


async def deploy_strategy(
    broker: Broker,
    *,
    strategy_id: str,
    td: list[int],
    md: list[str] | None = None,
    st_paras: dict[str, Any] | None = None,
    created_by: int,
    timeout: float = 30.0,
) -> dict[str, Any]:
    """Mint session_id, create STS, attach each TD api_id. Fail-closed on error."""
    session_id = uuid4().hex
    md = list(md or [])
    st_paras = dict(st_paras or {})
    attached: list[dict[str, Any]] = []

    await publish_sts_log(
        broker,
        session_id,
        f"deploy start strategy={strategy_id} td={td} md={md}",
        source="api",
    )

    try:
        sts = await request_domain(
            broker,
            Topics.STS,
            StsCreateSessionRequestEnvelope.wrap(
                StsCreateSessionRequest(
                    session_id=session_id,
                    created_by=created_by,
                    strategy=strategy_id,
                    td=list(td),
                    md=md,
                    st_paras=st_paras,
                ),
                type=STS_SESSION_CREATE,
                source="api",
                session_id=session_id,
            ),
            result_type=StsCreateSessionResult,
            timeout=10.0,
        )
        await publish_sts_log(
            broker,
            session_id,
            f"STS created strategy={sts.strategy}",
            source="api",
        )
    except DomainRpcError as exc:
        await publish_sts_log(
            broker,
            session_id,
            f"STS create failed: {exc.message}",
            source="api",
            level="error",
        )
        raise

    try:
        for api_id in td:
            await publish_sts_log(
                broker,
                session_id,
                f"TD attach starting api_id={api_id}",
                source="api",
            )
            result = await request_domain(
                broker,
                Topics.TD,
                TdAttachRequestEnvelope.wrap(
                    TdAttachRequest(
                        api_id=api_id,
                        session_id=session_id,
                        created_by=created_by,
                        timeout=timeout,
                    ),
                    type=TD_SESSION_ATTACH,
                    source="api",
                    session_id=session_id,
                ),
                result_type=TdAttachResult,
                timeout=timeout + 5.0,
            )
            attached.append(
                {
                    "api_id": result.api_id,
                    "refcount": result.refcount,
                }
            )
            await publish_sts_log(
                broker,
                session_id,
                f"TD attached api_id={result.api_id} refcount={result.refcount}",
                source="api",
            )
    except Exception as exc:
        logger.exception(
            "TD attach failed — rolling back STS session=%s", session_id
        )
        await publish_sts_log(
            broker,
            session_id,
            f"TD attach failed — rolling back STS: {exc}",
            source="api",
            level="error",
        )
        try:
            await request_domain(
                broker,
                Topics.STS,
                StsSessionControlRequestEnvelope.wrap(
                    StsSessionControlRequest(session_id=session_id),
                    type=STS_SESSION_STOP,
                    source="api",
                    session_id=session_id,
                ),
                result_type=StsSessionControlResult,
                timeout=10.0,
            )
            await publish_sts_log(
                broker,
                session_id,
                "STS stopped after rollback",
                source="api",
                level="warning",
            )
        except Exception:
            logger.exception("rollback STS stop failed session=%s", session_id)
        raise

    await publish_sts_log(
        broker,
        session_id,
        f"deploy complete strategy={sts.strategy} td={attached}",
        source="api",
    )
    return {
        "session_id": session_id,
        "strategy": sts.strategy,
        "td": attached,
        "md": md,
        "status": "live",
    }
