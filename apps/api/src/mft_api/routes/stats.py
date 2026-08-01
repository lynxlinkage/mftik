"""Home dashboard statistics."""

from __future__ import annotations

import logging

from fastapi import APIRouter
from mft.protocol import (
    STS_HEALTH,
    TD_HEALTH,
    HealthCheck,
    HealthCheckEnvelope,
    HealthStatus,
    Topics,
)
from mft_db.models.session import SessionDomain, SessionStatus
from mft_db.repositories import (
    MdSessionRepository,
    StsSessionRepository,
    TdSessionRepository,
)
from mft_db.session import session_scope

from mft_api.broker_rpc import DomainRpcError, request_domain
from mft_api.deps import BrokerDep
from mft_api.schemas import DomainStats, StatsResponse

logger = logging.getLogger(__name__)
router = APIRouter(tags=["stats"])

_HEALTH_PROBES = {
    SessionDomain.STS.value: (Topics.STS, STS_HEALTH),
    SessionDomain.TD.value: (Topics.TD, TD_HEALTH),
}


@router.get("/stats", response_model=StatsResponse)
async def get_stats(broker: BrokerDep) -> StatsResponse:
    async with session_scope() as db:
        sts = StsSessionRepository(db)
        td = TdSessionRepository(db)
        md = MdSessionRepository(db)
        live = {
            SessionDomain.STS.value: await sts.count(
                status=SessionStatus.LIVE.value
            ),
            SessionDomain.TD.value: await td.count(status=SessionStatus.LIVE.value),
            SessionDomain.MD.value: await md.count(status=SessionStatus.LIVE.value),
        }
        done = {
            SessionDomain.STS.value: await sts.count(
                status=SessionStatus.DONE.value
            ),
            SessionDomain.TD.value: await td.count(status=SessionStatus.DONE.value),
            SessionDomain.MD.value: await md.count(status=SessionStatus.DONE.value),
        }

    domains: list[DomainStats] = []
    for domain in (
        SessionDomain.STS.value,
        SessionDomain.TD.value,
        SessionDomain.MD.value,
    ):
        healthy: bool | None = None
        probe = _HEALTH_PROBES.get(domain)
        if probe is not None:
            subject, type_name = probe
            try:
                status = await request_domain(
                    broker,
                    subject,
                    HealthCheckEnvelope.wrap(
                        HealthCheck(), type=type_name, source="api"
                    ),
                    result_type=HealthStatus,
                    timeout=1.5,
                )
                healthy = status.status == "ok"
            except DomainRpcError:
                healthy = False
            except Exception:
                logger.exception("health probe failed domain=%s", domain)
                healthy = False

        domains.append(
            DomainStats(
                domain=domain,
                live=live.get(domain, 0),
                done=done.get(domain, 0),
                healthy=healthy,
            )
        )

    return StatsResponse(domains=domains)
