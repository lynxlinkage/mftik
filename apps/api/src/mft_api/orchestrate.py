"""Deploy orchestrator — API sequences STS then MD then TD."""

from __future__ import annotations

import logging
from typing import Any
from uuid import uuid4

from mft.broker import Broker
from mft.protocol import (
    MD_SESSION_ATTACH,
    STS_SESSION_CREATE,
    STS_SESSION_STOP,
    TD_SESSION_ATTACH,
    MdAttachRequest,
    MdAttachRequestEnvelope,
    MdAttachResult,
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
    publish_md_log,
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
    restart: str = "always",
) -> dict[str, Any]:
    """Mint session_id, create STS, attach MD then each TD api_id. Fail-closed."""
    session_id = uuid4().hex
    md = list(md or [])
    st_paras = dict(st_paras or {})
    attached_td: list[dict[str, Any]] = []
    attached_md: dict[str, Any] | None = None

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
                    restart=restart,
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
        if md:
            await publish_sts_log(
                broker,
                session_id,
                f"MD attach starting feeds={md}",
                source="api",
            )
            for venue in _md_venues(md):
                await publish_md_log(
                    broker,
                    venue,
                    f"attach starting sts={session_id} feeds={md}",
                    source="api",
                )
            md_result = await request_domain(
                broker,
                Topics.MD,
                MdAttachRequestEnvelope.wrap(
                    MdAttachRequest(
                        session_id=session_id,
                        created_by=created_by,
                        subscriptions=md,
                        timeout=timeout,
                    ),
                    type=MD_SESSION_ATTACH,
                    source="api",
                    session_id=session_id,
                ),
                result_type=MdAttachResult,
                timeout=timeout + 5.0,
            )
            attached_md = {
                "subscriptions": list(md_result.subscriptions),
                "refcounts": dict(md_result.refcounts),
            }
            await publish_sts_log(
                broker,
                session_id,
                f"MD attached feeds={md_result.subscriptions}",
                source="api",
            )
            for venue in _md_venues(md_result.subscriptions):
                await publish_md_log(
                    broker,
                    venue,
                    (
                        f"attach complete sts={session_id} "
                        f"feeds={md_result.subscriptions}"
                    ),
                    source="api",
                )

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
            attached_td.append(
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
        if isinstance(exc, DomainRpcError):
            logger.error(
                "MD/TD attach failed — rolling back STS session=%s: %s",
                session_id,
                exc,
            )
        else:
            logger.exception(
                "MD/TD attach failed — rolling back STS session=%s", session_id
            )
        await publish_sts_log(
            broker,
            session_id,
            f"attach failed — rolling back STS: {exc}",
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

    md_out = (
        list(attached_md["subscriptions"])
        if attached_md is not None
        else list(md)
    )
    await publish_sts_log(
        broker,
        session_id,
        f"deploy complete strategy={sts.strategy} td={attached_td} md={md_out}",
        source="api",
    )
    return {
        "session_id": session_id,
        "strategy": sts.strategy,
        "td": attached_td,
        "md": md_out,
        "status": "live",
    }


def _md_venues(feeds: list[str]) -> set[str]:
    venues: set[str] = set()
    for feed in feeds:
        try:
            _topic, ticker = Topics.parse_md_feed(feed)
        except ValueError:
            continue
        venues.add(ticker.venue)
    return venues
