from __future__ import annotations

from mftik.protocol import (
    TD_ERROR,
    TD_HEALTH,
    HealthCheck,
    HealthCheckEnvelope,
    HealthStatus,
    RpcError,
    UntypedEnvelope,
)
from mftik_td.rpc import dispatch


async def test_td_health_reply() -> None:
    """Direct handler call: B4-05 (#205)."""
    message = UntypedEnvelope.model_validate_json(
        HealthCheckEnvelope.wrap(
            HealthCheck(),
            type=TD_HEALTH,
            source="api",
        ).to_json()
    )

    reply = await dispatch(message)

    assert reply is not None
    assert reply.type == TD_HEALTH
    assert reply.source == "td"
    status = HealthStatus.model_validate(reply.payload)
    assert status.status == "ok"
    assert status.service == "td"


async def test_td_unknown_type_error() -> None:
    """Direct handler call: B4-05 (#205)."""
    message = UntypedEnvelope.model_validate_json(
        HealthCheckEnvelope.wrap(
            HealthCheck(note="nope"),
            type="td.not_a_method",
            source="api",
        ).to_json()
    )

    reply = await dispatch(message)

    assert reply is not None
    assert reply.type == TD_ERROR
    err = RpcError.model_validate(reply.payload)
    assert err.code == "unknown_type"
    assert "td.not_a_method" in err.message
