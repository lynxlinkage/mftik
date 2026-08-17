"""A strategy that rejects its configuration must not become a lease timeout.

The refusal happens inside ``on_start`` / ``on_ready``, before STS answers the
create — so the deploy knows at once, if it looks. What made this worth fixing
is what happened when it did not: MD waited out its full timeout for a
heartbeat from a session that had already stopped, and the operator was handed
that timeout instead of the sentence the strategy wrote.
"""

from __future__ import annotations

import pytest
from mftik.protocol import (
    MD_SESSION_ATTACH,
    STS_SESSION_CREATE,
    STS_SESSION_STOP,
    StsCreateSessionResult,
    StsCreateSessionResultEnvelope,
)
from mftik_api.broker_rpc import DomainRpcError
from mftik_api.orchestrate import deploy_strategy

REFUSAL = (
    "no bestquote feed for BinanceFuture_Perp_BTCUSDT in md "
    "['aggtrade.BinanceFuture_Perp_BTCUSDT']; macd_dollar prices its IOCs "
    "through the touch and has no book without one"
)


class FakeBroker:
    """Records every subject asked of it, so the test can see what was skipped."""

    def __init__(self, *, status: str, reason: str | None) -> None:
        self.status = status
        self.reason = reason
        self.types: list[str] = []

    async def publish_log(self, topic, envelope, **_kwargs):  # noqa: ANN001
        return 1

    async def publish(self, topic, envelope):  # noqa: ANN001
        return 1

    async def request(self, subject, envelope, *, timeout=None):  # noqa: ANN001
        self.types.append(envelope.type)
        if envelope.type == STS_SESSION_CREATE:
            return StsCreateSessionResultEnvelope.wrap(
                StsCreateSessionResult(
                    session_id=envelope.payload.session_id,
                    strategy="macd_dollar",
                    td=[1],
                    status=self.status,
                    reason=self.reason,
                ),
                type=STS_SESSION_CREATE,
                source="sts",
            )
        raise AssertionError(f"should not have been called: {envelope.type}")


async def test_a_refused_strategy_never_reaches_the_attach() -> None:
    broker = FakeBroker(status="failed", reason=REFUSAL)

    with pytest.raises(DomainRpcError) as caught:
        await deploy_strategy(
            broker,
            strategy_id="MacdDollarBars",
            td=[1],
            md=["aggtrade.BinanceFuture_Perp_BTCUSDT"],
            created_by=1,
        )

    assert caught.value.code == "strategy_refused"
    # The strategy's own words, not a timeout naming a lease.
    assert caught.value.message == REFUSAL
    # Create and nothing else: no MD attach to wait out, and no rollback stop
    # for a session that has already stopped itself.
    assert broker.types == [STS_SESSION_CREATE]
    assert MD_SESSION_ATTACH not in broker.types
    assert STS_SESSION_STOP not in broker.types


async def test_an_early_natural_end_is_reported_the_same_way() -> None:
    """`done` is not a failure, but it is still not something to attach to."""
    broker = FakeBroker(status="done", reason="work_done")

    with pytest.raises(DomainRpcError) as caught:
        await deploy_strategy(
            broker, strategy_id="noop", td=[], md=[], created_by=1
        )

    assert caught.value.code == "strategy_refused"
    assert caught.value.message == "work_done"


async def test_a_terminal_status_with_no_reason_still_says_something() -> None:
    """Nothing should surface as an empty detail on a 400."""
    broker = FakeBroker(status="failed", reason=None)

    with pytest.raises(DomainRpcError) as caught:
        await deploy_strategy(
            broker, strategy_id="noop", td=[], md=[], created_by=1
        )

    assert "failed" in caught.value.message
