"""A strategy that rejects its configuration must not become a lease timeout.

The refusal happens inside ``on_start`` / ``on_ready``, before STS answers the
create — so the deploy knows at once, if it looks. What made this worth fixing
is what happened when it did not: MD waited out its full timeout for a
heartbeat from a session that had already stopped, and the operator was handed
that timeout instead of the sentence the strategy wrote.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from mftik.broker.errors import RequestTimeoutError
from mftik.protocol import (
    MD_SESSION_ATTACH,
    STS_ERROR,
    STS_SESSION_CREATE,
    STS_SESSION_FAIL,
    STS_SESSION_FORCE_STOP,
    STS_SESSION_STOP,
    RpcError,
    RpcErrorEnvelope,
    StsCreateSessionResult,
    StsCreateSessionResultEnvelope,
    StsSessionControlResult,
    StsSessionControlResultEnvelope,
    TdAccountRef,
    Topics,
)
from mftik_api import orchestrate
from mftik_api.broker_rpc import DomainRpcError
from mftik_api.orchestrate import deploy_strategy


@pytest.fixture(autouse=True)
def _named_sts_without_a_database(monkeypatch) -> None:
    """These tests are about create/attach outcomes, not STS placement."""

    async def _target(instance, td):  # noqa: ANN001
        return instance or "sts"

    async def _ok(broker, instance):  # noqa: ANN001
        return None

    async def _mint() -> str:
        return "aabb01"

    monkeypatch.setattr(orchestrate, "_sts_target", _target)
    monkeypatch.setattr(orchestrate, "_check_sts_instance", _ok)
    monkeypatch.setattr(orchestrate, "mint_session_id", _mint)


REFUSAL = (
    "no bestquote feed for BinanceUM_Perp_BTCUSDT in md "
    "['aggtrade.BinanceUM_Perp_BTCUSDT']; macd_dollar prices its IOCs "
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
            td={"paper": TdAccountRef(api_id=1)},
            md=["aggtrade.BinanceUM_Perp_BTCUSDT"],
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
    assert STS_SESSION_FAIL not in broker.types


async def test_an_early_natural_end_is_reported_the_same_way() -> None:
    """`done` is not a failure, but it is still not something to attach to."""
    broker = FakeBroker(status="done", reason="work_done")

    with pytest.raises(DomainRpcError) as caught:
        await deploy_strategy(broker, strategy_id="noop", td={}, md=[], created_by=1)

    assert caught.value.code == "strategy_refused"
    assert caught.value.message == "work_done"


async def test_a_terminal_status_with_no_reason_still_says_something() -> None:
    """Nothing should surface as an empty detail on a 400."""
    broker = FakeBroker(status="failed", reason=None)

    with pytest.raises(DomainRpcError) as caught:
        await deploy_strategy(broker, strategy_id="noop", td={}, md=[], created_by=1)

    assert "failed" in caught.value.message


class AttachFailBroker:
    """STS create succeeds; MD attach raises — the rollback path."""

    def __init__(self) -> None:
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
                    strategy="noop",
                    status="live",
                ),
                type=STS_SESSION_CREATE,
                source="sts",
            )
        if envelope.type == MD_SESSION_ATTACH:
            raise DomainRpcError("attach_failed", "md refused the feed")
        if envelope.type == STS_SESSION_FAIL:
            return StsSessionControlResultEnvelope.wrap(
                StsSessionControlResult(
                    session_id=envelope.payload.session_id,
                    status="failed",
                    reason=envelope.payload.reason,
                ),
                type=STS_SESSION_FAIL,
                source="sts",
            )
        raise AssertionError(f"should not have been called: {envelope.type}")


async def test_attach_failure_fails_the_session_not_stops_it() -> None:
    broker = AttachFailBroker()

    with pytest.raises(DomainRpcError) as caught:
        await deploy_strategy(
            broker,
            strategy_id="noop",
            td={},
            md=["orderbook.Paper_Spot_BTCUSDT"],
            created_by=1,
        )

    assert caught.value.code == "attach_failed"
    assert STS_SESSION_FAIL in broker.types
    assert STS_SESSION_STOP not in broker.types


class _LoggingBroker:
    def __init__(self) -> None:
        self.types: list[str] = []
        self.subjects: list[str] = []
        self.payloads: list[object] = []

    async def publish_log(self, topic, envelope, **_kwargs):  # noqa: ANN001
        return 1

    async def publish(self, topic, envelope):  # noqa: ANN001
        return 1


async def test_a_start_deadline_does_not_attach() -> None:
    class Broker(_LoggingBroker):
        async def request(self, subject, envelope, *, timeout=None):  # noqa: ANN001
            self.types.append(envelope.type)
            self.subjects.append(subject)
            return RpcErrorEnvelope.wrap(
                RpcError(code="start_deadline", message="on_start exceeded 8s"),
                type=STS_ERROR,
                source="sts",
                session_id=envelope.session_id,
            )

    broker = Broker()
    with pytest.raises(DomainRpcError) as caught:
        await deploy_strategy(
            broker,
            strategy_id="noop",
            td={},
            md=["orderbook.Paper_Spot_BTCUSDT"],
            created_by=1,
        )

    assert caught.value.code == "start_deadline"
    assert caught.value.message == "on_start exceeded 8s"
    assert broker.types == [STS_SESSION_CREATE]
    assert MD_SESSION_ATTACH not in broker.types


async def test_a_create_timeout_kills_that_sts_and_reports_the_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    row = SimpleNamespace(status="live", reason=None)

    async def load(session_id: str) -> SimpleNamespace:
        return row

    monkeypatch.setattr(orchestrate, "_load_sts_row", load)

    class Broker(_LoggingBroker):
        async def request(self, subject, envelope, *, timeout=None):  # noqa: ANN001
            self.types.append(envelope.type)
            self.subjects.append(subject)
            self.payloads.append(envelope.payload)
            if envelope.type == STS_SESSION_CREATE:
                raise RequestTimeoutError("sts.sts", "req-1", 10.0)
            if envelope.type == STS_SESSION_FORCE_STOP:
                row.status = "failed"
                row.reason = "on_start exceeded 8s"
                return StsSessionControlResultEnvelope.wrap(
                    StsSessionControlResult(
                        session_id=envelope.payload.session_id,
                        status="failed",
                        reason=row.reason,
                    ),
                    type=STS_SESSION_FORCE_STOP,
                    source="sts",
                    session_id=envelope.payload.session_id,
                )
            raise AssertionError(envelope.type)

    broker = Broker()
    with pytest.raises(DomainRpcError) as caught:
        await deploy_strategy(
            broker,
            strategy_id="noop",
            td={},
            md=["orderbook.Paper_Spot_BTCUSDT"],
            created_by=1,
        )

    assert caught.value.code == "start_deadline"
    assert caught.value.message == "on_start exceeded 8s"
    assert MD_SESSION_ATTACH not in broker.types
    assert broker.types == [STS_SESSION_CREATE, STS_SESSION_FORCE_STOP]
    assert broker.subjects[1] == Topics.sts("sts")
    payload = broker.payloads[1]
    assert payload.abort_start is True
    assert payload.only_if_silent is False


async def test_a_create_timeout_with_no_worker_stays_a_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """In-process mode has nothing to kill. The row stays live."""
    row = SimpleNamespace(status="live", reason=None)

    async def load(session_id: str) -> SimpleNamespace:
        return row

    monkeypatch.setattr(orchestrate, "_load_sts_row", load)

    class Broker(_LoggingBroker):
        async def request(self, subject, envelope, *, timeout=None):  # noqa: ANN001
            self.types.append(envelope.type)
            if envelope.type == STS_SESSION_CREATE:
                raise RequestTimeoutError("sts.sts", "req-1", 10.0)
            return RpcErrorEnvelope.wrap(
                RpcError(code="not_found", message="no worker"),
                type=STS_ERROR,
                source="sts",
                session_id=envelope.session_id,
            )

    broker = Broker()
    with pytest.raises(DomainRpcError) as caught:
        await deploy_strategy(broker, strategy_id="noop", td={}, md=[], created_by=1)

    assert caught.value.code == "timeout"
    assert row.status == "live"
    assert STS_SESSION_FORCE_STOP in broker.types
    assert MD_SESSION_ATTACH not in broker.types
