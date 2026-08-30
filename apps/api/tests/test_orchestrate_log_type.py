"""Deploy lines carry ``strategy_type``; ``session_id`` stays a hex uuid."""

from __future__ import annotations

from mftik.protocol import (
    STS_SESSION_CREATE,
    TD_SESSION_ATTACH,
    StsCreateSessionResult,
    StsCreateSessionResultEnvelope,
    TdAccountRef,
    TdAttachResult,
    TdAttachResultEnvelope,
)
from mftik_api.orchestrate import deploy_strategy


class RecordingBroker:
    def __init__(self) -> None:
        self.logs: list[object] = []

    async def publish_log(self, topic: str, envelope: object, **_kwargs: object) -> int:
        self.logs.append(envelope)
        return 1

    async def publish(self, topic: str, envelope: object) -> int:
        return 1

    async def request(self, subject, envelope, *, timeout=None):  # noqa: ANN001
        if envelope.type == STS_SESSION_CREATE:
            return StsCreateSessionResultEnvelope.wrap(
                StsCreateSessionResult(
                    session_id=envelope.payload.session_id,
                    strategy="tiny",
                    status="live",
                ),
                type=STS_SESSION_CREATE,
                source="sts",
            )
        raise AssertionError(f"unexpected rpc: {envelope.type}")


async def test_deploy_lines_carry_strategy_type() -> None:
    broker = RecordingBroker()
    result = await deploy_strategy(
        broker,
        strategy_id="tiny",
        td={},
        md=[],
        created_by=1,
        strategy_type="private::Tiny",
    )
    assert len(result["session_id"]) == 32
    assert result["session_id"].isalnum()
    payloads = [e.payload for e in broker.logs]  # type: ignore[attr-defined]
    assert payloads
    assert all(p.type == "private::Tiny" for p in payloads)
    assert any(p.message.startswith("deploy start") for p in payloads)
    assert any(p.message.startswith("STS created") for p in payloads)
    assert any(p.message.startswith("deploy complete") for p in payloads)


async def test_deploy_without_strategy_type_leaves_type_null() -> None:
    broker = RecordingBroker()
    await deploy_strategy(
        broker, strategy_id="tiny", td={}, md=[], created_by=1
    )
    payloads = [e.payload for e in broker.logs]  # type: ignore[attr-defined]
    assert payloads
    assert all(p.type is None for p in payloads)


class CaptureCreateBroker:
    def __init__(self) -> None:
        self.create: object | None = None

    async def publish_log(self, topic: str, envelope: object, **_kwargs: object) -> int:
        return 1

    async def publish(self, topic: str, envelope: object) -> int:
        return 1

    async def request(self, subject, envelope, *, timeout=None):  # noqa: ANN001
        if envelope.type == STS_SESSION_CREATE:
            self.create = envelope.payload
            return StsCreateSessionResultEnvelope.wrap(
                StsCreateSessionResult(
                    session_id=envelope.payload.session_id,
                    strategy="tiny",
                    status="live",
                ),
                type=STS_SESSION_CREATE,
                source="sts",
            )
        if envelope.type == TD_SESSION_ATTACH:
            return TdAttachResultEnvelope.wrap(
                TdAttachResult(
                    session_id=envelope.payload.session_id,
                    api_id=envelope.payload.api_id,
                    refcount=1,
                ),
                type=TD_SESSION_ATTACH,
                source="td",
            )
        raise AssertionError(f"unexpected rpc: {envelope.type}")


async def test_deploy_create_payload_keeps_account_names() -> None:
    broker = CaptureCreateBroker()
    td = {
        "paper trader": TdAccountRef(api_id=3),
        "binance quoter": TdAccountRef(api_id=7),
    }
    await deploy_strategy(
        broker, strategy_id="tiny", td=td, md=[], created_by=1
    )
    assert broker.create is not None
    assert list(broker.create.td) == ["paper trader", "binance quoter"]  # type: ignore[attr-defined]
    assert broker.create.td["paper trader"].api_id == 3  # type: ignore[attr-defined]
    assert broker.create.td["binance quoter"].api_id == 7  # type: ignore[attr-defined]
