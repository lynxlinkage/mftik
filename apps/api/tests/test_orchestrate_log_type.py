"""Deploy lines carry ``strategy_type``; ``session_id`` stays a hex uuid."""

from __future__ import annotations

from mftik.protocol import (
    STS_SESSION_CREATE,
    StsCreateSessionResult,
    StsCreateSessionResultEnvelope,
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
                    td=[],
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
        td=[],
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
        broker, strategy_id="tiny", td=[], md=[], created_by=1
    )
    payloads = [e.payload for e in broker.logs]  # type: ignore[attr-defined]
    assert payloads
    assert all(p.type is None for p in payloads)
