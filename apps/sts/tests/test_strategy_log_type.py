"""``Strategy.log`` stamps the session's type and ignores ``type=`` in extra."""

from __future__ import annotations

from mftik.strategy import Strategy


class RecordingBroker:
    def __init__(self) -> None:
        self.envelopes: list[object] = []

    async def publish_log(self, topic: str, envelope: object, **_kwargs: object) -> int:
        self.envelopes.append(envelope)
        return 1


class TypedSession:
    def __init__(self, broker: RecordingBroker, type: str | None) -> None:
        self.broker = broker
        self.session_id = "s-typed"
        self.cid_slot = 0
        self.type = type


class BareSession:
    """The shape of the fakes under ``apps/sts/tests`` — no ``type``."""

    def __init__(self, broker: RecordingBroker) -> None:
        self.broker = broker
        self.session_id = "s-bare"
        self.cid_slot = 0


class Probe(Strategy):
    name = "probe"


async def test_strategy_log_stamps_session_type() -> None:
    broker = RecordingBroker()
    strategy = Probe()
    strategy.bind(TypedSession(broker, "private::Tiny"))  # type: ignore[arg-type]
    await strategy.log('risk value = {%f}, 0.995')
    assert broker.envelopes[0].payload.type == "private::Tiny"  # type: ignore[attr-defined]


async def test_strategy_log_ignores_type_in_extra() -> None:
    broker = RecordingBroker()
    strategy = Probe()
    strategy.bind(TypedSession(broker, "private::Tiny"))  # type: ignore[arg-type]
    await strategy.log("x", type="CrossArb")
    assert broker.envelopes[0].payload.type == "private::Tiny"  # type: ignore[attr-defined]


async def test_a_session_without_type_still_logs() -> None:
    broker = RecordingBroker()
    strategy = Probe()
    strategy.bind(BareSession(broker))  # type: ignore[arg-type]
    await strategy.log("x")
    assert broker.envelopes[0].payload.type is None  # type: ignore[attr-defined]
