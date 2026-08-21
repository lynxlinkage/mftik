"""``publish_sts_log`` stamps ``Log.type`` only from the explicit argument."""

from __future__ import annotations

from mftik.protocol.session_log import publish_sts_log


class RecordingBroker:
    def __init__(self) -> None:
        self.envelopes: list[object] = []

    async def publish_log(self, topic: str, envelope: object, **_kwargs: object) -> int:
        self.envelopes.append(envelope)
        return 1


async def test_type_comes_from_the_argument() -> None:
    broker = RecordingBroker()
    await publish_sts_log(
        broker, "s1", "hello", source="sts", type="private::Tiny"
    )
    payload = broker.envelopes[0].payload  # type: ignore[attr-defined]
    assert payload.type == "private::Tiny"
    assert payload.message == "hello"


async def test_omitting_type_leaves_it_null() -> None:
    broker = RecordingBroker()
    await publish_sts_log(broker, "s1", "hello", source="sts")
    assert broker.envelopes[0].payload.type is None  # type: ignore[attr-defined]


async def test_other_extra_survives_beside_type() -> None:
    broker = RecordingBroker()
    await publish_sts_log(
        broker,
        "s1",
        "hello",
        source="sts",
        type="private::Tiny",
        foo="bar",
    )
    payload = broker.envelopes[0].payload  # type: ignore[attr-defined]
    assert payload.type == "private::Tiny"
    assert payload.foo == "bar"
