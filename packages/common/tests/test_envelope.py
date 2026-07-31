from mft.protocol import MessageEnvelope, Topics


def test_envelope_roundtrip() -> None:
    original = MessageEnvelope(
        type="heartbeat",
        source="test",
        payload={"status": "ok"},
    )
    restored = MessageEnvelope.from_json(original.to_json())
    assert restored.type == "heartbeat"
    assert restored.source == "test"
    assert restored.payload == {"status": "ok"}


def test_log_session_topic() -> None:
    assert Topics.log_session("abc") == "log.session.abc"
