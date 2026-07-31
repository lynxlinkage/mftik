from mft.protocol import (
    Envelope,
    Heartbeat,
    HeartbeatEnvelope,
    Log,
    LogEnvelope,
    Topics,
    UntypedEnvelope,
)


def test_typed_heartbeat_roundtrip() -> None:
    original = HeartbeatEnvelope.wrap(
        Heartbeat(status="ok"),
        type="heartbeat",
        source="md",
    )
    restored = HeartbeatEnvelope.from_json(original.to_json())

    assert restored.type == "heartbeat"
    assert restored.source == "md"
    assert isinstance(restored.payload, Heartbeat)
    assert restored.payload.status == "ok"
    assert restored.id == original.id


def test_envelope_generic_specialization() -> None:
    env = Envelope[Heartbeat](
        type="heartbeat",
        source="td",
        payload=Heartbeat(),
    )
    assert env.payload.status == "ok"


def test_log_envelope_extra_fields() -> None:
    env = LogEnvelope.wrap(
        Log(level="info", message="hello", symbol="BTCUSDT"),
        type="log",
        source="strategy.noop",
        session_id="abc",
    )
    restored = LogEnvelope.from_json(env.to_json())
    assert restored.payload.message == "hello"
    assert restored.payload.level == "info"
    assert restored.session_id == "abc"
    assert restored.payload.model_extra == {"symbol": "BTCUSDT"}


def test_untyped_envelope_accepts_dict_payload() -> None:
    raw = UntypedEnvelope(
        type="ticker",
        source="md",
        payload={"symbol": "BTCUSDT", "last": 1.0},
    )
    restored = UntypedEnvelope.from_json(raw.to_json())
    assert restored.payload["symbol"] == "BTCUSDT"
    assert restored.payload["last"] == 1.0


def test_envelope_is_frozen() -> None:
    env = HeartbeatEnvelope.wrap(Heartbeat(), type="heartbeat", source="md")
    try:
        env.source = "other"  # type: ignore[misc]
    except Exception:
        return
    raise AssertionError("Envelope should be frozen")


def test_log_session_topic() -> None:
    assert Topics.log_session("abc") == "log.session.abc"
