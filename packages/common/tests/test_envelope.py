from mftik.protocol import (
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


def test_seq_is_optional_and_roundtrips() -> None:
    """MD per-atom seq (F25). A frame that omits it still parses, and ``pv`` stays."""
    plain = UntypedEnvelope.wrap(
        {"symbol": "BTCUSDT"},
        type="md.orderbook",
        source="md/conn/Paper/public/0",
    )
    assert plain.seq is None
    assert UntypedEnvelope.from_json(plain.to_json()).seq is None

    omitted = (
        '{"id":"a","type":"md.orderbook","source":"md","pv":2,"ts":1,"payload":{}}'
    )
    assert UntypedEnvelope.from_json(omitted).seq is None

    stamped = UntypedEnvelope.wrap(
        {"symbol": "BTCUSDT"},
        type="md.orderbook",
        source="md/conn/Paper/public/0",
        seq=3,
    )
    assert stamped.seq == 3
    assert stamped.pv == plain.pv
    assert UntypedEnvelope.from_json(stamped.to_json()).seq == 3


def test_envelope_is_frozen() -> None:
    env = HeartbeatEnvelope.wrap(Heartbeat(), type="heartbeat", source="md")
    try:
        env.source = "other"  # type: ignore[misc]
    except Exception:
        return
    raise AssertionError("Envelope should be frozen")


def test_log_session_topic() -> None:
    assert Topics.log_sts("abc") == "log.sts.abc"
    assert Topics.log_td(7) == "log.td.7"
    assert Topics.log_session("abc") == "log.sts.abc"
