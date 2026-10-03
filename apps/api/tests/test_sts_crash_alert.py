"""The controller's crash line is an ordinary error log the alerter matches.

B5-06 publishes it with the same helper the session worker uses. This
file does not change ``apps/api/src``. The pipeline is the one already
shipped: parse the topic, then an error-level matcher.
"""

from __future__ import annotations

from mftik.protocol import UntypedEnvelope
from mftik.protocol.session_log import publish_sts_log
from mftik_api.alert_eval import evaluate
from mftik_api.alert_match import MatcherRec, line_from_envelope
from mftik_api.log_persist import parse_log_topic
from mftik_sts.controller.decisions import CONTROLLER_LOG_SOURCE, crash_log_message
from mftik_sts.controller.types import CrashClass


class _Capture:
    def __init__(self) -> None:
        self.sent: list[tuple[str, object]] = []

    async def publish(self, topic: str, envelope: object) -> None:
        self.sent.append((topic, envelope))


async def test_the_crash_line_matches_an_error_level_matcher() -> None:
    message = crash_log_message(
        crash_class=CrashClass.C,
        reason="crash_class_c",
        incarnation=2,
        unconfirmed={7: ("cid-a", "cid-b"), 8: ("timeout",)},
    )
    bus = _Capture()
    await publish_sts_log(
        bus,
        "abc123",
        message,
        source=CONTROLLER_LOG_SOURCE,
        level="error",
    )
    assert len(bus.sent) == 1
    topic, envelope = bus.sent[0]
    assert parse_log_topic(topic) == ("sts", "abc123")
    assert "class=C" in message
    assert "reason=crash_class_c" in message
    assert "incarnation=2" in message
    assert "api_id=7:cid-a,cid-b" in message
    assert "api_id=8:timeout" in message
    raw = UntypedEnvelope.model_validate(envelope.model_dump())  # type: ignore[attr-defined]
    line = line_from_envelope(topic, raw)
    assert line is not None
    assert line["level"] == "error"
    assert line["source"] == CONTROLLER_LOG_SOURCE
    assert line["message"] == message
    hits = await evaluate(
        line,
        [
            MatcherRec(
                id=1,
                name="errors",
                kind="level",
                spec={"levels": ["error"]},
            )
        ],
    )
    assert len(hits) == 1
