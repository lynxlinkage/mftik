"""``sts/session/<session_id>`` is the id owner GC parses back (§4.3)."""

from __future__ import annotations

from mftik.intent_gc import session_id_from_worker_id
from mftik_sts.controller import session_worker_id


def test_a_session_worker_id_round_trips() -> None:
    assert session_id_from_worker_id(session_worker_id("abc123")) == "abc123"


def test_other_worker_ids_are_not_sessions() -> None:
    assert session_id_from_worker_id("td/account/7") is None
    assert session_id_from_worker_id("md/conn/0") is None
    assert session_id_from_worker_id(session_worker_id("abc123") + "/extra") is None
