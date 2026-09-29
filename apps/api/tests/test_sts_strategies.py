"""GET /sts/strategies — the list is every STS session, not just successful deploys.

Attach failures persist a session and never record ``type`` / ``yaml_text``.
The list used to be driven by a sidecar table written only after deploy
succeeded, so those rows vanished. Pinning the endpoint is what keeps a
later ``WHERE type IS NOT NULL`` from bringing that back silently.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from db_harness import a_database, an_owner
from fastapi import HTTPException
from mftik.broker.errors import NoRespondersError
from mftik.protocol import (
    ANY_INSTANCE,
    STOP_CONTROL_TIMEOUT_S,
    STOP_FORCE_RPC_TIMEOUT_S,
    STS_REASON_STOP_TIMED_OUT,
    STS_SESSION_FORCE_STOP,
    STS_SESSION_STOP,
    StsSessionControlResult,
    Topics,
)
from mftik_api.broker_rpc import DomainRpcError, request_domain
from mftik_api.routes import sts as sts_routes
from mftik_db.models.session import SessionStatus
from mftik_db.repositories import InstanceRepository, StsSessionRepository


@pytest.fixture
async def db(monkeypatch, database_url):
    async with a_database(database_url) as database:
        async with database.scope() as session:
            await an_owner(session)
        monkeypatch.setattr(sts_routes, "session_scope", database.scope)
        yield database.scope


async def test_an_attach_failure_still_appears_on_the_list(db) -> None:
    async with db() as session:
        repo = StsSessionRepository(session)
        await repo.create_live(
            session_id="s-ok",
            created_by=1,
            type="private::Tiny",
            yaml_text="sts: {}\n",
        )
        await repo.create_live(session_id="s-orphan", created_by=1)
        await repo.mark_failed(
            "s-orphan", "attach failed — rolled back during deploy"
        )

    result = await sts_routes.list_strategies()
    by_id = {row.session_id: row for row in result.strategies}

    assert set(by_id) == {"s-ok", "s-orphan"}
    assert by_id["s-ok"].type == "private::Tiny"
    assert by_id["s-orphan"].type is None
    assert by_id["s-orphan"].status == "failed"
    assert by_id["s-ok"].td_api_ids == []
    assert by_id["s-ok"].md_ids == []
    assert result.has_more is False
    assert all("paused" not in row.model_dump() for row in result.strategies)


async def test_the_list_carries_attaches_from_the_row(db) -> None:
    async with db() as session:
        repo = StsSessionRepository(session)
        await repo.create_live(
            session_id="s-att",
            created_by=1,
            type="NoopStrategy",
            td={"a": {"api_id": 3}, "b": {"api_id": 7}},
            md_ids=["orderbook.Paper_Spot_BTCUSDT"],
        )

    result = await sts_routes.list_strategies()
    row = result.strategies[0]
    assert row.session_id == "s-att"
    assert row.td_api_ids == [3, 7]
    assert row.md_ids == ["orderbook.Paper_Spot_BTCUSDT"]


async def test_one_session_is_the_database_row(db) -> None:
    async with db() as session:
        repo = StsSessionRepository(session)
        await repo.create_live(
            session_id="s-one",
            created_by=1,
            type="NoopStrategy",
            td={"a": {"api_id": 2}},
            md_ids=["ticker.Paper_Spot_ETHUSDT"],
        )

    row = await sts_routes.get_strategy("s-one")
    assert row.session_id == "s-one"
    assert row.type == "NoopStrategy"
    assert row.td_api_ids == [2]
    assert row.md_ids == ["ticker.Paper_Spot_ETHUSDT"]


async def test_a_missing_session_is_a_404(db) -> None:
    with pytest.raises(HTTPException) as caught:
        await sts_routes.get_strategy("nope")
    assert caught.value.status_code == 404
    assert "nope" in str(caught.value.detail)


async def test_attention_is_only_failed_and_interrupted(db) -> None:
    async with db() as session:
        repo = StsSessionRepository(session)
        await repo.create_live(session_id="s-live", created_by=1)
        await repo.create_live(session_id="s-fail", created_by=1)
        await repo.mark_failed("s-fail", "boom")
        await repo.create_live(session_id="s-int", created_by=1)
        await repo.mark_finished(
            "s-int", status=SessionStatus.INTERRUPTED.value, reason="cut"
        )
        await repo.create_live(session_id="s-done", created_by=1)
        await repo.mark_done("s-done")

    result = await sts_routes.list_strategies(status="failed,interrupted")
    assert {row.session_id for row in result.strategies} == {"s-fail", "s-int"}
    assert result.has_more is False


async def test_live_is_the_database_alone(db) -> None:
    async with db() as session:
        repo = StsSessionRepository(session)
        await repo.create_live(session_id="s-live", created_by=1)
        await repo.create_live(session_id="s-done", created_by=1)
        await repo.mark_done("s-done")

    result = await sts_routes.list_strategies(status="live")
    assert [row.session_id for row in result.strategies] == ["s-live"]
    assert result.has_more is False


async def test_the_list_pages_on_offset(db) -> None:
    origin = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    async with db() as session:
        repo = StsSessionRepository(session)
        for offset, session_id in enumerate(("s-old", "s-mid", "s-new")):
            await repo.create_live(session_id=session_id, created_by=1)
            row = await repo.get_by_session_id(session_id)
            assert row is not None
            row.created_at = origin + timedelta(minutes=offset)
            await repo.mark_done(session_id)
            # mark_done does not touch created_at; keep the stamp we just set.
            row.created_at = origin + timedelta(minutes=offset)

    first = await sts_routes.list_strategies(status="done,ack", limit=2)
    assert [row.session_id for row in first.strategies] == ["s-new", "s-mid"]
    assert first.total == 3
    assert first.has_more is True

    second = await sts_routes.list_strategies(
        status="done,ack", offset=2, limit=2
    )
    assert [row.session_id for row in second.strategies] == ["s-old"]
    assert second.total == 3
    assert second.has_more is False


async def test_an_unknown_status_is_a_422(db) -> None:
    with pytest.raises(HTTPException) as caught:
        await sts_routes.list_strategies(status="faild")
    assert caught.value.status_code == 422
    assert "faild" in str(caught.value.detail)


async def test_a_status_of_only_commas_is_a_422(db) -> None:
    with pytest.raises(HTTPException) as caught:
        await sts_routes.list_strategies(status=" , ")
    assert caught.value.status_code == 422


async def test_the_feed_list_survives_the_instance_mapping(db) -> None:
    """What STS actually writes now, not the shape the tests above seed.

    ``md_ids`` holds instance name → feeds since INS-7, and an unpinned deploy
    stores ``{"*": [...]}``. Iterating that dict yields its *keys*, so a mapper
    written for a list renders every session's feeds as the single string
    ``"*"`` — the whole Strategy page, not an edge case. The tests above pass
    because they hand ``create_live`` a raw list, which is no longer what
    anything writes.
    """
    async with db() as session:
        repo = StsSessionRepository(session)
        await repo.create_live(
            session_id="s-unpinned",
            created_by=1,
            type="NoopStrategy",
            md_ids={ANY_INSTANCE: ["orderbook.Paper_Spot_BTCUSDT"]},
        )
        await repo.create_live(
            session_id="s-split",
            created_by=1,
            type="NoopStrategy",
            md_ids={
                "md-jp-1": ["orderbook.Paper_Spot_BTCUSDT"],
                "md-jp-2": ["ticker.Paper_Spot_ETHUSDT"],
            },
        )

    unpinned = await sts_routes.get_strategy("s-unpinned")
    assert unpinned.md_ids == ["orderbook.Paper_Spot_BTCUSDT"]

    split = await sts_routes.get_strategy("s-split")
    assert sorted(split.md_ids) == [
        "orderbook.Paper_Spot_BTCUSDT",
        "ticker.Paper_Spot_ETHUSDT",
    ], "a split session shows its feeds, not the instances holding them"


async def test_stopping_a_finished_session_is_an_immediate_404(db) -> None:
    """The table answers what the table already knows.

    Stop goes to a subject only the process holding the session serves, so a
    request for one that has ended waits in a list nobody reads — the caller
    would get a ten-second timeout where it used to get an instant 404. The
    row is what can say "already over" without asking a process, and it is
    consulted before anything is sent.
    """
    async with db() as session:
        repo = StsSessionRepository(session)
        await repo.create_live(session_id="s-done", created_by=1)
        await repo.mark_done("s-done")

    with pytest.raises(HTTPException) as caught:
        await sts_routes.stop_session("s-done", broker=None)  # type: ignore[arg-type]

    assert caught.value.status_code == 404
    assert "no active sts session" in str(caught.value.detail)


async def test_stopping_a_session_that_never_existed_is_a_404(db) -> None:
    with pytest.raises(HTTPException) as caught:
        await sts_routes.stop_session("never", broker=None)  # type: ignore[arg-type]

    assert caught.value.status_code == 404
    assert "unknown sts session" in str(caught.value.detail)


def _scripted_stop(monkeypatch: pytest.MonkeyPatch, steps: list[Any]) -> list[tuple]:
    """Each step is a result or a ``DomainRpcError`` the next RPC raises."""
    seen: list[tuple] = []

    async def request_domain(
        broker: object,
        subject: str,
        envelope: Any,
        *,
        result_type: type,
        timeout: float = 5.0,
        **_kwargs: object,
    ) -> Any:
        seen.append((subject, envelope.type, timeout))
        step = steps.pop(0)
        if isinstance(step, Exception):
            raise step
        return step

    monkeypatch.setattr(sts_routes, "request_domain", request_domain)

    async def _audit(**_kwargs: object) -> None:
        return None

    monkeypatch.setattr(sts_routes, "record_audit", _audit)
    return seen


async def test_an_unanswered_stop_kills_the_worker(db, monkeypatch) -> None:
    async with db() as session:
        await StsSessionRepository(session).create_live(
            session_id="s-stuck",
            created_by=1,
            type="NoopStrategy",
            instance="sts-a",
        )
    seen = _scripted_stop(
        monkeypatch,
        [
            DomainRpcError("timeout", "timed out"),
            StsSessionControlResult(
                session_id="s-stuck",
                status="failed",
                strategy="NoopStrategy",
                reason=STS_REASON_STOP_TIMED_OUT,
            ),
        ],
    )

    result = await sts_routes.stop_session("s-stuck", broker=None)  # type: ignore[arg-type]

    assert result.status == "failed"
    assert result.reason == STS_REASON_STOP_TIMED_OUT
    assert seen == [
        (Topics.sts_control("s-stuck"), STS_SESSION_STOP, STOP_CONTROL_TIMEOUT_S),
        (Topics.sts("sts-a"), STS_SESSION_FORCE_STOP, STOP_FORCE_RPC_TIMEOUT_S),
    ]


async def test_no_responders_is_a_timeout_stop_can_tell_from_a_stuck_worker() -> None:
    class _Broker:
        async def request(self, *_args: object, **_kwargs: object) -> object:
            raise NoRespondersError("sts.control.s", "req", 15.0)

    with pytest.raises(DomainRpcError) as caught:
        await request_domain(
            _Broker(),  # type: ignore[arg-type]
            "sts.control.s",
            object(),
            result_type=StsSessionControlResult,
        )

    assert caught.value.code == "timeout"
    assert caught.value.no_responders is True


async def test_a_stop_nobody_heard_is_not_a_kill(db, monkeypatch) -> None:
    """No subscriber is not a stuck worker. The stop was never delivered."""
    async with db() as session:
        await StsSessionRepository(session).create_live(
            session_id="s-starting",
            created_by=1,
            instance="sts-a",
        )
    seen = _scripted_stop(
        monkeypatch,
        [DomainRpcError("timeout", "nobody subscribed", no_responders=True)],
    )

    with pytest.raises(HTTPException) as caught:
        await sts_routes.stop_session("s-starting", broker=None)  # type: ignore[arg-type]

    assert caught.value.status_code == 502
    assert "not delivered" in str(caught.value.detail)
    assert "orphan reaper" not in str(caught.value.detail)
    assert seen == [
        (Topics.sts_control("s-starting"), STS_SESSION_STOP, STOP_CONTROL_TIMEOUT_S),
    ]
    async with db() as session:
        row = await StsSessionRepository(session).get_by_session_id("s-starting")
    assert row is not None
    assert row.status == "live"


async def test_a_stop_that_finishes_as_the_wait_expires_is_not_killed(
    db, monkeypatch
) -> None:
    """The control reply lost the race. The row is already terminal."""
    async with db() as session:
        await StsSessionRepository(session).create_live(
            session_id="s-late",
            created_by=1,
            type="NoopStrategy",
            instance="sts-a",
        )

    async def request_domain(
        broker: object,
        subject: str,
        envelope: Any,
        *,
        result_type: type,
        timeout: float = 5.0,
        **_kwargs: object,
    ) -> Any:
        assert envelope.type == STS_SESSION_STOP
        async with db() as session:
            await StsSessionRepository(session).mark_finished(
                "s-late",
                status=SessionStatus.DONE.value,
                reason="operator_stop",
            )
        raise DomainRpcError("timeout", "late")

    monkeypatch.setattr(sts_routes, "request_domain", request_domain)

    async def _audit(**_kwargs: object) -> None:
        return None

    monkeypatch.setattr(sts_routes, "record_audit", _audit)

    result = await sts_routes.stop_session("s-late", broker=None)  # type: ignore[arg-type]

    assert result.status == "done"
    assert result.reason == "operator_stop"


async def test_force_stop_not_found_on_a_live_row_is_not_an_unknown_session(
    db, monkeypatch
) -> None:
    async with db() as session:
        await StsSessionRepository(session).create_live(
            session_id="s-inplace",
            created_by=1,
            instance="sts-a",
        )
    _scripted_stop(
        monkeypatch,
        [
            DomainRpcError("timeout", "timed out"),
            DomainRpcError("not_found", "no active sts session s-inplace"),
        ],
    )

    with pytest.raises(HTTPException) as caught:
        await sts_routes.stop_session("s-inplace", broker=None)  # type: ignore[arg-type]

    assert caught.value.status_code == 502
    assert "no worker to kill" in str(caught.value.detail)
    assert "unknown sts session" not in str(caught.value.detail)
    assert "orphan reaper" not in str(caught.value.detail)


async def test_an_unpinned_stop_is_sent_to_the_derived_sts(
    db, monkeypatch
) -> None:
    async with db() as session:
        await StsSessionRepository(session).create_live(
            session_id="s-free",
            created_by=1,
            type="NoopStrategy",
            td={"main": {"api_id": 9}},
        )
    asked: list[list[int]] = []

    async def derived(self: object, api_ids: list[int]) -> str:
        asked.append(list(api_ids))
        return "sts-jp"

    monkeypatch.setattr(InstanceRepository, "derived_sts", derived)
    seen = _scripted_stop(
        monkeypatch,
        [
            DomainRpcError("timeout", "timed out"),
            StsSessionControlResult(
                session_id="s-free",
                status="failed",
                strategy="NoopStrategy",
                reason=STS_REASON_STOP_TIMED_OUT,
            ),
        ],
    )

    result = await sts_routes.stop_session("s-free", broker=None)  # type: ignore[arg-type]

    assert result.status == "failed"
    assert asked == [[9]]
    assert seen[1][0] == Topics.sts("sts-jp")


async def test_an_unpinned_stop_with_no_unique_sts_stays_a_502(
    db, monkeypatch
) -> None:
    async with db() as session:
        await StsSessionRepository(session).create_live(
            session_id="s-nowhere",
            created_by=1,
        )

    async def derived(self: object, api_ids: list[int]) -> None:
        return None

    monkeypatch.setattr(InstanceRepository, "derived_sts", derived)
    _scripted_stop(monkeypatch, [DomainRpcError("timeout", "timed out")])

    with pytest.raises(HTTPException) as caught:
        await sts_routes.stop_session("s-nowhere", broker=None)  # type: ignore[arg-type]

    assert caught.value.status_code == 502
    assert "not pinned to one STS" in str(caught.value.detail)
    assert "orphan reaper" not in str(caught.value.detail)
