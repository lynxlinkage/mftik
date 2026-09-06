"""Stop and fail reach the process holding the session, not whichever answers.

Issue #72's second half. `stop` and `fail` are answered from the receiving
process's own `_sessions`, so on the shared `sts` subject a second STS could
take a stop for a session it does not hold and reply `not_found` — leaving a
row that says live and that nobody can end. `serve` is a competing consumer;
one subject per session makes the holder the only consumer there is.

Better than recording where a session landed and addressing that, because it
cannot go stale: whichever process rebuilds a session starts serving this, and
one that dies stops serving it. Nothing has to be kept in step.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.broker.errors import RequestTimeoutError
from mftik.protocol import (
    STS_SESSION_STOP,
    StsCreateSessionRequest,
    StsSessionControlRequest,
    StsSessionControlRequestEnvelope,
    StsSessionControlResult,
    Topics,
)
from mftik.strategy import Strategy
from mftik_sts.impl import register
from mftik_sts.session import SessionManager


class Quiet(Strategy):
    name = "quiet-control"


@pytest.fixture
async def broker() -> Broker:
    async with a_broker("sts-control") as client:
        yield client


def _manager(broker: Broker, instance: str) -> SessionManager:
    register(Quiet)
    return SessionManager(
        broker,
        instance=instance,
        heartbeat_interval=0.05,
        strategy_factory=lambda name: Quiet(),
    )


async def _create(manager: SessionManager, session_id: str) -> None:
    await manager.create_session(
        StsCreateSessionRequest(
            session_id=session_id,
            created_by=1,
            strategy="quiet-control",
            type="Quiet",
        )
    )


async def _stop(
    broker: Broker, session_id: str, *, timeout: float = 2.0
) -> StsSessionControlResult:
    reply = await broker.request(
        Topics.sts_control(session_id),
        StsSessionControlRequestEnvelope.wrap(
            StsSessionControlRequest(session_id=session_id),
            type=STS_SESSION_STOP,
            source="api",
            session_id=session_id,
        ),
        timeout=timeout,
    )
    return StsSessionControlResult.model_validate(reply.payload)


@pytest.mark.asyncio
async def test_the_holder_answers_its_own_session(broker: Broker) -> None:
    manager = _manager(broker, "sts-1")
    try:
        await _create(manager, "own-1")
        result = await _stop(broker, "own-1")
        assert result.session_id == "own-1"
        assert manager.get("own-1") is None, "and it actually stopped"
    finally:
        await manager.close_all()


@pytest.mark.asyncio
async def test_a_peer_cannot_answer_for_a_session_it_does_not_hold(
    broker: Broker,
) -> None:
    """The bug, in the shape it was reported.

    ``sts-2`` is running and healthy and holds nothing. On the shared subject
    it would have taken this stop and replied ``not_found``; on the session's
    own subject it is not a consumer at all, so the request waits for the
    process that can answer it.
    """
    holder = _manager(broker, "sts-1")
    peer = _manager(broker, "sts-2")
    try:
        await _create(holder, "held-by-1")

        result = await _stop(broker, "held-by-1")

        assert result.session_id == "held-by-1"
        assert holder.get("held-by-1") is None
        assert peer.get("held-by-1") is None, "the peer never had it"
    finally:
        await holder.close_all()
        await peer.close_all()


@pytest.mark.asyncio
async def test_a_session_nobody_holds_is_not_answered_by_anybody(
    broker: Broker,
) -> None:
    """It waits rather than being told ``not_found`` by a bystander.

    Which is why the route checks the row first: the table is what can say
    "already over" without asking a process, and a timeout here would be a
    worse answer than the 404 it used to give.
    """
    peer = _manager(broker, "sts-2")
    try:
        with pytest.raises(RequestTimeoutError):
            await _stop(broker, "nobody-holds-this", timeout=0.2)
    finally:
        await peer.close_all()


@pytest.mark.asyncio
async def test_a_closed_session_stops_being_served(broker: Broker) -> None:
    """The subject exists for as long as the session does, and no longer."""
    manager = _manager(broker, "sts-1")
    try:
        await _create(manager, "gone-1")
        await _stop(broker, "gone-1")

        # One poll for the loop to notice it was asked to retire.
        await asyncio.sleep(0.2)
        with pytest.raises(RequestTimeoutError):
            await _stop(broker, "gone-1", timeout=0.2)
    finally:
        await manager.close_all()


@pytest.mark.asyncio
async def test_shutdown_leaves_no_control_loop_pending(
    broker: Broker,
) -> None:
    """A loop nobody waits for is a task pending at loop close."""
    manager = _manager(broker, "sts-1")
    await _create(manager, "shutdown-1")
    await manager.close_all()

    pending: set[Any] = manager._retiring  # noqa: SLF001
    assert all(t.done() for t in pending), (
        "close_all waits for the loops it asked to retire"
    )
