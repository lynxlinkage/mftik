"""``td.backfill`` — the work queue, and why it is not keyed by account.

The shape is the argument. Order entry is keyed by ``api_id`` because only the
process holding the lease may place an order; a history read is owned by
nobody, so this subject takes work from anyone and answers for accounts this
process has never traded. What that buys is the case a keyed subject cannot
serve at all: an account nobody is attached to any more, whose record is
exactly the one nothing else will repair.
"""

from __future__ import annotations

import asyncio

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.protocol import (
    TD_BACKFILL,
    Envelope,
    TdBackfill,
    TdBackfillResult,
    Topics,
)
from mftik_td.backfill.executor import BackfillOutcome
from mftik_td.backfill.session import BackfillSession

API_ID = 7


@pytest.fixture
async def broker() -> Broker:
    async with a_broker() as client:
        yield client


class FakeExecutor:
    def __init__(self, *, outcome: BackfillOutcome | None = None) -> None:
        self.outcome = outcome
        self.runs: list[tuple[int, tuple[str, ...], str]] = []
        self.gate: asyncio.Event | None = None

    async def run(self, api_id, *, tickers=(), reason="") -> BackfillOutcome:
        self.runs.append((api_id, tuple(tickers), reason))
        if self.gate is not None:
            await self.gate.wait()
        return self.outcome or BackfillOutcome(
            api_id=api_id, tickers=list(tickers) or ["Binance_Spot_BTCUSDT"]
        )


async def ask(broker: Broker, **over) -> TdBackfillResult:
    payload = {"api_id": API_ID}
    payload.update(over)
    reply = await broker.request(
        Topics.td_backfill(),
        Envelope[TdBackfill].wrap(
            TdBackfill.model_validate(payload), type=TD_BACKFILL, source="test"
        ),
        timeout=2.0,
    )
    return TdBackfillResult.model_validate(reply.payload)


@pytest.fixture
async def serving(broker: Broker):
    executor = FakeExecutor()
    session = BackfillSession(broker, executor)
    await session.start()
    yield session, executor
    await session.stop()


async def test_a_request_is_taken_and_answered(serving, broker) -> None:
    session, executor = serving

    result = await ask(broker, reason="cron")

    assert result.ok
    assert result.api_id == API_ID
    assert executor.runs == [(API_ID, (), "cron")]


async def test_a_request_may_name_the_instruments_to_walk(serving, broker) -> None:
    session, executor = serving

    await ask(broker, tickers=["Binance_Spot_ETHUSDT"], reason="detach")

    assert executor.runs == [(API_ID, ("Binance_Spot_ETHUSDT",), "detach")]


async def test_an_account_this_process_never_traded_is_still_served(
    serving, broker
) -> None:
    """The whole reason the subject is unkeyed.

    Nothing has attached api_id 999 here; a keyed subject would park the
    request until something did, which for a retired account is forever.
    """
    session, executor = serving

    result = await ask(broker, api_id=999)

    assert result.ok
    assert executor.runs == [(999, (), "")]


async def test_a_skipped_run_is_reported_rather_than_left_silent(
    broker
) -> None:
    """A caller waiting on this cannot otherwise tell skipped from lost."""
    executor = FakeExecutor(
        outcome=BackfillOutcome(api_id=API_ID, reason="another run holds it")
    )
    session = BackfillSession(broker, executor)
    await session.start()
    try:
        result = await ask(broker)
    finally:
        await session.stop()

    assert result.ok
    assert result.tickers == []
    assert "another run" in result.reason


async def test_a_failed_run_comes_back_as_not_ok(broker) -> None:
    executor = FakeExecutor(
        outcome=BackfillOutcome(api_id=API_ID, ok=False, reason="venue said no")
    )
    session = BackfillSession(broker, executor)
    await session.start()
    try:
        result = await ask(broker)
    finally:
        await session.stop()

    assert not result.ok
    assert result.reason == "venue said no"


async def test_an_unreadable_request_is_refused_not_dropped(serving, broker) -> None:
    session, executor = serving

    reply = await broker.request(
        Topics.td_backfill(),
        Envelope[dict].wrap({"nonsense": True}, type=TD_BACKFILL, source="test"),
        timeout=2.0,
    )

    assert TdBackfillResult.model_validate(reply.payload).ok is False
    assert executor.runs == []


async def test_a_long_walk_does_not_stall_the_queue_behind_it(broker) -> None:
    """A walk is minutes of venue round trips; the serve loop is one consumer."""
    executor = FakeExecutor()
    executor.gate = asyncio.Event()
    session = BackfillSession(broker, executor)
    await session.start()
    try:
        first = asyncio.create_task(ask(broker, api_id=1))
        for _ in range(50):
            await asyncio.sleep(0.02)
            if executor.runs:
                break
        second = asyncio.create_task(ask(broker, api_id=2))
        for _ in range(50):
            await asyncio.sleep(0.02)
            if len(executor.runs) == 2:
                break

        assert len(executor.runs) == 2, "the second was taken while the first ran"
        executor.gate.set()
        assert (await first).ok
        assert (await second).ok
    finally:
        executor.gate.set()
        await session.stop()


async def test_too_many_runs_at_once_are_refused_not_queued(broker) -> None:
    """Refused, because a request held here is one nothing can see the state of."""
    executor = FakeExecutor()
    executor.gate = asyncio.Event()
    session = BackfillSession(broker, executor, max_in_flight=1)
    await session.start()
    try:
        held = asyncio.create_task(ask(broker, api_id=1))
        for _ in range(50):
            await asyncio.sleep(0.02)
            if executor.runs:
                break

        refused = await ask(broker, api_id=2)
        assert refused.ok
        assert "already in flight" in refused.reason

        executor.gate.set()
        assert (await held).ok
    finally:
        executor.gate.set()
        await session.stop()
