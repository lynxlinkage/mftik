"""Session teardown order — the strategy cleans up while it still can."""

from __future__ import annotations

import asyncio

import fakeredis.aioredis
import pytest
from mftik.broker import Broker, BrokerConfig
from mftik.protocol import (
    TD_SESSION_DETACH,
    TdDetachRequest,
    TdDetachResult,
    TdDetachResultEnvelope,
    Topics,
)
from mftik_sts.session import session as session_mod
from mftik_sts.session.session import StsSession
from mftik_sts.strategy import Strategy

SESSION_ID = "sts-stop-order"
API_ID = 7


@pytest.fixture
async def broker() -> Broker:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test-stop"),
        redis_client=redis,
    )
    await client.connect()
    yield client
    await client.close()
    await redis.aclose()


class DetachWatcher:
    """Stands in for TD, recording when the detach lands.

    Serves the process-level RPC subject rather than the session stream: a
    detach is a request now, and one that nobody answers is retried and then
    reported, which is the point of having moved it.
    """

    def __init__(self, broker: Broker, log: list[str]) -> None:
        self.broker = broker
        self.log = log
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None

    async def listen(self) -> None:
        async def _serve() -> None:
            async for req in self.broker.serve(Topics.TD, stop=self._stop):
                if req.envelope.type != TD_SESSION_DETACH:
                    continue
                payload = TdDetachRequest.model_validate(req.envelope.payload)
                self.log.append("detach")
                await req.reply(
                    TdDetachResultEnvelope.wrap(
                        TdDetachResult(
                            session_id=payload.session_id,
                            api_id=payload.api_id,
                            refcount=0,
                        ),
                        type=TD_SESSION_DETACH,
                        source="td",
                        session_id=payload.session_id,
                    )
                )

        self._task = asyncio.create_task(_serve())
        await asyncio.sleep(0.05)

    async def close(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)


@pytest.mark.asyncio
async def test_on_stop_runs_before_the_detach(broker: Broker) -> None:
    """A strategy cancelling a resting order must still be attached to TD.

    TD refuses an order request from a session it no longer holds a link for
    (``TD_SESSION_NOT_ATTACHED``), so detaching first leaves whatever the
    strategy meant to cancel resting at the venue with nothing managing it.
    """
    order: list[str] = []

    class Cleaner(Strategy):
        name = "cleaner"
        id = 91

        async def on_stop(self) -> None:
            order.append("on_stop")

    watcher = DetachWatcher(broker, order)
    await watcher.listen()

    sts = StsSession(
        session_id=SESSION_ID,
        broker=broker,
        created_by=1,
        strategy=Cleaner(),
        td_api_ids=[API_ID],
        heartbeat_interval=0.1,
    )
    await sts.start()
    await asyncio.sleep(0.05)
    await sts.stop()
    await asyncio.sleep(0.1)

    assert order == ["on_stop", "detach"], order
    await watcher.close()


@pytest.mark.asyncio
async def test_a_hung_on_stop_does_not_hold_the_attach(
    broker: Broker, monkeypatch
) -> None:
    """Bounded, so a wedged strategy cannot keep a trading attach open.

    The detach goes out without it. Its cancel will be refused once it lands,
    which is the lesser harm — an attach nobody releases outlives the process
    that owned it.
    """
    monkeypatch.setattr(session_mod, "ON_STOP_TIMEOUT_S", 0.2)
    order: list[str] = []
    released = asyncio.Event()

    class Wedged(Strategy):
        name = "wedged"
        id = 90

        async def on_stop(self) -> None:
            order.append("on_stop_entered")
            await released.wait()

    watcher = DetachWatcher(broker, order)
    await watcher.listen()

    sts = StsSession(
        session_id=SESSION_ID,
        broker=broker,
        created_by=1,
        strategy=Wedged(),
        td_api_ids=[API_ID],
        heartbeat_interval=0.1,
    )
    await sts.start()
    await asyncio.sleep(0.05)

    await sts.stop()
    await asyncio.sleep(0.1)

    assert order == ["on_stop_entered", "detach"], order
    released.set()
    await watcher.close()


@pytest.mark.asyncio
async def test_a_hung_on_stop_is_left_running_not_cancelled(
    broker: Broker, monkeypatch
) -> None:
    """Cancelling it would be worse than waiting.

    ``on_stop`` typically parks in an order ack, which is a blocking Redis
    read; cancelling one mid-command hands the connection back with its reply
    unread and breaks whatever borrows it next.
    """
    monkeypatch.setattr(session_mod, "ON_STOP_TIMEOUT_S", 0.2)
    released = asyncio.Event()
    finished = asyncio.Event()

    class Wedged(Strategy):
        name = "wedged"
        id = 89

        async def on_stop(self) -> None:
            await released.wait()
            finished.set()

    # Not asserted on here, but a detach nobody answers is retried before it
    # gives up — and this test has no interest in waiting that out.
    watcher = DetachWatcher(broker, [])
    await watcher.listen()

    sts = StsSession(
        session_id=SESSION_ID,
        broker=broker,
        created_by=1,
        strategy=Wedged(),
        td_api_ids=[API_ID],
        heartbeat_interval=0.1,
    )
    await sts.start()
    await asyncio.sleep(0.05)
    await sts.stop()

    # Overran, and was left alone rather than cancelled.
    assert sts._on_stop_task is not None
    assert not sts._on_stop_task.done()

    released.set()
    await asyncio.wait_for(finished.wait(), timeout=1.0)
    assert not sts._on_stop_task.cancelled()
    await watcher.close()


@pytest.mark.asyncio
async def test_a_raising_on_stop_still_detaches(broker: Broker) -> None:
    order: list[str] = []

    class Broken(Strategy):
        name = "broken"
        id = 88

        async def on_stop(self) -> None:
            order.append("on_stop")
            raise RuntimeError("boom")

    watcher = DetachWatcher(broker, order)
    await watcher.listen()

    sts = StsSession(
        session_id=SESSION_ID,
        broker=broker,
        created_by=1,
        strategy=Broken(),
        td_api_ids=[API_ID],
        heartbeat_interval=0.1,
    )
    await sts.start()
    await asyncio.sleep(0.05)
    await sts.stop()
    await asyncio.sleep(0.1)

    assert order == ["on_stop", "detach"], order
    assert sts.destroyed
    await watcher.close()
