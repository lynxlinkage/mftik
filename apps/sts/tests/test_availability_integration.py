"""The session ingress's own NATS reconnect (§5.3, §5.6).

One real reconnect, stand-in publishers, no database. The worker runs
in-process. ``force_reconnect`` drives the client path CI's NATS server
already allows; the assertion is the strategy hooks, and the whole test
stays inside the integration cap.
"""

from __future__ import annotations

import asyncio
import threading
import time
import uuid
from decimal import Decimal

import pytest
from broker_harness import unique_key_prefix
from mftik.broker import Broker, BrokerConfig
from mftik.broker.handler import serve
from mftik.exchange.atoms import AtomOptions
from mftik.exchange.models import BookLevel, OrderBook
from mftik.exchange.oms import LedgerView, OmsView
from mftik.exchange.paper.atoms import atoms_for
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import (
    MD_ORDERBOOK,
    TD_LEDGER_VIEW,
    TD_OMS_VIEW,
    Envelope,
    Topics,
    UntypedEnvelope,
)
from mftik.protocol.messages import StsCreateSessionRequest
from mftik.strategy import Ready, Strategy
from mftik_sts.session_worker import process as worker_process
from mftik_sts.session_worker.process import amain

_API_ID = 7
_FEED = "orderbook.Paper_Spot_BTCUSDT"


def _paper_subject() -> str:
    ticker = UniversalTicker.parse("Paper_Spot_BTCUSDT")
    plan = atoms_for("orderbook", ticker, AtomOptions())
    return Topics.atom_subject(plan.atoms[0].atom_id)


class _Watch(Strategy):
    def __init__(self) -> None:
        super().__init__()
        self.ready = threading.Event()
        self._lock = threading.Lock()
        self.updates: list[tuple[str, str, str]] = []
        self.resyncs: list[tuple[int, str]] = []

    async def on_ready(self, ready: Ready) -> None:
        del ready
        self.ready.set()

    async def on_md_update(self, feed: str, state: str, reason: str) -> None:
        with self._lock:
            self.updates.append((feed, state, reason))

    async def on_resync(self, api_id: int, cause: str, view: OmsView) -> None:
        del view
        with self._lock:
            self.resyncs.append((api_id, cause))

    def seen(self) -> tuple[list[tuple[str, str, str]], list[tuple[int, str]]]:
        with self._lock:
            return list(self.updates), list(self.resyncs)


_settled: list[bool] = []


async def _views(message: UntypedEnvelope) -> Envelope[object] | None:
    if message.type == TD_OMS_VIEW:
        payload = message.payload if isinstance(message.payload, dict) else {}
        _settled.append(bool(payload.get("settled")))
        return Envelope[OmsView].wrap(OmsView(), type=TD_OMS_VIEW, source="td")
    if message.type == TD_LEDGER_VIEW:
        return Envelope[LedgerView].wrap(
            LedgerView(), type=TD_LEDGER_VIEW, source="td"
        )
    return None


async def _until(check, *, seconds: float) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if check():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("timed out")


@pytest.mark.integration
async def test_ingress_nats_reconnect_notifies_and_resyncs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _settled.clear()
    prefix = unique_key_prefix("b505")
    monkeypatch.setenv("BROKER_KEY_PREFIX", prefix)
    watch = _Watch()

    def _load(_name: str | None) -> Strategy:
        return watch

    monkeypatch.setattr(worker_process, "load_strategy", _load)
    connected: list[Broker] = []
    original = Broker.connect

    async def _connect(self: Broker) -> None:
        await original(self)
        connected.append(self)

    monkeypatch.setattr(Broker, "connect", _connect)

    session_id = uuid.uuid4().hex[:6]
    request = StsCreateSessionRequest(
        session_id=session_id,
        created_by=1,
        strategy="watch",
        td={"main": {"api_id": _API_ID}},
        md={"paper": [_FEED]},
    )
    stop = asyncio.Event()
    standin = Broker(BrokerConfig.from_env())
    await standin.connect()
    serve_task = asyncio.create_task(
        serve(standin, Topics.td_account(_API_ID), _views, stop=stop)
    )
    book = OrderBook(
        universal_ticker="Paper_Spot_BTCUSDT",
        bids=[BookLevel(price=Decimal("1"), qty=Decimal("1"))],
        asks=[BookLevel(price=Decimal("2"), qty=Decimal("1"))],
    )
    subject = _paper_subject()

    async def _books() -> None:
        seq = 1
        while not stop.is_set():
            envelope = Envelope[OrderBook].wrap(
                book, type=MD_ORDERBOOK, source="md", seq=seq
            )
            seq += 1
            await standin.publish(subject, envelope)
            try:
                await asyncio.wait_for(stop.wait(), timeout=0.05)
            except TimeoutError:
                pass

    books = asyncio.create_task(_books())
    task = asyncio.create_task(amain(request, install_signals=False))
    try:
        await _until(watch.ready.is_set, seconds=4)
        assert not task.done()
        ingress = next(
            broker
            for broker in connected
            if getattr(broker.transport.nc, "_disconnected_cb", None) is not None
        )
        await ingress.transport.nc.force_reconnect()

        def _down() -> bool:
            md, _resyncs = watch.seen()
            return any(
                state == "down" and reason == "ingress_reconnect"
                for _feed, state, reason in md
            )

        def _live_and_resync() -> bool:
            md, resyncs = watch.seen()
            return any(
                state == "live" and reason == "ingress_reconnect"
                for _feed, state, reason in md
            ) and (_API_ID, "reconnect") in resyncs

        await _until(_down, seconds=6)
        await _until(_live_and_resync, seconds=6)
        md, _resyncs = watch.seen()
        down_at = next(
            index
            for index, (_feed, state, reason) in enumerate(md)
            if state == "down" and reason == "ingress_reconnect"
        )
        live_at = next(
            index
            for index, (_feed, state, reason) in enumerate(md)
            if state == "live" and reason == "ingress_reconnect"
        )
        assert down_at < live_at
        assert watch.td.state(_API_ID) is None
        assert False in _settled
        assert _settled[-1] is True
        watch.exit()
        assert await asyncio.wait_for(task, timeout=4) == 0
    finally:
        stop.set()
        if not task.done():
            try:
                watch.exit()
            except RuntimeError:
                pass
            task.cancel()
        books.cancel()
        serve_task.cancel()
        await asyncio.gather(task, books, serve_task, return_exceptions=True)
        await standin.close()
