"""One real account worker, the paper engine, and ``td.order.{api_id}``."""

from __future__ import annotations

import asyncio
import os
import uuid
from decimal import Decimal
from pathlib import Path

import pytest
from db_harness import an_instance, an_owner
from mftik.broker import Broker, BrokerConfig
from mftik.exchange import PaperExchange
from mftik.exchange.models import OrderType, Side
from mftik.procman import CloseMode, Supervisor, WorkerPhase, log_path
from mftik.protocol import (
    STS_ORDER_SUBMIT,
    TD_ACCOUNT_TRADING,
    TD_OMS_VIEW,
    TD_ORDER_CANCEL_SESSION,
    Envelope,
    OrderAck,
    OrderSubmit,
    RejectCode,
    TdAccountTrading,
    TdCancelSessionRequest,
    TdCancelSessionResult,
    TdOmsViewRequest,
    Topics,
)
from mftik.strategy.client_order_id import format_client_order_id
from mftik_db.models import Base
from mftik_db.models.api import Api
from mftik_paper.rpc import dispatch
from mftik_td.controller.defaults import (
    ACCOUNT_HB_TIMEOUT_S,
    ACCOUNT_START_TIMEOUT_S,
    ACCOUNT_STOP_GRACE_S,
)
from mftik_td.controller.types import BoundAccount, account_worker_id
from mftik_td.controller.worker import account_worker_spec
from mftik_td.supervise import account_worker_argv
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

API_KEY = "paper-key"
API_SECRET = "paper-secret"


async def _serve_paper(
    broker: Broker, exchange: PaperExchange, stop: asyncio.Event
) -> None:
    async for req in broker.serve(Topics.PAPER, stop=stop):
        await dispatch(req, exchange=exchange)


async def _scratch_api(path: Path) -> tuple[str, str, int]:
    url = f"sqlite+aiosqlite:///{path}"
    sync = f"sqlite:///{path}"
    engine = create_async_engine(url)
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with maker() as session:
            await an_owner(session)
            instance = await an_instance(session, name="td")
            row = Api(
                owner_id=1,
                venue="Paper",
                api_key=API_KEY,
                api_secret=API_SECRET,
                instance_id=instance.id,
                cancel_on_disconnect=False,
            )
            session.add(row)
            await session.commit()
            api_id = row.id
    finally:
        await engine.dispose()
    return url, sync, api_id


def _logs(work: Path, api_id: int) -> str:
    chunks: list[str] = []
    for stream in ("stderr", "stdout"):
        path = log_path(work, account_worker_id(api_id), stream)
        if path.is_file():
            chunks.append(f"--- {stream} ---\n{path.read_text()}")
    return "\n".join(chunks)


@pytest.mark.integration
@pytest.mark.real_sleep(
    reason="the account worker is a real process and the paper engine runs"
)
async def test_a_paper_worker_accepts_an_order_on_its_subject(tmp_path: Path) -> None:
    url, sync, api_id = await _scratch_api(tmp_path / "td.db")
    exchange = PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")},
        tick_interval=60,
    )
    exchange.register_api(
        API_KEY,
        API_SECRET,
        balances={"USDT": Decimal("1000000")},
    )
    await exchange.start()
    stop = asyncio.Event()
    work = tmp_path / "work"
    supervisor = Supervisor(work, plane="td", instance="td", budget=None)
    paper_task: asyncio.Task[None] | None = None
    # The worker's Broker() reads BROKER_KEY_PREFIX. The paper RPC and
    # the order request have to be on that same root.
    prefix = f"td-int-{uuid.uuid4().hex[:10]}"
    nats_url = os.getenv("NATS_URL", "nats://localhost:4222")
    try:
        async with Broker(BrokerConfig(nats_url=nats_url, key_prefix=prefix)) as broker:
            paper_task = asyncio.create_task(
                _serve_paper(broker, exchange, stop), name="paper-rpc"
            )
            await asyncio.sleep(0.05)
            await supervisor.start()
            env = dict(os.environ)
            env.pop("MFTIK_STATUS_FD", None)
            env["DATABASE_URL"] = url
            env["DATABASE_URL_SYNC"] = sync
            env["NATS_URL"] = nats_url
            env["BROKER_KEY_PREFIX"] = prefix
            # No symbol plane in this test. A miss must not hold the order
            # for the broker's default request timeout.
            env["BROKER_REQUEST_TIMEOUT"] = "0.5"
            spec = account_worker_spec(
                BoundAccount(api_id=api_id, venue="Paper", instance="td"),
                incarnation=1,
                argv=account_worker_argv(api_id, 1, False),
                code_ref="test",
                start_timeout_s=ACCOUNT_START_TIMEOUT_S,
                hb_timeout_s=ACCOUNT_HB_TIMEOUT_S,
                stop_grace_s=ACCOUNT_STOP_GRACE_S,
                env=env,
            )
            await supervisor.spawn(spec)
            loop = asyncio.get_running_loop()
            deadline = loop.time() + 6.5
            ready = False
            phase = None
            while loop.time() < deadline:
                status = await supervisor.status(account_worker_id(api_id))
                if status is not None:
                    phase = status.phase
                    if status.ready:
                        ready = True
                        break
                    if status.phase in (
                        WorkerPhase.FAILED,
                        WorkerPhase.CRASHED,
                        WorkerPhase.FATAL,
                    ):
                        break
                await asyncio.sleep(0.05)
            assert ready, f"phase={phase}\n{_logs(work, api_id)}"
            detail = _logs(work, api_id)
            await _trading(broker, api_id, True, detail=detail)
            cid = "cid-wire"
            reply = await broker.request(
                Topics.td_order(api_id),
                Envelope[OrderSubmit].wrap(
                    OrderSubmit(
                        session_id="sess",
                        api_id=api_id,
                        universal_ticker="Paper_Spot_BTCUSDT",
                        side=Side.BUY,
                        type=OrderType.LIMIT,
                        qty=Decimal("0.01"),
                        price=Decimal("1"),
                        client_order_id=cid,
                    ),
                    type=STS_ORDER_SUBMIT,
                    source="test",
                    session_id="sess",
                ),
                timeout=3,
            )
            ack = OrderAck.model_validate(reply.payload)
            assert ack.accepted is True, _logs(work, api_id)
            assert ack.client_order_id == cid
            view_reply = await broker.request(
                Topics.td_account(api_id),
                Envelope[TdOmsViewRequest].wrap(
                    TdOmsViewRequest(api_id=api_id),
                    type=TD_OMS_VIEW,
                    source="test",
                ),
                timeout=3,
            )
            assert cid in (view_reply.payload or {}).get("orders", {})
    finally:
        stop.set()
        if paper_task is not None:
            paper_task.cancel()
            await asyncio.gather(paper_task, return_exceptions=True)
        try:
            await supervisor.close(CloseMode.STOP)
        except Exception:
            pass
        await exchange.stop()


async def _trading(
    broker: Broker, api_id: int, active: bool, *, detail: str = ""
) -> None:
    """Push the trading bit and require the worker to observe it."""
    reply = await broker.request(
        Topics.td_account(api_id),
        Envelope[TdAccountTrading].wrap(
            TdAccountTrading(api_id=api_id, active=active),
            type=TD_ACCOUNT_TRADING,
            source="test",
        ),
        timeout=8,
    )
    assert reply.type == TD_ACCOUNT_TRADING, detail or reply
    body = TdAccountTrading.model_validate(reply.payload)
    assert body.api_id == api_id
    assert body.active is active, detail or body


async def _submit(
    broker: Broker, api_id: int, *, session_id: str, client_order_id: str
) -> OrderAck:
    reply = await broker.request(
        Topics.td_order(api_id),
        Envelope[OrderSubmit].wrap(
            OrderSubmit(
                session_id=session_id,
                api_id=api_id,
                universal_ticker="Paper_Spot_BTCUSDT",
                side=Side.BUY,
                type=OrderType.LIMIT,
                qty=Decimal("0.01"),
                price=Decimal("1"),
                client_order_id=client_order_id,
            ),
            type=STS_ORDER_SUBMIT,
            source="test",
            session_id=session_id,
        ),
        timeout=3,
    )
    return OrderAck.model_validate(reply.payload)


@pytest.mark.integration
@pytest.mark.real_sleep(
    reason="the account worker is a real process and the paper engine runs"
)
async def test_cancel_session_on_the_paper_worker_drops_only_that_session(
    tmp_path: Path,
) -> None:
    """One ``td.order.cancel_session`` through the paper engine.

    The worker's client has no per-cid lookup, so the handler reconciles.
    A resting order whose cid names the session is cancelled. The other
    session's resting order stays. The handler returns as soon as that
    is confirmed; the caller's request timeout is not ``WAIT_TIMEOUT_S``.
    """
    url, sync, api_id = await _scratch_api(tmp_path / "td.db")
    exchange = PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")},
        tick_interval=60,
    )
    exchange.register_api(
        API_KEY,
        API_SECRET,
        balances={"USDT": Decimal("1000000")},
    )
    await exchange.start()
    stop = asyncio.Event()
    work = tmp_path / "work"
    supervisor = Supervisor(work, plane="td", instance="td", budget=None)
    paper_task: asyncio.Task[None] | None = None
    prefix = f"td-int-{uuid.uuid4().hex[:10]}"
    nats_url = os.getenv("NATS_URL", "nats://localhost:4222")
    session_a = "abc123"
    session_b = "def456"
    cid_a = format_client_order_id(session_a, 1, 1)
    cid_b = format_client_order_id(session_b, 1, 1)
    try:
        async with Broker(BrokerConfig(nats_url=nats_url, key_prefix=prefix)) as broker:
            paper_task = asyncio.create_task(
                _serve_paper(broker, exchange, stop), name="paper-rpc"
            )
            await asyncio.sleep(0.05)
            await supervisor.start()
            env = dict(os.environ)
            env.pop("MFTIK_STATUS_FD", None)
            env["DATABASE_URL"] = url
            env["DATABASE_URL_SYNC"] = sync
            env["NATS_URL"] = nats_url
            env["BROKER_KEY_PREFIX"] = prefix
            env["BROKER_REQUEST_TIMEOUT"] = "0.5"
            spec = account_worker_spec(
                BoundAccount(api_id=api_id, venue="Paper", instance="td"),
                incarnation=1,
                argv=account_worker_argv(api_id, 1, False),
                code_ref="test",
                start_timeout_s=ACCOUNT_START_TIMEOUT_S,
                hb_timeout_s=ACCOUNT_HB_TIMEOUT_S,
                stop_grace_s=ACCOUNT_STOP_GRACE_S,
                env=env,
            )
            await supervisor.spawn(spec)
            loop = asyncio.get_running_loop()
            deadline = loop.time() + 6.5
            ready = False
            phase = None
            while loop.time() < deadline:
                status = await supervisor.status(account_worker_id(api_id))
                if status is not None:
                    phase = status.phase
                    if status.ready:
                        ready = True
                        break
                    if status.phase in (
                        WorkerPhase.FAILED,
                        WorkerPhase.CRASHED,
                        WorkerPhase.FATAL,
                    ):
                        break
                await asyncio.sleep(0.05)
            assert ready, f"phase={phase}\n{_logs(work, api_id)}"
            await _trading(broker, api_id, True, detail=_logs(work, api_id))
            ack_a = await _submit(
                broker, api_id, session_id=session_a, client_order_id=cid_a
            )
            ack_b = await _submit(
                broker, api_id, session_id=session_b, client_order_id=cid_b
            )
            assert ack_a.accepted is True, _logs(work, api_id)
            assert ack_b.accepted is True, _logs(work, api_id)
            reply = await broker.request(
                Topics.td_order(api_id),
                Envelope[TdCancelSessionRequest].wrap(
                    TdCancelSessionRequest(session_id=session_a),
                    type=TD_ORDER_CANCEL_SESSION,
                    source="test",
                    session_id=session_a,
                ),
                timeout=5,
            )
            assert reply.type == TD_ORDER_CANCEL_SESSION, _logs(work, api_id)
            result = TdCancelSessionResult.model_validate(reply.payload)
            assert result.ok is True, _logs(work, api_id)
            assert result.session_id == session_a
            assert result.unconfirmed == []
            view_reply = await broker.request(
                Topics.td_account(api_id),
                Envelope[TdOmsViewRequest].wrap(
                    TdOmsViewRequest(api_id=api_id),
                    type=TD_OMS_VIEW,
                    source="test",
                ),
                timeout=3,
            )
            orders = (view_reply.payload or {}).get("orders", {})
            assert cid_a not in orders, _logs(work, api_id)
            assert cid_b in orders
    finally:
        stop.set()
        if paper_task is not None:
            paper_task.cancel()
            await asyncio.gather(paper_task, return_exceptions=True)
        try:
            await supervisor.close(CloseMode.STOP)
        except Exception:
            pass
        await exchange.stop()


@pytest.mark.integration
@pytest.mark.real_sleep(
    reason="the account worker is a real process and the paper engine runs"
)
async def test_paper_trading_bit_toggles_and_the_worker_stays_ready(
    tmp_path: Path,
) -> None:
    """Off, on, an order, off, refused, on again. The process stays ready.

    Paper has no HTTP pool, so the resident layer staying up is this
    same incarnation still ready across both switches. A live testnet
    run per venue is operator work, not this test.
    """
    url, sync, api_id = await _scratch_api(tmp_path / "td.db")
    exchange = PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")},
        tick_interval=60,
    )
    exchange.register_api(
        API_KEY,
        API_SECRET,
        balances={"USDT": Decimal("1000000")},
    )
    await exchange.start()
    stop = asyncio.Event()
    work = tmp_path / "work"
    supervisor = Supervisor(work, plane="td", instance="td", budget=None)
    paper_task: asyncio.Task[None] | None = None
    prefix = f"td-int-{uuid.uuid4().hex[:10]}"
    nats_url = os.getenv("NATS_URL", "nats://localhost:4222")
    try:
        async with Broker(BrokerConfig(nats_url=nats_url, key_prefix=prefix)) as broker:
            paper_task = asyncio.create_task(
                _serve_paper(broker, exchange, stop), name="paper-rpc"
            )
            await asyncio.sleep(0.05)
            await supervisor.start()
            env = dict(os.environ)
            env.pop("MFTIK_STATUS_FD", None)
            env["DATABASE_URL"] = url
            env["DATABASE_URL_SYNC"] = sync
            env["NATS_URL"] = nats_url
            env["BROKER_KEY_PREFIX"] = prefix
            env["BROKER_REQUEST_TIMEOUT"] = "0.5"
            spec = account_worker_spec(
                BoundAccount(api_id=api_id, venue="Paper", instance="td"),
                incarnation=1,
                argv=account_worker_argv(api_id, 1, False),
                code_ref="test",
                start_timeout_s=ACCOUNT_START_TIMEOUT_S,
                hb_timeout_s=ACCOUNT_HB_TIMEOUT_S,
                stop_grace_s=ACCOUNT_STOP_GRACE_S,
                env=env,
            )
            await supervisor.spawn(spec)
            loop = asyncio.get_running_loop()
            deadline = loop.time() + 6.5
            ready = False
            phase = None
            while loop.time() < deadline:
                status = await supervisor.status(account_worker_id(api_id))
                if status is not None:
                    phase = status.phase
                    if status.ready:
                        ready = True
                        break
                    if status.phase in (
                        WorkerPhase.FAILED,
                        WorkerPhase.CRASHED,
                        WorkerPhase.FATAL,
                    ):
                        break
                await asyncio.sleep(0.05)
            detail = _logs(work, api_id)
            assert ready, f"phase={phase}\n{detail}"
            held = await supervisor.status(account_worker_id(api_id))
            assert held is not None
            pid = held.pid

            refused = await _submit(
                broker, api_id, session_id="sess", client_order_id="cid-off"
            )
            assert refused.accepted is False, detail
            assert refused.error_code == RejectCode.TD_VENUE_NOT_CONNECTED

            await _trading(broker, api_id, True, detail=detail)
            taken = await _submit(
                broker, api_id, session_id="sess", client_order_id="cid-on"
            )
            assert taken.accepted is True, _logs(work, api_id)

            await _trading(broker, api_id, False, detail=_logs(work, api_id))
            again = await _submit(
                broker, api_id, session_id="sess", client_order_id="cid-off-2"
            )
            assert again.accepted is False, _logs(work, api_id)
            assert again.error_code == RejectCode.TD_VENUE_NOT_CONNECTED

            still = await supervisor.status(account_worker_id(api_id))
            assert still is not None
            assert still.ready is True
            assert still.pid == pid
            assert still.phase is WorkerPhase.RUNNING

            await _trading(broker, api_id, True, detail=_logs(work, api_id))
            taken_again = await _submit(
                broker, api_id, session_id="sess", client_order_id="cid-on-2"
            )
            assert taken_again.accepted is True, _logs(work, api_id)
    finally:
        stop.set()
        if paper_task is not None:
            paper_task.cancel()
            await asyncio.gather(paper_task, return_exceptions=True)
        try:
            await supervisor.close(CloseMode.STOP)
        except Exception:
            pass
        await exchange.stop()
