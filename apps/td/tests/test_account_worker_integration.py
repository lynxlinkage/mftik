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
    TD_OMS_VIEW,
    Envelope,
    OrderAck,
    OrderSubmit,
    TdOmsViewRequest,
    Topics,
)
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
        async with Broker(
            BrokerConfig(nats_url=nats_url, key_prefix=prefix)
        ) as broker:
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
