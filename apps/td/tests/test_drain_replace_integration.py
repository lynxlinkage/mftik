"""Drain-replace with a venue stand-in that outlives the worker (F27).

The resting-order case uses a Binance spot stand-in. The paper engine's
remote client talks to a process outside the worker, but this ticket
says not to use paper for that assertion, and not to change paper. Paper
is only the operator path: CLI, the API route, and one TD process.
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import os
import socket
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any

import mftik_db.session as db_session
import pytest
import uvicorn
from binance_stub import FakeBinanceApi, keypair
from db_harness import an_instance, an_owner
from fastapi import FastAPI
from mftik.broker import Broker, BrokerConfig
from mftik.broker.errors import NoRespondersError, RequestTimeoutError
from mftik.broker.handler import serve
from mftik.cli import config
from mftik.cli.app import main as cli_main
from mftik.cli.config import Profile
from mftik.exchange import PaperExchange
from mftik.exchange.binance.spot import methods as m
from mftik.exchange.models import OrderType, Side
from mftik.procman import CloseMode, Supervisor, WorkerPhase, log_path
from mftik.protocol import (
    STS_ORDER_CANCEL,
    STS_ORDER_SUBMIT,
    SYM_LIST,
    TD_ACCOUNT_TRADING,
    TD_OMS_VIEW,
    Envelope,
    IntentOwner,
    OrderAck,
    OrderCancel,
    OrderSubmit,
    RejectCode,
    SymbolInfo,
    SymListResult,
    TdAccountTrading,
    TdIntentPut,
    TdOmsViewRequest,
    Topics,
)
from mftik_api.routes.td import router as td_router
from mftik_db.models import Base
from mftik_db.models.api import Api, ApiType
from mftik_paper.rpc import dispatch as paper_dispatch
from mftik_td.controller import TdOrchestrator, intent_book
from mftik_td.controller.defaults import (
    ACCOUNT_HB_TIMEOUT_S,
    ACCOUNT_START_TIMEOUT_S,
    ACCOUNT_STOP_GRACE_S,
)
from mftik_td.controller.types import BoundAccount, account_worker_id
from mftik_td.controller.worker import account_worker_spec
from mftik_td.rpc.router import dispatch as td_dispatch
from mftik_td.supervise import (
    account_restart_intensity,
    account_worker_argv,
    run_drain_replace,
    serve_account_drain,
)
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from websockets.asyncio.server import serve as ws_serve

API_KEY = "bn-int-key"
REST_CID = "restbtc1"
TICKER = "Binance_Spot_BTCUSDT"
_SYMBOL = SymbolInfo(
    universal_ticker=TICKER,
    base="BTC",
    quote="USDT",
    exch_ticker="BTCUSDT",
    is_active=True,
)
class _Venue(FakeBinanceApi):
    """Spot order book that stays up when the worker process does not."""

    def __init__(self, public_key: Any) -> None:
        super().__init__(public_key=public_key)
        self._orders: dict[str, dict[str, Any]] = {}
        self._seq = 1000

    async def _answer(self, websocket: Any, msg: dict[str, Any]) -> None:
        method = str(msg.get("method") or "")
        if method in (
            m.ORDER_PLACE,
            m.ORDER_CANCEL,
            m.OPEN_ORDERS_STATUS,
            m.ACCOUNT_STATUS,
        ):
            await self._trade(websocket, msg)
            return
        await super()._answer(websocket, msg)

    async def _trade(self, websocket: Any, msg: dict[str, Any]) -> None:
        method = str(msg.get("method") or "")
        params = msg.get("params") or {}
        req_id = msg.get("id")
        if "timestamp" not in params:
            await self._send(
                websocket,
                {
                    "id": req_id,
                    "status": 400,
                    "error": {"code": -1102, "msg": "timestamp"},
                },
            )
            return
        if method == m.ORDER_PLACE:
            cid = str(params.get("newClientOrderId") or "")
            self._seq += 1
            ack = {
                "symbol": str(params.get("symbol") or "BTCUSDT"),
                "orderId": self._seq,
                "clientOrderId": cid,
                "price": str(params.get("price") or "0"),
                "origQty": str(params.get("quantity") or "0"),
                "executedQty": "0",
                "cummulativeQuoteQty": "0",
                "status": "NEW",
                "timeInForce": str(params.get("timeInForce") or "GTC"),
                "type": str(params.get("type") or "LIMIT"),
                "side": str(params.get("side") or "BUY"),
            }
            self._orders[cid] = ack
            await self._ok(websocket, req_id, ack)
            return
        if method == m.ORDER_CANCEL:
            cid = str(params.get("origClientOrderId") or "")
            current = self._orders.pop(cid, None)
            if current is None:
                await self._send(
                    websocket,
                    {
                        "id": req_id,
                        "status": 400,
                        "error": {"code": -2011, "msg": "Unknown order sent."},
                    },
                )
                return
            canceled = dict(current)
            canceled["status"] = "CANCELED"
            canceled["origClientOrderId"] = cid
            await self._ok(websocket, req_id, canceled)
            return
        if method == m.OPEN_ORDERS_STATUS:
            await self._ok(websocket, req_id, list(self._orders.values()))
            return
        await self._ok(
            websocket,
            req_id,
            {
                "accountType": "SPOT",
                "canTrade": True,
                "balances": [
                    {"asset": "USDT", "free": "1000000", "locked": "0"},
                    {"asset": "BTC", "free": "1", "locked": "0"},
                ],
            },
        )

    async def _ok(self, websocket: Any, req_id: Any, result: Any) -> None:
        await self._send(
            websocket,
            {"id": req_id, "status": 200, "result": result, "rateLimits": []},
        )


def _push_env(updates: dict[str, str | None]) -> dict[str, str | None]:
    previous = {key: os.environ.get(key) for key in updates}
    for key, value in updates.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    return previous


def _pop_env(previous: dict[str, str | None]) -> None:
    for key, value in previous.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


async def _drop_db_engine() -> None:
    engine = db_session._engine
    db_session._engine = None
    db_session._session_factory = None
    if engine is not None:
        await engine.dispose()


def _logs(work: Path, api_id: int) -> str:
    chunks: list[str] = []
    for stream in ("stderr", "stdout"):
        path = log_path(work, account_worker_id(api_id), stream)
        if path.is_file():
            chunks.append(f"--- {stream} ---\n{path.read_text()}")
    return "\n".join(chunks)


async def _http_time() -> tuple[str, asyncio.Server, asyncio.Task[None]]:
    async def _handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            data = b""
            while b"\r\n\r\n" not in data and len(data) < 8192:
                chunk = await reader.read(1024)
                if not chunk:
                    break
                data += chunk
            body = b'{"serverTime":1}'
            head = (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: application/json\r\n"
                f"Content-Length: {len(body)}\r\n"
                "Connection: close\r\n\r\n"
            ).encode()
            writer.write(head + body)
            await writer.drain()
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    server = await asyncio.start_server(_handle, "127.0.0.1", 0)
    task = asyncio.create_task(server.serve_forever())
    sockets = server.sockets or []
    port = sockets[0].getsockname()[1]
    return f"http://127.0.0.1:{port}", server, task


async def _scratch_api(
    path: Path, *, venue: str, secret: str, key: str
) -> tuple[str, str, int]:
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
                venue=venue,
                api_key=key,
                api_secret=secret,
                type=ApiType.ED25519.value,
                instance_id=instance.id,
                cancel_on_disconnect=False,
            )
            session.add(row)
            await session.commit()
            api_id = row.id
    finally:
        await engine.dispose()
    return url, sync, api_id


def _worker_env() -> dict[str, str]:
    env = dict(os.environ)
    env.pop("MFTIK_STATUS_FD", None)
    return env


async def _spawn(
    supervisor: Supervisor,
    *,
    api_id: int,
    venue: str,
    incarnation: int,
) -> None:
    account = BoundAccount(api_id=api_id, venue=venue, instance="td")
    spec = account_worker_spec(
        account,
        incarnation=incarnation,
        argv=account_worker_argv(api_id, incarnation, False),
        code_ref="test",
        start_timeout_s=ACCOUNT_START_TIMEOUT_S,
        hb_timeout_s=ACCOUNT_HB_TIMEOUT_S,
        stop_grace_s=ACCOUNT_STOP_GRACE_S,
        env=_worker_env(),
    )
    await supervisor.spawn(spec)


async def _ready(supervisor: Supervisor, work: Path, api_id: int) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 6.5
    phase = None
    while loop.time() < deadline:
        status = await supervisor.status(account_worker_id(api_id))
        if status is not None:
            phase = status.phase
            if status.ready:
                return
            if status.phase in (
                WorkerPhase.FAILED,
                WorkerPhase.CRASHED,
                WorkerPhase.FATAL,
            ):
                break
        await asyncio.sleep(0.05)
    raise AssertionError(f"phase={phase}\n{_logs(work, api_id)}")


async def _trading(broker: Broker, api_id: int, active: bool) -> None:
    reply = await broker.request(
        Topics.td_account(api_id),
        Envelope[TdAccountTrading].wrap(
            TdAccountTrading(api_id=api_id, active=active),
            type=TD_ACCOUNT_TRADING,
            source="test",
        ),
        timeout=8,
    )
    body = TdAccountTrading.model_validate(reply.payload)
    assert body.active is active, reply.payload


def _order(api_id: int, cid: str, *, ticker: str) -> Envelope[OrderSubmit]:
    return Envelope[OrderSubmit].wrap(
        OrderSubmit(
            session_id="sess",
            api_id=api_id,
            universal_ticker=ticker,
            side=Side.BUY,
            type=OrderType.LIMIT,
            qty=Decimal("0.001"),
            price=Decimal("60000"),
            client_order_id=cid,
        ),
        type=STS_ORDER_SUBMIT,
        source="test",
        session_id="sess",
    )


async def _submit(broker: Broker, api_id: int, cid: str, *, ticker: str) -> str:
    try:
        reply = await broker.request(
            Topics.td_order(api_id),
            _order(api_id, cid, ticker=ticker),
            timeout=1.5,
        )
    except NoRespondersError:
        return "no_responders"
    except RequestTimeoutError as exc:
        return f"timeout:{exc}"
    if str(reply.type).endswith(".error"):
        return f"error:{reply.payload}"
    ack = OrderAck.model_validate(reply.payload)
    if ack.accepted:
        return "accepted"
    code = int(ack.error_code)
    if code == int(RejectCode.TD_DRAINING):
        return "td_draining"
    if code == int(RejectCode.TD_VENUE_NOT_CONNECTED):
        return "td_venue_not_connected"
    return f"refused:{code}:{ack.reason}"


async def _cancel(broker: Broker, api_id: int, cid: str) -> None:
    try:
        await broker.request(
            Topics.td_order(api_id),
            Envelope[OrderCancel].wrap(
                OrderCancel(
                    session_id="sess",
                    api_id=api_id,
                    client_order_id=cid,
                ),
                type=STS_ORDER_CANCEL,
                source="test",
                session_id="sess",
            ),
            timeout=1.5,
        )
    except (NoRespondersError, RequestTimeoutError):
        return


async def _view_orders(broker: Broker, api_id: int) -> dict[str, Any]:
    reply = await broker.request(
        Topics.td_account(api_id),
        Envelope[TdOmsViewRequest].wrap(
            TdOmsViewRequest(api_id=api_id),
            type=TD_OMS_VIEW,
            source="test",
        ),
        timeout=3,
    )
    payload = reply.payload or {}
    orders = payload.get("orders", {})
    assert isinstance(orders, dict), payload
    return orders


async def _serve_sym(broker: Broker, stop: asyncio.Event) -> None:
    async for req in broker.serve(Topics.SYM, stop=stop):
        payload = req.envelope.payload or {}
        ticker = payload.get("universal_ticker")
        offset = int(payload.get("offset") or 0)
        if ticker:
            rows = [_SYMBOL] if ticker == TICKER else []
            total = len(rows)
        elif offset > 0:
            rows = []
            total = 1
        else:
            rows = [_SYMBOL]
            total = 1
        await req.reply(
            Envelope[SymListResult].wrap(
                SymListResult(symbols=rows, total=total),
                type=SYM_LIST,
                source="test",
            )
        )


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.mark.integration
@pytest.mark.real_sleep(
    reason="two real account workers and a Binance stand-in that outlives them"
)
async def test_drain_replace_keeps_a_resting_order_and_refuses_the_rest(
    tmp_path: Path,
) -> None:
    private, pem = keypair()
    url, sync, api_id = await _scratch_api(
        tmp_path / "td.db",
        venue="Binance",
        secret=pem,
        key=API_KEY,
    )
    venue = _Venue(public_key=private.public_key())
    ws = await ws_serve(venue.handler, "127.0.0.1", 0)
    sockets = ws.sockets or []
    ws_port = sockets[0].getsockname()[1]
    keepalive, http_server, http_task = await _http_time()
    prefix = f"td-int-{uuid.uuid4().hex[:10]}"
    nats_url = os.getenv("NATS_URL", "nats://localhost:4222")
    work = tmp_path / "work"
    supervisor = Supervisor(work, plane="td", instance="td", budget=None)
    stop = asyncio.Event()
    sym_task: asyncio.Task[None] | None = None
    chaos: asyncio.Task[None] | None = None
    book = intent_book()
    book.clear()
    outcomes: dict[str, str] = {}
    previous = _push_env(
        {
            "DATABASE_URL": url,
            "DATABASE_URL_SYNC": sync,
            "NATS_URL": nats_url,
            "BROKER_KEY_PREFIX": prefix,
            "BROKER_REQUEST_TIMEOUT": "2",
            "MFTIK_TD_VENUE_WS_URL": f"ws://127.0.0.1:{ws_port}",
            "MFTIK_TD_KEEPALIVE_URL": keepalive,
            "MFTIK_STATUS_FD": None,
        }
    )
    await _drop_db_engine()
    try:
        async with Broker(
            BrokerConfig(nats_url=nats_url, key_prefix=prefix)
        ) as broker:
            sym_task = asyncio.create_task(
                _serve_sym(broker, stop), name="sym"
            )
            await asyncio.sleep(0.05)
            await supervisor.start()
            await _spawn(
                supervisor, api_id=api_id, venue="Binance", incarnation=1
            )
            await _ready(supervisor, work, api_id)
            await _trading(broker, api_id, True)
            first = await _submit(broker, api_id, REST_CID, ticker=TICKER)
            assert first == "accepted", _logs(work, api_id)
            book.put(
                TdIntentPut(
                    session_id="drain-int",
                    owner=IntentOwner(
                        sts_instance="sts", session_id="drain-int"
                    ),
                    api_ids=[api_id],
                )
            )
            chaos_stop = asyncio.Event()

            async def _chaos() -> None:
                n = 0
                accepted: list[str] = []
                while not chaos_stop.is_set():
                    n += 1
                    cid = f"c{n:04d}"
                    outcomes[cid] = await _submit(
                        broker, api_id, cid, ticker=TICKER
                    )
                    if outcomes[cid] == "accepted":
                        accepted.append(cid)
                        if len(accepted) % 2 == 0:
                            await _cancel(broker, api_id, accepted[-1])

            chaos = asyncio.create_task(_chaos(), name="chaos")
            await asyncio.sleep(0.05)
            orch = TdOrchestrator(
                supervisor,
                intensity=account_restart_intensity(),
                code_ref="test",
            )
            result = await run_drain_replace(
                supervisor,
                orch,
                broker,
                BoundAccount(api_id=api_id, venue="Binance", instance="td"),
                cancel_on_disconnect={api_id: False},
            )
            chaos_stop.set()
            await chaos
            chaos = None
            assert result.ok is True, f"{result}\n{_logs(work, api_id)}"
            assert result.incarnation == 2, result
            status = await supervisor.status(account_worker_id(api_id))
            assert status is not None and status.spec.incarnation == 2
            assert status.ready is True
            orders = await _view_orders(broker, api_id)
            assert REST_CID in orders, _logs(work, api_id)
            allowed = {
                "accepted",
                "td_draining",
                "td_venue_not_connected",
                "no_responders",
            }
            bad = {cid: kind for cid, kind in outcomes.items() if kind not in allowed}
            assert not bad, f"{bad}\n{_logs(work, api_id)}"
            assert outcomes, "the client submitted nothing during the replace"
    finally:
        if chaos is not None:
            chaos.cancel()
            await asyncio.gather(chaos, return_exceptions=True)
        stop.set()
        http_server.close()
        await http_server.wait_closed()
        http_task.cancel()
        if sym_task is not None:
            sym_task.cancel()
            await asyncio.gather(sym_task, return_exceptions=True)
        ws.close()
        await ws.wait_closed()
        try:
            await supervisor.close(CloseMode.STOP)
        except Exception:
            pass
        book.clear()
        _pop_env(previous)
        await _drop_db_engine()


@pytest.mark.integration
@pytest.mark.real_sleep(
    reason="CLI, API and a paper account worker, two real process starts"
)
async def test_cli_drain_replaces_the_paper_account(tmp_path: Path) -> None:
    url, sync, api_id = await _scratch_api(
        tmp_path / "td.db",
        venue="Paper",
        secret="paper-secret",
        key="paper-key",
    )
    exchange = PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")},
        tick_interval=60,
    )
    exchange.register_api(
        "paper-key",
        "paper-secret",
        balances={"USDT": Decimal("1000000")},
    )
    await exchange.start()
    prefix = f"td-int-{uuid.uuid4().hex[:10]}"
    nats_url = os.getenv("NATS_URL", "nats://localhost:4222")
    work = tmp_path / "work"
    supervisor = Supervisor(work, plane="td", instance="td", budget=None)
    stop = asyncio.Event()
    paper_task: asyncio.Task[None] | None = None
    control: asyncio.Task[None] | None = None
    server: uvicorn.Server | None = None
    server_task: asyncio.Task[None] | None = None
    book = intent_book()
    book.clear()
    config_path = tmp_path / "cli.toml"
    previous = _push_env(
        {
            "DATABASE_URL": url,
            "DATABASE_URL_SYNC": sync,
            "NATS_URL": nats_url,
            "BROKER_KEY_PREFIX": prefix,
            "BROKER_REQUEST_TIMEOUT": "2",
            "MFTIK_STATUS_FD": None,
            "MFTIK_AUTH_ENABLED": "0",
            "MFTIK_CONFIG": str(config_path),
            "MFTIK_TD_VENUE_WS_URL": None,
            "MFTIK_TD_KEEPALIVE_URL": None,
        }
    )
    await _drop_db_engine()
    try:
        async with Broker(
            BrokerConfig(nats_url=nats_url, key_prefix=prefix)
        ) as broker:

            async def _paper() -> None:
                async for req in broker.serve(Topics.PAPER, stop=stop):
                    await paper_dispatch(req, exchange=exchange)

            paper_task = asyncio.create_task(_paper(), name="paper")
            await asyncio.sleep(0.05)
            await supervisor.start()
            await _spawn(supervisor, api_id=api_id, venue="Paper", incarnation=1)
            await _ready(supervisor, work, api_id)
            orch = TdOrchestrator(
                supervisor,
                intensity=account_restart_intensity(),
                code_ref="test",
            )

            async def _drain(message: Any) -> Any:
                return await serve_account_drain(
                    message,
                    supervisor=supervisor,
                    orchestrator=orch,
                    broker=broker,
                    instance="td",
                )

            control = asyncio.create_task(
                serve(
                    broker,
                    Topics.td("td"),
                    lambda message: td_dispatch(
                        message, broker=broker, instance="td", drain=_drain
                    ),
                    stop=stop,
                ),
                name="td-control",
            )
            await asyncio.sleep(0.05)
            app = FastAPI()
            app.include_router(td_router)
            app.state.broker = broker
            port = _free_port()
            server = uvicorn.Server(
                uvicorn.Config(
                    app,
                    host="127.0.0.1",
                    port=port,
                    log_level="warning",
                    access_log=False,
                )
            )
            server_task = asyncio.create_task(server.serve())
            while not server.started:
                await asyncio.sleep(0.01)
            config.put(
                Profile(name="local", url=f"http://127.0.0.1:{port}", token=None)
            )
            out = io.StringIO()
            err = io.StringIO()

            def _cli() -> int:
                with (
                    contextlib.redirect_stdout(out),
                    contextlib.redirect_stderr(err),
                ):
                    return cli_main(["td", "drain", str(api_id)])

            code = await asyncio.to_thread(_cli)
            printed = out.getvalue()
            assert code == 0, f"{printed}\n{err.getvalue()}\n{_logs(work, api_id)}"
            assert f"api_id={api_id}" in printed
            assert "incarnation=2" in printed
            status = await supervisor.status(account_worker_id(api_id))
            assert status is not None and status.spec.incarnation == 2
            server.should_exit = True
            await server_task
            server = None
            server_task = None
    finally:
        stop.set()
        if server is not None:
            server.should_exit = True
        if server_task is not None:
            await server_task
        for task in (paper_task, control):
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        try:
            await supervisor.close(CloseMode.STOP)
        except Exception:
            pass
        await exchange.stop()
        book.clear()
        _pop_env(previous)
        await _drop_db_engine()
