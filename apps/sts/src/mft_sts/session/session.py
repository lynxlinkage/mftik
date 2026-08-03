"""STS session — pub/sub lease + TD OMS / recon + MD wiring."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from mft.broker import Broker
from mft.exchange.models import (
    Balance,
    BestQuote,
    Fill,
    Kline,
    Order,
    OrderBook,
    Ticker,
    Trade,
)
from mft.protocol import (
    MD_BEST_QUOTE,
    MD_DETACH,
    MD_KLINE,
    MD_LEASE_ACK,
    MD_ORDERBOOK,
    MD_TICKER,
    MD_TRADE,
    STS_DETACH,
    STS_LEASE_HEARTBEAT,
    TD_BALANCE_UPDATE,
    TD_CANCEL_REJECT,
    TD_FILL,
    TD_LEASE_ACK,
    TD_ORDER_REJECT,
    TD_ORDER_UPDATE,
    TD_RECON_DONE,
    CancelReject,
    Envelope,
    LeaseAck,
    LeaseHeartbeat,
    MdDetach,
    MdLeaseAck,
    OrderReject,
    ReconDone,
    StsDetach,
    Topics,
    UntypedEnvelope,
    publish_sts_log,
)
from mft.symbols import SymbolClient
from pydantic import BaseModel

from mft_sts.strategy import Strategy

logger = logging.getLogger(__name__)

ExitHandler = Callable[[str, str], Awaitable[None]]

#: MD message type → (strategy hook, payload model). One entry per feed topic
#: MD publishes; anything else on ``md.{session_id}`` is logged and dropped.
MD_HANDLERS: dict[str, tuple[str, type[BaseModel]]] = {
    MD_TICKER: ("on_ticker", Ticker),
    MD_ORDERBOOK: ("on_order_book", OrderBook),
    MD_KLINE: ("on_kline", Kline),
    MD_TRADE: ("on_trade", Trade),
    MD_BEST_QUOTE: ("on_best_quote", BestQuote),
}

#: TD global message type → (strategy hook, payload model).
TD_GLOBAL_HANDLERS: dict[str, tuple[str, type[BaseModel]]] = {
    TD_ORDER_UPDATE: ("on_order_update", Order),
    TD_FILL: ("on_fill", Fill),
    TD_ORDER_REJECT: ("on_order_reject", OrderReject),
    TD_CANCEL_REJECT: ("on_cancel_reject", CancelReject),
    TD_BALANCE_UPDATE: ("on_balance_update", Balance),
}


class StsSession:
    """Strategy session with TD/MD pub/sub links and fencing lease heartbeat."""

    def __init__(
        self,
        *,
        session_id: str,
        broker: Broker,
        created_by: int,
        strategy: Strategy,
        td_api_ids: list[int] | None = None,
        md_ids: list[str] | None = None,
        st_paras: dict[str, Any] | None = None,
        heartbeat_interval: float = 1.0,
        cid_slot: int = 0,
        symbols: SymbolClient | None = None,
        on_exit: ExitHandler | None = None,
    ) -> None:
        self.session_id = session_id
        self.broker = broker
        self.created_by = created_by
        self.strategy = strategy
        self.td_api_ids = list(td_api_ids or [])
        self.md_ids = list(md_ids or [])
        self.st_paras = dict(st_paras or {})
        self.heartbeat_interval = heartbeat_interval
        #: 16-bit id packed into every client_order_id this session mints.
        #: Unique among concurrently live sessions (see ``SessionManager``).
        self.cid_slot = cid_slot
        #: Symbol plane reads. Strategies round their own prices and sizes,
        #: so they need tick/step/notional at hand — TD does not check.
        self.symbols = symbols or SymbolClient(broker)
        self._on_exit = on_exit

        # Must follow cid_slot: bind() builds the client_order_id factory.
        strategy.bind(self)
        self.strategy.paras = type(strategy).on_initialized(self.st_paras)

        self._tasks: list[asyncio.Task[Any]] = []
        self._stop = asyncio.Event()
        self._started = False
        self._destroyed = False
        self._exit_requested = False
        self._token = 0
        self._ack_tokens: dict[int, int] = {}
        self._md_ack_token: int | None = None
        self._md_lease_logged = False
        self._recon_sent: set[int] = set()

    @property
    def destroyed(self) -> bool:
        return self._destroyed

    @property
    def strategy_name(self) -> str:
        return self.strategy.name

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._stop.clear()

        self._tasks = [
            asyncio.create_task(
                self._lease_heartbeat_loop(),
                name=f"sts-{self.session_id}-lease",
            )
        ]
        if self.md_ids:
            self._tasks.append(
                asyncio.create_task(
                    self._pump_md_session(),
                    name=f"sts-{self.session_id}-md",
                )
            )
        for api_id in self.td_api_ids:
            self._tasks.append(
                asyncio.create_task(
                    self._pump_td_global(api_id),
                    name=f"sts-{self.session_id}-g-{api_id}",
                )
            )
            self._tasks.append(
                asyncio.create_task(
                    self._pump_td_session(api_id),
                    name=f"sts-{self.session_id}-s-{api_id}",
                )
            )

        await self.strategy.on_start()
        await self.strategy.on_ready()
        await publish_sts_log(
            self.broker,
            self.session_id,
            (
                f"session started strategy={self.strategy_name} "
                f"td={self.td_api_ids} md={self.md_ids}"
            ),
            source="sts",
        )
        logger.info(
            "STS session started id=%s strategy=%s td=%s md=%s",
            self.session_id,
            self.strategy_name,
            self.td_api_ids,
            self.md_ids,
        )

    async def pause(self) -> None:
        if self._destroyed or not self._started:
            return
        if self.strategy.paused:
            return
        await self.strategy.on_pause()

    async def resume(self) -> None:
        if self._destroyed or not self._started:
            return
        if not self.strategy.paused:
            return
        await self.strategy.on_resume()

    def request_exit(self, reason: str = "strategy_exit") -> None:
        """Ask the session manager to end this session (natural strategy exit).

        Scheduled on the event loop so it is safe to call from a timer callback.
        """
        if self._destroyed or self._exit_requested:
            return
        self._exit_requested = True
        logger.info(
            "STS session exit requested id=%s reason=%s",
            self.session_id,
            reason,
        )
        if self._on_exit is not None:
            asyncio.create_task(
                self._on_exit(self.session_id, reason),
                name=f"sts-{self.session_id}-exit",
            )
        else:
            asyncio.create_task(
                self.stop(), name=f"sts-{self.session_id}-exit-stop"
            )

    async def stop(self) -> None:
        if self._destroyed:
            return
        self._destroyed = True
        self._exit_requested = True
        # Tell TD/MD to drop attaches before heartbeats stop.
        await self._publish_detaches()
        self._stop.set()
        try:
            await self.strategy.on_stop()
        except Exception:
            logger.exception(
                "strategy on_stop failed session=%s", self.session_id
            )
        self.strategy.timer.close()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        self._started = False
        try:
            await publish_sts_log(
                self.broker,
                self.session_id,
                "session stopped",
                source="sts",
            )
        except Exception:
            pass
        logger.info("STS session stopped id=%s", self.session_id)

    async def _publish_detaches(self) -> None:
        td_topic = Topics.sts_td_session(self.session_id)
        for api_id in self.td_api_ids:
            try:
                await self.broker.publish(
                    td_topic,
                    Envelope[StsDetach].wrap(
                        StsDetach(session_id=self.session_id, api_id=api_id),
                        type=STS_DETACH,
                        source="sts",
                        session_id=self.session_id,
                    ),
                )
                await publish_sts_log(
                    self.broker,
                    self.session_id,
                    f"detach requested api_id={api_id}",
                    source="sts",
                )
            except Exception:
                logger.exception(
                    "STS detach publish failed session=%s api_id=%s",
                    self.session_id,
                    api_id,
                )
        if self.md_ids:
            try:
                await self.broker.publish(
                    Topics.sts_md_session(self.session_id),
                    Envelope[MdDetach].wrap(
                        MdDetach(session_id=self.session_id),
                        type=MD_DETACH,
                        source="sts",
                        session_id=self.session_id,
                    ),
                )
                await publish_sts_log(
                    self.broker,
                    self.session_id,
                    "MD detach requested",
                    source="sts",
                )
            except Exception:
                logger.exception(
                    "STS MD detach publish failed session=%s",
                    self.session_id,
                )

    async def _lease_heartbeat_loop(self) -> None:
        """Publish fencing heartbeats on sts.td.* and/or sts.md.*."""
        while not self._stop.is_set():
            self._token += 1
            hb = LeaseHeartbeat(session_id=self.session_id, token=self._token)
            env = Envelope[LeaseHeartbeat].wrap(
                hb,
                type=STS_LEASE_HEARTBEAT,
                source="sts",
                session_id=self.session_id,
            )
            try:
                if self.td_api_ids:
                    await self.broker.publish(
                        Topics.sts_td_session(self.session_id), env
                    )
                if self.md_ids:
                    await self.broker.publish(
                        Topics.sts_md_session(self.session_id), env
                    )
            except Exception:
                logger.exception(
                    "STS lease heartbeat failed session=%s", self.session_id
                )
                return
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self.heartbeat_interval
                )
            except TimeoutError:
                continue

    async def _pump_md_session(self) -> None:
        topic = Topics.md_session(self.session_id)
        try:
            async for env in self.broker.subscribe(topic, stop=self._stop):
                if env.type == MD_LEASE_ACK:
                    await self._on_md_lease_ack(env)
                    continue
                if env.type in MD_HANDLERS:
                    await self._on_market_data(env)
                    continue
                self._on_message("md", env)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "STS md session pump failed session=%s", self.session_id
            )

    async def _on_md_lease_ack(self, env: UntypedEnvelope) -> None:
        try:
            ack = MdLeaseAck.model_validate(env.payload)
            self._md_ack_token = ack.token
        except Exception:
            return
        if self._md_lease_logged:
            return
        self._md_lease_logged = True
        await publish_sts_log(
            self.broker,
            self.session_id,
            "MD lease established",
            source="sts",
        )

    async def _on_market_data(self, env: UntypedEnvelope) -> None:
        name, model = MD_HANDLERS[env.type]
        try:
            payload = model.model_validate(env.payload)
        except Exception:
            logger.exception(
                "invalid md payload session=%s type=%s",
                self.session_id,
                env.type,
            )
            return
        handler = getattr(self.strategy, name)
        try:
            await handler(payload)
        except Exception:
            logger.exception(
                "strategy %s failed session=%s type=%s",
                name,
                self.session_id,
                env.type,
            )

    async def _pump_td_session(self, api_id: int) -> None:
        topic = Topics.td_session(api_id, self.session_id)
        try:
            async for env in self.broker.subscribe(topic, stop=self._stop):
                if env.type == TD_LEASE_ACK:
                    await self._on_lease_ack(api_id, env)
                    continue
                if env.type == TD_RECON_DONE:
                    await self._on_recon_done(env)
                    continue
                self._on_message(f"td-{api_id}", env)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "STS td session pump failed session=%s api_id=%s",
                self.session_id,
                api_id,
            )

    async def _pump_td_global(self, api_id: int) -> None:
        topic = Topics.td_global(api_id)
        try:
            async for env in self.broker.subscribe(topic, stop=self._stop):
                await self._on_td_global(api_id, env)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "STS td global pump failed session=%s api_id=%s",
                self.session_id,
                api_id,
            )

    async def _on_td_global(self, api_id: int, env: UntypedEnvelope) -> None:
        entry = TD_GLOBAL_HANDLERS.get(env.type)
        if entry is None:
            self._on_message(f"global-{api_id}", env)
            return
        name, model = entry
        try:
            payload = model.model_validate(env.payload)
        except Exception:
            logger.exception(
                "invalid td global payload session=%s api_id=%s type=%s",
                self.session_id,
                api_id,
                env.type,
            )
            return
        handler = getattr(self.strategy, name)
        try:
            await handler(api_id, payload)
        except Exception:
            logger.exception(
                "strategy %s failed session=%s api_id=%s type=%s",
                name,
                self.session_id,
                api_id,
                env.type,
            )

    async def _on_lease_ack(self, api_id: int, env: UntypedEnvelope) -> None:
        try:
            ack = LeaseAck.model_validate(env.payload)
            self._ack_tokens[api_id] = ack.token
        except Exception:
            return
        # First ACK means TD session is established → Strategy sends Recon.
        if api_id in self._recon_sent:
            return
        self._recon_sent.add(api_id)
        try:
            await self.strategy.send_recon(api_id)
            await publish_sts_log(
                self.broker,
                self.session_id,
                f"TD lease established — sent recon api_id={api_id}",
                source="sts",
            )
            logger.info(
                "STS sent recon session=%s api_id=%s",
                self.session_id,
                api_id,
            )
        except Exception:
            logger.exception(
                "STS send_recon failed session=%s api_id=%s",
                self.session_id,
                api_id,
            )

    async def _on_recon_done(self, env: UntypedEnvelope) -> None:
        try:
            msg = ReconDone.model_validate(env.payload)
        except Exception:
            return
        await publish_sts_log(
            self.broker,
            self.session_id,
            f"recon done api_id={msg.api_id}",
            source="sts",
        )
        try:
            await self.strategy.on_recon_done(msg)
        except Exception:
            logger.exception(
                "strategy on_recon_done failed session=%s api_id=%s",
                self.session_id,
                msg.api_id,
            )

    def _on_message(self, peer: str, env: UntypedEnvelope) -> None:
        logger.debug(
            "STS session=%s from=%s type=%s",
            self.session_id,
            peer,
            env.type,
        )
