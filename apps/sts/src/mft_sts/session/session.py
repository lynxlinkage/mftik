"""STS session — pub/sub lease + TD OMS / recon wiring."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from mft.broker import Broker
from mft.exchange.oms import OmsView
from mft.protocol import (
    STS_DETACH,
    STS_LEASE_HEARTBEAT,
    TD_LEASE_ACK,
    TD_OMS_VIEW,
    TD_RECON_DONE,
    Envelope,
    LeaseAck,
    LeaseHeartbeat,
    ReconDone,
    StsDetach,
    Topics,
    UntypedEnvelope,
    publish_sts_log,
)

from mft_sts.strategy import Strategy

logger = logging.getLogger(__name__)


class StsSession:
    """Strategy session with TD pub/sub links and fencing lease heartbeat."""

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
    ) -> None:
        self.session_id = session_id
        self.broker = broker
        self.created_by = created_by
        self.strategy = strategy
        self.td_api_ids = list(td_api_ids or [])
        self.md_ids = list(md_ids or [])
        self.st_paras = dict(st_paras or {})
        self.heartbeat_interval = heartbeat_interval

        strategy.bind(self)
        self.strategy.paras = strategy.validate_paras(self.st_paras)

        self._tasks: list[asyncio.Task[Any]] = []
        self._stop = asyncio.Event()
        self._started = False
        self._destroyed = False
        self._token = 0
        self._ack_tokens: dict[int, int] = {}
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
        for api_id in self.td_api_ids:
            self._tasks.append(
                asyncio.create_task(
                    self._pump_topic(
                        Topics.td_global(api_id), f"global-{api_id}"
                    ),
                    name=f"sts-{self.session_id}-g-{api_id}",
                )
            )
            self._tasks.append(
                asyncio.create_task(
                    self._pump_td_session(api_id),
                    name=f"sts-{self.session_id}-s-{api_id}",
                )
            )
            self._tasks.append(
                asyncio.create_task(
                    self._pump_oms(api_id),
                    name=f"sts-{self.session_id}-oms-{api_id}",
                )
            )

        await self.strategy.on_start()
        await self.strategy.on_ready()
        await publish_sts_log(
            self.broker,
            self.session_id,
            f"session started strategy={self.strategy_name} td={self.td_api_ids}",
            source="sts",
        )
        logger.info(
            "STS session started id=%s strategy=%s td=%s",
            self.session_id,
            self.strategy_name,
            self.td_api_ids,
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

    async def stop(self) -> None:
        if self._destroyed:
            return
        self._destroyed = True
        # Tell TD to drop attaches before heartbeats stop (refcount N→N-1).
        await self._publish_detaches()
        self._stop.set()
        try:
            await self.strategy.on_stop()
        except Exception:
            logger.exception(
                "strategy on_stop failed session=%s", self.session_id
            )
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
        topic = Topics.sts_session(self.session_id)
        for api_id in self.td_api_ids:
            try:
                await self.broker.publish(
                    topic,
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

    async def _lease_heartbeat_loop(self) -> None:
        topic = Topics.sts_session(self.session_id)
        while not self._stop.is_set():
            self._token += 1
            try:
                await self.broker.publish(
                    topic,
                    Envelope[LeaseHeartbeat].wrap(
                        LeaseHeartbeat(
                            session_id=self.session_id, token=self._token
                        ),
                        type=STS_LEASE_HEARTBEAT,
                        source="sts",
                        session_id=self.session_id,
                    ),
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

    async def _pump_oms(self, api_id: int) -> None:
        topic = Topics.td_oms(api_id)
        try:
            async for env in self.broker.subscribe(topic, stop=self._stop):
                if env.type != TD_OMS_VIEW:
                    continue
                try:
                    view = OmsView.model_validate(env.payload)
                except Exception:
                    logger.warning(
                        "STS bad OMS view session=%s api_id=%s",
                        self.session_id,
                        api_id,
                    )
                    continue
                self.strategy.oms.update(api_id, view)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "STS OMS pump failed session=%s api_id=%s",
                self.session_id,
                api_id,
            )

    async def _pump_topic(self, topic: str, label: str) -> None:
        try:
            async for env in self.broker.subscribe(topic, stop=self._stop):
                self._on_message(label, env)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "STS pump failed session=%s topic=%s", self.session_id, topic
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
