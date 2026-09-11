"""Pattern E — a fenced session link.

STS publishes heartbeats on one topic; MD or TD acks on the other, echoes the
fencing token, and detaches after three missed intervals. Both domains used
to write that loop themselves.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from mftik.protocol import (
    LEASE_HEARTBEAT_INTERVAL_S,
    LEASE_MISS_LIMIT,
    STS_LEASE_HEARTBEAT,
    Envelope,
    LeaseHeartbeat,
    UntypedEnvelope,
)

if TYPE_CHECKING:
    from mftik.broker.client import Broker

logger = logging.getLogger(__name__)

AckFactory = Callable[[LeaseHeartbeat], Envelope[Any]]
HeartbeatHook = Callable[[LeaseHeartbeat], Awaitable[None]]
MessageHook = Callable[[UntypedEnvelope], Awaitable[bool]]
Action = Callable[[], Awaitable[None]]


class LeasedSessionLink:
    """Subscribe ``rx``, ack on ``tx``, expire after missed intervals.

    Counting starts on the first heartbeat. Counting from start would kill
    every deploy: attach waits for that first beat with its own timeout.
    After that, silence of ``interval * miss_limit`` (three beats at 1 Hz)
    is a dead peer. ``grace`` overrides that window for tests.

    Domain traffic that is not a heartbeat — subscribe, recon, an in-band
    detach — goes to ``on_message``. Returning True from that hook ends the
    loop (the peer already tore the attach down). Expiry and an unexpected
    exit run on a sibling task so they cannot cancel this loop from inside
    itself.
    """

    def __init__(
        self,
        broker: Broker,
        *,
        rx: str,
        tx: str,
        stop: asyncio.Event,
        ready: asyncio.Event,
        ack: AckFactory,
        on_expired: Action,
        on_heartbeat: HeartbeatHook | None = None,
        on_message: MessageHook | None = None,
        on_died: Action | None = None,
        resubscribe_delay: float = 0.5,
        watch_interval: float = 0.5,
        interval: float = LEASE_HEARTBEAT_INTERVAL_S,
        miss_limit: int = LEASE_MISS_LIMIT,
        grace: float | None = None,
        name: str = "lease",
    ) -> None:
        if not rx or not tx:
            raise ValueError("rx and tx topics are required")
        if rx == tx:
            raise ValueError("rx and tx must be different topics")
        if interval <= 0:
            raise ValueError("interval must be positive")
        if miss_limit < 1:
            raise ValueError("miss_limit must be at least 1")
        if grace is not None and grace <= 0:
            raise ValueError("grace must be positive")
        self._broker = broker
        self.rx = rx
        self.tx = tx
        self.stop = stop
        self.ready = ready
        self._ack = ack
        self._on_expired = on_expired
        self._on_heartbeat = on_heartbeat
        self._on_message = on_message
        self._on_died = on_died
        self._resubscribe_delay = resubscribe_delay
        self._watch_interval = watch_interval
        self._interval = interval
        self._miss_limit = miss_limit
        self._grace = grace
        self._name = name
        self.last_token = 0

    def _window(self, interval: float) -> float:
        if self._grace is not None:
            return self._grace
        return interval * self._miss_limit

    async def run(self) -> None:
        last_seen: float | None = None
        interval = self._interval

        async def _watch_timeout() -> None:
            nonlocal last_seen
            while not self.stop.is_set():
                await asyncio.sleep(self._watch_interval)
                if last_seen is None:
                    continue
                if (
                    asyncio.get_running_loop().time() - last_seen
                    > self._window(interval)
                ):
                    logger.warning("%s lease expired", self._name)
                    asyncio.create_task(
                        self._on_expired(), name=f"{self._name}-expired"
                    )
                    return

        async def _pump() -> bool:
            nonlocal last_seen, interval
            async for env in self._broker.subscribe(self.rx, stop=self.stop):
                if env.type == STS_LEASE_HEARTBEAT:
                    try:
                        hb = LeaseHeartbeat.model_validate(env.payload)
                    except Exception:
                        continue
                    if hb.interval > 0:
                        interval = hb.interval
                    last_seen = asyncio.get_running_loop().time()
                    self.last_token = hb.token
                    if not self.ready.is_set():
                        self.ready.set()
                    if self._on_heartbeat is not None:
                        try:
                            await self._on_heartbeat(hb)
                        except Exception:
                            logger.exception(
                                "%s heartbeat hook failed", self._name
                            )
                    try:
                        await self._broker.publish(self.tx, self._ack(hb))
                    except Exception:
                        logger.exception("%s lease ack failed", self._name)
                    continue
                if self._on_message is not None:
                    try:
                        if await self._on_message(env):
                            return True
                    except Exception:
                        logger.exception(
                            "%s message hook failed type=%s",
                            self._name,
                            env.type,
                        )
            return False

        watchdog = asyncio.create_task(
            _watch_timeout(), name=f"{self._name}-wd"
        )
        try:
            while not self.stop.is_set():
                try:
                    if await _pump():
                        return
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "%s lease subscription failed — resubscribing",
                        self._name,
                    )
                try:
                    await asyncio.wait_for(
                        self.stop.wait(), timeout=self._resubscribe_delay
                    )
                except TimeoutError:
                    continue
        finally:
            watchdog.cancel()
            await asyncio.gather(watchdog, return_exceptions=True)
            if (
                not self.stop.is_set()
                and self._on_died is not None
            ):
                logger.error(
                    "%s lease loop exited unexpectedly — detaching",
                    self._name,
                )
                asyncio.create_task(
                    self._on_died(), name=f"{self._name}-died"
                )
