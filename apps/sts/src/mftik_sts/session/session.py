"""STS session — pub/sub lease + TD OMS / recon + MD wiring."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from mftik.broker import Broker, RequestTimeoutError
from mftik.exchange.models import (
    AggTrade,
    Balance,
    BestQuote,
    Fill,
    FundingRate,
    Kline,
    Liquidation,
    OpenInterest,
    Order,
    OrderBook,
    Ticker,
    Trade,
)
from mftik.exchange.oms import Position
from mftik.protocol import (
    ANY_INSTANCE,
    LEASE_MISS_LIMIT,
    MD_AGG_TRADE,
    MD_BEST_QUOTE,
    MD_BESTQUOTE_RESULT,
    MD_FUNDING_HISTORY_RESULT,
    MD_FUNDING_RATE,
    MD_KLINE,
    MD_KLINES_RESULT,
    MD_LEASE_ACK,
    MD_LIQUIDATION,
    MD_OPEN_INTEREST,
    MD_OPEN_INTEREST_RESULT,
    MD_ORDERBOOK,
    MD_ORDERBOOK_RESULT,
    MD_SESSION_DETACH,
    MD_TICKER,
    MD_TRADE,
    STS_LEASE_HEARTBEAT,
    TD_BALANCE_UPDATE,
    TD_CANCEL_REJECT,
    TD_FILL,
    TD_LEASE_ACK,
    TD_ORDER_REJECT,
    TD_ORDER_UPDATE,
    TD_POSITION_UPDATE,
    TD_RECON_DONE,
    TD_SESSION_DETACH,
    CancelReject,
    Envelope,
    LeaseAck,
    LeaseHeartbeat,
    MdBestQuoteResult,
    MdDetachRequest,
    MdDetachRequestEnvelope,
    MdFundingHistoryResult,
    MdKlinesResult,
    MdLeaseAck,
    MdOpenInterestResult,
    MdOrderBookResult,
    OrderReject,
    ReconDone,
    TdAccountRef,
    TdDetachRequest,
    TdDetachRequestEnvelope,
    Topics,
    UntypedEnvelope,
    load_md,
    load_td,
    md_feeds_of,
    md_instances_of,
    publish_sts_log,
    td_api_ids_of,
)
from mftik.strategy import Strategy
from mftik.strategy.eventlog import EventLog
from mftik.symbols import SymbolClient
from pydantic import BaseModel

logger = logging.getLogger(__name__)

#: How long a strategy's ``on_stop`` may take before the session detaches
#: without it. Generous enough for a couple of order cancels, each of which is
#: an ack round-trip; short enough that a wedged strategy cannot hold a trading
#: attach open behind it.
ON_STOP_TIMEOUT_S = 10.0

#: ``(session_id, reason, failed)`` — the manager tears the session down and
#: records the terminal status.
ExitHandler = Callable[[str, str, bool], Awaitable[None]]

#: ``(session_id, key, value)`` — persist one fact for a later rebuild.
RememberHandler = Callable[[str, str, str], Awaitable[None]]

#: MD message type → (strategy hook, payload model). One entry per feed topic
#: MD publishes; anything else on ``md.{session_id}`` is logged and dropped.
MD_HANDLERS: dict[str, tuple[str, type[BaseModel]]] = {
    MD_TICKER: ("on_ticker", Ticker),
    MD_ORDERBOOK: ("on_order_book", OrderBook),
    MD_KLINE: ("on_kline", Kline),
    MD_TRADE: ("on_trade", Trade),
    MD_AGG_TRADE: ("on_agg_trade", AggTrade),
    MD_BEST_QUOTE: ("on_best_quote", BestQuote),
    MD_LIQUIDATION: ("on_liquidation", Liquidation),
    MD_FUNDING_RATE: ("on_funding_rate", FundingRate),
    MD_OPEN_INTEREST: ("on_open_interest", OpenInterest),
}

#: Query result type → (strategy hook, payload model). Separate from
#: :data:`MD_HANDLERS` and from the feed channel: these arrive on the session's
#: own reply channel, one per ``mds.fetch_*`` call, and reach hooks the feeds
#: never touch.
MD_FETCH_HANDLERS: dict[str, tuple[str, type[BaseModel]]] = {
    MD_KLINES_RESULT: ("on_fetch_klines", MdKlinesResult),
    MD_ORDERBOOK_RESULT: ("on_fetch_orderbook", MdOrderBookResult),
    MD_BESTQUOTE_RESULT: ("on_fetch_bestquote", MdBestQuoteResult),
    MD_FUNDING_HISTORY_RESULT: (
        "on_fetch_funding_history",
        MdFundingHistoryResult,
    ),
    MD_OPEN_INTEREST_RESULT: (
        "on_fetch_open_interest",
        MdOpenInterestResult,
    ),
}

#: TD global message type → (strategy hook, payload model).
TD_GLOBAL_HANDLERS: dict[str, tuple[str, type[BaseModel]]] = {
    TD_ORDER_UPDATE: ("on_order_update", Order),
    TD_FILL: ("on_fill", Fill),
    TD_ORDER_REJECT: ("on_order_reject", OrderReject),
    TD_CANCEL_REJECT: ("on_cancel_reject", CancelReject),
    TD_BALANCE_UPDATE: ("on_balance_update", Balance),
    TD_POSITION_UPDATE: ("on_position_update", Position),
}


#: ``api_id`` → the TD instance allowed to use that credential.
TdInstanceLookup = Callable[[int], Awaitable[str | None]]

#: How many missed heartbeat intervals an attached peer may go silent
#: before this session gives up on it. Same fuse both ways: one drop is a
#: lost core message, three is a dead peer. Armed by the first
#: acknowledgement from each instance (MD) or api_id (TD) rather than at
#: start — counting from zero would fail every deploy.
PEER_MISS_LIMIT = LEASE_MISS_LIMIT

#: How long a detach may wait for a reply. The lease is the real teardown;
#: this is promptness. Must stay well under the old two-attempt five-second
#: wait that used to hold a stop open.
DETACH_TIMEOUT_S = 1.5


class StsSession:
    """Strategy session with TD/MD pub/sub links and fencing lease heartbeat."""

    def __init__(
        self,
        *,
        session_id: str,
        broker: Broker,
        created_by: int,
        strategy: Strategy,
        td: dict[str, TdAccountRef] | None = None,
        td_api_ids: list[int] | None = None,
        md_ids: list[str] | None = None,
        md: dict[str, list[str]] | None = None,
        st_paras: dict[str, Any] | None = None,
        heartbeat_interval: float = 1.0,
        symbols: SymbolClient | None = None,
        on_exit: ExitHandler | None = None,
        remember: RememberHandler | None = None,
        event_log: EventLog | None = None,
        strategy_type: str | None = None,
        td_instance: TdInstanceLookup | None = None,
        md_ack_grace: float | None = None,
    ) -> None:
        self.session_id = session_id
        self.broker = broker
        self.created_by = created_by
        #: Qualified registry key. Distinct from :attr:`strategy_name`.
        #: Named ``type`` on the instance so it matches ``sts_sessions.type``
        #: and :class:`SessionView`; the constructor argument is
        #: ``strategy_type`` so it does not shadow the builtin.
        self.type = strategy_type
        self.strategy = strategy
        if td is not None:
            self.td = dict(td)
        else:
            self.td = load_td(list(td_api_ids or []))
        #: ``api_id`` → the TD instance holding that account, for addressing
        #: a detach. Getting it wrong costs the lease's grace and nothing else
        #: — both sides tear down on a heartbeat that stops — but a detach sent
        #: to a subject nobody serves sits in its list rather than vanishing,
        #: so it is worth addressing properly.
        self._td_instance_lookup = td_instance
        #: Instance name → feeds, for addressing attach and detach.
        self.md = load_md(md) if md is not None else load_md(md_ids)
        #: Feed → the MD instance that answered attach (or the first lease
        #: ack, for an unpinned ``*``). Named YAML instances do not need
        #: this: ``StrategyTape`` reads them off :attr:`md` before attach.
        self.md_owners: dict[str, str] = {}
        #: Every feed, flat, whatever instance holds it. This is what a
        #: strategy reads — ``TwapStrategy``, ``OneCancelOther`` and
        #: ``NoopStrategy`` all take ``md_ids[0]`` to find the instrument they
        #: were configured for. Which MD serves a feed is a deployment's
        #: business and never a strategy's, so the shape a strategy sees does
        #: not change.
        self.md_ids = md_feeds_of(self.md)
        self.st_paras = dict(st_paras or {})
        self.heartbeat_interval = heartbeat_interval
        #: Symbol plane reads. Strategies round their own prices and sizes,
        #: so they need tick/step/notional at hand — TD does not check.
        self.symbols = symbols or SymbolClient(broker)
        self._on_exit = on_exit
        self._remember = remember
        #: Audit trail of every event this session was handed and every call it
        #: made. Off unless ``STS_EVENTLOG_DIR`` is set — see
        #: :mod:`mftik.strategy.eventlog`. Built before ``bind`` so oms / mds / tape
        #: can reach it through the session from their first call.
        self.event_log = event_log or EventLog.from_env(session_id)

        # bind() builds the client_order_id factory from session_id.
        strategy.bind(self)
        self.strategy.paras = type(strategy).on_initialized(self.st_paras)

        self._tasks: list[asyncio.Task[Any]] = []
        self._stop = asyncio.Event()
        self._started = False
        self._destroyed = False
        self._exit_requested = False
        self._exit_reason: str | None = None
        self._exit_failed = False
        self._token = 0
        self._ack_tokens: dict[int, int] = {}
        self._md_ack_token: int | None = None
        #: Instance name → when it last acknowledged, on this loop's clock.
        #: Keyed per instance because a session's feeds may be split across
        #: MDs: one of them going quiet is the case worth catching, and a
        #: single timestamp would be kept fresh by whichever one was still
        #: talking.
        self._md_acks: dict[str, float] = {}
        #: ``api_id`` → when that TD last acknowledged. Same arming rule as
        #: MD: a quiet TD stops the strategy, but only after it has acked
        #: once. Attach is what catches a TD that never answers.
        self._td_acks: dict[int, float] = {}
        self._md_ack_grace = md_ack_grace
        self._md_lease_logged = False
        self._on_stop_task: asyncio.Task[Any] | None = None
        self._recon_sent: set[int] = set()

    @property
    def td_api_ids(self) -> list[int]:
        return td_api_ids_of(self.td)

    @td_api_ids.setter
    def td_api_ids(self, ids: list[int]) -> None:
        self.td = load_td(list(ids or []))

    def note_md_owner(self, feeds: list[str], instance: str) -> None:
        """Remember which MD holds ``feeds`` so a tape read can address it.

        ``*`` is not an owner. An empty name is a mixed-version attach
        result and is ignored the same way.
        """
        if not instance or instance == ANY_INSTANCE:
            return
        for feed in feeds:
            self.md_owners[feed] = instance

    def td_account(self, name: str) -> TdAccountRef:
        try:
            return self.td[name]
        except KeyError:
            raise KeyError(
                f"session {self.session_id} has no td account named {name!r}"
            ) from None

    def td_sole(self) -> int:
        if len(self.td) != 1:
            raise RuntimeError(
                f"session {self.session_id} needs exactly one td account, "
                f"got {list(self.td)}"
            )
        return next(iter(self.td.values())).api_id

    @property
    def destroyed(self) -> bool:
        return self._destroyed

    @property
    def exit_requested(self) -> bool:
        """Whether this session has asked to end, teardown run or not.

        Set synchronously by :meth:`request_exit`, which is what makes it
        readable the moment :meth:`start` returns — a strategy that rejects
        its configuration in ``on_start`` has already set this by then.
        """
        return self._exit_requested

    @property
    def exit_reason(self) -> str | None:
        return self._exit_reason

    @property
    def exit_failed(self) -> bool:
        """Whether the end was a failure rather than a natural finish."""
        return self._exit_failed

    @property
    def strategy_name(self) -> str:
        return self.strategy.name

    async def _publish_log(
        self, message: str, *, source: str = "sts", level: str = "info"
    ) -> None:
        await publish_sts_log(
            self.broker,
            self.session_id,
            message,
            source=source,
            level=level,
            type=self.type,
        )

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._stop.clear()

        # Before the pumps: the first thing on a feed must not arrive with
        # nowhere to be written.
        await self.event_log.start()
        self.event_log.record(
            "lifecycle",
            "session_start",
            dir="self",
            strategy=self.strategy_name,
            td=self.td_api_ids,
            md=self.md_ids,
            paras=self.st_paras or None,
        )

        self._tasks = [
            asyncio.create_task(
                self._lease_heartbeat_loop(),
                name=f"sts-{self.session_id}-lease",
            ),
            # Unconditional, unlike the feed pump below. A query needs no
            # subscription and no attach, so a session that asked for no market
            # data can still make one — and its answer has to have somewhere to
            # land before it does.
            asyncio.create_task(
                self._pump_fetch_replies(),
                name=f"sts-{self.session_id}-fetch",
            ),
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

        # Recorded on the way in rather than on the way out: a hook that raises
        # takes the session down with it, and the log should show which of the
        # two it was standing in when that happened.
        self.event_log.record("lifecycle", "on_start", dir="self")
        await self.strategy.on_start()
        self.event_log.record("lifecycle", "on_ready", dir="self")
        await self.strategy.on_ready()
        await self._publish_log(
            f"session started strategy={self.strategy_name} "
            f"td={self.td_api_ids} md={self.md_ids}"
        )
        logger.info(
            "STS session started id=%s strategy=%s td=%s md=%s",
            self.session_id,
            self.strategy_name,
            self.td_api_ids,
            self.md_ids,
        )

    async def remember(self, key: str, value: str) -> None:
        """Persist one fact for this session — see ``Strategy.remember``."""
        if self._remember is None:
            return
        self.event_log.record("remember", key, dir="out", value=value)
        await self._remember(self.session_id, key, value)

    def request_exit(
        self, reason: str = "strategy_exit", *, failed: bool = False
    ) -> None:
        """Ask the session manager to end this session.

        ``failed`` marks the session ``failed`` rather than ``done`` and keeps
        ``reason`` on the row. First call wins: a session already on its way
        out is not re-labelled, so cancel-then-fail sequences keep the reason
        that started the teardown.

        Scheduled on the event loop so it is safe to call from a timer callback.
        """
        if self._destroyed or self._exit_requested:
            return
        self._exit_requested = True
        # Kept, not just passed on. The teardown runs as its own task, so a
        # caller that asks "did this session survive being started?" the
        # instant ``start`` returns needs an answer that does not depend on
        # whether that task has had a turn yet.
        self._exit_reason = reason
        self._exit_failed = failed
        self.event_log.record(
            "lifecycle", "exit_requested", dir="self", reason=reason, failed=failed
        )
        logger.info(
            "STS session exit requested id=%s failed=%s reason=%s",
            self.session_id,
            failed,
            reason,
        )
        if self._on_exit is not None:
            asyncio.create_task(
                self._on_exit(self.session_id, reason, failed),
                name=f"sts-{self.session_id}-exit",
            )
        else:
            asyncio.create_task(
                self.stop(), name=f"sts-{self.session_id}-exit-stop"
            )

    def _fail_from_infrastructure(self, what: str) -> None:
        """End the session as ``failed`` after a pump or lease loop died.

        These loops do not come back: once ``subscribe`` or the heartbeat
        publish raises, the session keeps its row marked live while receiving
        nothing and holding no lease. Ending it makes that visible instead of
        leaving a session that looks running and is not.
        """
        self.request_exit(f"{what} stopped: session can no longer run", failed=True)

    async def stop(self) -> None:
        if self._destroyed:
            return
        self._destroyed = True
        self._exit_requested = True
        # The strategy goes first. ``on_stop`` is where a resting order gets
        # cancelled, and that cancel reaches TD as this session — which stops
        # being true the moment the detach below lands, leaving the order
        # resting at the venue with nothing left to manage it.
        await self._run_on_stop()
        # Then the detaches, still ahead of the heartbeat stopping: TD and MD
        # expire a lease that goes quiet, and being told is a cleaner ending
        # than being timed out.
        await self._publish_detaches()
        self._stop.set()
        self.strategy.timer.close()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        self._started = False
        try:
            await self._publish_log("session stopped")
        except Exception:
            pass
        self.event_log.record("lifecycle", "session_stop", dir="self")
        # Last, and awaited: the records above are the ones a post-mortem opens
        # the file for, and they are still in the queue at this point.
        await self.event_log.close()
        logger.info("STS session stopped id=%s", self.session_id)

    async def _run_on_stop(self) -> None:
        """Let the strategy clean up, bounded, before its attaches go away.

        Waited on rather than cancelled when it overruns. ``on_stop`` typically
        parks in an order ack, which is a blocking broker read; on a pooled
        transport cancelling one mid-command hands the connection back with its
        reply unread and breaks whatever borrows it next. So a slow strategy is
        left running and the detach goes out anyway — its cancel will be
        refused, but a stuck strategy must not hold an attach open indefinitely
        either.
        """
        self.event_log.record("lifecycle", "on_stop", dir="self")
        self._on_stop_task = asyncio.create_task(
            self.strategy.on_stop(), name=f"sts-{self.session_id}-on-stop"
        )
        done, _ = await asyncio.wait(
            {self._on_stop_task}, timeout=ON_STOP_TIMEOUT_S
        )
        if not done:
            self.event_log.record(
                "lifecycle",
                "on_stop_timeout",
                dir="self",
                timeout_s=ON_STOP_TIMEOUT_S,
            )
            logger.warning(
                "STS on_stop still running after %ss session=%s — detaching "
                "anyway; anything it still had to cancel will be refused",
                ON_STOP_TIMEOUT_S,
                self.session_id,
            )
            return
        exc = self._on_stop_task.exception()
        if exc is not None:
            logger.error(
                "strategy on_stop failed session=%s", self.session_id,
                exc_info=exc,
            )

    async def _publish_detaches(self) -> None:
        """Tell TD and MD this session is over. Do not wait to be thanked.

        Posted to each domain's process-level subject rather than published on
        the session stream. That stream is read by one lease loop per attach,
        and a detach is only ever acted on by the loop whose attach it names —
        so when that loop is the thing that has already stopped, the message is
        read by a sibling, filtered out, and lost. The RPC queue is a list: it
        is taken by whichever process is serving the subject, and one that is
        down leaves it there for the next.

        Sent without a reply because there is nothing to learn from one. The
        lease is what actually ends an attach: both domains watch this
        session's heartbeat and run the identical teardown when it stops — MD
        after 3 seconds, TD after 5 — so a detach that never lands costs those
        seconds and nothing else. What this buys is promptness and a reason on
        the row (``sts_stop`` rather than ``lease_expired``), neither of which
        is worth holding a stop open for.

        It used to be request-reply with two five-second attempts per attach,
        which is up to ten seconds of a stopping session's life spent waiting
        to save MD three — and an ERROR when it timed out, for a teardown that
        was about to happen anyway.
        """
        posts = [
            self._post_detach(
                what=f"td api_id={api_id}",
                subject=Topics.td(await self._detach_instance(api_id)),
                envelope=TdDetachRequestEnvelope.wrap(
                    TdDetachRequest(
                        session_id=self.session_id, api_id=api_id
                    ),
                    type=TD_SESSION_DETACH,
                    source="sts",
                    session_id=self.session_id,
                ),
            )
            for api_id in self.td_api_ids
        ]
        for instance in md_instances_of(self.md):
            posts.append(
                self._post_detach(
                    what=f"md instance={instance}",
                    subject=(
                        Topics.MD
                        if instance == ANY_INSTANCE
                        else Topics.md(instance)
                    ),
                    envelope=MdDetachRequestEnvelope.wrap(
                        MdDetachRequest(session_id=self.session_id),
                        type=MD_SESSION_DETACH,
                        source="sts",
                        session_id=self.session_id,
                    ),
                )
            )
        if posts:
            await asyncio.gather(*posts, return_exceptions=True)

    async def _detach_instance(self, api_id: int) -> str:
        """Which TD to address this detach to.

        Best-effort in the same way the detach itself is: if nothing can
        answer, the plane name is what a node with one TD runs under, and a
        detach that lands nowhere costs the lease's grace rather than
        correctness.
        """
        if self._td_instance_lookup is None:
            return "td"
        try:
            return await self._td_instance_lookup(api_id) or "td"
        except Exception:
            logger.warning(
                "STS could not resolve the TD instance for api_id=%s on "
                "detach session=%s — the lease will expire the attach",
                api_id,
                self.session_id,
                exc_info=True,
            )
            return "td"

    async def _post_detach(
        self, *, what: str, subject: str, envelope: Any
    ) -> None:
        """Ask for one detach. A failure here is logged, not waited on."""
        self.event_log.record(
            "detach", envelope.type, dir="out", what=what
        )
        try:
            await self.broker.request(
                subject, envelope, timeout=DETACH_TIMEOUT_S
            )
        except RequestTimeoutError as exc:
            self.event_log.record(
                "detach", "detach_unanswered", dir="self", what=what,
                error=repr(exc),
            )
            logger.warning(
                "STS detach %s session=%s had no responder — the lease will "
                "expire it",
                what,
                self.session_id,
            )
            return
        except Exception as exc:
            # The lease covers this. Worth a line because a broker that cannot
            # take a write is a problem in its own right, not because the
            # attach is now stuck.
            self.event_log.record(
                "detach", "detach_request_failed", dir="self", what=what,
                error=repr(exc),
            )
            logger.warning(
                "STS could not request detach %s session=%s: %s — the lease "
                "will expire it",
                what,
                self.session_id,
                exc,
            )
            return
        try:
            await self._publish_log(f"detach sent {what}")
        except Exception:
            logger.exception(
                "STS detach log failed session=%s", self.session_id
            )

    def _peer_grace(self) -> float:
        """Silence a peer may keep after its first ack.

        Tests pass ``md_ack_grace`` as an absolute window. Production counts
        :data:`PEER_MISS_LIMIT` of this session's heartbeat interval.
        """
        if self._md_ack_grace is not None:
            return self._md_ack_grace
        return self.heartbeat_interval * PEER_MISS_LIMIT

    async def _lease_heartbeat_loop(self) -> None:
        """Publish fencing heartbeats on sts.td.* and/or sts.md.*."""
        while not self._stop.is_set():
            self._token += 1
            hb = LeaseHeartbeat(
                session_id=self.session_id,
                token=self._token,
                interval=self.heartbeat_interval,
            )
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
                self._fail_from_infrastructure("lease heartbeat")
                return

            # Armed per instance / api_id on the first ack. One quiet MD of
            # two stops the session; a quiet TD does too — there is no book
            # in a cache to keep trading against.
            grace = self._peer_grace()
            stale_md = self._stale_keys(self._md_acks, grace)
            if stale_md:
                logger.error(
                    "STS lost market data from %s session=%s",
                    ", ".join(stale_md),
                    self.session_id,
                )
                await self._publish_log(
                    f"no market-data acknowledgement from {', '.join(stale_md)} "
                    f"for {grace:.0f}s",
                    level="error",
                )
                self._fail_from_infrastructure(
                    f"md feed from {', '.join(stale_md)}"
                )
                return
            stale_td = self._stale_keys(self._td_acks, grace)
            if stale_td:
                names = [f"api_id={api}" for api in stale_td]
                logger.error(
                    "STS lost trading desk from %s session=%s",
                    ", ".join(names),
                    self.session_id,
                )
                await self._publish_log(
                    f"no trading-desk acknowledgement from {', '.join(names)} "
                    f"for {grace:.0f}s",
                    level="error",
                )
                self._fail_from_infrastructure(
                    f"td from {', '.join(names)}"
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
            self._fail_from_infrastructure("md feed")

    async def _pump_fetch_replies(self) -> None:
        """Deliver query answers to ``on_fetch_klines``.

        Its own channel and its own task, not a branch of the feed pump. The
        two have different lifetimes — this one runs whether or not the session
        subscribed to anything — and a failure here should not read as the
        market data having died, so it does not fail the session the way a
        broken feed does. A strategy that only queries is not receiving
        anything it can be starved of.
        """
        topic = Topics.md_fetch_reply(self.session_id)
        try:
            async for env in self.broker.subscribe(topic, stop=self._stop):
                if env.type in MD_FETCH_HANDLERS:
                    await self._on_fetch_result(env)
                    continue
                self._on_message("md-fetch", env)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "STS md fetch pump failed session=%s", self.session_id
            )

    async def _on_fetch_result(self, env: UntypedEnvelope) -> None:
        """Hand one query answer to the hook that asked for it."""
        name, model = MD_FETCH_HANDLERS[env.type]
        self._record_in("md_fetch", env, hook=name)
        try:
            result = model.model_validate(env.payload)
        except Exception as exc:
            self.event_log.record(
                "error",
                "payload_invalid",
                dir="self",
                hook=name,
                type=env.type,
                env_id=env.id,
                error=repr(exc),
            )
            logger.exception(
                "invalid md fetch result session=%s type=%s",
                self.session_id,
                env.type,
            )
            return
        try:
            await getattr(self.strategy, name)(result)
        except Exception as exc:
            self._record_hook_failed(name, env, exc)
            logger.exception(
                "strategy %s failed session=%s query_id=%s",
                name,
                self.session_id,
                result.query_id,
            )

    async def _on_md_lease_ack(self, env: UntypedEnvelope) -> None:
        self._record_in("lease", env)
        try:
            ack = MdLeaseAck.model_validate(env.payload)
            self._md_ack_token = ack.token
        except Exception:
            return
        # Under the plane name when the sender does not say: a single-process
        # node calls itself ``md``, so an MD that predates the field is tracked
        # as the one instance it is rather than not tracked at all.
        instance = ack.instance or "md"
        first = instance not in self._md_acks
        self._md_acks[instance] = asyncio.get_running_loop().time()
        # Unpinned feeds have no name in the YAML. The first ack is the
        # first moment we know who took them — too late for on_start, but
        # enough for a later read.
        for feed in self.md.get(ANY_INSTANCE, []):
            self.md_owners.setdefault(feed, instance)
        if first and self._md_acks:
            self.event_log.record(
                "lease", "md_ack_armed", dir="self", what=instance
            )
        if self._md_lease_logged:
            return
        self._md_lease_logged = True
        await self._publish_log("MD lease established")

    def _stale_keys(self, seen: dict[Any, float], grace: float) -> list[Any]:
        """Peers that have acked once and then gone quiet.

        Only keys that have acknowledged at least once are considered.
        Arming on the first ACK is what makes this safe to run from the
        moment the session starts: a session begins heartbeating before
        the peer has attached to hear it.
        """
        if not seen:
            return []
        now = asyncio.get_running_loop().time()
        return sorted(
            key for key, at in seen.items() if now - at > grace
        )

    async def _on_market_data(self, env: UntypedEnvelope) -> None:
        name, model = MD_HANDLERS[env.type]
        # The wire dict, not the model built from it. It is what arrived, it
        # costs nothing to record — the parse has already happened, upstream —
        # and a payload that fails validation below is exactly the one worth
        # having on disk in the shape it came in.
        self._record_in("md", env, hook=name)
        try:
            payload = model.model_validate(env.payload)
        except Exception as exc:
            self.event_log.record(
                "error",
                "payload_invalid",
                dir="self",
                hook=name,
                type=env.type,
                env_id=env.id,
                error=repr(exc),
            )
            logger.exception(
                "invalid md payload session=%s type=%s",
                self.session_id,
                env.type,
            )
            return
        handler = getattr(self.strategy, name)
        try:
            await handler(payload)
        except Exception as exc:
            self._record_hook_failed(name, env, exc)
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
            self._fail_from_infrastructure(f"td session feed api_id={api_id}")

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
            self._fail_from_infrastructure(f"td global feed api_id={api_id}")

    async def _on_td_global(self, api_id: int, env: UntypedEnvelope) -> None:
        entry = TD_GLOBAL_HANDLERS.get(env.type)
        if entry is None:
            self._on_message(f"global-{api_id}", env)
            return
        name, model = entry
        # Recorded before ``owns`` has a say — that filter belongs to the
        # strategy, and an audit trail that only kept this session's own fills
        # could not show the account moving underneath it.
        self._record_in("td", env, hook=name, api_id=api_id)
        try:
            payload = model.model_validate(env.payload)
        except Exception as exc:
            self.event_log.record(
                "error",
                "payload_invalid",
                dir="self",
                hook=name,
                type=env.type,
                env_id=env.id,
                api_id=api_id,
                error=repr(exc),
            )
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
        except Exception as exc:
            self._record_hook_failed(name, env, exc, api_id=api_id)
            logger.exception(
                "strategy %s failed session=%s api_id=%s type=%s",
                name,
                self.session_id,
                api_id,
                env.type,
            )

    async def _on_lease_ack(self, api_id: int, env: UntypedEnvelope) -> None:
        # The closest thing this system has to a login: TD has accepted the
        # lease and this session may now trade the account.
        self._record_in("lease", env, api_id=api_id)
        try:
            ack = LeaseAck.model_validate(env.payload)
            self._ack_tokens[api_id] = ack.token
        except Exception:
            return
        first = api_id not in self._td_acks
        self._td_acks[api_id] = asyncio.get_running_loop().time()
        if first:
            self.event_log.record(
                "lease", "td_ack_armed", dir="self", api_id=api_id
            )
        # First ACK means TD session is established → Strategy sends Recon.
        if api_id in self._recon_sent:
            return
        self._recon_sent.add(api_id)
        try:
            await self.strategy.send_recon(api_id)
            await self._publish_log(
                f"TD lease established — sent recon api_id={api_id}"
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
        self._record_in("recon", env, hook="on_recon_done")
        try:
            msg = ReconDone.model_validate(env.payload)
        except Exception:
            return
        await self._publish_log(f"recon done api_id={msg.api_id}")
        try:
            await self.strategy.on_recon_done(msg)
        except Exception as exc:
            self._record_hook_failed("on_recon_done", env, exc, api_id=msg.api_id)
            logger.exception(
                "strategy on_recon_done failed session=%s api_id=%s",
                self.session_id,
                msg.api_id,
            )

    def _on_message(self, peer: str, env: UntypedEnvelope) -> None:
        # Worth a line of its own: this is where a message arrives that no hook
        # claims. In the process log it is a DEBUG nobody runs with, and the
        # symptom it produces — a strategy that is simply never called — looks
        # from the inside exactly like a feed that went quiet.
        self._record_in("unhandled", env, peer=peer)
        logger.debug(
            "STS session=%s from=%s type=%s",
            self.session_id,
            peer,
            env.type,
        )

    # --- event log ---------------------------------------------------------

    def _record_in(
        self, kind: str, env: UntypedEnvelope, **fields: Any
    ) -> None:
        """Log one inbound envelope, as it arrived.

        ``sent_ts`` is the sender's stamp and ``ts`` is ours, so the wire
        latency of any message is the difference between two fields on one
        line rather than a correlation across two services' logs.
        """
        self.event_log.record(
            kind,
            env.type,
            env_id=env.id,
            sent_ts=env.ts,
            source=env.source,
            payload=env.payload,
            **fields,
        )

    def _record_hook_failed(
        self,
        hook: str,
        env: UntypedEnvelope,
        exc: BaseException,
        **fields: Any,
    ) -> None:
        """Log a strategy hook that raised on an event it was handed."""
        self.event_log.record(
            "error",
            "hook_failed",
            dir="self",
            hook=hook,
            type=env.type,
            env_id=env.id,
            error=repr(exc),
            **fields,
        )
