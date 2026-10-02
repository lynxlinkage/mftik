"""STS session — TD OMS / recon + MD wiring."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from mftik.broker import Broker, RequestTimeoutError
from mftik.protocol import (
    ANY_INSTANCE,
    MD_BESTQUOTE_RESULT,
    MD_FUNDING_HISTORY_RESULT,
    MD_INTENT_DELETE,
    MD_KLINES_RESULT,
    MD_OPEN_INTEREST_RESULT,
    MD_ORDERBOOK_RESULT,
    ON_STOP_TIMEOUT_S,
    TD_INTENT_DELETE,
    TD_RECON_DONE,
    IntentOwner,
    MdBestQuoteResult,
    MdFundingHistoryResult,
    MdIntentDelete,
    MdIntentDeleteEnvelope,
    MdKlinesResult,
    MdOpenInterestResult,
    MdOrderBookResult,
    ReconDone,
    TdAccountRef,
    TdIntentDelete,
    TdIntentDeleteEnvelope,
    Topics,
    UntypedEnvelope,
    load_md,
    load_td,
    md_feeds_of,
    md_instances_of,
    publish_sts_log,
    td_api_ids_of,
)
from mftik.strategy import Ready, Strategy
from mftik.strategy.eventlog import EventLog
from mftik.symbols import SymbolClient
from pydantic import BaseModel

from mftik_sts.session_worker.dispatch import MD_HANDLERS, dispatch_md, dispatch_td

logger = logging.getLogger(__name__)

#: ``(session_id, reason, failed)`` — the manager tears the session down and
#: records the terminal status.
ExitHandler = Callable[[str, str, bool], Awaitable[None]]

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

#: ``api_id`` → the TD instance allowed to use that credential.
TdInstanceLookup = Callable[[int], Awaitable[str | None]]

#: How long a detach may wait for a reply. Promptness, not the teardown
#: itself. Must stay well under the old two-attempt five-second wait that
#: used to hold a stop open.
DETACH_TIMEOUT_S = 1.5


class StsSession:
    """Strategy session with TD/MD pub/sub links."""

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
        symbols: SymbolClient | None = None,
        on_exit: ExitHandler | None = None,
        event_log: EventLog | None = None,
        strategy_type: str | None = None,
        td_instance: TdInstanceLookup | None = None,
    ) -> None:
        self.session_id = session_id
        self.broker = broker
        self.created_by = created_by
        #: Qualified registry key (``CrossArb``, ``private::Tiny``). Named
        #: ``type`` on the instance so it matches ``sts_sessions.type`` and
        #: :class:`SessionView`; the constructor argument is ``strategy_type``
        #: so it does not shadow the builtin. ``bind`` copies it onto the
        #: strategy, which otherwise only knows its class.
        self.type = strategy_type
        self.strategy = strategy
        if td is not None:
            self.td = dict(td)
        else:
            self.td = load_td(list(td_api_ids or []))
        #: ``api_id`` → the TD instance holding that account, for addressing
        #: a detach. A detach sent to a subject nobody serves sits in its list
        #: rather than vanishing, so it is worth addressing properly.
        self._td_instance_lookup = td_instance
        #: Instance name → feeds, for addressing attach and detach.
        self.md = load_md(md) if md is not None else load_md(md_ids)
        #: Feed → the MD instance that answered attach. Named YAML instances
        #: do not need this: ``StrategyTape`` reads them off :attr:`md`
        #: before attach.
        self.md_owners: dict[str, str] = {}
        #: Every feed, flat, whatever instance holds it. This is what a
        #: strategy reads — ``TwapStrategy``, ``OneCancelOther`` and
        #: ``NoopStrategy`` all take ``md_ids[0]`` to find the instrument they
        #: were configured for. Which MD serves a feed is a deployment's
        #: business and never a strategy's, so the shape a strategy sees does
        #: not change.
        self.md_ids = md_feeds_of(self.md)
        self.st_paras = dict(st_paras or {})
        #: Symbol plane reads. Strategies round their own prices and sizes,
        #: so they need tick/step/notional at hand — TD does not check.
        self.symbols = symbols or SymbolClient(broker)
        self._on_exit = on_exit
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
        self._on_stop_task: asyncio.Task[Any] | None = None

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
        return self.strategy.registry_key

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
        # Nothing is waited on before this, so nothing can be missing yet. The
        # ingress that computes readiness, and the report that can be non-empty,
        # arrive with the session worker (IF-05).
        await self.strategy.on_ready(Ready())
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
        # Then the detaches: being told is a cleaner ending than being left
        # attached to a session that has stopped answering.
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

        Sent without a reply because there is nothing to learn from one. What
        this buys is promptness and a reason on the row, which is not worth
        holding a stop open for.

        It used to be request-reply with two five-second attempts per attach,
        which is up to ten seconds of a stopping session's life spent waiting
        to save MD three — and an ERROR when it timed out, for a teardown that
        was about to happen anyway.
        """
        # This path is the leftover session object (B4-03 replaces it). It
        # does not know which STS instance it is running on, so the owner
        # carries an empty instance. Nothing production calls it.
        owner = IntentOwner(sts_instance="", session_id=self.session_id)
        posts = [
            self._post_detach(
                what=f"td api_id={api_id}",
                subject=Topics.td(await self._detach_instance(api_id)),
                envelope=TdIntentDeleteEnvelope.wrap(
                    TdIntentDelete(
                        session_id=self.session_id,
                        api_ids=[api_id],
                        owner=owner,
                    ),
                    type=TD_INTENT_DELETE,
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
                    envelope=MdIntentDeleteEnvelope.wrap(
                        MdIntentDelete(
                            session_id=self.session_id,
                            owner=owner,
                        ),
                        type=MD_INTENT_DELETE,
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

    async def _pump_md_session(self) -> None:
        topic = Topics.md_session(self.session_id)
        try:
            async for env in self.broker.subscribe(topic, stop=self._stop):
                if env.type in MD_HANDLERS:
                    await self._on_market_data(env)
                    continue
                self._on_message("md", env)
        except asyncio.CancelledError:
            raise
        except Exception:
            # Logged and nothing more. This pump does not come back, so the
            # session is now running without market data — B5-05 turns that
            # into a notification.
            logger.exception(
                "STS md session pump failed session=%s", self.session_id
            )

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

    async def _on_market_data(self, env: UntypedEnvelope) -> None:
        # Decode and the hook live with the session worker. This shell
        # swallows a hook exception so the event log can record it and
        # the session continues; the worker does not.
        await dispatch_md(self.strategy, self.event_log, env, swallow=True)

    async def _pump_td_session(self, api_id: int) -> None:
        topic = Topics.td_session(api_id, self.session_id)
        try:
            async for env in self.broker.subscribe(topic, stop=self._stop):
                if env.type == TD_RECON_DONE:
                    await self._on_recon_done(env)
                    continue
                self._on_message(f"td-{api_id}", env)
        except asyncio.CancelledError:
            raise
        except Exception:
            # Logged and nothing more — see :meth:`_pump_md_session`.
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
            # Logged and nothing more — see :meth:`_pump_md_session`.
            logger.exception(
                "STS td global pump failed session=%s api_id=%s",
                self.session_id,
                api_id,
            )

    async def _on_td_global(self, api_id: int, env: UntypedEnvelope) -> None:
        # Same dispatch as the session worker. ``False`` is a type no
        # hook claims; the shell still logs that on its own channel.
        if not await dispatch_td(
            self.strategy, self.event_log, api_id, env, swallow=True
        ):
            self._on_message(f"global-{api_id}", env)

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
