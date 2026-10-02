"""Base strategy — one instance per STS session."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from mftik.exchange.models import (
    AggTrade,
    Balance,
    BestQuote,
    FeedEnd,
    Fill,
    FundingRate,
    Greeks,
    Kline,
    Liquidation,
    OpenInterest,
    Order,
    OrderBook,
    Ticker,
    Trade,
)
from mftik.exchange.oms import OmsView, Position
from mftik.protocol import (
    CancelReject,
    MdBestQuoteResult,
    MdFundingHistoryResult,
    MdKlinesResult,
    MdOpenInterestResult,
    MdOrderBookResult,
    OrderReject,
    ReconDone,
    publish_sts_log,
)
from mftik.strategy.artifacts import StrategyArtifacts
from mftik.strategy.budget import HookSlow
from mftik.strategy.client_order_id import VERSION, session_id_of, version_of
from mftik.strategy.ledger import StrategyLedger
from mftik.strategy.md import FeedState, StrategyMd
from mftik.strategy.mds import StrategyMds
from mftik.strategy.offload import OffloadPool
from mftik.strategy.oms import StrategyOms
from mftik.strategy.ready import Ready
from mftik.strategy.session import SessionView
from mftik.strategy.symbols import StrategySymbols
from mftik.strategy.tape import StrategyTape
from mftik.strategy.td import AccountState, StrategyTd
from mftik.strategy.timer import Timer
from mftik.strategy.universe import UniverseChange


class Strategy:
    """Base class for STS strategy implementations.

    Session ↔ Strategy is 1-1. Override hooks as needed.

    Process control (wired):
        on_start, on_ready(ready), on_stop
        exit() — natural end → session stop → on_stop → status "done"
        fail(reason) — same teardown, but status "failed" and reason is
        persisted for the UI
        ``on_start`` may be long and may be synchronous, and it may not trade:
        TD has not been subscribed and nothing has reconciled, so order entry
        will raise :class:`~mftik.strategy.errors.NotReady` there (the gate
        itself lands with the session worker — IF-06 defines the exception).
        ``on_ready`` fires once, and fires even when a feed is missing — what
        is missing is in ``ready.missing_feeds`` and what to do about it is the
        strategy's call. See :mod:`mftik.strategy.ready`.

    Heavy computation (interface only — IF-06, lands in B5-03):
        await self.offload(func, *args) — run it off the strategy loop so
        fills, timers and on_stop keep being served while it runs
        await self.offload(func, *args, isolate=True) — in a child process,
        for pure-Python work or anything with a memory risk
        pool = await self.offload_pool(init=..., init_args=...) then
        await pool.call(func, *args) — a child process with state loaded once
        See :mod:`mftik.strategy.offload`. This replaces the old breathe /
        slice_deadline pacing: hand over the whole computation rather than
        cutting it into slices.

    Availability, not content (the session worker delivers these):
        on_md_update(feed, state, reason) — "live" | "down"
        on_td_update(api_id, state, reason) — "ready" | "degraded" |
        "unavailable"
        self.md.state(feed) / self.td.state(api_id) — ask at any moment
        Losing a feed or an account does not fail the session; the platform
        notifies and the strategy decides. An ``unavailable`` account refuses
        submits and cancels locally. These hooks carry connection and
        availability only: prices arrive on on_ticker and friends, orders
        on on_order_update.

    Selector universes (interface only — IF-06, lands in B9):
        on_universe_change(name, change) — change.added / removed / epoch, and
        change.current for a rolling future
        self.md.universe(name) / self.md.current(name)
        A ``select:`` block in strategy.yml names a shape — the two nearest
        expiries, the front quarterly — and the platform derives the members.
        A contract sends nothing before its ``added`` and nothing after its
        ``removed``.

    TD recon (wired):
        on_recon_done
        self.oms — read OMS snapshots from ``td.oms.{api_id}``
        self.ledger — read balances from ``td.ledger.{api_id}``; TD owns
        them, so this is a view: available() is free minus TD's pre-locks.
        Contract strategies also call ledger.ensure_leverage(ticker) so TD
        caches per-symbol leverage for perp pre-locks (notional / leverage).

    Gaps in the event stream (the session worker delivers ``on_resync``):
        on_resync(api_id, cause, view) — the platform reconciled because the
        stream may have a hole in it (the session's own NATS connection
        reconnected, or the account worker rebuilt its book from the venue).
        ``view`` is that worker's settled ``oms.view``: UNKNOWN orders were
        chased before the hook ran. Correct whatever was accumulated from
        events against it. A ``td.error`` (the trading layer is closed, or
        the payload was refused) or a timeout skips the hook. That skip is
        not an empty book.
        await self.oms.view(settled=True) — the same convergence on demand,
        for a strategy that needs UNKNOWN orders resolved before it acts.
        Strategies do not reconcile themselves: there is no send_recon.

    Order entry — request-reply on ``td.order.{api_id}`` (wired):
        submit_order / cancel_order return True once TD acks the request.
        False means it never reached the venue (no ack, or TD refused it).
        cancel_order also returns False without sending when the cid is
        still pending (PENDING_NEW / PENDING_CANCEL) — there is no venue
        id to cancel against. A True says nothing about the venue's answer
        — that arrives below.
        submit_order mints the uint64 client_order_id
        (ver | session_id | seconds since 2026-01-01 | seq++) and leaves it in
        oms.last_client_order_id; cancel_order takes that id.
        wait_cids(api_id, cids, until=..., timeout=...) parks until every
        cid satisfies ``until`` (typically ``status is not PENDING_NEW``)
        or the timeout fires. Use it from on_stop before cancel_order.

    Private events from ``td.{api_id}.global`` (wired):
        on_order_update, on_fill, on_balance_update
        on_position_update (contract venues only — spot has no positions)
        on_order_reject (submit fail), on_cancel_reject (cancel fail)
        submit → on_order_update | on_order_reject
        cancel → on_order_update | on_cancel_reject
        NOTE: account-wide fan-out — other sessions on the same api_id show
        up here too. Filter with ``self.owns(cid)``.

    Symbol plane (wired):
        self.symbols — exch_ticker / filters per universal ticker

    Timer tokens (wired):
        self.timer.token().register(first_ms, interval_ms, func, label=...)
        token.cancel()  — timestamps are unix ms
        ``label`` names the token in the event log below; it defaults to the
        callback's name, which is ambiguous when one method backs two tokens.

    Public events from ``md.{session_id}`` (wired):
        on_ticker, on_order_book, on_kline, on_trade, on_agg_trade,
        on_best_quote, on_liquidation, on_funding_rate, on_open_interest,
        on_greeks
        One hook per feed topic subscribed in ``md_ids``
        (``topic.UniversalTicker``; kline carries its interval in the topic,
        e.g. ``paper.kline_1m.BTCUSDT``).
        on_feed_end — not a subscribed topic. MD fires it once per
        (session, topic) when a feed that did open later ends: the
        pump died, the socket gave up, or the instrument reached its
        listed expiry. One ticker with two topics is two calls. A
        feed that cannot be opened fails the attach instead.

    Market-data queries — request-reply on ``md.fetch`` (wired):
        self.mds.fetch_klines(ticker, interval, limit=...)
        self.mds.fetch_order_book(ticker, depth=...)
        self.mds.fetch_best_quote(ticker)
        self.mds.fetch_funding_history(ticker, limit=...)
        self.mds.fetch_open_interest(ticker)
        Each returns a query_id once MD acks, or None if it never left
        (refused, or no MD running — mds.last_reject_reason says which). The
        answer arrives later at on_fetch_klines / on_fetch_orderbook /
        on_fetch_bestquote / on_fetch_funding_history /
        on_fetch_open_interest, carrying that query_id.
        Independent of md_ids: a session that subscribes to nothing can still
        query, and any venue is reachable whether or not it is streaming.
        NOTE: these are not the feed hooks. A feed pushes every change for as
        long as the session lives; a query answers once, when asked. Needing
        the book at one moment is a query, not a subscription.
        ``interval`` is canonical (``1m``, ``4h``, ``1mo``) — see
        ``mftik.exchange.intervals``; the month is ``1mo``, never ``1M``.

    Recorded tape — warm-up on prints from before this session (wired):
        self.tape.read(ticker, topic="aggtrade") — the trade history MD kept
        while something else held the feed, as the same Trade / AggTrade the
        live hooks are handed, on ``records``. Pass ``on_print`` and each
        print is handed to that callback instead, and ``records`` comes back
        empty: the caller is keeping the series, and the slice only says
        what it covers (``count``, ``span_ms``, ``continuous_since_ms``,
        ``recording``, ``gaps``). ``len`` is the number of prints either
        way. Only ``aggtrade`` and ``trade`` are recorded. Routed to the MD
        instance this session attached; a feed that is not on the session's
        ``md`` map raises. A count of prints is not a length of history,
        and a strategy that needs N of something has to check rather than
        assume. Empty from the right MD is a normal answer — nothing was
        holding the feed, or recording is off.

    Artifacts — opaque bytes on this STS's disk (wired):
        self.artifacts.read(path) / stat(path) / write(path, body)
        self.artifacts.reading(path) / writing(path) — the same object as a
        file, so a large checkpoint is not also held as ``bytes``.
        One relative path is one object. ``weights/model.pt`` is not
        ``sessions/{session_id}/weights/model.pt``. A missing key is None;
        a key that is not a relative path raises. These read or replace a
        whole object — ``on_start``, ``on_stop``, not a hot hook. See
        :mod:`mftik.strategy.artifacts`.

    Event log (wired, nothing to call):
        Every event reaching a hook here, and every order, cancel and query
        going the other way, is written to one jsonl per session — see
        :mod:`mftik.strategy.eventlog`. Recorded at the session's dispatch points
        rather than inside the hooks, so a strategy that ignores an event
        still logs having been offered it, and nothing needs to be added to a
        strategy to be audited. Off unless ``STS_EVENTLOG_DIR`` is set.

        It is not a substitute for :meth:`log`: this records what happened,
        while :meth:`log` records what the strategy made of it, and only the
        second reaches the UI.
    """

    def __init__(self) -> None:
        self.session: SessionView | None = None
        #: Qualified registry key (``CrossArb``, ``private::Tiny``). Set in
        #: :meth:`bind` from the session. Until then, the class name — a unit
        #: test that never binds still has something to log.
        self.registry_key: str = type(self).__name__
        self.paras: dict[str, Any] = {}
        self.oms = StrategyOms()
        #: On-demand market-data reads — history the feeds do not carry.
        self.mds = StrategyMds()
        #: Whether the feeds are live, and which contracts a ``select:`` block
        #: chose. Not market data, and not ``self.mds``: that one asks a venue
        #: a question, this one asks the platform about the subscriptions this
        #: session holds.
        self.md = StrategyMd()
        #: Whether an account can trade right now. What it holds is
        #: :attr:`oms` and :attr:`ledger`.
        self.td = StrategyTd()
        #: How often a hook has blocked the strategy loop past the warning
        #: line (F15). Written by the platform, read by the status progress.
        self.hook_slow = HookSlow()
        #: Read-only balances from TD's ledger (available / free / prelock).
        self.ledger = StrategyLedger()
        #: Recorded trade history from MD, for warming up on what this session
        #: was not running for.
        self.tape = StrategyTape()
        #: Objects on this STS's disk. Bound in :meth:`bind` — an unbound
        #: strategy has no session, and must not invent a key for one.
        self.artifacts = StrategyArtifacts()
        #: Symbol plane reads, recorded like the rest of them. Bound here
        #: rather than in :meth:`bind` because it needs nothing from the
        #: session but a way back to it, and a strategy handed a session
        #: directly — as the strategy unit tests do — must still be able to
        #: read the plane.
        self._symbols = StrategySymbols()
        self._symbols.bind(self)
        self.timer = Timer()

    def bind(self, session: SessionView) -> None:
        """Attach this strategy to its session (called once by the session)."""
        if self.session is not None:
            raise RuntimeError(
                f"strategy {self.registry_key!r} already bound to session "
                f"{self.session.session_id}"
            )
        self.session = session
        qualified = getattr(session, "type", None)
        if qualified:
            self.registry_key = qualified
        self.oms.bind(self)
        self.mds.bind(self)
        self.md.bind(self)
        self.td.bind(self)
        self.ledger.bind(self)
        self.tape.bind(self)
        self.artifacts.bind(self)
        self.timer.bind(self)

    @classmethod
    def on_initialized(cls, params: Any) -> dict[str, Any]:
        """Deserialize ``strategy.yml`` ``sts.config`` into runtime paras.

        Called once before the session starts. Override per strategy class.
        """
        if params is None:
            return {}
        if not isinstance(params, dict):
            raise TypeError(
                f"{cls.__name__}.on_initialized expects a mapping, "
                f"got {type(params).__name__}"
            )
        return dict(params)

    def validate_paras(self, paras: dict[str, Any]) -> dict[str, Any]:
        """Backward-compatible alias for :meth:`on_initialized`."""
        return type(self).on_initialized(paras)

    @property
    def session_id(self) -> str | None:
        return self.session.session_id if self.session is not None else None

    @property
    def symbols(self):
        """Symbol plane reads — instrument spelling and trading filters.

        TD does not validate orders against these; rounding price and size to
        the venue's ``price_tick`` / ``qty_step`` and clearing ``min_notional``
        is the strategy's job::

            info = await self.symbols.get(
                UniversalTicker.parse("Gate_Spot_BTCUSDT")
            )
            tick = info.filter("price_tick")
            price = (price / tick).quantize(Decimal(1)) * tick
        """
        return self._symbols if self.session is not None else None

    def owns(self, client_order_id: str | int | None) -> bool:
        """Whether ``client_order_id`` was minted by this session.

        ``td.{api_id}.global`` is account-wide: every session attached to the
        same api_id sees the other sessions' order updates and fills. Filter
        with this before feeding an event into your own position tracking::

            async def on_fill(self, api_id, fill):
                if not self.owns(fill.client_order_id):
                    return
        """
        if client_order_id is None or self.session is None:
            return False
        try:
            return (
                version_of(client_order_id) == VERSION
                and session_id_of(client_order_id) == self.session.session_id
            )
        except (TypeError, ValueError):
            return False

    # --- process control ---------------------------------------------------

    async def on_start(self) -> None:
        """Called when the session starts strategy infrastructure."""

    async def on_ready(self, ready: Ready) -> None:
        """Called once, when everything the session declared is ready (F12).

        Order entry opens here: by this point every account has reconciled, so
        ``self.oms`` and ``self.ledger`` describe what is actually held, and a
        submit before this raises :class:`~mftik.strategy.errors.NotReady`.

        It fires even when market data did not all arrive. TD is a hard
        condition and a session whose accounts did not reconcile fails instead
        of reaching here; MD is a soft one, and whatever is still missing is in
        ``ready.missing_feeds`` for the strategy to judge — wait, trade the legs
        it has, or :meth:`fail`. See :class:`mftik.strategy.ready.Ready`.

        A restarted session arrives here with positions it did not open (R3):
        ``restart: on_failure`` cleans up resting orders, not exposure.
        """

    async def on_stop(self) -> None:
        """Called when the session is shutting down."""

    # --- availability (F14, §5.6) -------------------------------------------
    #
    # Connectivity, not content. A feed going down does not fail the session and
    # an account going away does not either: the platform forwards what MD and
    # TD say about themselves, and what to do about it is the strategy's.

    async def on_md_update(
        self, feed: str, state: FeedState, reason: str
    ) -> None:
        """Handle a feed becoming ``"live"`` or ``"down"``.

        Connectivity only. This hook does not carry a book, a trade, or any
        other market-data print — those stay on their own hooks.

        ``reason`` is why the feed moved: the venue dropped the socket, the
        connection worker changed incarnation, the broadcasts went silent,
        the session's own broker reconnected.

        There is no gap notification (F23). A strategy that needs to know what
        it missed records the ``down`` and works it out from the ``live`` that
        follows; on an ``all`` feed, ``event.seq`` is the other half of that.

        A composite feed is ``down`` when any of its atoms is, and ``live`` only
        when all of them are back (F19). Terminal endings are not here —
        expiry and delisting arrive as :meth:`on_feed_end`.
        """

    async def on_td_update(
        self, api_id: int, state: AccountState, reason: str
    ) -> None:
        """Handle an account becoming ``"ready"``, ``"degraded"`` or
        ``"unavailable"``.

        Availability only. Fills, rejects and order updates stay on their own
        hooks; this one does not carry them.

        ``degraded`` means orders can still be sent but confirmations will be
        late — submits are not refused, because a strategy that has to flatten
        is better served by a slow answer than by none. ``unavailable`` means
        submits and cancels are refused locally and never sent.

        An account coming back from ``unavailable`` because its worker was
        replaced arrives as :meth:`on_resync` first, then ``ready``.
        """

    # --- TD recon ----------------------------------------------------------

    async def on_recon_done(self, msg: ReconDone) -> None:
        """Handle reconciliation-complete from TD. OMS is in ``self.oms``."""

    async def on_resync(self, api_id: int, cause: str, view: OmsView) -> None:
        """Handle a book that had to be rebuilt, or a stream that may have a
        hole in it (F13).

        Only ever after :meth:`on_ready`, and only from the platform — a
        strategy does not ask for this. Two causes:

        ``"reconnect"``
            The session's own broker connection dropped. Fills and order
            updates published while it was gone were not retained.
        ``"account_reset"``
            The TD account worker changed incarnation and rebuilt its book from
            the venue.

        ``view`` is the account worker's settled ``oms.view``, read on the
        ingress, off this thread. UNKNOWN orders were chased first. Anything
        the strategy accumulated from events should be corrected against it
        rather than trusted: a chase that missed a fill will otherwise
        re-send an order for size it already has. The hook is not called
        when that read is refused or times out, so a missing call is not
        an empty book.

        TD's own reconcile after a venue reconnect does not arrive here. Its
        findings reach the strategy as ordinary order updates.
        """

    # --- private events (td.{api_id}.global) --------------------------------
    #
    # Session validates wire JSON into these models before the hook runs.

    async def on_order_update(self, api_id: int, order: Order) -> None:
        """Handle order status updates from TD."""

    async def on_fill(self, api_id: int, fill: Fill) -> None:
        """Handle fill / execution reports from TD."""

    async def on_order_reject(self, api_id: int, reject: OrderReject) -> None:
        """Handle submit rejects from TD."""

    async def on_cancel_reject(self, api_id: int, reject: CancelReject) -> None:
        """Handle cancel rejects from TD."""

    async def on_balance_update(self, api_id: int, balance: Balance) -> None:
        """Handle balance updates from TD."""

    async def on_position_update(self, api_id: int, position: Position) -> None:
        """Handle position changes from TD — contract venues only.

        The venue's own figure for one instrument, not something inferred from
        this strategy's fills: a position also moves on funding, ADL and
        liquidation, none of which arrive as a fill. Whatever this says is
        what the venue will settle on.

        ``position.qty`` is signed, and a position closing arrives as a zero
        rather than as a message that stops coming. Spot venues never call
        this — they have no positions, which is not the same as having flat
        ones.
        """

    # --- public events (md.{session_id}) -----------------------------------
    #
    # One hook per md feed topic. A session only receives what it subscribed
    # to in ``md_ids`` (``topic.UniversalTicker``). Session validates wire JSON
    # into the shared ``mftik.exchange.models`` shapes before the hook runs.

    async def on_ticker(self, ticker: Ticker) -> None:
        """Handle ticker updates from MD — 24h stats + top of book.

        Feed topic ``ticker``. On an Option an empty side is ``0`` — there is
        no size on a :class:`Ticker` to say so, and no ``last`` fallback. Do
        not average ``bid`` and ``ask`` without checking both are non-zero.
        """

    async def on_order_book(self, book: OrderBook) -> None:
        """Handle order book updates from MD.

        Feed topic ``orderbook``. Every message is a full snapshot; MD does not
        forward depth diffs, so there is no sequencing to do here.
        """

    async def on_kline(self, kline: Kline) -> None:
        """Handle candle updates from MD.

        Feed topic ``kline_{interval}`` (e.g. ``kline_1m.Paper_Spot_BTCUSDT``). The
        in-progress candle is re-pushed as it moves; only ``closed`` candles
        are final.
        """

    async def on_trade(self, trade: Trade) -> None:
        """Handle public tape updates from MD.

        Feed topic ``trade``. ``side`` is the taker's, and one message is one
        match.
        """

    async def on_agg_trade(self, trade: AggTrade) -> None:
        """Handle coalesced tape updates from MD.

        Feed topic ``aggtrade``. The same flow as :meth:`on_trade` with the
        venue's own aggregation applied: every match one aggressing order took
        at one price arrives as a single print. Same volume, far fewer
        messages, so this is the cheaper feed for anything reading price and
        size — and :attr:`~mftik.exchange.models.AggTrade.match_count` tells you
        how many resting orders the print consumed, which the raw tape only
        yields by counting.

        Not every venue has the concept. Binance publishes it; Gate does not,
        and subscribing to ``aggtrade`` there is refused at attach rather than
        silently producing nothing.
        """

    async def on_best_quote(self, quote: BestQuote) -> None:
        """Handle top-of-book updates from MD.

        Feed topic ``bestquote``. Best bid/ask with sizes, at book speed.
        """

    async def on_liquidation(self, liquidation: Liquidation) -> None:
        """Handle public forced-liquidation prints from MD.

        Feed topic ``liquidation``. Other accounts being closed out for
        insufficient margin — not a fill of ours. ``side`` is the liquidated
        position (``buy`` = long closed out), and ``price`` is the bankruptcy
        price the venue reported.

        Not every venue publishes this. Bybit does via ``allLiquidation``;
        subscribing where it is absent is refused at attach rather than
        silently producing nothing.
        """

    async def on_funding_rate(self, funding: FundingRate) -> None:
        """Handle live funding-rate updates from MD.

        Feed topic ``funding_rate``. ``rate`` is the still-moving prediction
        for the upcoming settlement — not a locked period constant, and not
        a settled history row. Positive means longs pay shorts.

        ``ts`` is the best available stamp: the venue's event time when the
        print carries one, local receive time when it does not.

        Not every venue publishes this. Perpetual books do; spot and paper
        do not, and subscribing there is refused at attach rather than
        silently producing nothing.
        """

    async def on_open_interest(self, open_interest: OpenInterest) -> None:
        """Handle live open-interest updates from MD.

        Feed topic ``open_interest``. ``qty`` is one side, in base on every
        base-denominated book — BinanceCM reports contracts instead,
        like the rest of that venue's public sizes.

        ``ts`` is the best available stamp: the venue's event time when the
        print carries one, local receive time when it does not.

        Not every venue publishes this. Bybit, OKX and GateFutures do on
        their contract books; Binance futures has no stream, and spot and
        paper have none. Subscribing there is refused at attach rather
        than silently producing nothing.
        """

    async def on_greeks(self, greeks: Greeks) -> None:
        """Handle live option greeks / IV / mark from MD.

        Feed topic ``greeks``. ``delta``, ``gamma``, ``theta`` and
        ``vega`` are always set. ``rho`` is ``None`` when the venue
        has none. IVs are decimal fractions (``0.65`` = 65%), ``None``
        for an empty side. Units are one convention across venues —
        BS, per one base, quote currency, vega per vol point, theta
        per calendar day; see :class:`~mftik.exchange.models.Greeks`.
        Open interest is not here — that is :meth:`on_open_interest`.

        Not every venue publishes this. Deribit Option does, on the
        same ticker row as bid/ask. Subscribing where it is absent is
        refused at attach rather than silently producing nothing.
        """

    async def on_feed_end(self, end: FeedEnd) -> None:
        """Handle a feed subscription that has reached a terminal outcome.

        Not a feed topic and not listed in ``md_ids``. One call per
        topic this session held or had just asked for. ``end.state``
        and ``end.code`` say which outcome; ``end.reason`` is a
        sentence. ``end.expiry`` is set only when the code is
        ``expired``.

        ``down`` means this attempt is over. ``rejected`` and
        ``expired`` mean subscribing again gets the same answer.
        Detach and an explicit unsubscribe do not arrive here.
        A feed that cannot be opened at attach fails the attach
        instead of arriving here. ``reason`` carries the venue's
        own words, including its code when it sent one.
        """
        await self.log(
            f"{end.topic} {end.state}/{end.code}: {end.reason}",
            level="warning",
        )

    async def on_universe_change(self, name: str, change: UniverseChange) -> None:
        """Handle a ``select:`` block's membership moving (F33).

        ``name`` is the ``select:`` name from ``strategy.yml`` (``btc_chain``).
        ``change.added`` and ``change.removed`` are the contracts that joined
        and left, ``change.epoch`` orders one change against the next, and
        ``change.current`` is a rolling future's front contract — a roll comes
        through here too, not through a hook of its own.

        A contract in ``added`` is already subscribed when this is called, and a
        contract in ``removed`` sends nothing after it returns, including
        anything already queued (I-SEL1). So a strategy can key its own state on
        this hook without a window where an event arrives for a contract it has
        not set up, or for one it has torn down.

        The old contract of a roll stays in the universe until it expires, and
        arrives in ``removed`` after an :meth:`on_feed_end`.
        """

    # --- query answers -----------------------------------------------------
    #
    # One hook per kind of query, each firing once per ``mds.fetch_*`` call and
    # carrying the ``query_id`` that call returned. Nothing here is subscribed
    # to and nothing arrives unasked.
    #
    # Every one of them fires on failure too, with ``ok`` False, the payload
    # empty and the reason in ``error_code`` — see
    # :mod:`mftik.protocol.query_codes`, and use ``is_retryable`` rather than the
    # band to decide whether asking again could help. A strategy holding a
    # query_id always learns its fate; a hook that only fired on success would
    # leave failure indistinguishable from delay.

    async def on_fetch_klines(self, result: MdKlinesResult) -> None:
        """Handle the answer to ``self.mds.fetch_klines(...)``.

        Distinct from :meth:`on_kline`, which is the live ``kline_{interval}``
        feed pushing the window in progress. This one carries history, as a
        batch, on ``result.klines``.

        An ``ok`` result with no candles is not a failure: the venue has no
        history that far back for this instrument.
        """

    async def on_fetch_orderbook(self, result: MdOrderBookResult) -> None:
        """Handle the answer to ``self.mds.fetch_order_book(...)``.

        ``result.book`` is a whole book, capped at the depth asked for, as it
        stood when the venue answered. Distinct from :meth:`on_order_book`,
        which pushes a new one on the venue's own schedule for as long as the
        feed is subscribed — this is the book at one moment, because that is
        what was asked for.

        ``book`` is None only on failure; an ``ok`` result with an empty side
        is a real book with nothing resting there.
        """

    async def on_fetch_bestquote(self, result: MdBestQuoteResult) -> None:
        """Handle the answer to ``self.mds.fetch_best_quote(...)``.

        ``result.quote`` carries the touch with its resting sizes. Distinct
        from :meth:`on_best_quote`, which pushes one on every change.

        ``quote`` is None when the query failed **or** when a side of a
        non-option book was empty. The second is not an error and not a quote
        either: a strategy checking whether its own price can rest has nothing
        to check against, and should ask again rather than read it as a quote
        of zero.

        Option books are one-sided too often for that, so an Option answer is
        always a quote and an empty side is ``price == qty == 0``. Read
        ``bid_qty == 0`` as "no bid" — never price off a zero side.
        """

    async def on_fetch_funding_history(self, result: MdFundingHistoryResult) -> None:
        """Handle the answer to ``self.mds.fetch_funding_history(...)``.

        Distinct from :meth:`on_funding_rate`, which is the live prediction
        for the upcoming settlement. This one carries locked history, oldest
        first, on ``result.rates``. Each row's ``ts`` is a past settlement.

        An ``ok`` result with no rows is not a failure: the venue has no
        history that far back for this instrument.
        """

    async def on_fetch_open_interest(self, result: MdOpenInterestResult) -> None:
        """Handle the answer to ``self.mds.fetch_open_interest(...)``.

        Distinct from :meth:`on_open_interest`, which pushes every change
        for as long as the feed is subscribed. This is the figure at one
        moment, because that is what was asked for.

        ``open_interest`` is None only on failure; an ``ok`` result with
        ``qty`` of zero is a real print.
        """

    # --- heavy computation (F9, §5.5) ---------------------------------------

    async def offload(
        self, func: Callable[..., Any], /, *args: Any, isolate: bool = False
    ) -> Any:
        """Run ``func(*args)`` off the strategy loop and return its result.

        For anything that would otherwise occupy the loop long enough to matter
        to the strategy itself — a fit, an inference, folding a tape. While it
        runs the loop is free, so fills, timers and a stop still get served, and
        a hook that awaits this is not counted as blocked (F15).

        ``isolate=False`` (default) uses a thread, which suits work that
        releases the GIL — numpy, torch — and needs no pickling. It cannot be
        interrupted: at teardown the await is cancelled and the thread runs on.

        ``isolate=True`` uses a child process, which suits pure-Python work, C
        extensions that hold the GIL, and anything that might exhaust memory.
        ``func`` and ``args`` have to be picklable and ``func`` has to be
        importable at module level, and the child is killable. It raises
        :class:`~mftik.strategy.errors.OffloadWorkerLost` if the child dies.

        ``func`` must not call the SDK. Pass what it needs and return what it
        produced; in thread mode it must not mutate the strategy either, which
        is still running its own loop alongside. Parallelism comes from
        ``limits.offload_threads`` / ``limits.offload_processes``.

        Raises :class:`NotImplementedError` until B5-03.
        """
        raise NotImplementedError("IF-06")

    async def offload_pool(
        self,
        *,
        init: Callable[..., Any] | None = None,
        init_args: tuple[Any, ...] = (),
        workers: int = 1,
    ) -> OffloadPool:
        """A process pool that keeps what ``init`` loaded, between calls.

        ``init(*init_args)`` runs once in each worker and its return value is
        handed to every :meth:`~mftik.strategy.offload.OffloadPool.call` as the
        first argument — so a model is loaded once in the child rather than
        pickled on every call::

            self.ml = await self.offload_pool(init=load_model, init_args=(p,))
            signal = await self.ml.call(predict, features)

        ``workers`` is how many child processes to run.

        Usable from ``on_start``, which is where a warm-up of this kind belongs.
        The workers are part of the session's process tree: counted against its
        memory at admission, killed with it, and rebuilt — ``init`` and all — if
        one is lost.

        Raises :class:`NotImplementedError` until B5-03.
        """
        raise NotImplementedError("IF-06")

    # --- helpers -----------------------------------------------------------

    def exit(self, reason: str = "strategy_exit") -> None:
        """Naturally end this strategy session (triggers ``on_stop`` via manager).

        The session lands in ``done``. Use :meth:`fail` for an end that the
        operator needs to see as a problem.
        """
        if self.session is None:
            raise RuntimeError("strategy is not bound to a session")
        self.session.request_exit(reason)

    def fail(self, reason: str) -> None:
        """End this session as ``failed``, recording ``reason``.

        Teardown is identical to :meth:`exit` — orders are not unwound for
        you, so cancel what must not outlive the session before calling this.
        ``reason`` is persisted and shown in the UI, so make it something an
        operator can act on rather than an internal code.
        """
        if self.session is None:
            raise RuntimeError("strategy is not bound to a session")
        self.session.request_exit(reason, failed=True)

    async def log(self, message: str, *, level: str = "info", **extra: Any) -> None:
        if self.session is None:
            return
        extra.pop("type", None)
        await publish_sts_log(
            self.session.broker,
            self.session.session_id,
            message,
            source=f"strategy.{self.registry_key}",
            level=level,
            type=getattr(self.session, "type", None),
            **extra,
        )
