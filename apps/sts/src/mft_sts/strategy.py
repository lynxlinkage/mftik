"""Base strategy — one instance per STS session."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

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
    STS_RECON,
    CancelReject,
    Envelope,
    MdKlinesResult,
    OrderReject,
    Recon,
    ReconDone,
    Topics,
    publish_sts_log,
)

from mft_sts.client_order_id import slot_of
from mft_sts.ledger import StrategyLedger
from mft_sts.mds import StrategyMds
from mft_sts.oms import StrategyOms
from mft_sts.timer import Timer

if TYPE_CHECKING:
    from mft_sts.session.session import StsSession


class Strategy:
    """Base class for STS strategy implementations.

    Session ↔ Strategy is 1-1. Override hooks as needed.

    Process control (wired):
        on_start, on_ready, on_stop, on_pause, on_resume
        exit() — natural end → session stop → on_stop → status "done"
        fail(reason) — same teardown, but status "failed" and reason is
        persisted for the UI
        remember(key, value) — keep a fact a rebuilt session could not
        re-derive; handed back to on_rebuild (not wired yet)

    TD recon (wired):
        send_recon (auto on first lease ACK), on_recon_done
        self.oms — read OMS snapshots from ``td.oms.{api_id}``
        self.ledger — read balances from ``td.ledger.{api_id}``; TD owns
        them, so this is a view: available() is free minus TD's pre-locks

    Order entry — request-reply on ``td.order.{api_id}`` (wired):
        submit_order / cancel_order return True once TD acks the request.
        False means it never reached the venue (no ack, or TD refused it).
        A True says nothing about the venue's answer — that arrives below.
        submit_order mints the uint64 client_order_id
        (session cid_slot | ms since 2026-01-01 | seq++) and leaves it in
        oms.last_client_order_id; cancel_order takes that id.

    Private events from ``td.{api_id}.global`` (wired):
        on_order_update, on_fill, on_balance_update
        on_order_reject (submit fail), on_cancel_reject (cancel fail)
        submit → on_order_update | on_order_reject
        cancel → on_order_update | on_cancel_reject
        NOTE: account-wide fan-out — other sessions on the same api_id show
        up here too. Filter with ``self.owns(cid)``.

    Symbol plane (wired):
        self.symbols — exch_ticker / filters per (venue, symbol)

    Timer tokens (wired):
        self.timer.token().register(first_ms, interval_ms, func)
        token.cancel()  — timestamps are unix ms

    Public events from ``md.{session_id}`` (wired):
        on_ticker, on_order_book, on_kline, on_trade, on_best_quote
        One hook per feed topic subscribed in ``md_ids``
        (``venue.topic.symbol``; kline carries its interval in the topic,
        e.g. ``paper.kline_1m.BTCUSDT``).

    Market-data queries — request-reply on ``md.fetch`` (wired):
        self.mds.fetch_klines(venue, symbol, interval, limit=...) returns a
        query_id once MD acks the query, or None if it never left (refused, or
        no MD running — mds.last_reject_reason says which). The candles arrive
        later at on_fetch_klines, carrying that query_id.
        Independent of md_ids: a session that subscribes to nothing can still
        query, and any venue is reachable whether or not it is streaming.
        NOTE: on_fetch_klines is not on_kline. The feed pushes the window in
        progress; the query returns history, in a batch, only when asked.
        ``interval`` is canonical (``1m``, ``4h``, ``1mo``) — see
        ``mft.exchange.intervals``; the month is ``1mo``, never ``1M``.
    """

    name: str = "base"
    #: Whether a session running this strategy may be restored after STS
    #: restarts. Off until the class implements :meth:`on_rebuild`: a rebuilt
    #: strategy that does not know it was ever away treats recon as a clean
    #: account and starts over, placing orders alongside the ones it left
    #: resting at the venue. Readiness is a property of the strategy, not of
    #: whoever set the environment variable.
    rebuildable: bool = False
    #: Numeric id for this strategy *class*. Not packed into client_order_id —
    #: that carries the per-session slot; see :mod:`mft_sts.client_order_id`.
    id: int = 0

    def __init__(self) -> None:
        self.session: StsSession | None = None
        self._paused = False
        self.paras: dict[str, Any] = {}
        self.oms = StrategyOms()
        #: On-demand market-data reads — history the feeds do not carry.
        self.mds = StrategyMds()
        #: Read-only balances from TD's ledger (available / free / prelock).
        self.ledger = StrategyLedger()
        self.timer = Timer()

    def bind(self, session: StsSession) -> None:
        """Attach this strategy to its session (called once by the session)."""
        if self.session is not None:
            raise RuntimeError(
                f"strategy {self.name!r} already bound to session "
                f"{self.session.session_id}"
            )
        self.session = session
        self.oms.bind(self, cid_slot=session.cid_slot)
        self.mds.bind(self)
        self.ledger.bind(self)
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
    def paused(self) -> bool:
        return self._paused

    @property
    def symbols(self):
        """Symbol plane reads — instrument spelling and trading filters.

        TD does not validate orders against these; rounding price and size to
        the venue's ``price_tick`` / ``qty_step`` and clearing ``min_notional``
        is the strategy's job::

            info = await self.symbols.get("gate_spot", "BTCUSDT")
            tick = info.filter("price_tick")
            price = (price / tick).quantize(Decimal(1)) * tick
        """
        return self.session.symbols if self.session is not None else None

    @property
    def cid_slot(self) -> int | None:
        """This session's 16-bit ``client_order_id`` slot."""
        return self.session.cid_slot if self.session is not None else None

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
            return slot_of(client_order_id) == self.session.cid_slot
        except (TypeError, ValueError):
            return False

    # --- process control ---------------------------------------------------

    async def on_start(self) -> None:
        """Called when the session starts strategy infrastructure."""

    async def on_ready(self) -> None:
        """Called after start when the session is ready to run."""

    async def on_stop(self) -> None:
        """Called when the session is shutting down."""

    async def on_rebuild(self, remembered: dict[str, str]) -> None:
        """Called when this session ran before and is being restored.

        Not wired to anything yet — rebuilding sessions is not implemented.
        The contract it will have:

        Recon follows as usual, and what it brings is *yours*: the orders it
        reports are the ones this session placed before the restart, not
        another session's. ``remembered`` carries whatever was written with
        :meth:`remember`.

        Restore from those two, not from a saved copy of your own attributes.
        Anything the venue can tell you (resting orders, fills, position) must
        come from recon, because between the restart and now an order can have
        filled, been cancelled or been rejected, and a stale copy would have
        you act on something that is no longer true.
        """

    async def on_pause(self) -> None:
        """Called when the strategy is paused."""
        self._paused = True

    async def on_resume(self) -> None:
        """Called when the strategy resumes from pause."""
        self._paused = False

    # --- TD recon ----------------------------------------------------------

    async def send_recon(self, api_id: int) -> None:
        """Ask TD to reconcile OMS for ``api_id`` (open orders / pos / balances)."""
        if self.session is None:
            raise RuntimeError("strategy is not bound to a session")
        await self.session.broker.publish(
            Topics.sts_td_session(self.session.session_id),
            Envelope[Recon].wrap(
                Recon(session_id=self.session.session_id, api_id=api_id),
                type=STS_RECON,
                source=f"strategy.{self.name}",
                session_id=self.session.session_id,
            ),
        )

    async def on_recon_done(self, msg: ReconDone) -> None:
        """Handle reconciliation-complete from TD. OMS is in ``self.oms``."""

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

    # --- public events (md.{session_id}) -----------------------------------
    #
    # One hook per md feed topic. A session only receives what it subscribed
    # to in ``md_ids`` (``venue.topic.symbol``). Session validates wire JSON
    # into the shared ``mft.exchange.models`` shapes before the hook runs.

    async def on_ticker(self, ticker: Ticker) -> None:
        """Handle ticker updates from MD — 24h stats + top of book.

        Feed topic ``ticker``.
        """

    async def on_order_book(self, book: OrderBook) -> None:
        """Handle order book updates from MD.

        Feed topic ``orderbook``. Every message is a full snapshot; MD does not
        forward depth diffs, so there is no sequencing to do here.
        """

    async def on_kline(self, kline: Kline) -> None:
        """Handle candle updates from MD.

        Feed topic ``kline_{interval}`` (e.g. ``paper.kline_1m.BTCUSDT``). The
        in-progress candle is re-pushed as it moves; only ``closed`` candles
        are final.
        """

    async def on_trade(self, trade: Trade) -> None:
        """Handle public tape updates from MD.

        Feed topic ``trade``. ``side`` is the taker's.
        """

    async def on_best_quote(self, quote: BestQuote) -> None:
        """Handle top-of-book updates from MD.

        Feed topic ``bestquote``. Best bid/ask with sizes, at book speed.
        """

    async def on_fetch_klines(self, result: MdKlinesResult) -> None:
        """Handle the answer to a ``self.mds.fetch_klines(...)`` query.

        Not a feed: this fires once per query, carrying the ``query_id`` that
        :meth:`~mft_sts.mds.StrategyMds.fetch_klines` returned. Nothing here is
        subscribed to and nothing arrives unasked.

        Distinct from :meth:`on_kline`, which is the live ``kline_{interval}``
        feed pushing the window in progress. This one carries history, as a
        batch, on ``result.klines``.

        It fires on failure too, with ``result.ok`` False, ``klines`` empty and
        the reason in ``result.error_code`` — see
        :mod:`mft.protocol.query_codes`, and use ``is_retryable`` rather than
        the band to decide whether asking again could help. An ``ok`` result
        with no candles is not a failure: the venue has no history that far
        back for this instrument.
        """

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

    async def remember(self, key: str, value: str) -> None:
        """Persist one fact so a rebuilt session can have it back.

        For what a restart destroys and nothing else can supply — the price a
        chase anchored its slippage guard on, the moment a clock started.
        Configuration is already kept (``st_paras``) and anything the venue
        knows comes back through recon; this is for neither.

        Write at the moment the fact becomes true, not on the way out: a
        process that is killed outright runs no shutdown code, and that is
        exactly the case a rebuild exists for. Facts written here are expected
        to be established once and never change — re-``remember``ing a moving
        value stores something that will be wrong by the time it is read.
        """
        if self.session is None:
            raise RuntimeError("strategy is not bound to a session")
        await self.session.remember(key, value)

    async def log(self, message: str, *, level: str = "info", **extra: Any) -> None:
        if self.session is None:
            return
        await publish_sts_log(
            self.session.broker,
            self.session.session_id,
            message,
            source=f"strategy.{self.name}",
            level=level,
            **extra,
        )
