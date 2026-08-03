"""Base strategy — one instance per STS session."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from mft.protocol import (
    STS_RECON,
    Envelope,
    Recon,
    ReconDone,
    Topics,
    UntypedEnvelope,
    publish_sts_log,
)

from mft_sts.client_order_id import slot_of
from mft_sts.ledger import StrategyLedger
from mft_sts.oms import StrategyOms
from mft_sts.timer import Timer

if TYPE_CHECKING:
    from mft_sts.session.session import StsSession


class Strategy:
    """Base class for STS strategy implementations.

    Session ↔ Strategy is 1-1. Override hooks as needed.

    Process control (wired):
        on_start, on_ready, on_stop, on_pause, on_resume
        exit() — natural end → session stop → on_stop

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
    """

    name: str = "base"
    #: Numeric id for this strategy *class*. Not packed into client_order_id —
    #: that carries the per-session slot; see :mod:`mft_sts.client_order_id`.
    id: int = 0

    def __init__(self) -> None:
        self.session: StsSession | None = None
        self._paused = False
        self.paras: dict[str, Any] = {}
        self.oms = StrategyOms()
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

            async def on_fill(self, api_id, msg):
                if not self.owns(msg.payload.get("client_order_id")):
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

    async def on_order_update(self, api_id: int, msg: UntypedEnvelope) -> None:
        """Handle order status updates from TD."""

    async def on_fill(self, api_id: int, msg: UntypedEnvelope) -> None:
        """Handle fill / execution reports from TD."""

    async def on_order_reject(self, api_id: int, msg: UntypedEnvelope) -> None:
        """Handle submit rejects from TD."""

    async def on_cancel_reject(self, api_id: int, msg: UntypedEnvelope) -> None:
        """Handle cancel rejects from TD."""

    async def on_balance_update(self, api_id: int, msg: UntypedEnvelope) -> None:
        """Handle balance updates from TD."""

    # --- public events (md.{session_id}) -----------------------------------
    #
    # One hook per md feed topic. A session only receives what it subscribed
    # to in ``md_ids`` (``venue.topic.symbol``), and every hook is fed from the
    # same ``md.{session_id}`` stream, so the payloads are the shared
    # ``mft.exchange.models`` shapes — parse with ``Model.model_validate``.

    async def on_ticker(self, msg: UntypedEnvelope) -> None:
        """Handle ticker updates from MD — ``Ticker`` (24h stats + top of book).

        Feed topic ``ticker``.
        """

    async def on_order_book(self, msg: UntypedEnvelope) -> None:
        """Handle order book updates from MD — ``OrderBook``.

        Feed topic ``orderbook``. Every message is a full snapshot; MD does not
        forward depth diffs, so there is no sequencing to do here.
        """

    async def on_kline(self, msg: UntypedEnvelope) -> None:
        """Handle candle updates from MD — ``Kline``.

        Feed topic ``kline_{interval}`` (e.g. ``paper.kline_1m.BTCUSDT``). The
        in-progress candle is re-pushed as it moves; only ``closed`` candles
        are final.
        """

    async def on_trade(self, msg: UntypedEnvelope) -> None:
        """Handle public tape updates from MD — ``Trade``.

        Feed topic ``trade``. ``side`` is the taker's.
        """

    async def on_best_quote(self, msg: UntypedEnvelope) -> None:
        """Handle top-of-book updates from MD — ``BestQuote``.

        Feed topic ``bestquote``. Best bid/ask with sizes, at book speed.
        """

    # --- helpers -----------------------------------------------------------

    def exit(self, reason: str = "strategy_exit") -> None:
        """Naturally end this strategy session (triggers ``on_stop`` via manager)."""
        if self.session is None:
            raise RuntimeError("strategy is not bound to a session")
        self.session.request_exit(reason)

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
