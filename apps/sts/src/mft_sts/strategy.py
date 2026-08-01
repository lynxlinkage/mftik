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
        self.oms — read OMS snapshots from ``td.oms.{api_id}``;
        submit_order mints uint64 client_order_id
        (strategy.id | ms since 2026-01-01 | seq++) / cancel by that id

    Private events from ``td.{api_id}.global`` (wired):
        on_order_update, on_fill, on_balance_update
        on_order_reject (submit fail), on_cancel_reject (cancel fail)
        submit → on_order_update | on_order_reject
        cancel → on_order_update | on_cancel_reject

    Timer tokens (wired):
        self.timer.token().register(first_ms, interval_ms, func)
        token.cancel()  — timestamps are unix ms

    Public events (stubs for later wiring):
        on_kline, on_order_book
    """

    name: str = "base"
    #: Numeric strategy id packed into uint64 ``client_order_id`` (16-bit).
    id: int = 0

    def __init__(self) -> None:
        self.session: StsSession | None = None
        self._paused = False
        self.paras: dict[str, Any] = {}
        self.oms = StrategyOms()
        self.timer = Timer()

    def bind(self, session: StsSession) -> None:
        """Attach this strategy to its session (called once by the session)."""
        if self.session is not None:
            raise RuntimeError(
                f"strategy {self.name!r} already bound to session "
                f"{self.session.session_id}"
            )
        self.session = session
        self.oms.bind(self)
        self.timer.bind(self)

    def validate_paras(self, paras: dict[str, Any]) -> dict[str, Any]:
        """Validate / normalize deploy ``st_paras``. Override per strategy."""
        return dict(paras)

    @property
    def session_id(self) -> str | None:
        return self.session.session_id if self.session is not None else None

    @property
    def paused(self) -> bool:
        return self._paused

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
            Topics.sts_session(self.session.session_id),
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

    # --- public events (not wired yet) -------------------------------------

    async def on_kline(self, msg: UntypedEnvelope) -> None:
        """Handle kline / candle updates from MD."""

    async def on_order_book(self, msg: UntypedEnvelope) -> None:
        """Handle order book updates from MD."""

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
