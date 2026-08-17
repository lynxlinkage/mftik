"""Strategy-side balance mirror — read-only view of TD's ledger.

TD owns the money: it holds the venue balances and it is the only side that
can pre-lock against them. A strategy reads this to size an order; it has no
way to mutate it, which is deliberate. Two strategies sharing an ``api_id``
would otherwise each believe their own arithmetic, and the whole point of the
pre-lock is that there is one answer to "what is still spendable".

Snapshots arrive on ``td.ledger.{api_id}`` and land here via
:meth:`update`. Between a submit and the snapshot that follows it the numbers
here are one update stale, so treat :meth:`available` as a floor to plan
against rather than a guarantee — TD re-checks it before every order anyway,
and that check is the authoritative one.

Contract accounts also expose per-symbol leverage through
:meth:`ensure_leverage`: STS asks TD, TD asks the venue (or its own cache),
and both sides remember the figure so perp pre-locks can be sized as
``notional / leverage``.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING

from mftik.broker.errors import RequestTimeoutError
from mftik.exchange.models import Balance
from mftik.exchange.oms import LedgerView
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import (
    STS_ENSURE_LEVERAGE,
    EnsureLeverage,
    Envelope,
    LeverageAck,
    RejectCode,
    Topics,
)
from mftik.protocol.reject_codes import describe

from mftik_sts.eventlog import session_log

if TYPE_CHECKING:
    from mftik_sts.strategy import Strategy

logger = logging.getLogger(__name__)

ZERO = Decimal("0")

#: Venue leverage lookup can need a REST round-trip; longer than order ack.
LEVERAGE_ACK_TIMEOUT_S = 5.0


class StrategyLedger:
    """Latest :class:`LedgerView` per TD ``api_id``, plus leverage ensure."""

    def __init__(self, *, ack_timeout: float = LEVERAGE_ACK_TIMEOUT_S) -> None:
        self._strategy: Strategy | None = None
        self._ack_timeout = ack_timeout
        #: ``(api_id, universal_ticker)`` → leverage confirmed by TD.
        self._leverage: dict[tuple[int, str], Decimal] = {}
        self._last_reason: str = ""
        self._last_code: int | str = RejectCode.NONE

    def bind(self, strategy: Strategy) -> None:
        self._strategy = strategy

    @property
    def api_ids(self) -> list[int]:
        session = self._strategy.session if self._strategy is not None else None
        return list(session.td_api_ids) if session is not None else []

    @property
    def last_reject_reason(self) -> str:
        """Why the most recent :meth:`ensure_leverage` failed, or ``""``."""
        return self._last_reason

    @property
    def last_reject_code(self) -> int | str:
        """:attr:`last_reject_reason` as a code — see reject_codes."""
        return self._last_code

    async def view(self, api_id: int | None = None) -> LedgerView:
        """Read ``td.ledger.{api_id}``: asset → free / prelock / lock."""
        resolved = self._resolve(api_id)
        log = session_log(self._strategy)
        if resolved is None or self._strategy is None:
            log.record("read", "ledger.view", dir="out", resolved=False)
            return LedgerView()
        session = self._strategy.session
        if session is None:
            log.record("read", "ledger.view", dir="out", resolved=False)
            return LedgerView()
        rows = await session.broker.state_all(Topics.td_ledger(resolved))
        # Every sizing decision downstream rests on these numbers, and they are
        # TD's, read at one moment. Nothing else in the log can reconstruct
        # what the balance was when the strategy asked.
        log.record(
            "read",
            "ledger.view",
            dir="out",
            api_id=resolved,
            count=len(rows),
            payload=rows,
        )
        return LedgerView.from_rows(resolved, rows)

    async def available(self, asset: str, api_id: int | None = None) -> Decimal:
        """Spendable ``asset`` — venue-free minus TD's pre-locks."""
        return (await self.view(api_id)).available(asset)

    async def free(self, asset: str, api_id: int | None = None) -> Decimal:
        """What the venue calls free, ignoring pre-locks."""
        return (await self.view(api_id)).free(asset)

    async def prelock(self, asset: str, api_id: int | None = None) -> Decimal:
        """Committed by orders TD has sent but the venue has not confirmed."""
        return (await self.view(api_id)).prelock(asset)

    async def balances(self, api_id: int | None = None) -> dict[str, Balance]:
        return dict((await self.view(api_id)).balances)

    def leverage(
        self, ticker: UniversalTicker | str, api_id: int | None = None
    ) -> Decimal | None:
        """Cached leverage for ``ticker``, or None if never ensured on STS."""
        resolved = self._resolve(api_id)
        if resolved is None:
            return None
        return self._leverage.get((resolved, str(ticker)))

    async def ensure_leverage(
        self,
        ticker: UniversalTicker | str,
        api_id: int | None = None,
    ) -> Decimal | None:
        """Ask TD for this account's leverage on ``ticker``; cache and return it.

        Returns the positive leverage on success. ``None`` means the lookup
        failed (spot ticker, unsupported venue, timeout, attach missing) —
        :attr:`last_reject_reason` / :attr:`last_reject_code` say which.

        A local cache hit skips the RPC. TD also caches, so a second STS
        session on the same account still benefits after the first ensure.
        """
        self._last_reason = ""
        self._last_code = RejectCode.NONE
        resolved = self._resolve(api_id)
        if resolved is None:
            self._refuse(
                "api_id is required when more than one TD is attached",
                RejectCode.TD_INVALID_REQUEST,
            )
            self._record_leverage(str(ticker), None, None)
            return None
        key = str(UniversalTicker.resolve(str(ticker)))
        cached = self._leverage.get((resolved, key))
        if cached is not None:
            # Recorded even though nothing left the process. The strategy was
            # given a number and acted on it, and a reader reconstructing the
            # session cannot tell a cache hit from a call that never happened.
            self._record_leverage(key, resolved, cached, cached_hit=True)
            return cached

        session = self._require_session()
        envelope = Envelope[EnsureLeverage].wrap(
            EnsureLeverage(
                session_id=session.session_id,
                api_id=resolved,
                universal_ticker=key,
            ),
            type=STS_ENSURE_LEVERAGE,
            source=f"strategy.{session.strategy.name}",
            session_id=session.session_id,
        )
        try:
            reply = await session.broker.request(
                Topics.td_account(resolved),
                envelope,
                timeout=self._ack_timeout,
            )
        except RequestTimeoutError:
            logger.warning(
                "TD leverage ack timed out api_id=%s ticker=%s",
                resolved,
                key,
            )
            self._refuse("no ack from TD", RejectCode.TD_NO_ACK)
            self._record_leverage(key, resolved, None)
            return None

        try:
            ack = LeverageAck.model_validate(reply.payload)
        except Exception:
            logger.warning(
                "TD leverage ack unreadable api_id=%s ticker=%s payload=%r",
                resolved,
                key,
                reply.payload,
            )
            self._refuse("unreadable leverage ack", RejectCode.TD_UNREADABLE_ACK)
            self._record_leverage(key, resolved, None)
            return None

        if not ack.ok or ack.leverage is None or ack.leverage <= ZERO:
            self._refuse(
                ack.reason or "leverage unavailable",
                ack.error_code
                if ack.error_code != RejectCode.NONE
                else RejectCode.TD_LEVERAGE_UNAVAILABLE,
            )
            self._record_leverage(key, resolved, None)
            return None

        self._leverage[(resolved, key)] = ack.leverage
        self._record_leverage(key, resolved, ack.leverage)
        return ack.leverage

    def _record_leverage(
        self,
        ticker: str,
        api_id: int | None,
        leverage: Decimal | None,
        *,
        cached_hit: bool = False,
    ) -> None:
        """Log one leverage answer, however it was arrived at."""
        session_log(self._strategy).record(
            "read",
            "ledger.leverage",
            dir="out",
            api_id=api_id,
            ticker=ticker,
            leverage=leverage,
            cached=cached_hit or None,
            reason=self._last_reason or None,
            code=(
                None
                if self._last_code == RejectCode.NONE
                else self._last_code
            ),
        )

    def _refuse(self, reason: str, code: int | str) -> None:
        self._last_reason = reason
        self._last_code = code
        logger.debug(
            "StrategyLedger ensure_leverage refused [%s]: %s",
            describe(code),
            reason,
        )

    def _resolve(self, api_id: int | None) -> int | None:
        if api_id is not None:
            return api_id
        attached = self.api_ids
        return attached[0] if len(attached) == 1 else None

    def _require_session(self):
        if self._strategy is None or self._strategy.session is None:
            raise RuntimeError("strategy ledger is not bound to a session")
        return self._strategy.session
