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
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from mft.exchange.models import Balance
from mft.exchange.oms import LedgerView
from mft.protocol import Topics

if TYPE_CHECKING:
    from mft_sts.strategy import Strategy

ZERO = Decimal("0")


class StrategyLedger:
    """Latest :class:`LedgerView` per TD ``api_id``."""

    def __init__(self) -> None:
        self._strategy: Strategy | None = None

    def bind(self, strategy: Strategy) -> None:
        self._strategy = strategy

    @property
    def api_ids(self) -> list[int]:
        session = self._strategy.session if self._strategy is not None else None
        return list(session.td_api_ids) if session is not None else []

    async def view(self, api_id: int | None = None) -> LedgerView:
        """Read ``td.ledger.{api_id}``: asset → free / prelock / lock."""
        resolved = self._resolve(api_id)
        if resolved is None or self._strategy is None:
            return LedgerView()
        session = self._strategy.session
        if session is None:
            return LedgerView()
        rows = await session.broker.state_all(Topics.td_ledger(resolved))
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

    def _resolve(self, api_id: int | None) -> int | None:
        if api_id is not None:
            return api_id
        attached = self.api_ids
        return attached[0] if len(attached) == 1 else None
