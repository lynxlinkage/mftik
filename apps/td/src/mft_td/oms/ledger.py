"""TD's balance ledger — venue balances plus local pre-locks.

The venue is the authority on how much money exists, but it only learns about
an order once the order arrives. Between "strategy asked" and "venue answered"
there is a window where the funds are spoken for and nothing on the venue side
says so. Two orders issued inside that window would each see the full balance
and together overspend it.

The ledger closes that window: a submit reserves (``prelock``) before the order
leaves TD, and the reservation is released when the order reaches a state where
the venue is accounting for it — or reaches one where it never will.

Reservations are keyed by ``client_order_id`` so release is idempotent: a
release for an id we do not hold is a no-op, which is what lets the fill path,
the reject path and the recon path all call it without coordinating.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING

from mft.exchange.models import Balance, OrderType, Side
from mft.exchange.tickers import Category

if TYPE_CHECKING:
    from mft.exchange.models import Instrument, PlaceOrderRequest

logger = logging.getLogger(__name__)

ZERO = Decimal("0")
ONE = Decimal("1")


class InsufficientAvailable(Exception):
    """A reservation was refused because the asset has too little available."""

    def __init__(self, asset: str, wanted: Decimal, available: Decimal) -> None:
        self.asset = asset
        self.wanted = wanted
        self.available = available
        super().__init__(
            f"{asset}: need {wanted}, available {available}"
        )


def reservation_for(
    request: PlaceOrderRequest,
    instrument: Instrument,
    *,
    leverage: Decimal | None = None,
) -> tuple[str, Decimal] | None:
    """What an order commits: ``(asset, amount)``, or None if unknowable.

    Spot: a buy commits quote currency, a sell commits base. A market buy has
    no price to size the commitment with, so it returns None — the caller
    decides whether to let it through unreserved rather than having a guess
    baked in here.

    Perp: both sides commit margin in the quote (settle) asset. The amount is
    ``notional / leverage``. Missing or non-positive ``leverage`` is treated
    as ``1`` — conservative until :meth:`Session.ensure_leverage` has filled
    the cache.
    """
    if request.category is Category.PERP:
        if request.type is OrderType.MARKET or request.price is None:
            return None
        lev = leverage if leverage is not None and leverage > ZERO else ONE
        return instrument.quote, (request.qty * request.price) / lev
    if request.side is Side.SELL:
        return instrument.base, request.qty
    if request.type is OrderType.MARKET or request.price is None:
        return None
    return instrument.quote, request.qty * request.price


class Ledger:
    """Per-account balances with local reservations layered on top.

    Not thread-safe and not async: every mutation is a plain dict update, so
    callers get atomicity for free as long as they do not await mid-update.
    """

    def __init__(self) -> None:
        #: asset → what the venue last told us
        self._venue: dict[str, Balance] = {}
        #: asset → total currently reserved locally
        self._prelocked: dict[str, Decimal] = {}
        #: client_order_id → (asset, amount) so release needs no other context
        self._by_cid: dict[str, tuple[str, Decimal]] = {}

    # --- reads -------------------------------------------------------------

    def balance(self, asset: str) -> Balance:
        """The asset's balance with our reservation folded in."""
        venue = self._venue.get(asset)
        prelock = self._prelocked.get(asset, ZERO)
        if venue is None:
            # Reserved against an asset the venue has not reported yet.
            return Balance(asset=asset, free=ZERO, locked=ZERO, prelock=prelock)
        return venue.model_copy(update={"prelock": prelock})

    def snapshot(self) -> dict[str, Balance]:
        assets = set(self._venue) | set(self._prelocked)
        return {asset: self.balance(asset) for asset in sorted(assets)}

    def available(self, asset: str) -> Decimal:
        return self.balance(asset).available

    @property
    def reserved_cids(self) -> list[str]:
        return list(self._by_cid)

    # --- writes ------------------------------------------------------------

    def apply_venue(self, balance: Balance) -> None:
        """Record what the venue says. Reservations are untouched.

        The venue's ``prelock`` field is ignored on purpose — it is ours to
        know, and a venue payload carrying one would be meaningless.
        """
        self._venue[balance.asset] = balance.model_copy(
            update={"prelock": ZERO}
        )

    def apply_venue_many(self, balances: list[Balance]) -> None:
        for balance in balances:
            self.apply_venue(balance)

    def reserve(
        self, client_order_id: str, asset: str, amount: Decimal
    ) -> None:
        """Hold ``amount`` of ``asset`` against ``client_order_id``.

        Raises :class:`InsufficientAvailable` and changes nothing if the
        account cannot cover it, so a refused reservation cannot leave a
        partial hold behind.
        """
        if amount <= ZERO:
            return
        if client_order_id in self._by_cid:
            raise ValueError(
                f"client_order_id {client_order_id!r} already holds a reservation"
            )
        available = self.available(asset)
        if amount > available:
            raise InsufficientAvailable(asset, amount, available)
        self._prelocked[asset] = self._prelocked.get(asset, ZERO) + amount
        self._by_cid[client_order_id] = (asset, amount)
        logger.debug(
            "ledger reserve cid=%s %s %s (available now %s)",
            client_order_id,
            amount,
            asset,
            self.available(asset),
        )

    def release(self, client_order_id: str) -> bool:
        """Drop the reservation for ``client_order_id``. True if one existed."""
        held = self._by_cid.pop(client_order_id, None)
        if held is None:
            return False
        asset, amount = held
        remaining = self._prelocked.get(asset, ZERO) - amount
        if remaining > ZERO:
            self._prelocked[asset] = remaining
        else:
            self._prelocked.pop(asset, None)
        logger.debug(
            "ledger release cid=%s %s %s (available now %s)",
            client_order_id,
            amount,
            asset,
            self.available(asset),
        )
        return True

    def release_all(self) -> None:
        """Drop every reservation — used when the account is torn down."""
        self._by_cid.clear()
        self._prelocked.clear()

    def has_reservation(self, client_order_id: str) -> bool:
        return client_order_id in self._by_cid

    def asset_for(self, client_order_id: str) -> str | None:
        """Which asset a reservation holds, for callers that must persist it."""
        held = self._by_cid.get(client_order_id)
        return None if held is None else held[0]
