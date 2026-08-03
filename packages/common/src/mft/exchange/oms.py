"""Shared OMS snapshot models (TD publisher + STS strategy mirror)."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field

from mft.exchange.models import Balance, Order


class Position(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    qty: Decimal


class OmsView(BaseModel):
    """Immutable snapshot of OMS live state."""

    model_config = ConfigDict(frozen=True)

    orders: dict[str, Order] = Field(default_factory=dict)
    positions: dict[str, Position] = Field(default_factory=dict)
    balances: dict[str, Balance] = Field(default_factory=dict)


class LedgerEntry(BaseModel):
    """One asset's row in the ``td.ledger.{api_id}`` hash.

    This is the stored shape, deliberately narrower than :class:`Balance`:
    the asset is the hash field, so the value carries only the three numbers.
    ``lock`` is the venue's own hold; ``prelock`` is TD's.
    """

    model_config = ConfigDict(frozen=True)

    free: Decimal = Decimal("0")
    prelock: Decimal = Decimal("0")
    lock: Decimal = Decimal("0")

    @classmethod
    def of(cls, balance: Balance) -> LedgerEntry:
        return cls(
            free=balance.free, prelock=balance.prelock, lock=balance.locked
        )

    def to_balance(self, asset: str) -> Balance:
        return Balance(
            asset=asset, free=self.free, locked=self.lock, prelock=self.prelock
        )


class LedgerView(BaseModel):
    """Immutable snapshot of TD's balance ledger, keyed by asset.

    Read-only by construction: TD owns the ledger and publishes this; a
    strategy consults it to size an order but never writes to it.
    """

    model_config = ConfigDict(frozen=True)

    api_id: int = 0
    balances: dict[str, Balance] = Field(default_factory=dict)

    def get(self, asset: str) -> Balance | None:
        return self.balances.get(asset)

    def available(self, asset: str) -> Decimal:
        """Spendable ``asset`` — venue-free minus TD's pre-locks."""
        balance = self.balances.get(asset)
        return balance.available if balance is not None else Decimal("0")

    def free(self, asset: str) -> Decimal:
        """What the venue calls free, ignoring our reservations."""
        balance = self.balances.get(asset)
        return balance.free if balance is not None else Decimal("0")

    def prelock(self, asset: str) -> Decimal:
        balance = self.balances.get(asset)
        return balance.prelock if balance is not None else Decimal("0")

    @classmethod
    def from_rows(
        cls, api_id: int, rows: dict[str, dict[str, object]]
    ) -> LedgerView:
        """Build from a raw ``td.ledger.{api_id}`` hash read."""
        return cls(
            api_id=api_id,
            balances={
                asset: LedgerEntry.model_validate(row).to_balance(asset)
                for asset, row in rows.items()
            },
        )
