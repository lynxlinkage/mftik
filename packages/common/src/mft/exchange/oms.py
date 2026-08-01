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
