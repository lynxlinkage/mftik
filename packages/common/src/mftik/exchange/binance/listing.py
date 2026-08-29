"""Shared Binance listing helpers — filter list keyed by ``filterType``."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from mftik.symbols.listed import listing_decimal


def filters_by_type(filters: Any) -> dict[str, dict[str, Any]]:
    """Binance's filter list, keyed by ``filterType``."""
    return {
        str(f.get("filterType", "")): f
        for f in filters or []
        if isinstance(f, dict)
    }


def bound(value: Any) -> Decimal | None:
    return listing_decimal(value)
