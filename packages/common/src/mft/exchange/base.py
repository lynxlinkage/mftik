from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class ExchangeAdapter(ABC):
    """Venue connectivity interface used by md (public) and td (private)."""

    name: str = "base"

    @abstractmethod
    async def connect(self) -> None:
        """Establish REST/WS connections."""

    @abstractmethod
    async def close(self) -> None:
        """Tear down connections."""

    async def subscribe_ticker(self, symbol: str) -> None:
        raise NotImplementedError

    async def place_order(self, **kwargs: Any) -> dict[str, Any]:
        raise NotImplementedError
