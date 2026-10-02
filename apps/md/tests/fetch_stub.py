"""Reader factory for the fetch-worker roll test.

The worker is a real process. This factory is what it builds instead of
a venue client, so the test never opens a connection to an exchange.
``python -m mftik_md.fetch fetch_stub:build`` is the only way in.
"""

from __future__ import annotations

from decimal import Decimal

from mftik.exchange.models import Kline
from mftik.exchange.tickers import UniversalTicker

_CLOSE = Decimal("1.5")


class _Reader:
    def __init__(self) -> None:
        self.connects = 0

    async def connect(self) -> None:
        self.connects += 1

    async def close(self) -> None:
        return None

    async def fetch_klines(
        self, ticker: UniversalTicker, interval: str, *, limit: int
    ) -> list[Kline]:
        return [
            Kline(
                universal_ticker=str(ticker),
                interval=interval,
                open_time=1_700_000_000,
                open=_CLOSE,
                high=_CLOSE,
                low=_CLOSE,
                close=_CLOSE,
                volume=Decimal("1"),
                closed=True,
            )
        ]


class _Factory:
    async def create(self, venue: str) -> _Reader:
        del venue
        return _Reader()


def build() -> _Factory:
    return _Factory()
