"""Strategy-side symbol reads — the same plane, recorded.

:class:`~mftik.symbols.SymbolClient` is shared by every domain, so the session
event log has no business inside it. This wraps it for the one caller that
needs its answers recorded: a strategy rounds its own prices and sizes against
what comes back here, which makes a tick size as much an input to an order as
the price that prompted it.

Recorded per call rather than per fetch. The client caches with a TTL, so the
wire sees a fraction of what the strategy asks for — and a reader reconstructing
the session needs what the strategy was told, not what happened to be a cache
miss at the time.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any

from mftik.exchange.tickers import Category, UniversalTicker
from mftik.protocol import SymbolInfo
from mftik.symbols import SymbolClient

from mftik_sts.eventlog import session_log

if TYPE_CHECKING:
    from mftik_sts.strategy import Strategy


class StrategySymbols:
    """Recording view of the symbol plane, bound to one strategy."""

    def __init__(self) -> None:
        self._strategy: Strategy | None = None

    def bind(self, strategy: Strategy) -> None:
        self._strategy = strategy

    @property
    def client(self) -> SymbolClient:
        """The underlying shared client."""
        session = self._strategy.session if self._strategy is not None else None
        if session is None:
            raise RuntimeError("strategy symbols is not bound to a session")
        return session.symbols

    def __getattr__(self, name: str) -> Any:
        """Anything not wrapped here still reaches the client, unrecorded.

        A read this class has not been taught about should not be a read a
        strategy cannot make. The cost is that it goes unlogged, which is why
        every method the strategies actually use is spelled out below.
        """
        return getattr(self.client, name)

    async def get(self, ticker: UniversalTicker | str) -> SymbolInfo:
        """One instrument, or :class:`SymbolNotFoundError`."""
        resolved = UniversalTicker.resolve(str(ticker))
        try:
            info = await self.client.get(resolved)
        except Exception as exc:
            # A missing instrument is an answer the strategy acts on — usually
            # by failing the session — so it belongs in the log as much as a
            # successful lookup does.
            self._record("symbols.get", ticker=str(resolved), error=repr(exc))
            raise
        self._record("symbols.get", ticker=str(resolved), payload=info)
        return info

    async def list(
        self, venue: str, *, category: Category | str = Category.SPOT
    ) -> list[SymbolInfo]:
        """Every instrument on one venue and category."""
        infos = await self.client.list(venue, category=category)
        # The count, not the table. A venue listing is thousands of rows that
        # say nothing about this session, and a strategy branching on one of
        # them reaches it through ``get`` — which does record.
        self._record(
            "symbols.list",
            venue=venue,
            category=str(Category(category).value),
            count=len(infos),
        )
        return infos

    async def exch_ticker(self, ticker: UniversalTicker | str) -> str:
        """Universal → the venue's spelling."""
        resolved = UniversalTicker.resolve(str(ticker))
        spelling = await self.client.exch_ticker(resolved)
        self._record(
            "symbols.exch_ticker", ticker=str(resolved), payload=spelling
        )
        return spelling

    async def symbol_for(
        self, venue: str, exch_ticker: str, *, category: Category | str
    ) -> UniversalTicker:
        """The venue's spelling → the universal ticker."""
        try:
            found = await self.client.symbol_for(
                venue, exch_ticker, category=category
            )
        except Exception as exc:
            self._record(
                "symbols.symbol_for",
                venue=venue,
                exch_ticker=exch_ticker,
                error=repr(exc),
            )
            raise
        self._record(
            "symbols.symbol_for",
            venue=venue,
            exch_ticker=exch_ticker,
            payload=str(found),
        )
        return found

    async def filter(
        self, ticker: UniversalTicker | str, name: str
    ) -> Decimal | None:
        """One trading restriction, e.g. ``price_tick``."""
        resolved = UniversalTicker.resolve(str(ticker))
        value = await self.client.filter(resolved, name)
        self._record(
            "symbols.filter",
            ticker=str(resolved),
            filter=name,
            payload=value,
        )
        return value

    async def venues(self) -> Any:
        result = await self.client.venues()
        self._record("symbols.venues", payload=result)
        return result

    async def refresh(self, venue: str | None = None) -> dict[str, object]:
        """Ask the plane to re-pull. An action, not a read."""
        result = await self.client.refresh(venue)
        self._record("symbols.refresh", venue=venue, payload=result)
        return dict(result)

    def _record(self, event: str, **fields: Any) -> None:
        session_log(self._strategy).record("read", event, dir="out", **fields)
