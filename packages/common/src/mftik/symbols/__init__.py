"""Symbol plane access for the trading domains.

``ListedInstrument`` is imported eagerly so adapters can map venue rows onto
it. It does not import :mod:`mftik.exchange` at load time, so
``from mftik.symbols import SymbolClient`` does not cycle through the adapter
barrel. The client stays lazy for the same reason.
"""

from mftik.symbols.listed import ListedInstrument

__all__ = [
    "DEFAULT_TTL",
    "ListedInstrument",
    "SymbolClient",
    "SymbolNotFoundError",
]


def __getattr__(name: str):
    if name in {"DEFAULT_TTL", "SymbolClient", "SymbolNotFoundError"}:
        from mftik.symbols.client import (
            DEFAULT_TTL,
            SymbolClient,
            SymbolNotFoundError,
        )

        return {
            "DEFAULT_TTL": DEFAULT_TTL,
            "SymbolClient": SymbolClient,
            "SymbolNotFoundError": SymbolNotFoundError,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
