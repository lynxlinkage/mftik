"""Symbol plane access for the trading domains.

``ListedInstrument`` is imported eagerly: adapters map venue rows onto it
during ``mftik.exchange`` import, which is still initializing the broker.
The client stays lazy so that cycle does not close.
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
