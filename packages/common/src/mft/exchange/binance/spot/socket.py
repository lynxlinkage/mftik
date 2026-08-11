"""The spot sockets' connection machinery, which is the venue-wide one.

Kept as a name here because both spot classes are built on it and because it
used to live in this module; the implementation is in
:mod:`mft.exchange.binance.socket`, shared with the futures adapter.
"""

from __future__ import annotations

from mft.exchange.binance.socket import BinanceSocket

__all__ = ["BinanceSocket"]
