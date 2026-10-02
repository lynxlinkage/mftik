"""Re-export of the trading book.

B4-05 moved the class into the trading layer
(:mod:`mftik_td.account.session`). This module stays so the factory, the
settled-view helper, the OMS type check, and the tests that already
import :class:`Session` from here keep working.
"""

from mftik_td.account.session import (
    PENDING_NEW_TIMEOUT_S,
    Session,
    TradingConnector,
)

__all__ = [
    "PENDING_NEW_TIMEOUT_S",
    "Session",
    "TradingConnector",
]
