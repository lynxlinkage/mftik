"""Countdown cancel-on-disconnect, one slot per venue (F37).

The account setting defaults to off and lives on the account, not in
``strategy.yml``: the account worker is shared by every session on that
account. The authority for the setting is the ``apis`` row (IF-14 adds
the column). This layer only reads the flag it was started with.

**What it is.** A dead-man's switch for the TD worker, not a "socket
down, therefore cancel". The worker refreshes a venue countdown while
the trading layer is active and orders are resting. The process dying
or stalling is what stops the refresh, and the venue cancels when the
countdown expires. An ordinary reconnect keeps refreshing, so it does
not cancel (D4).

**What it is not.** Deribit's cancel-on-disconnect (it cancels on every
reconnect) and Bybit's DCP. Those slots exist so every registered venue
resolves, and they must not call those mechanisms (D5). Deribit cannot
place orders yet; its slot makes no venue call.

Durations and refresh intervals are measured in B6-07 and written into
the adapter then. This module does not invent them, and no method here
contacts a venue.

* **D1.** Off unless the account says otherwise.
* **D2.** A countdown. Not "the socket dropped, so cancel".
* **D3.** Refresh only while the trading layer is active and orders rest.
* **D4.** An ordinary reconnect keeps refreshing, so it does not cancel.
* **D5.** Deribit COD and Bybit DCP are not this interface.
* **D6.** Drain-replace lengthens the countdown before the old process
  stops, and the new one restores it.
"""

from __future__ import annotations

from mftik_td.account._ticket import TICKET


class DeadMansSwitch:
    """One venue's countdown slot.

    ``supported`` describes the venue, not the account flag.
    The worker refreshes only when the account flag is on **and**
    ``supported`` is true. A venue that returns false is never refreshed,
    which is how Deribit's COD and Bybit's DCP stay unreachable.

    Subclasses set :attr:`venue` to the registry name and otherwise
    inherit the null methods. B6-07 fills the ones F37 names.
    """

    venue: str

    def supported(self) -> bool:
        """Whether F37 will use a countdown on this venue.

        Independent of the account's cancel-on-disconnect setting.
        """
        raise NotImplementedError(TICKET)

    async def refresh(self, *, symbols: frozenset[str]) -> None:
        """Refresh the countdown for the orders that are resting.

        Called only while the trading layer is active and the book has
        resting orders (D3). ``symbols`` is that resting set. Binance
        UM and CM key the countdown on the symbol; a venue whose
        countdown is per account ignores the set.

        Refreshing is not a cancel. Stopping the refresh, by the
        process dying, is what lets the countdown expire (D2).
        """
        raise NotImplementedError(TICKET)

    async def extend(self) -> None:
        """Lengthen the countdown before a drain-replace (F27, D6).

        How far is the adapter's, measured in B6-07. The new incarnation
        restores the normal countdown after it takes the book.
        """
        raise NotImplementedError(TICKET)

    async def stop(self) -> None:
        """Stop refreshing. Stopping is not itself a cancel (D2)."""
        raise NotImplementedError(TICKET)
