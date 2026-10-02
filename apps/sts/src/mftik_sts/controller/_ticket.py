"""The interface ticket the stubs name in ``NotImplementedError``."""

from __future__ import annotations

from typing import NoReturn

TICKET = "IF-04"


def unimplemented() -> NoReturn:
    """Behaviour a later ticket fills in. The message is the ticket id."""
    raise NotImplementedError(TICKET)
