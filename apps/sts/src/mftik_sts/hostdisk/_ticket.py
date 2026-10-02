"""The interface ticket the host-disk stubs name in ``NotImplementedError``."""

from __future__ import annotations

from typing import NoReturn

TICKET = "IF-16"


def unimplemented() -> NoReturn:
    """Behaviour B5-10 or B5-11 fills in. The message is the ticket id."""
    raise NotImplementedError(TICKET)
