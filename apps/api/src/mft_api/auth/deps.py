"""Reading the principal the middleware attached.

The middleware decides whether a request gets in at all. These say what a
handler wants from it — today only "the Owner's id", which is what every
``created_by`` and audit line has been getting from ``MFT_DEFAULT_USER_ID``.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, HTTPException, Request, WebSocket

from mft_api.auth.principal import ANONYMOUS, Principal


def principal_of(scope_holder: Request | WebSocket) -> Principal:
    return getattr(scope_holder.state, "principal", ANONYMOUS)


def get_principal(request: Request) -> Principal:
    return principal_of(request)


PrincipalDep = Annotated[Principal, Depends(get_principal)]


def current_user_id(request: Request) -> int:
    """The Owner's id, for rows that record who did something.

    A 401 here would mean the middleware let an anonymous request reach a
    gated route, so it is a bug rather than a login prompt — but it is still
    the safe answer, and cheaper than every handler defending itself.
    """
    principal = principal_of(request)
    if principal.user_id is None:
        raise HTTPException(status_code=401, detail="authentication required")
    return principal.user_id


#: The user id a handler records as having done something.
#:
#: Handlers write it as ``owner: OwnerId = DEFAULT_USER_ID``. Served through
#: FastAPI the dependency wins and the default is never reached; called
#: directly — which is how this suite tests routes — it stands in for the
#: request that is not there. Both halves are wanted: production reads the
#: principal, and a unit test does not have to build one.
OwnerId = Annotated[int, Depends(current_user_id)]
