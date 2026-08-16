"""``/auth/*`` — claim the instance, log in, log out, say who you are."""

from __future__ import annotations

import time
from typing import Literal

from fastapi import APIRouter, HTTPException, Request, Response
from mft_db.models.user import User
from mft_db.repositories import UserRepository
from mft_db.session import session_scope
from pydantic import BaseModel, Field

from mft_api.audit_util import record_audit
from mft_api.auth import passwords, sessions
from mft_api.auth.deps import PrincipalDep
from mft_api.auth.middleware import auth_enabled

router = APIRouter(prefix="/auth", tags=["auth"])

#: Providers this instance can prove an identity with. OAuth joins the list
#: when it is wired; the UI reads this rather than guessing from a build flag.
_PROVIDERS = ("password",)

#: Failed attempts one address gets per window before it waits. Generous
#: enough that a mistyped password is never a lockout, small enough that a
#: single-owner box on the public internet is not worth grinding at.
_MAX_FAILURES = 10
_WINDOW_SECONDS = 300.0

#: address → (window start, failures). In-process because there is one API
#: process per instance and this survives exactly as long as it needs to; a
#: shared store would be machinery for a limit that only has to stop a script.
_failures: dict[str, tuple[float, int]] = {}


class StatusOut(BaseModel):
    #: Whether the gate is on at all (``MFT_AUTH_ENABLED``). Off, every
    #: request is already the Owner, and the UI has no business offering to
    #: sign anybody in or out — the answer to both would be a no-op.
    enabled: bool
    #: True while the Owner has no password — either no user row at all, or
    #: the one ``seed`` creates so foreign keys resolve. Both are "run setup".
    setup_required: bool
    providers: list[str]
    authenticated: bool
    username: str | None = None


class MeOut(BaseModel):
    user_id: int
    username: str | None
    display_name: str
    email: str | None
    #: Which proof this request arrived with.
    via: str


class SetupBody(BaseModel):
    username: str = Field(min_length=1, max_length=64)
    password: str = Field(min_length=passwords.MIN_LENGTH)


class LoginBody(BaseModel):
    username: str
    password: str


class LogoutOut(BaseModel):
    status: Literal["ok"] = "ok"


def _client(request: Request) -> str | None:
    return request.client.host if request.client else None


def _throttle(request: Request) -> None:
    address = _client(request) or "unknown"
    start, count = _failures.get(address, (0.0, 0))
    now = time.monotonic()
    if now - start >= _WINDOW_SECONDS:
        return
    if count >= _MAX_FAILURES:
        raise HTTPException(
            status_code=429,
            detail="too many failed attempts; try again later",
            headers={"Retry-After": str(int(_WINDOW_SECONDS - (now - start)))},
        )


def _record_failure(request: Request) -> None:
    address = _client(request) or "unknown"
    start, count = _failures.get(address, (0.0, 0))
    now = time.monotonic()
    if now - start >= _WINDOW_SECONDS:
        _failures[address] = (now, 1)
    else:
        _failures[address] = (start, count + 1)


def _clear_failures(request: Request) -> None:
    _failures.pop(_client(request) or "unknown", None)


async def _start_session(
    request: Request, response: Response, *, user: User, via: str
) -> None:
    async with session_scope() as db:
        token = await sessions.issue(
            db,
            user_id=user.id,
            via=via,
            user_agent=request.headers.get("user-agent"),
            ip=_client(request),
        )
    sessions.set_cookie(response, token)


@router.get("/status", response_model=StatusOut)
async def status(principal: PrincipalDep) -> StatusOut:
    """What the login screen needs before anyone has proved anything."""
    async with session_scope() as db:
        owner = await UserRepository(db).get_owner()
    return StatusOut(
        enabled=auth_enabled(),
        setup_required=owner is None or owner.password_hash is None,
        providers=list(_PROVIDERS),
        authenticated=principal.authenticated,
        username=owner.username if owner is not None else None,
    )


@router.post("/setup", response_model=MeOut, status_code=201)
async def setup(body: SetupBody, request: Request, response: Response) -> MeOut:
    """Claim the instance.

    Gated on the Owner having no password rather than on ``users`` being
    empty. ``seed`` creates a passwordless Owner before the API starts, so
    "no rows" is false on a stack that has never been logged into, and a 409
    there would lock you out of something you just built. When that row
    exists this fills it in; it never inserts a second Owner.
    """
    _throttle(request)
    username = body.username.strip()
    if not username:
        raise HTTPException(status_code=400, detail="username is required")

    async with session_scope() as db:
        users = UserRepository(db)
        owner = await users.get_owner()
        if owner is not None and owner.password_hash is not None:
            _record_failure(request)
            raise HTTPException(
                status_code=409, detail="this instance already has an owner"
            )
        if owner is None:
            owner = await users.add(
                User(
                    username=username,
                    password_hash=passwords.hash_password(body.password),
                    display_name=username,
                )
            )
        else:
            owner.username = username
            owner.password_hash = passwords.hash_password(body.password)
            if not owner.display_name:
                owner.display_name = username
        user_id = owner.id
        out = MeOut(
            user_id=user_id,
            username=owner.username,
            display_name=owner.display_name,
            email=owner.email,
            via="password",
        )
        user = owner

    await _start_session(request, response, user=user, via="password")
    _clear_failures(request)
    await record_audit(
        user_id=user_id, operation="auth.setup", result=f"username={username}"
    )
    return out


@router.post("/login/password", response_model=MeOut)
async def login_password(
    body: LoginBody, request: Request, response: Response
) -> MeOut:
    _throttle(request)
    async with session_scope() as db:
        owner = await UserRepository(db).get_by_username(body.username.strip())
        # Verify even when there is no such user, so a wrong username and a
        # wrong password cost the same and neither confirms the other.
        ok = passwords.verify_password(
            owner.password_hash if owner is not None else None, body.password
        )
        if not ok or owner is None:
            _record_failure(request)
            if owner is not None:
                await record_audit(
                    user_id=owner.id,
                    operation="auth.login.denied",
                    result=f"username={body.username} ip={_client(request)}",
                )
            raise HTTPException(status_code=401, detail="invalid credentials")
        out = MeOut(
            user_id=owner.id,
            username=owner.username,
            display_name=owner.display_name,
            email=owner.email,
            via="password",
        )
        user = owner

    await _start_session(request, response, user=user, via="password")
    _clear_failures(request)
    await record_audit(
        user_id=out.user_id,
        operation="auth.login",
        result=f"via=password ip={_client(request)}",
    )
    return out


@router.post("/logout", response_model=LogoutOut)
async def logout(
    request: Request, response: Response, principal: PrincipalDep
) -> LogoutOut:
    token = request.cookies.get(sessions.COOKIE_NAME)
    if token:
        async with session_scope() as db:
            await sessions.revoke(db, token)
    sessions.clear_cookie(response)
    if principal.user_id is not None:
        await record_audit(
            user_id=principal.user_id,
            operation="auth.logout",
            result=f"via={principal.via}",
        )
    return LogoutOut()


@router.get("/me", response_model=MeOut)
async def me(principal: PrincipalDep) -> MeOut:
    """Who this request is, and the request session keepalive makes.

    Keepalive cannot use ``/health``: that route is public so compose and CI
    can probe it, which means it answers 200 to an expired session and can
    never be the thing that notices one.
    """
    if principal.user_id is None:
        raise HTTPException(status_code=401, detail="authentication required")
    async with session_scope() as db:
        owner = await UserRepository(db).get(principal.user_id)
    if owner is None:
        raise HTTPException(status_code=401, detail="authentication required")
    return MeOut(
        user_id=owner.id,
        username=owner.username,
        display_name=owner.display_name,
        email=owner.email,
        via=principal.via,
    )
