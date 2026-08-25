"""``/auth/*`` — claim the instance, log in, log out, say who you are."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Literal

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import RedirectResponse
from mftik_db.models.auth import AuthIdentity, AuthKey
from mftik_db.models.user import User
from mftik_db.repositories import (
    AuthIdentityRepository,
    AuthKeyRepository,
    UserRepository,
)
from mftik_db.session import session_scope
from pydantic import BaseModel, Field

from mftik_api.audit_util import record_audit
from mftik_api.auth import keys, oauth, passwords, sessions
from mftik_api.auth.deps import PrincipalDep, SessionDep
from mftik_api.auth.middleware import auth_enabled

router = APIRouter(prefix="/auth", tags=["auth"])

#: Providers this instance can prove an identity with. OAuth joins the list
#: when it is wired; the UI reads this rather than guessing from a build flag.
_PASSWORD_PROVIDER = "password"

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
    #: Whether the gate is on at all (``MFTIK_AUTH_ENABLED``). Off, every
    #: request is already the Owner, and the UI has no business offering to
    #: sign anybody in or out — the answer to both would be a no-op.
    enabled: bool
    #: True while the Owner has no password — either no user row at all, or
    #: the one ``seed`` creates so foreign keys resolve. Both are "run setup".
    setup_required: bool
    providers: list[str]
    authenticated: bool
    #: The Owner's name, and only once the caller has proved they are
    #: someone. This route is deliberately open — the login screen has to
    #: reach it before anybody has proved anything — so filling this in
    #: unconditionally published the Owner's username to the internet
    #: (issue #20). Anonymous callers get ``None``; the login form and
    #: ``mftik connect`` both lose a prefilled default and nothing else.
    username: str | None = None
    #: Published rather than duplicated. The rule is enforced here, and a
    #: number the login form also hard-codes is a number that drifts from it.
    min_password_length: int = passwords.MIN_LENGTH


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
        providers=[_PASSWORD_PROVIDER, *oauth.configured()],
        authenticated=principal.authenticated,
        username=(
            owner.username if principal.authenticated and owner is not None else None
        ),
        min_password_length=passwords.MIN_LENGTH,
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
        user_id=user_id,
        operation="auth.setup",
        result=f"username={username}",
        via="password",
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
                    via="password",
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
        result=f"ip={_client(request)}",
        via="password",
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
            result="",
            principal=principal,
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


# ------------------------------------------------------------------ keys ---


class KeyOut(BaseModel):
    id: int
    name: str
    kind: str
    #: `mftik_ak_abc12345…`. Everything of the token that is knowable from here.
    prefix: str
    scopes: list[str]
    created_at: float
    last_used_at: float | None = None
    revoked_at: float | None = None


class KeyCreatedOut(KeyOut):
    """The mint response, and the only place the secret ever appears.

    Everything else about a key can be looked up again. This cannot: the
    database has a SHA-256 of it. Whoever shows this to a person has one
    chance to make that clear.
    """

    token: str


class KeyCreateBody(BaseModel):
    name: str = Field(min_length=1, max_length=64)
    #: `registry` is for handing to another node. It reads what this one
    #: publishes and is refused everywhere else, so it is safe to give away in
    #: a way an API key never is.
    kind: Literal["api", "registry"] = "api"


class KeyListOut(BaseModel):
    keys: list[KeyOut]


def _epoch(value: datetime | None) -> float | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.timestamp()


def _key_out(row: AuthKey) -> KeyOut:
    return KeyOut(
        id=row.id,
        name=row.name,
        kind=row.kind,
        prefix=keys.display(row),
        scopes=list(row.scopes),
        created_at=_epoch(row.created_at) or 0.0,
        last_used_at=_epoch(row.last_used_at),
        revoked_at=_epoch(row.revoked_at),
    )


@router.post("/keys", response_model=KeyCreatedOut, status_code=201)
async def create_key(
    body: KeyCreateBody, principal: SessionDep
) -> KeyCreatedOut:
    """Mint a key and return it once."""
    minted = keys.mint(body.kind)
    async with session_scope() as db:
        row = await AuthKeyRepository(db).add(
            AuthKey(
                user_id=principal.user_id,
                kind=body.kind,
                name=body.name.strip(),
                prefix=minted.prefix,
                key_hash=minted.key_hash,
                scopes=list(minted.scopes),
            )
        )
        out = _key_out(row)
        key_id = row.id

    await record_audit(
        user_id=principal.user_id,
        operation="auth.key.mint",
        result=f"id={key_id} kind={body.kind} name={body.name}",
        principal=principal,
    )
    return KeyCreatedOut(**out.model_dump(), token=minted.token)


@router.get("/keys", response_model=KeyListOut)
async def list_keys(principal: SessionDep) -> KeyListOut:
    """Every key this Owner has issued. Never a secret, revoked ones included."""
    async with session_scope() as db:
        rows = await AuthKeyRepository(db).list_for_user(principal.user_id)
        return KeyListOut(keys=[_key_out(row) for row in rows])


@router.delete("/keys/{key_id}", response_model=KeyOut)
async def revoke_key(key_id: int, principal: SessionDep) -> KeyOut:
    """Stop a key working. The row stays so the audit trail still resolves."""
    async with session_scope() as db:
        row = await AuthKeyRepository(db).revoke(
            key_id, principal.user_id, datetime.now(UTC)
        )
        if row is None:
            raise HTTPException(status_code=404, detail=f"unknown key: {key_id}")
        out = _key_out(row)

    await record_audit(
        user_id=principal.user_id,
        operation="auth.key.revoke",
        result=f"id={key_id} name={out.name}",
        principal=principal,
    )
    return out


# ------------------------------------------------------------- identities ---


class IdentityOut(BaseModel):
    """One way of proving you are the Owner.

    ``id`` is null for the password, which is not a row and cannot be removed
    — the UI lists it alongside the others so both look like what they are,
    and gets `removable: false` rather than having to special-case a name.
    """

    id: int | None
    provider: str
    label: str | None = None
    email: str | None = None
    linked_at: float | None = None
    removable: bool = True


class IdentityListOut(BaseModel):
    identities: list[IdentityOut]


async def _identities(db, owner: User) -> list[IdentityOut]:
    rows = await AuthIdentityRepository(db).list_for_user(owner.id)
    out = [
        IdentityOut(
            id=None,
            provider="password",
            label=owner.username,
            linked_at=_epoch(owner.created_at),
            removable=False,
        )
    ]
    out.extend(
        IdentityOut(
            id=row.id,
            provider=row.provider,
            label=row.label,
            email=row.email,
            linked_at=_epoch(row.created_at),
        )
        for row in rows
    )
    return out


@router.get("/identities", response_model=IdentityListOut)
async def list_identities(principal: SessionDep) -> IdentityListOut:
    async with session_scope() as db:
        owner = await UserRepository(db).get(principal.user_id)
        if owner is None:
            raise HTTPException(status_code=401, detail="authentication required")
        return IdentityListOut(identities=await _identities(db, owner))


@router.delete("/identities/{identity_id}", response_model=IdentityOut)
async def unlink_identity(identity_id: int, principal: SessionDep) -> IdentityOut:
    """Detach an OAuth account. The password is not one of these and stays."""
    async with session_scope() as db:
        row = await AuthIdentityRepository(db).unlink(identity_id, principal.user_id)
        if row is None:
            raise HTTPException(
                status_code=404, detail=f"unknown identity: {identity_id}"
            )
        out = IdentityOut(
            id=row.id,
            provider=row.provider,
            label=row.label,
            email=row.email,
            linked_at=_epoch(row.created_at),
        )

    await record_audit(
        user_id=principal.user_id,
        operation="auth.identity.unlink",
        result=f"provider={out.provider} label={out.label}",
        principal=principal,
    )
    return out


# ------------------------------------------------------------------ oauth ---


def _provider(name: str) -> oauth.Provider:
    provider = oauth.PROVIDERS.get(name)
    if provider is None or not provider.configured():
        raise HTTPException(status_code=404, detail=f"unknown provider: {name}")
    return provider


@router.get("/login/{provider_name}", include_in_schema=False)
async def oauth_login(provider_name: str, request: Request) -> RedirectResponse:
    """Start a login. Public, because logging in is what you have not done yet."""
    provider = _provider(provider_name)
    async with session_scope() as db:
        state, verifier = await oauth.start(
            db, provider=provider_name, mode=oauth.MODE_LOGIN, session_id=None
        )
    return RedirectResponse(
        provider.authorize(state=state, verifier=verifier), status_code=307
    )


@router.get("/connect/{provider_name}", include_in_schema=False)
async def oauth_connect(
    provider_name: str, request: Request, principal: SessionDep
) -> RedirectResponse:
    """Start a link, bound to the session starting it.

    That binding is what stops a link begun in one browser being finished in
    another — including one the Owner is not sitting at.
    """
    provider = _provider(provider_name)
    async with session_scope() as db:
        state, verifier = await oauth.start(
            db,
            provider=provider_name,
            mode=oauth.MODE_CONNECT,
            session_id=principal.session_id,
        )
    return RedirectResponse(
        provider.authorize(state=state, verifier=verifier), status_code=307
    )


@router.get("/callback/{provider_name}", include_in_schema=False)
async def oauth_callback(
    provider_name: str,
    request: Request,
    response: Response,
    state: str = "",
    code: str = "",
) -> RedirectResponse:
    """One callback, two meanings, decided by the record and never the URL.

    Everything below reads ``mode`` from the row that ``state`` names. A
    readable ``state=connect`` would let anyone walk the Owner's browser into
    linking *their* account and logging in as the Owner from then on.
    """
    provider = _provider(provider_name)
    if not state or not code:
        raise HTTPException(status_code=400, detail="missing state or code")

    async with session_scope() as db:
        record = await oauth.consume(db, state)
    if record is None or record.provider != provider_name:
        raise HTTPException(status_code=403, detail="this is not a flow we started")

    try:
        profile = await provider.exchange(code, record.verifier)
    except oauth.OAuthError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    if record.mode == oauth.MODE_CONNECT:
        return await _finish_connect(request, record, profile, provider_name)
    return await _finish_login(request, response, profile, provider_name)


async def _finish_connect(
    request: Request,
    record: oauth.StateRecord,
    profile: oauth.Profile,
    provider_name: str,
) -> RedirectResponse:
    presented = request.cookies.get(sessions.COOKIE_NAME)
    async with session_scope() as db:
        live = await sessions.resolve(db, presented) if presented else None
        if live is None or live.id != record.session_id:
            raise HTTPException(
                status_code=403,
                detail="finish connecting in the browser that started it",
            )

        identities = AuthIdentityRepository(db)
        existing = await identities.get(provider_name, profile.subject)
        if existing is not None:
            if existing.user_id != live.user_id:
                raise HTTPException(
                    status_code=409,
                    detail="that account is linked to a different owner",
                )
            return RedirectResponse("/settings", status_code=303)

        await identities.add(
            AuthIdentity(
                user_id=live.user_id,
                provider=provider_name,
                subject=profile.subject,
                label=profile.label,
                email=profile.email,
            )
        )
        user_id = live.user_id
        via = live.via

    await record_audit(
        user_id=user_id,
        operation="auth.identity.connect",
        result=f"provider={provider_name} label={profile.label}",
        via=via,
    )
    return RedirectResponse("/settings", status_code=303)


async def _finish_login(
    request: Request,
    response: Response,
    profile: oauth.Profile,
    provider_name: str,
) -> RedirectResponse:
    """Sign in an account that is already linked. Never create one.

    An unknown account is refused rather than welcomed. That refusal is the
    whole "no second owner" rule: OAuth attaches to the existing Owner or it
    does nothing at all.
    """
    async with session_scope() as db:
        identity = await AuthIdentityRepository(db).get(provider_name, profile.subject)
        if identity is None:
            raise HTTPException(
                status_code=403,
                detail=(
                    "this account is not connected to this instance — sign in "
                    "with your password, then connect it in settings"
                ),
            )
        user_id = identity.user_id

    redirect = RedirectResponse("/", status_code=303)
    async with session_scope() as db:
        token = await sessions.issue(
            db,
            user_id=user_id,
            via=provider_name,
            user_agent=request.headers.get("user-agent"),
            ip=_client(request),
        )
    sessions.set_cookie(redirect, token)
    await record_audit(
        user_id=user_id,
        operation="auth.login",
        result=f"ip={_client(request)}",
        via=provider_name,
    )
    return redirect
