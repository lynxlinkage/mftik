"""OAuth providers, and the record that decides what a callback means.

The code flow, server side, exchanging the code for an access token and then
reading the profile from the provider's own userinfo endpoint. No ID token
verification: that would mean JWKS and RSA and a crypto dependency to learn
something a second TLS call to the same provider already tells us. It also
makes every provider the same shape, which is why step 6 is a table entry.

Only the provider's stable subject is wanted — a Discord snowflake, later a
Google ``sub``. Not the email. So the scope asked for is the narrowest one
that yields an id, and the consent screen is correspondingly short.
"""

from __future__ import annotations

import base64
import hashlib
import os
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import httpx
from mftik_db.models.auth import AuthOAuthState
from mftik_db.repositories import AuthOAuthStateRepository
from sqlalchemy.ext.asyncio import AsyncSession

_TIMEOUT = 15.0

#: How long a started flow stays completable. Long enough to read a consent
#: screen, short enough that an abandoned one is gone before it is forgotten.
STATE_TTL = timedelta(minutes=10)

MODE_LOGIN = "login"
MODE_CONNECT = "connect"


@dataclass(frozen=True, slots=True)
class Profile:
    """What a provider tells us, reduced to what is stored."""

    subject: str
    label: str | None = None
    email: str | None = None


@dataclass(frozen=True, slots=True)
class Provider:
    name: str
    authorize_url: str
    token_url: str
    userinfo_url: str
    scope: str
    #: What to make the provider ask before handing back an account. Without
    #: it a provider may silently reuse an existing grant, which turns
    #: "connect a different account" into something the Owner cannot do from
    #: the UI. The right word differs: Discord re-shows consent, Google needs
    #: telling that the account itself is the choice.
    prompt: str = "consent"

    def client_id(self) -> str:
        return os.getenv(f"{self.name.upper()}_CLIENT_ID", "").strip()

    def client_secret(self) -> str:
        return os.getenv(f"{self.name.upper()}_CLIENT_SECRET", "").strip()

    def configured(self) -> bool:
        return bool(self.client_id() and self.client_secret() and redirect_base())

    def redirect_uri(self) -> str:
        return f"{redirect_base()}/auth/callback/{self.name}"

    def authorize(self, *, state: str, verifier: str) -> str:
        params = {
            "client_id": self.client_id(),
            "redirect_uri": self.redirect_uri(),
            "response_type": "code",
            "scope": self.scope,
            "state": state,
            "code_challenge": _challenge(verifier),
            "code_challenge_method": "S256",
            "prompt": self.prompt,
        }
        return f"{self.authorize_url}?{httpx.QueryParams(params)}"

    async def exchange(
        self, code: str, verifier: str, *, client: httpx.AsyncClient | None = None
    ) -> Profile:
        own = client is None
        http = client or httpx.AsyncClient(timeout=_TIMEOUT)
        try:
            token = await http.post(
                self.token_url,
                data={
                    "client_id": self.client_id(),
                    "client_secret": self.client_secret(),
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": self.redirect_uri(),
                    "code_verifier": verifier,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            token.raise_for_status()
            access = token.json().get("access_token")
            if not isinstance(access, str) or not access:
                raise OAuthError(f"{self.name} returned no access token")

            profile = await http.get(
                self.userinfo_url, headers={"Authorization": f"Bearer {access}"}
            )
            profile.raise_for_status()
            return self.parse(profile.json())
        except httpx.HTTPError as exc:
            raise OAuthError(f"{self.name} refused the exchange: {exc}") from exc
        finally:
            if own:
                await http.aclose()

    def parse(self, body: object) -> Profile:
        if not isinstance(body, dict):
            raise OAuthError(f"{self.name} returned a profile that is not an object")
        subject = body.get("id") or body.get("sub")
        if not isinstance(subject, str) or not subject:
            raise OAuthError(f"{self.name} returned a profile with no id")
        label = body.get("username") or body.get("global_name") or body.get("name")
        email = body.get("email")
        return Profile(
            subject=subject,
            label=label if isinstance(label, str) else None,
            email=email if isinstance(email, str) else None,
        )


class OAuthError(RuntimeError):
    """The provider said no, or said something we cannot use."""


#: Discord. ``identify`` and nothing else: it yields the snowflake, which is
#: the only durable thing about an account, and skipping ``email`` keeps a
#: field we would never match on out of the consent screen entirely.
DISCORD = Provider(
    name="discord",
    authorize_url="https://discord.com/oauth2/authorize",
    token_url="https://discord.com/api/oauth2/token",
    userinfo_url="https://discord.com/api/users/@me",
    scope="identify",
)

#: Google, over OIDC. ``openid`` alone would give the ``sub`` this keys on
#: and nothing else — which would leave the settings page able to say an
#: account is connected but not *which*, and noticing the wrong one is
#: attached is the reason that column exists. Discord has a username to show;
#: Google has only the address, so ``email`` is asked for and displayed.
#:
#: It is still never matched on. The identity is ``sub``, which Google does
#: not reuse; the address is a label, and a label that changes hands is a
#: cosmetic problem rather than a way in.
GOOGLE = Provider(
    name="google",
    authorize_url="https://accounts.google.com/o/oauth2/v2/auth",
    token_url="https://oauth2.googleapis.com/token",
    userinfo_url="https://openidconnect.googleapis.com/v1/userinfo",
    scope="openid email",
    prompt="select_account",
)

PROVIDERS: dict[str, Provider] = {DISCORD.name: DISCORD, GOOGLE.name: GOOGLE}


def redirect_base() -> str:
    """Public origin the browser reaches this API on, including any prefix.

    Cannot be derived from the request. Traefik strips ``/api`` before the app
    sees anything, and locally the Vite proxy does the same, so the API's own
    view of its URL is exactly the one the provider must not be given. It has
    to be configured: `http://localhost:5173/api` locally,
    `https://mftik.lynkora.com/api` in production.
    """
    return os.getenv("MFTIK_OAUTH_REDIRECT_BASE", "").strip().rstrip("/")


def configured() -> list[str]:
    """Providers this instance can actually complete a flow with."""
    return [name for name, p in PROVIDERS.items() if p.configured()]


def _challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


async def start(
    db: AsyncSession,
    *,
    provider: str,
    mode: str,
    session_id: str | None,
) -> tuple[str, str]:
    """Record a flow and return ``(state, verifier)``."""
    state = secrets.token_urlsafe(32)[:64]
    verifier = secrets.token_urlsafe(64)[:128]
    await AuthOAuthStateRepository(db).add(
        AuthOAuthState(
            state=state,
            provider=provider,
            mode=mode,
            verifier=verifier,
            session_id=session_id,
            expires_at=datetime.now(UTC) + STATE_TTL,
        )
    )
    return state, verifier


@dataclass(frozen=True, slots=True)
class StateRecord:
    """A snapshot of the row, taken before it is destroyed.

    Deliberately not the ORM object. ``consume`` deletes the row in the same
    transaction it reads it, and handing back something whose attributes may
    or may not survive that would make the callback's correctness depend on
    SQLAlchemy's identity map rather than on anything written here.
    """

    provider: str
    mode: str
    verifier: str
    session_id: str | None


async def consume(db: AsyncSession, state: str) -> StateRecord | None:
    """The record this callback belongs to, if there is a live one.

    Unknown, expired and already-used all come back as None, and all mean the
    same thing to the caller: this callback is not one we started.
    """
    row = await AuthOAuthStateRepository(db).consume(state, datetime.now(UTC))
    if row is None:
        return None
    return StateRecord(
        provider=row.provider,
        mode=row.mode,
        verifier=row.verifier,
        session_id=row.session_id,
    )
