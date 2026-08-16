"""argon2id hashing for the one password this instance has."""

from __future__ import annotations

from argon2 import PasswordHasher
from argon2.exceptions import Argon2Error, VerifyMismatchError

#: Library defaults are argon2id at parameters the Argon2 authors recommend.
#: Nothing here is tuned: this verifies a password a handful of times a day,
#: so the cost that matters is the attacker's.
_hasher = PasswordHasher()

#: A real hash of a value nobody knows, verified against when the account has
#: no password at all. Without it a login attempt returns immediately in that
#: case and takes ~50ms otherwise, which is a timing oracle for "is this
#: instance set up yet" — the one question the login form must not answer.
_ABSENT = _hasher.hash("password-that-does-not-exist")

#: Long enough that the argon2 cost is what an attacker faces rather than the
#: keyspace. This is the Owner's only password and it is typed rarely.
MIN_LENGTH = 12


def hash_password(raw: str) -> str:
    return _hasher.hash(raw)


def verify_password(hashed: str | None, raw: str) -> bool:
    """True when ``raw`` matches. A missing hash costs the same as a wrong one."""
    try:
        _hasher.verify(hashed if hashed is not None else _ABSENT, raw)
    except (VerifyMismatchError, Argon2Error):
        return False
    return hashed is not None
