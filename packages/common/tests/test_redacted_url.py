"""The connect line must not print the Redis password.

Every service logs "Connected to Redis at ..." on every connect, so whatever
this returns ends up in ``docker logs`` for the whole fleet. The property
worth testing is not the shape of the output — it is that the secret is
absent from it.
"""

from __future__ import annotations

import pytest
from mftik.broker.client import redacted_url

#: The shape production actually uses: username, password, host, port, db.
PROD = "redis://default:yPbyy0QcqZRppAb2fcFBIM3TH1Y08@172.238.24.139:6379/0"


def test_the_password_is_gone_and_the_address_survives() -> None:
    out = redacted_url(PROD)
    assert "yPbyy0QcqZRppAb2fcFBIM3TH1Y08" not in out
    # Still has to answer the question the log line exists to answer.
    assert "172.238.24.139:6379" in out
    assert out.startswith("redis://default:")
    assert out.endswith("/0")


@pytest.mark.parametrize(
    "password",
    [
        "p@ssw0rd",  # an @ — splitting on the last one finds the wrong host
        "a:b:c",  # colons — splitting on the first finds the wrong user
        "@@@:::@@@",  # both, adversarially
        "s p a c e",
        "%40encoded",
    ],
)
def test_a_password_full_of_delimiters_is_still_removed(password: str) -> None:
    """What a regex over ``:`` or ``@`` gets wrong, and why this parses."""
    out = redacted_url(f"redis://user:{password}@host:6379/0")
    assert password not in out
    assert "host:6379" in out


def test_a_url_with_no_password_is_left_alone() -> None:
    """The local stack runs without one; mangling it would help nobody."""
    for url in ("redis://localhost:6379/0", "redis://user@localhost:6379/0"):
        assert redacted_url(url) == url


def test_something_unparseable_does_not_fall_back_to_the_original() -> None:
    """A parse failure must not print the string this exists to hide.

    ``urlsplit`` raises on a malformed port, and the tempting ``except:
    return url`` would leak the credential on exactly the inputs nobody
    anticipated.
    """
    hostile = "redis://user:secret-value@host:not-a-port/0"
    out = redacted_url(hostile)
    assert "secret-value" not in out


def test_a_database_url_is_covered_by_the_same_helper() -> None:
    """Nothing logs this today. It is one import away from doing so."""
    out = redacted_url("postgresql+asyncpg://mftik:hunter2@db.internal:5432/mftik")
    assert "hunter2" not in out
    assert "db.internal:5432" in out
