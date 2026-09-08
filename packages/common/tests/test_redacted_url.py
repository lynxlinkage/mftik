"""The connect line must not print the broker's password.

Every plane logs ``transport.describe()`` on every connect, so whatever this
returns ends up in ``docker logs`` for the whole fleet. The property worth
testing is not the shape of the output — it is that the secret is absent from
it, and the last test here holds every registered transport to that rather than
only the helper they are all supposed to use.
"""

from __future__ import annotations

import pytest
from mftik.broker import transport as transports
from mftik.broker.config import BrokerConfig
from mftik.broker.transport.base import redacted_url

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


#: One password, put into every URL a transport might read, so the assertion
#: below is the same string whichever store answered.
SECRET = "n0t-in-the-logs-please"


@pytest.mark.parametrize("name", transports.names())
def test_no_transport_prints_its_password_on_the_startup_line(name: str) -> None:
    """The ``describe`` contract, checked on each transport rather than trusted.

    Not connected, on purpose: this is the path a plane takes when it logs the
    line before its first round trip, and it is the branch a transport is most
    likely to write by hand. A new transport is covered the moment it is
    registered.
    """
    config = BrokerConfig(
        transport=name,
        nats_url=f"nats://user:{SECRET}@nats.internal:4222",
        redis_url=f"redis://user:{SECRET}@redis.internal:6379/0",
    )
    line = transports.build(config).describe()
    assert SECRET not in line
    assert ".internal:" in line, f"{name} does not say where it is: {line!r}"
