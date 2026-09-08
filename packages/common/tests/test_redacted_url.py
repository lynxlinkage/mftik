"""The connect line must not print the broker's password.

Every plane logs ``transport.describe()`` on every connect, so whatever this
returns ends up in ``docker logs`` for the whole fleet. The property worth
testing is not the shape of the output — it is that the secret is absent from
it, and the last test here holds the transport to that rather than only the
helper it is supposed to use.
"""

from __future__ import annotations

import pytest
from mftik.broker import transport as transports
from mftik.broker.config import BrokerConfig
from mftik.broker.transport.base import redacted_url

#: The shape production actually uses: username, password, host, port.
PROD = "nats://default:yPbyy0QcqZRppAb2fcFBIM3TH1Y08@172.238.24.139:4222"


def test_the_password_is_gone_and_the_address_survives() -> None:
    out = redacted_url(PROD)
    assert "yPbyy0QcqZRppAb2fcFBIM3TH1Y08" not in out
    # Still has to answer the question the log line exists to answer.
    assert "172.238.24.139:4222" in out
    assert out.startswith("nats://default:")


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
    out = redacted_url(f"nats://user:{password}@host:4222")
    assert password not in out
    assert "host:4222" in out


def test_a_url_with_no_password_is_left_alone() -> None:
    """The local stack runs without one; mangling it would help nobody."""
    for url in ("nats://localhost:4222", "nats://user@localhost:4222"):
        assert redacted_url(url) == url


def test_something_unparseable_does_not_fall_back_to_the_original() -> None:
    """A parse failure must not print the string this exists to hide.

    ``urlsplit`` raises on a malformed port, and the tempting ``except:
    return url`` would leak the credential on exactly the inputs nobody
    anticipated.
    """
    hostile = "nats://user:secret-value@host:not-a-port"
    out = redacted_url(hostile)
    assert "secret-value" not in out


def test_a_database_url_is_covered_by_the_same_helper() -> None:
    """Nothing logs this today. It is one import away from doing so."""
    out = redacted_url("postgresql+asyncpg://mftik:hunter2@db.internal:5432/mftik")
    assert "hunter2" not in out
    assert "db.internal:5432" in out


#: One password, put into the URL the transport reads, so the assertion
#: below is the same string the helper already proved.
SECRET = "n0t-in-the-logs-please"


def test_no_transport_prints_its_password_on_the_startup_line() -> None:
    """The ``describe`` contract, checked on the transport rather than trusted.

    Not connected, on purpose: this is the path a plane takes when it logs the
    line before its first round trip, and it is the branch a transport is most
    likely to write by hand.
    """
    config = BrokerConfig(
        nats_url=f"nats://user:{SECRET}@nats.internal:4222",
    )
    line = transports.build(config).describe()
    assert SECRET not in line
    assert ".internal:" in line, f"does not say where it is: {line!r}"
