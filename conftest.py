"""Workspace-wide fixtures. Currently one: which database a test runs against.

Every DB-touching fixture in the suite takes ``database_url``, so adding an
engine here fans the whole database suite out over it rather than editing nine
fixtures in five packages.
"""

from __future__ import annotations

import os

import pytest
from db_harness import POSTGRES_URL_ENV, dialect_urls


def pytest_sessionstart(session: pytest.Session) -> None:
    """In CI, sqlite-only is a failure rather than a quiet degradation.

    Without this, a Postgres service that failed to come up would not turn the
    build red — the suite would run against sqlite alone and pass, which is the
    state this parametrisation exists to end.
    """
    if os.getenv("CI") and "postgres" not in dialect_urls():
        raise pytest.UsageError(
            f"{POSTGRES_URL_ENV} is unset: CI would test sqlite only."
        )


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if "database_url" in metafunc.fixturenames:
        urls = dialect_urls()
        metafunc.parametrize(
            "database_url", list(urls.values()), ids=list(urls), scope="function"
        )
