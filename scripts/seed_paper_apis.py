#!/usr/bin/env python3
"""Seed a dev user and two paper venue API credentials (idempotent).

Intended for local / docker-compose testing. Re-running is safe.

Credentials match ``PaperSessionFactory`` / paper-engine seeds:
  paper-key-1 / paper-secret-1  → 1 BTC + 100000 USDT  (strategy trading)
  paper-key-2 / paper-secret-2  → 10 BTC + 500000 USDT (liquidity maker)

Paper engine seeds resting book from key-2: bid [[49999, 10]], ask [[50001, 10]].
"""

from __future__ import annotations

import asyncio
import logging
import sys

from mft_db.models.api import Api, ApiType
from mft_db.models.user import User
from mft_db.repositories import ApiRepository, UserRepository
from mft_db.session import session_scope

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("seed")

DEV_EMAIL = "dev@mft.local"
DEV_DISPLAY_NAME = "Dev"

# Two isolated paper accounts for testing.
PAPER_APIS: tuple[dict[str, str], ...] = (
    {
        "api_key": "paper-key-1",
        "api_secret": "paper-secret-1",
        "venue": "paper",
    },
    {
        "api_key": "paper-key-2",
        "api_secret": "paper-secret-2",
        "venue": "paper",
    },
)


async def seed() -> None:
    summary: list[str] = []
    async with session_scope() as db:
        users = UserRepository(db)
        apis = ApiRepository(db)

        user = await users.get_by_email(DEV_EMAIL)
        if user is None:
            user = await users.add(
                User(email=DEV_EMAIL, display_name=DEV_DISPLAY_NAME)
            )
            logger.info("created user id=%s email=%s", user.id, user.email)
        else:
            logger.info("user exists id=%s email=%s", user.id, user.email)

        summary.append(f"  user_id={user.id} email={DEV_EMAIL}")

        for spec in PAPER_APIS:
            existing = await apis.get_by_api_key(spec["api_key"])
            if existing is not None:
                logger.info(
                    "api exists id=%s key=%s venue=%s",
                    existing.id,
                    existing.api_key,
                    existing.venue,
                )
                row = existing
            else:
                row = await apis.add(
                    Api(
                        owner_id=user.id,
                        venue=spec["venue"],
                        api_key=spec["api_key"],
                        api_secret=spec["api_secret"],
                        type=ApiType.HMAC.value,
                    )
                )
                logger.info(
                    "created api id=%s key=%s venue=%s",
                    row.id,
                    row.api_key,
                    row.venue,
                )
            summary.append(
                f"  api_id={row.id} venue={row.venue} "
                f"api_key={row.api_key} api_secret={row.api_secret}"
            )

    print("seed complete:")
    print("\n".join(summary))


def main() -> None:
    try:
        asyncio.run(seed())
    except Exception:
        logger.exception("seed failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
