#!/usr/bin/env python3
"""Seed a dev user and its venue API credentials (idempotent).

Intended for local / docker-compose testing. Re-running is safe.

Paper credentials match ``PaperSessionFactory`` / paper-engine seeds:
  paper-key-1 / paper-secret-1  → 1 BTC + 100000 USDT  (strategy trading)
  paper-key-2 / paper-secret-2  → 10 BTC + 500000 USDT (liquidity maker)

Paper engine seeds resting book from key-2: bid [[49999, 10]], ask [[50001, 10]].

A ``Gate`` credential is registered too when ``GATE_SPOT_API_KEY`` and
``GATE_SPOT_API_SECRET`` are set — real venue keys cannot be hard-coded, so
this is opt-in::

    GATE_SPOT_API_KEY=... GATE_SPOT_API_SECRET=... just seed
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys

from mftik.exchange import venues
from mftik_db.models.api import Api, ApiType
from mftik_db.models.user import User
from mftik_db.repositories import AccountRepository, ApiRepository, UserRepository
from mftik_db.session import session_scope

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("seed")

DEV_EMAIL = "dev@mft.local"
DEV_DISPLAY_NAME = "Dev"

# Two isolated paper accounts for testing (api ↔ account is 1-1).
PAPER_APIS: tuple[dict[str, str], ...] = (
    {
        "name": "paper trader",
        "api_key": "paper-key-1",
        "api_secret": "paper-secret-1",
        "venue": venues.PAPER.name,
    },
    {
        "name": "paper liquidity",
        "api_key": "paper-key-2",
        "api_secret": "paper-secret-2",
        "venue": venues.PAPER.name,
    },
)


def live_venue_apis() -> tuple[dict[str, str], ...]:
    """Credentials for real venues, from env. Empty unless both vars are set."""
    key = os.getenv("GATE_SPOT_API_KEY", "").strip()
    secret = os.getenv("GATE_SPOT_API_SECRET", "").strip()
    if not key or not secret:
        return ()
    return (
        {
            "name": os.getenv("GATE_SPOT_ACCOUNT_NAME", "gate spot").strip(),
            "api_key": key,
            "api_secret": secret,
            "venue": venues.GATE.name,
        },
    )


async def seed() -> None:
    summary: list[str] = []
    async with session_scope() as db:
        users = UserRepository(db)
        apis = ApiRepository(db)
        accounts = AccountRepository(db)

        user = await users.get_by_email(DEV_EMAIL)
        if user is None:
            user = await users.add(
                User(email=DEV_EMAIL, display_name=DEV_DISPLAY_NAME)
            )
            logger.info("created user id=%s email=%s", user.id, user.email)
        else:
            logger.info("user exists id=%s email=%s", user.id, user.email)

        summary.append(f"  user_id={user.id} email={DEV_EMAIL}")

        for spec in (*PAPER_APIS, *live_venue_apis()):
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

            account = await accounts.get_by_api_id(row.id)
            if account is None:
                account = await accounts.create(
                    name=spec["name"],
                    api_id=row.id,
                    created_by=user.id,
                )
                logger.info(
                    "created account id=%s api_id=%s name=%s",
                    account.id,
                    row.id,
                    account.name,
                )
            else:
                if account.name != spec["name"]:
                    # Prefer seed display names used by strategy.yml td refs.
                    account.name = spec["name"]
                    await db.flush()
                    logger.info(
                        "renamed account id=%s api_id=%s name=%s",
                        account.id,
                        row.id,
                        account.name,
                    )
                else:
                    logger.info(
                        "account exists id=%s api_id=%s name=%s",
                        account.id,
                        row.id,
                        account.name,
                    )

            simulated = venues.get(row.venue)
            secret = (
                row.api_secret
                if simulated is not None and simulated.simulated
                else "***"
            )
            summary.append(
                f"  api_id={row.id} account_id={account.id} "
                f"name={account.name} venue={row.venue} "
                f"api_key={row.api_key} api_secret={secret}"
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
