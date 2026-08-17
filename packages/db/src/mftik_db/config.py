from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class DatabaseConfig:
    url: str = "postgresql+asyncpg://mftik:mftik@localhost:5432/mftik"
    sync_url: str = "postgresql+psycopg://mftik:mftik@localhost:5432/mftik"

    @classmethod
    def from_env(cls) -> DatabaseConfig:
        return cls(
            url=os.getenv(
                "DATABASE_URL",
                "postgresql+asyncpg://mftik:mftik@localhost:5432/mftik",
            ),
            sync_url=os.getenv(
                "DATABASE_URL_SYNC",
                "postgresql+psycopg://mftik:mftik@localhost:5432/mftik",
            ),
        )
