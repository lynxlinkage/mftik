from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class DatabaseConfig:
    url: str = "postgresql+asyncpg://mft:mft@localhost:5432/mft"
    sync_url: str = "postgresql+psycopg://mft:mft@localhost:5432/mft"

    @classmethod
    def from_env(cls) -> DatabaseConfig:
        return cls(
            url=os.getenv(
                "DATABASE_URL",
                "postgresql+asyncpg://mft:mft@localhost:5432/mft",
            ),
            sync_url=os.getenv(
                "DATABASE_URL_SYNC",
                "postgresql+psycopg://mft:mft@localhost:5432/mft",
            ),
        )
