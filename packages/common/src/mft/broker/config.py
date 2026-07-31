from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class BrokerConfig:
    redis_url: str = "redis://localhost:6379/0"

    @classmethod
    def from_env(cls) -> BrokerConfig:
        return cls(redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
