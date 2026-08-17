"""Run Alembic upgrades for the mftik-db migration package."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from alembic import command
from alembic.config import Config


def _find_alembic_ini() -> Path:
    env_path = os.getenv("ALEMBIC_CONFIG")
    if env_path:
        path = Path(env_path)
        if path.is_file():
            return path
        raise FileNotFoundError(f"ALEMBIC_CONFIG not found: {env_path}")

    here = Path(__file__).resolve()
    # Dev / Docker checkout: packages/db/alembic.ini
    candidate = here.parents[2] / "alembic.ini"
    if candidate.is_file():
        return candidate

    raise FileNotFoundError(
        "Could not locate packages/db/alembic.ini; set ALEMBIC_CONFIG"
    )


def main(argv: list[str] | None = None) -> None:
    args = list(sys.argv[1:] if argv is None else argv)
    revision = args[0] if args else "head"

    ini = _find_alembic_ini()
    cfg = Config(str(ini))
    command.upgrade(cfg, revision)


if __name__ == "__main__":
    main()
