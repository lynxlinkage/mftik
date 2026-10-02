"""Snapshot and verify a B10-01 rehearsal database.

The database URL and its name both come from the command line. This
script does not read ``DATABASE_URL``, ``DATABASE_URL_SYNC``, or a
``.env`` file. Stdout is pass/fail, a timing, and row counts. It does
not print cell values, primary keys, or the URL.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "packages" / "db" / "tests"))

from prod_shape import (  # noqa: E402
    HEAD_REVISION,
    PRE_REVISION,
    alembic_problems,
    async_url_for,
    compare_surviving,
    dropped_columns_absent,
    dropped_columns_are_defaults,
    history_problems,
    require_explicit_database,
    revision_of,
    sequences_continue,
    shape_is_0034,
    snapshot,
)
from sqlalchemy import create_engine  # noqa: E402
from sqlalchemy.engine.url import make_url  # noqa: E402
from sqlalchemy.pool import NullPool  # noqa: E402

_SYNC = {"postgresql+psycopg", "postgresql", "sqlite"}


def _parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--url", required=True, help="sync SQLAlchemy URL")
    common.add_argument(
        "--database",
        required=True,
        help="database name; must equal the name in --url",
    )
    parser = argparse.ArgumentParser(
        prog="b10_01_rehearse.py",
        description=(
            "Record or check surviving-column digests. "
            "Does not read DATABASE_URL or a .env file."
        ),
    )
    commands = parser.add_subparsers(dest="command", required=True)
    snap = commands.add_parser("snapshot", parents=[common])
    snap.add_argument("--out", required=True, help="local snapshot file")
    verify = commands.add_parser("verify", parents=[common])
    verify.add_argument("--snapshot", required=True, help="file from snapshot")
    return parser


def _refuse_async(url: str) -> None:
    driver = make_url(url).drivername
    if driver not in _SYNC:
        raise SystemExit(
            "refusing to run: --url must be a sync driver "
            "(postgresql+psycopg or sqlite). "
            "This command does not read DATABASE_URL or a .env file."
        )


def _public(taken: dict) -> dict:
    """Counts, column names, and digests. No primary keys, no cell values."""
    tables = {}
    for name, info in taken["tables"].items():
        tables[name] = {
            "count": info["count"],
            "columns": list(info["columns"]),
            "surviving": list(info["surviving"]),
            "digests": [row["digest"] for row in info["rows"]],
        }
    return {
        "kind": "b10-01-snapshot",
        "revision": taken["revision"],
        "tables": tables,
    }


def _load(path: Path) -> dict:
    raw = json.loads(path.read_text())
    if raw.get("kind") != "b10-01-snapshot":
        raise SystemExit("refusing to run: snapshot file is not a b10-01 snapshot")
    tables = {}
    for name, info in raw["tables"].items():
        tables[name] = {
            "count": info["count"],
            "columns": info["columns"],
            "surviving": info["surviving"],
            "rows": [{"digest": digest} for digest in info["digests"]],
        }
    return {"revision": raw["revision"], "tables": tables}


def _counts(snap: dict) -> list[str]:
    lines = []
    total = 0
    for name in sorted(snap["tables"]):
        count = int(snap["tables"][name]["count"])
        total += count
        lines.append(f"table={name} rows={count}")
    lines.insert(0, f"tables={len(snap['tables'])}")
    lines.insert(1, f"rows={total}")
    return lines


def _emit(result: str, revision: str | None, elapsed: float, lines: list[str]) -> None:
    print(result)
    print(f"revision={revision}")
    print(f"elapsed_s={elapsed:.3f}")
    for line in lines:
        print(line)


def _check(connection, loaded: dict, url: str) -> list[str]:
    revision = revision_of(connection)
    if revision == HEAD_REVISION:
        problems = (
            compare_surviving(connection, loaded)
            + dropped_columns_absent(connection)
            + sequences_continue(connection)
            + alembic_problems(connection)
        )
        problems.extend(asyncio.run(history_problems(async_url_for(url), None)))
        return problems
    if revision == PRE_REVISION:
        return (
            compare_surviving(connection, loaded)
            + shape_is_0034(connection, loaded)
            + dropped_columns_are_defaults(connection)
        )
    return [
        f"revision {revision!r} is neither {HEAD_REVISION} nor {PRE_REVISION}"
    ]


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    _refuse_async(args.url)
    require_explicit_database(args.url, args.database)
    started = time.perf_counter()
    engine = create_engine(args.url, poolclass=NullPool)
    try:
        with engine.connect() as connection:
            if args.command == "snapshot":
                taken = snapshot(connection)
                path = Path(args.out)
                path.write_text(
                    json.dumps(_public(taken), indent=2, sort_keys=True) + "\n"
                )
                _emit(
                    "pass",
                    revision_of(connection),
                    time.perf_counter() - started,
                    _counts(taken),
                )
                return 0
            loaded = _load(Path(args.snapshot))
            problems = _check(connection, loaded, args.url)
            connection.commit()
            lines = _counts(loaded)
            if problems:
                lines.append(f"problems={len(problems)}")
                lines.extend(f"problem={item}" for item in problems)
                _emit(
                    "fail",
                    revision_of(connection),
                    time.perf_counter() - started,
                    lines,
                )
                return 1
            _emit(
                "pass",
                revision_of(connection),
                time.perf_counter() - started,
                lines,
            )
            return 0
    except SystemExit:
        raise
    except Exception as exc:
        # The exception text can carry bound row values. Withhold it.
        print("fail")
        print(f"error={type(exc).__name__}")
        print(
            "detail=the error text is withheld because it can contain row values"
        )
        return 1
    finally:
        engine.dispose()


if __name__ == "__main__":
    sys.exit(main())
