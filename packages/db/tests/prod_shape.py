"""Synthetic main-shaped rows, and the snapshot a B10-01 rehearsal checks.

The filler upgrades an empty database to ``0034_strategy_type_key`` — the
shape a ``main`` deployment has — and inserts obviously fake rows through
Core against the reflected tables. Callers pass a sync URL or a connection.
Nothing in this module reads ``DATABASE_URL``, ``DATABASE_URL_SYNC``, or a
``.env`` file.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import sqlalchemy as sa
from alembic import context
from alembic.autogenerate import RevisionContext
from alembic.config import Config
from alembic.runtime.environment import EnvironmentContext
from alembic.script import ScriptDirectory
from mftik_db.models import Base
from mftik_db.models.session import SessionStatus
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.schema import MetaData, Table, UniqueConstraint
from sqlalchemy.sql.sqltypes import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Enum,
    Float,
    Integer,
    Numeric,
    SmallInteger,
    String,
    Text,
)
from sqlalchemy.types import JSON as JSONType

PRE_REVISION = "0034_strategy_type_key"
HEAD_REVISION = "0037_drop_rebuild_facts"
DROPPED = ("rebuild_count", "st_facts")
_SKIP = frozenset({"alembic_version"})
_INI = Path(__file__).resolve().parents[1] / "alembic.ini"
_INT = (Integer, BigInteger, SmallInteger)
_WHEN = datetime(2024, 6, 1, 12, 0, tzinfo=UTC)

# Coverage is a floor. ``rows`` scales every other table; these three still
# have to show every status, both restart values, and both fact shapes even
# when ``rows`` is smaller than that set.
_STS_FLOOR = 5
_ATTACH_FLOOR = 2


def database_name(url: str) -> str:
    """The database component of a SQLAlchemy URL, or ``""`` when it has none."""
    name = make_url(url).database
    return name or ""


def require_explicit_database(url: str, database: str) -> None:
    """Refuse unless ``database`` is the database named by ``url``.

    The rehearsal script takes both on the command line. It does not fall
    back to an environment variable, so a forgotten flag cannot pick up a
    URL from the surrounding process.
    """
    actual = database_name(url)
    if not database or actual != database:
        raise SystemExit(
            "refusing to run: pass --database with the database name in "
            "--url. "
            f"--database is {database!r}; the URL names {actual!r}. "
            "This command does not read DATABASE_URL or a .env file."
        )


def _sqlite_sql(statement: str) -> str:
    """Rewrite the few Postgres-only statements the old chain emits.

    The chain is not edited. An empty sqlite database still has to parse
    ``::jsonb`` and ``UPDATE ... FROM`` on the way to 0034, because those
    statements run even when they match zero rows. The rehearsal itself
    is Postgres; this exists so the same helper can walk sqlite.
    """
    rewritten = (
        statement.replace("::jsonb", "")
        .replace("::text", "")
        .replace(" IS DISTINCT FROM ", " IS NOT ")
    )
    if "UPDATE sts_sessions AS s" in rewritten and "FROM strategies" in rewritten:
        rewritten = rewritten.replace(
            "UPDATE sts_sessions AS s", "UPDATE sts_sessions"
        )
        rewritten = rewritten.replace("s.session_id", "sts_sessions.session_id")
    return rewritten


def _listen_sqlite_sql(
    _conn, _cursor, statement, parameters, _context, _executemany
):
    if isinstance(statement, str):
        statement = _sqlite_sql(statement)
    return statement, parameters


def _install_sqlite_chain(connection: Connection) -> list[tuple[Any, str, Any]]:
    """Route ALTER that SQLite rejects through batch recreate.

    ``now()`` is registered because several revisions use it as a server
    default. The patches are restored by :func:`_restore_sqlite_chain`.
    """
    from alembic.operations.ops import (
        AlterColumnOp,
        CreateForeignKeyOp,
        CreateUniqueConstraintOp,
        DropConstraintOp,
    )

    raw = connection.connection
    raw.create_function(
        "now", 0, lambda: "2024-06-01 12:00:00", deterministic=True
    )
    sa.event.listen(
        connection, "before_cursor_execute", _listen_sqlite_sql, retval=True
    )

    saved: list[tuple[Any, str, Any]] = []

    def _wrap(op_cls: Any, name: str, batch_call) -> None:
        original_method = getattr(op_cls, name)
        saved.append((op_cls, name, original_method))

        def wrapped(cls, operations, *args, **kwargs):
            bind = operations.get_bind()
            if bind is not None and bind.dialect.name == "sqlite":
                return batch_call(operations, *args, **kwargs)
            return original_method.__func__(cls, operations, *args, **kwargs)

        setattr(op_cls, name, classmethod(wrapped))

    def _drop(
        operations,
        constraint_name,
        table_name,
        type_=None,
        *,
        schema=None,
        if_exists=None,
    ):
        with operations.batch_alter_table(table_name, schema=schema) as batch:
            batch.drop_constraint(constraint_name, type_=type_)
        return None

    def _alter(operations, table_name, column_name, **kwargs):
        schema = kwargs.pop("schema", None)
        with operations.batch_alter_table(table_name, schema=schema) as batch:
            batch.alter_column(column_name, **kwargs)
        return None

    def _unique(operations, constraint_name, table_name, columns, *, schema=None, **kw):
        with operations.batch_alter_table(table_name, schema=schema) as batch:
            batch.create_unique_constraint(constraint_name, columns, **kw)
        return None

    def _fk(
        operations,
        constraint_name,
        source_table,
        referent_table,
        local_cols,
        remote_cols,
        *,
        onupdate=None,
        ondelete=None,
        deferrable=None,
        initially=None,
        match=None,
        source_schema=None,
        referent_schema=None,
        **dialect_kw,
    ):
        with operations.batch_alter_table(source_table, schema=source_schema) as batch:
            batch.create_foreign_key(
                constraint_name,
                referent_table,
                local_cols,
                remote_cols,
                onupdate=onupdate,
                ondelete=ondelete,
                deferrable=deferrable,
                initially=initially,
                match=match,
                referent_schema=referent_schema,
                **dialect_kw,
            )
        return None

    _wrap(DropConstraintOp, "drop_constraint", _drop)
    _wrap(AlterColumnOp, "alter_column", _alter)
    _wrap(CreateUniqueConstraintOp, "create_unique_constraint", _unique)
    _wrap(CreateForeignKeyOp, "create_foreign_key", _fk)
    return saved


def _restore_sqlite_chain(
    connection: Connection, saved: list[tuple[Any, str, Any]]
) -> None:
    sa.event.remove(connection, "before_cursor_execute", _listen_sqlite_sql)
    for op_cls, name, original in saved:
        setattr(op_cls, name, original)


def migrate(connection: Connection, destination: str) -> None:
    """Upgrade or downgrade this connection to ``destination``.

    Uses the connection it is given. It does not open one from the
    environment, which is how ``env.py`` normally finds a URL.
    """
    config = Config(str(_INI))
    script = ScriptDirectory.from_config(config)
    current = _revision(connection)
    # ``head`` and a named revision both go through the same walker. A
    # destination at or behind ``current`` is a downgrade, including the
    # step back to 0034 from head.
    downward = _is_downgrade(script, current, destination)

    def steps(rev, _ctx):
        if downward:
            return script._downgrade_revs(destination, rev)
        return script._upgrade_revs(destination, rev)

    saved: list[tuple[Any, str, Any]] = []
    if connection.dialect.name == "sqlite":
        saved = _install_sqlite_chain(connection)
    try:
        with EnvironmentContext(
            config,
            script,
            fn=steps,
            destination_rev=destination,
        ):
            context.configure(
                connection=connection, target_metadata=Base.metadata
            )
            with context.begin_transaction():
                context.run_migrations()
    finally:
        if saved:
            _restore_sqlite_chain(connection, saved)


def revision_of(connection: Connection) -> str | None:
    return _revision(connection)


def fill(connection: Connection, rows: int) -> None:
    """Insert ``rows`` fake rows into every user table.

    ``sts_sessions`` always covers every status, ``restart`` of ``always``
    and ``never``, ``rebuild_count`` 0..3, ``st_facts`` of ``{}`` and a
    nested document, a NULL ``instance``, and non-trivial ``td`` /
    ``md_ids`` / ``st_paras``. ``md_sessions`` and ``td_sessions`` always
    cover ``live`` and ``done``. Foreign keys point at rows this function
    inserted, or at the ``td`` / ``md`` / ``sts`` instances migration 0031
    already seeded.
    """
    if rows < 1:
        raise ValueError("rows must be at least 1")
    meta = MetaData()
    meta.reflect(bind=connection)
    pks: dict[str, list[dict[str, Any]]] = {}
    # ``td_sessions.api_id`` is not a foreign key, so reflection will not
    # order that table after ``apis``. The parents still have to exist.
    order = {"users": 0, "instances": 1, "apis": 2, "accounts": 3}
    tables = sorted(
        meta.sorted_tables, key=lambda table: order.get(table.name, 50)
    )
    for table in tables:
        if table.name in _SKIP:
            continue
        pks[table.name] = _load_pks(connection, table)
        count = _how_many(table.name, rows)
        for index in range(count):
            values = _values_for(table, index, pks, connection.dialect.name)
            if table.name == "sts_sessions":
                values.update(_sts_overlay(table, index, pks))
            elif table.name == "td_sessions":
                values.update(_td_overlay(table, index, pks))
            elif table.name == "md_sessions":
                values.update(_md_overlay(table, index, pks))
            _execute_insert(connection, table, values)
        pks[table.name] = _load_pks(connection, table)


def snapshot(connection: Connection) -> dict[str, Any]:
    """Row counts and a per-row digest of every column the drop keeps.

    ``rebuild_count`` and ``st_facts`` are not in the digest: they are the
    columns the upgrade removes. Primary keys are stored so a mismatch can
    be counted, and the rehearsal script does not print them.
    """
    meta = MetaData()
    meta.reflect(bind=connection)
    tables: dict[str, Any] = {}
    for table in meta.sorted_tables:
        if table.name in _SKIP:
            continue
        surviving = [
            column.name
            for column in table.columns
            if column.name not in DROPPED
        ]
        rows = []
        for record in _fetch(connection, table, surviving):
            rows.append(
                {
                    "pk": _pk_token(table, record),
                    "digest": _digest(record, surviving),
                }
            )
        tables[table.name] = {
            "count": len(rows),
            "columns": sorted(column.name for column in table.columns),
            "surviving": surviving,
            "rows": rows,
        }
    return {"revision": revision_of(connection), "tables": tables}


def compare_surviving(
    connection: Connection, snap: dict[str, Any]
) -> list[str]:
    """Counts and surviving-column digests against ``snap``.

    Reports table names and how many digests disagree. It does not report
    cell values.
    """
    problems: list[str] = []
    meta = MetaData()
    meta.reflect(bind=connection)
    present = {table.name for table in meta.sorted_tables}
    for name, info in snap["tables"].items():
        if name not in present:
            problems.append(f"{name} is missing")
            continue
        table = meta.tables[name]
        surviving = list(info["surviving"])
        missing = [col for col in surviving if col not in table.c]
        if missing:
            problems.append(f"{name} lost {', '.join(missing)}")
            continue
        fetched = _fetch(connection, table, surviving)
        if len(fetched) != info["count"]:
            problems.append(
                f"{name} count {len(fetched)} != snapshot {info['count']}"
            )
        got = {_digest(record, surviving) for record in fetched}
        want = {row["digest"] for row in info["rows"]}
        # Multisets: two identical surviving rows are two digests.
        got_multi = sorted(_digest(record, surviving) for record in fetched)
        want_multi = sorted(row["digest"] for row in info["rows"])
        if got_multi != want_multi:
            problems.append(
                f"{name} digest mismatch "
                f"({len(want - got)} missing, {len(got - want)} unexpected)"
            )
    return problems


def dropped_columns_absent(connection: Connection) -> list[str]:
    problems: list[str] = []
    if revision_of(connection) != HEAD_REVISION:
        problems.append(
            f"revision is {revision_of(connection)!r}, want {HEAD_REVISION}"
        )
    names = {
        column["name"]
        for column in sa.inspect(connection).get_columns("sts_sessions")
    }
    still = [name for name in DROPPED if name in names]
    if still:
        problems.append("still present: " + ", ".join(still))
    return problems


def shape_is_0034(connection: Connection, snap: dict[str, Any]) -> list[str]:
    """The reflected tables and columns match the pre-upgrade snapshot."""
    problems: list[str] = []
    if revision_of(connection) != PRE_REVISION:
        problems.append(
            f"revision is {revision_of(connection)!r}, want {PRE_REVISION}"
        )
    inspector = sa.inspect(connection)
    now = set(inspector.get_table_names()) - _SKIP
    then = set(snap["tables"])
    if now != then:
        extra = sorted(now - then)
        missing = sorted(then - now)
        if extra:
            problems.append("unexpected tables: " + ", ".join(extra))
        if missing:
            problems.append("missing tables: " + ", ".join(missing))
    for name, info in snap["tables"].items():
        if name not in now:
            continue
        cols = {column["name"] for column in inspector.get_columns(name)}
        if cols != set(info["columns"]):
            problems.append(f"{name} column set differs from the 0034 snapshot")
    return problems


def dropped_columns_are_defaults(connection: Connection) -> list[str]:
    """After a downgrade, both restored columns read the server default."""
    problems: list[str] = []
    rows = connection.execute(
        text("SELECT rebuild_count, st_facts FROM sts_sessions")
    ).all()
    bad_count = 0
    bad_facts = 0
    for rebuild_count, st_facts in rows:
        if rebuild_count != 0:
            bad_count += 1
        if _json_value(st_facts) != {}:
            bad_facts += 1
    if bad_count:
        problems.append(f"rebuild_count is not 0 on {bad_count} rows")
    if bad_facts:
        problems.append(f"st_facts is not {{}} on {bad_facts} rows")
    return problems


def sequences_continue(connection: Connection) -> list[str]:
    """A new id is the previous maximum plus one.

    Postgres checks ``nextval`` of every serial column, and also inserts
    one row into ``users``, ``td_sessions`` and ``md_sessions`` (then
    deletes it) so the check is an insert and not only a sequence read.
    ``nextval`` is not rolled back, so each serial sequence ends one
    value ahead. Sqlite inserts and deletes the same three, and checks
    ``sqlite_sequence`` for every other autoincrement table.
    """
    if connection.dialect.name == "postgresql":
        return _postgres_sequences(connection)
    if connection.dialect.name == "sqlite":
        return _sqlite_sequences(connection)
    return [f"no sequence check for {connection.dialect.name}"]


def alembic_problems(connection: Connection) -> list[str]:
    """The same comparison ``alembic check`` runs, on this connection."""
    config = Config(str(_INI))
    script = ScriptDirectory.from_config(config)
    command_args = {
        "message": None,
        "autogenerate": True,
        "sql": False,
        "head": "head",
        "splice": False,
        "branch_label": None,
        "version_path": None,
        "rev_id": None,
        "depends_on": None,
    }
    revision_context = RevisionContext(config, script, command_args)

    def retrieve(rev, ctx):
        revision_context.run_autogenerate(rev, ctx)
        return []

    with EnvironmentContext(
        config,
        script,
        fn=retrieve,
        as_sql=False,
        template_args=revision_context.template_args,
        revision_context=revision_context,
    ):
        context.configure(connection=connection, target_metadata=Base.metadata)
        with context.begin_transaction():
            context.run_migrations()

    migration_script = revision_context.generated_revisions[-1]
    diffs: list[Any] = []
    for upgrade_ops in migration_script.upgrade_ops_list:
        for item in upgrade_ops.as_diffs():
            # AlterColumnOp.to_diff_tuple returns a list of tuples.
            if isinstance(item, list):
                diffs.extend(item)
            else:
                diffs.append(item)
    if connection.dialect.name == "sqlite":
        # Migrations emit BigInteger. SQLite reflects that as BIGINT, and
        # the model renders Integer there (``with_variant``). ``alembic
        # check`` on Postgres does not see this. It is not a 0037 diff.
        diffs = [item for item in diffs if not _sqlite_integer_render(item)]
    if not diffs:
        return []
    kinds = sorted({str(item[0]) for item in diffs if item})
    return [f"alembic check found {len(diffs)} diffs ({', '.join(kinds)})"]


def _sqlite_integer_render(diff: Any) -> bool:
    if not diff or diff[0] != "modify_type" or len(diff) < 7:
        return False
    return _integer_type(diff[5]) and _integer_type(diff[6])


def _integer_type(type_: Any) -> bool:
    return type(type_).__name__ in {"BIGINT", "BigInteger", "INTEGER", "Integer"}


async def history_problems(url: str, schema: str | None) -> list[str]:
    """Every session row loads through the current repositories' reads."""
    from mftik_db.repositories import (
        MdSessionRepository,
        StsSessionRepository,
        TdSessionRepository,
    )

    engine = _async_engine(url, schema)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    problems: list[str] = []
    try:
        async with maker() as session:
            sts = StsSessionRepository(session)
            td = TdSessionRepository(session)
            md = MdSessionRepository(session)
            sts_n = await sts.count()
            td_n = await td.count()
            md_n = await md.count()
            sts_rows = list(await sts.list_sessions(status=None, limit=max(sts_n, 1)))
            td_rows = list(await td.list_sessions(status=None, limit=max(td_n, 1)))
            md_rows = list(await md.list_sessions(status=None, limit=max(md_n, 1)))
            if len(sts_rows) != sts_n:
                problems.append(
                    f"sts list loaded {len(sts_rows)} of {sts_n}"
                )
            if len(td_rows) != td_n:
                problems.append(f"td list loaded {len(td_rows)} of {td_n}")
            if len(md_rows) != md_n:
                problems.append(f"md list loaded {len(md_rows)} of {md_n}")
            for row in sts_rows:
                loaded = await sts.get_by_session_id(row.session_id)
                if loaded is None:
                    problems.append("sts get_by_session_id missed a listed row")
                    break
            # Aggregates have to run. A NULL instance is omitted on purpose;
            # the assertion is that the call returns and does not exceed the
            # rows that named an instance.
            sts_by = await sts.count_by_instance()
            td_by = await td.count_by_instance()
            md_by = await md.count_by_instance()
            if _sum_counts(sts_by) > sts_n:
                problems.append("sts count_by_instance exceeds the table")
            if _sum_counts(td_by) > td_n:
                problems.append("td count_by_instance exceeds the table")
            if _sum_counts(md_by) > md_n:
                problems.append("md count_by_instance exceeds the table")
            api_ids = {row.api_id for row in td_rows}
            for api_id in api_ids:
                live = await td.count_live_for_api(api_id)
                expected = sum(
                    1
                    for row in td_rows
                    if row.api_id == api_id
                    and row.status == SessionStatus.LIVE.value
                )
                if live != expected:
                    problems.append(
                        "count_live_for_api disagrees with the loaded rows"
                    )
                    break
    finally:
        await engine.dispose()
    return problems


def async_url_for(sync_url: str) -> str:
    if sync_url.startswith("sqlite"):
        return "sqlite+aiosqlite://" + sync_url.removeprefix("sqlite://")
    if sync_url.startswith("postgresql+psycopg"):
        return "postgresql+asyncpg" + sync_url.removeprefix("postgresql+psycopg")
    if sync_url.startswith("postgresql://"):
        return "postgresql+asyncpg://" + sync_url.removeprefix("postgresql://")
    return sync_url


def _async_engine(url: str, schema: str | None):
    kwargs: dict[str, Any] = {"poolclass": NullPool}
    if schema and url.startswith("postgresql"):
        kwargs["connect_args"] = {"server_settings": {"search_path": schema}}
    return create_async_engine(url, **kwargs)


def _sum_counts(folded: dict[str, dict[str, int]]) -> int:
    return sum(sum(counts.values()) for counts in folded.values())


def _revision(connection: Connection) -> str | None:
    inspector = sa.inspect(connection)
    if not inspector.has_table("alembic_version"):
        return None
    row = connection.execute(text("SELECT version_num FROM alembic_version")).first()
    if row is None:
        return None
    return str(row[0])


def _is_downgrade(
    script: ScriptDirectory, current: str | None, destination: str
) -> bool:
    if current is None:
        return False
    if destination == "head":
        return False
    # Walking from ``current`` down to ``destination`` yields steps only
    # when ``destination`` is an ancestor. The empty walk means upgrade
    # (or already there).
    try:
        steps = list(
            script.iterate_revisions(
                current, destination, select_for_downgrade=True
            )
        )
    except Exception:
        return False
    return bool(steps) and destination != current


def _how_many(table: str, rows: int) -> int:
    if table == "sts_sessions":
        return max(rows, _STS_FLOOR)
    if table in {"td_sessions", "md_sessions"}:
        return max(rows, _ATTACH_FLOOR)
    return rows


def _load_pks(connection: Connection, table: Table) -> list[dict[str, Any]]:
    columns = list(table.primary_key.columns)
    if not columns:
        return []
    rows = connection.execute(sa.select(*columns)).all()
    names = [column.name for column in columns]
    return [dict(zip(names, row, strict=True)) for row in rows]


def _unique_single(table: Table) -> set[str]:
    names: set[str] = set()
    for column in table.columns:
        if column.unique:
            names.add(column.name)
    for constraint in table.constraints:
        if not isinstance(constraint, UniqueConstraint):
            continue
        columns = list(constraint.columns)
        if len(columns) == 1:
            names.add(columns[0].name)
    for index in table.indexes:
        cols = list(index.columns)
        if index.unique and len(cols) == 1:
            names.add(cols[0].name)
    return names


def _values_for(
    table: Table,
    index: int,
    pks: dict[str, list[dict[str, Any]]],
    dialect: str,
) -> dict[str, Any]:
    unique = _unique_single(table)
    values: dict[str, Any] = {}
    for column in table.columns:
        if _skip_column(column, dialect):
            continue
        if column.foreign_keys:
            values[column.name] = _fk_value(
                column, index, pks, column.name in unique
            )
            continue
        values[column.name] = _literal(column, index)
    return values


def _skip_column(column: sa.Column, dialect: str) -> bool:
    # Postgres serial columns take their id from the sequence. Sqlite only
    # does that for a lone ``INTEGER PRIMARY KEY``; a ``BIGINT`` key is an
    # ordinary column and has to be supplied.
    alone = len(list(column.table.primary_key.columns)) == 1
    assigns = False
    if (
        alone
        and column.primary_key
        and isinstance(column.type, _INT)
        and not column.foreign_keys
    ):
        if dialect == "postgresql":
            assigns = True
        elif dialect == "sqlite" and not isinstance(column.type, BigInteger):
            assigns = True
    if assigns:
        return True
    # Server-side timestamps. The digest reads them back; omitting them
    # is what shows a default survived the upgrade.
    return isinstance(column.type, DateTime) and column.server_default is not None


def _fk_value(
    column: sa.Column,
    index: int,
    pks: dict[str, list[dict[str, Any]]],
    unique: bool,
) -> Any:
    fk = next(iter(column.foreign_keys))
    parent = fk.column.table.name
    parent_col = fk.column.name
    choices = pks.get(parent) or []
    if not choices:
        if column.nullable:
            return None
        raise RuntimeError(
            f"{column.table.name}.{column.name} needs a {parent} row"
        )
    if unique:
        if index >= len(choices):
            raise RuntimeError(
                f"{column.table.name}.{column.name} has fewer parents than rows"
            )
        return choices[index][parent_col]
    return choices[index % len(choices)][parent_col]


def _literal(column: sa.Column, index: int) -> Any:
    type_ = column.type
    if isinstance(type_, JSONType):
        return {"fake": index, "label": "合成"}
    if isinstance(type_, Boolean):
        return index % 2 == 0
    if isinstance(type_, Enum):
        values = list(type_.enums)
        return values[index % len(values)]
    if isinstance(type_, _INT):
        return index + 1
    if isinstance(type_, DateTime):
        return _WHEN
    if isinstance(type_, Date):
        return _WHEN.date()
    # ``Float`` subclasses ``Numeric``. A cursor timestamp is a float, not
    # a decimal amount.
    if isinstance(type_, Float):
        return 1700000000.5 + index
    if isinstance(type_, Numeric):
        return Decimal("1.250000000000000000")
    if isinstance(type_, (String, Text)):
        return _text(column, index)
    raise RuntimeError(
        f"no fake value for {column.table.name}.{column.name} "
        f"({type_.__class__.__name__})"
    )


def _text(column: sa.Column, index: int) -> str:
    length = getattr(column.type, "length", None)
    token = f"f{index}"
    if length is None:
        return f"fake-{column.name}-{index}"
    if length <= len(token):
        return token[:length]
    prefix = "fake"
    body = f"{prefix}-{column.name}-{token}"
    return body[:length]


def _execute_insert(
    connection: Connection, table: Table, values: dict[str, Any]
) -> None:
    allowed = {column.name for column in table.columns}
    payload = {key: value for key, value in values.items() if key in allowed}
    try:
        connection.execute(table.insert(), payload)
    except Exception as exc:
        raise RuntimeError(f"insert into {table.name} failed") from exc


def _user_id(pks: dict[str, list[dict[str, Any]]]) -> int:
    users = pks.get("users") or []
    if not users:
        raise RuntimeError("sts/td/md rows need a user")
    return int(users[0]["id"])


def _sts_overlay(
    table: Table, index: int, pks: dict[str, list[dict[str, Any]]]
) -> dict[str, Any]:
    status, restart, rebuild, facts, null_instance, rich = _STS[index % len(_STS)]
    long_note = "字" * 2000
    nested = {
        "started_ms": 1700000000000,
        "note": "合成「不是生產」",
        "anchor": "價格",
        "long": long_note,
        "nested": {"k": [1, "二", None, True]},
    }
    rich_td = {
        "fake-main": {"api_id": 1, "settings": {"leverage": 3}},
        "fake-hedge": {"api_id": 2, "settings": {"reduce_only": True}},
    }
    rich_md = {"fake-md": ["FakeVenue_Spot_FAKEBTC", "FakeVenue_Perp_FAKEETH"]}
    rich_paras = {
        "window": 12,
        "note": "合成參數",
        "symbols": ["FAKE-BTC", "FAKE-ETH"],
    }
    terminal = status != SessionStatus.LIVE.value
    overlay: dict[str, Any] = {
        "session_id": f"fake-sts-{index}",
        "created_by": _user_id(pks),
        "status": status,
        "restart": restart,
        "rebuild_count": rebuild,
        "st_facts": {} if facts == "empty" else nested,
        "instance": None if null_instance else "fake-sts",
        "td": rich_td if rich else {"fake-one": {"api_id": 1}},
        "md_ids": rich_md if rich else ["FakeVenue_Spot_FAKEBTC"],
        "st_paras": rich_paras if rich else {"note": "短"},
        "reason": "fake-failure" if status == SessionStatus.FAILED.value else None,
        "finished_at": _WHEN if terminal else None,
        "type": "FakeStrategy" if index % 2 == 0 else None,
        "legacy_strategy": "fake-old" if status == SessionStatus.DONE.value else None,
        "yaml_text": "sts:\n  note: 合成\n" if rich else None,
    }
    return {key: value for key, value in overlay.items() if key in table.c}


_STS = (
    (SessionStatus.LIVE.value, "always", 0, "empty", True, False),
    (SessionStatus.DONE.value, "never", 1, "nested", False, True),
    (SessionStatus.FAILED.value, "always", 2, "nested", False, True),
    (SessionStatus.INTERRUPTED.value, "never", 3, "empty", True, False),
    (SessionStatus.ACK.value, "never", 0, "nested", False, True),
)


def _td_overlay(
    table: Table, index: int, pks: dict[str, list[dict[str, Any]]]
) -> dict[str, Any]:
    apis = pks.get("apis") or []
    if not apis:
        raise RuntimeError("td_sessions needs an api")
    status = (
        SessionStatus.LIVE.value if index % 2 == 0 else SessionStatus.DONE.value
    )
    overlay: dict[str, Any] = {
        "session_id": f"fake-td-{index}",
        "api_id": apis[index % len(apis)]["id"],
        "created_by": _user_id(pks),
        "status": status,
        "finished_at": None if status == SessionStatus.LIVE.value else _WHEN,
    }
    return {key: value for key, value in overlay.items() if key in table.c}


def _md_overlay(
    table: Table, index: int, pks: dict[str, list[dict[str, Any]]]
) -> dict[str, Any]:
    status = (
        SessionStatus.LIVE.value if index % 2 == 0 else SessionStatus.DONE.value
    )
    overlay: dict[str, Any] = {
        "instance": "fake-md",
        "venue": f"FakeVenue{index}",
        "session_id": f"fake-md-{index}",
        "created_by": _user_id(pks),
        "status": status,
        "finished_at": None if status == SessionStatus.LIVE.value else _WHEN,
    }
    return {key: value for key, value in overlay.items() if key in table.c}


def _fetch(
    connection: Connection, table: Table, columns: list[str]
) -> list[dict[str, Any]]:
    selected = [table.c[name] for name in columns]
    order = list(table.primary_key.columns) or selected
    rows = connection.execute(sa.select(*selected).order_by(*order)).mappings().all()
    return [dict(row) for row in rows]


def _pk_token(table: Table, record: dict[str, Any]) -> str:
    payload = {
        column.name: _canon(record.get(column.name))
        for column in table.primary_key.columns
    }
    return json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)


def _digest(record: dict[str, Any], columns: list[str]) -> str:
    payload = {name: _canon(record.get(name)) for name in columns}
    raw = json.dumps(
        payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str
    )
    return hashlib.sha256(raw.encode()).hexdigest()


def _canon(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        return format(value, ".17g")
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, str):
        stripped = value.lstrip()
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                return _canon(json.loads(value))
            except json.JSONDecodeError:
                return value
        return value
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(UTC)
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (dict, list)):
        return json.loads(
            json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)
        )
    if isinstance(value, (bytes, memoryview)):
        return bytes(value).hex()
    return str(value)


def _json_value(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _serial_columns(connection: Connection) -> list[tuple[str, str]]:
    inspector = sa.inspect(connection)
    found: list[tuple[str, str]] = []
    for table in inspector.get_table_names():
        if table in _SKIP:
            continue
        pk = inspector.get_pk_constraint(table)["constrained_columns"]
        if len(pk) != 1:
            continue
        column = next(
            item for item in inspector.get_columns(table) if item["name"] == pk[0]
        )
        if column.get("autoincrement") not in (True, "auto"):
            continue
        if not isinstance(column["type"], _INT):
            continue
        found.append((table, pk[0]))
    return found


def _ident(name: str) -> str:
    if not name.isascii() or not name.replace("_", "").isalnum():
        raise RuntimeError(f"unexpected identifier {name!r}")
    return '"' + name + '"'


def _max_id(connection: Connection, table: str, column: str) -> int | None:
    value = connection.execute(
        text(f"SELECT MAX({_ident(column)}) FROM {_ident(table)}")
    ).scalar()
    return None if value is None else int(value)


def _postgres_sequences(connection: Connection) -> list[str]:
    # The three inserts consume those sequences. Every other serial is
    # checked with nextval, which is the id the next insert would take.
    problems = _insert_probes(connection)
    for table, column in _serial_columns(connection):
        if table in {"users", "td_sessions", "md_sessions"}:
            continue
        seq = connection.execute(
            text("SELECT pg_get_serial_sequence(:table, :column)"),
            {"table": table, "column": column},
        ).scalar()
        if not seq:
            problems.append(f"{table}.{column} has no sequence")
            continue
        current = _max_id(connection, table, column)
        expected = 1 if current is None else current + 1
        nxt = connection.execute(
            text("SELECT nextval(:seq)"), {"seq": seq}
        ).scalar()
        if int(nxt) != expected:
            problems.append(f"{table}.{column} next value is not max+1")
    return problems


def _sqlite_sequences(connection: Connection) -> list[str]:
    problems = _insert_probes(connection)
    names = {
        row[0]
        for row in connection.execute(text("SELECT name FROM sqlite_sequence")).all()
    } if _has_sqlite_sequence(connection) else set()
    for table, column in _serial_columns(connection):
        if table in {"users", "td_sessions", "md_sessions"}:
            continue
        current = _max_id(connection, table, column)
        if current is None:
            continue
        if table not in names:
            problems.append(f"{table} is missing from sqlite_sequence")
            continue
        seq = connection.execute(
            text("SELECT seq FROM sqlite_sequence WHERE name = :name"),
            {"name": table},
        ).scalar()
        if int(seq) != current:
            problems.append(f"{table}.{column} sqlite_sequence is not max(id)")
    return problems


def _has_sqlite_sequence(connection: Connection) -> bool:
    row = connection.execute(
        text(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' "
            "AND name = 'sqlite_sequence'"
        )
    ).first()
    return row is not None


def _insert_probes(connection: Connection) -> list[str]:
    """Insert one row into three serial tables and delete it again."""
    problems: list[str] = []
    user_max = _max_id(connection, "users", "id")
    # ``created_at`` is supplied. Several revisions default it to ``now()``,
    # which Postgres has and a fresh sqlite connection does not.
    user_id = connection.execute(
        text(
            "INSERT INTO users (display_name, email, created_at) "
            "VALUES ('fake-probe', 'fake-probe@example.invalid', "
            "'2024-06-01 12:00:00') "
            "RETURNING id"
        )
    ).scalar()
    if int(user_id) != (1 if user_max is None else user_max + 1):
        problems.append("users insert did not get the next id")
    owner = connection.execute(
        text("SELECT id FROM users ORDER BY id LIMIT 1")
    ).scalar()
    for table, sql in (
        (
            "td_sessions",
            "INSERT INTO td_sessions "
            "(session_id, api_id, created_by, status, created_at) "
            "VALUES ('fake-probe-td', 1, :owner, 'done', "
            "'2024-06-01 12:00:00') RETURNING id",
        ),
        (
            "md_sessions",
            "INSERT INTO md_sessions "
            "(instance, venue, session_id, created_by, status, created_at) "
            "VALUES ('fake-probe', 'FakeProbe', 'fake-probe-md', :owner, "
            "'done', '2024-06-01 12:00:00') RETURNING id",
        ),
    ):
        before = _max_id(connection, table, "id")
        new_id = connection.execute(text(sql), {"owner": owner}).scalar()
        if int(new_id) != (1 if before is None else before + 1):
            problems.append(f"{table} insert did not get the next id")
        connection.execute(
            text(f"DELETE FROM {_ident(table)} WHERE id = :id"),
            {"id": new_id},
        )
    connection.execute(text("DELETE FROM users WHERE id = :id"), {"id": user_id})
    return problems
