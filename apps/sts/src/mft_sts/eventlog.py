"""Session event log — everything one session was told, and everything it asked.

One jsonl file per session, append-only, oldest line first. It exists to answer
the two questions a strategy log cannot: *what did the session actually see*,
and *what did it do about it*. Both halves are needed. A log of inbound events
alone shows a fill arriving and never explains which submit produced it; a log
of orders alone shows an order going out with no account of the price that
prompted it.

Written from the dispatch points rather than from the strategy, so a strategy
that ignores an event still logs it. That is the case worth having: a hook that
never fired and a hook that fired and did nothing look identical from inside the
strategy, and only one of them is a bug.

Three costs are dealt with here, because an audit trail that changes how the
session trades is worse than no audit trail:

*Latency.* :meth:`EventLog.record` neither serializes nor writes. It stamps a
sequence number, drops a dict on a bounded queue and returns, so what sits in
the path between a book update and the order it triggers is a ``put_nowait``.
The ``json.dumps`` and the write happen on a worker thread.

*Memory.* The queue is bounded, so a disk that stalls costs a fixed number of
records rather than the process.

*Honesty.* Which means the bound has to be visible. A full queue drops the
record and counts it, and the count is written into the file as its own line as
soon as there is room — twice over, in fact, since ``seq`` is stamped before the
queue is offered anything and a dropped record therefore leaves a hole in the
numbering that no reader can miss. A log that quietly skipped a fill under load
would be worse than useless: it would read as proof the fill never happened.

Enabled by ``STS_EVENTLOG_DIR``. Unset — the default, and what the test suite
runs with — makes every method here a no-op.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

#: Where the per-session files land. Unset or empty disables the log entirely,
#: which is the default: writing a quarter of a gigabyte an hour is something a
#: deployment opts into, having given it somewhere to go.
DIR_ENV = "STS_EVENTLOG_DIR"
#: Records held between the session and the disk. Sized in records rather than
#: bytes because it is the dispatch path it protects, not the file.
QUEUE_ENV = "STS_EVENTLOG_QUEUE"
#: Bytes per file before it rotates, and how many rotations to keep. The disk
#: budget for one session is one times the other, plus the file in hand.
MAX_BYTES_ENV = "STS_EVENTLOG_MAX_BYTES"
BACKUPS_ENV = "STS_EVENTLOG_BACKUPS"

#: Deep enough to ride out a disk hiccup of a second or two on a busy book
#: feed, shallow enough that the records held are worth what they cost.
DEFAULT_QUEUE = 20_000
DEFAULT_MAX_BYTES = 256 * 1024 * 1024
DEFAULT_BACKUPS = 2

#: Records serialized per write. The queue is drained in batches so one busy
#: feed costs one thread hop and one flush per batch rather than per print.
BATCH_MAX = 1024

#: How long :meth:`EventLog.close` waits for the queue to drain. A session is
#: stopping and something else is waiting on it, so a wedged disk loses the
#: tail rather than holding the shutdown.
CLOSE_TIMEOUT_S = 5.0

#: session_id reaches this from an API request and is about to become a file
#: name. Anything outside this set is replaced rather than trusted.
_SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]")

_SENTINEL: dict[str, Any] = {"__close__": True}


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        logger.warning("ignoring %s=%r — not an integer", name, raw)
        return default
    return value if value > 0 else default


def _json_default(obj: Any) -> Any:
    """Render what ``json`` will not, on the writer thread.

    Reached only for values the encoder cannot take directly, so the wire dicts
    — which are already plain JSON — pay nothing for it. Decimals become
    strings: a price that survives a round trip as ``0.1`` and comes back as
    ``0.1000000000000000055`` is not the price the venue was sent.
    """
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, Enum):
        return obj.value
    dump = getattr(obj, "model_dump", None)
    if dump is not None:
        try:
            return dump(mode="json")
        except Exception:
            return repr(obj)
    if isinstance(obj, (set, frozenset)):
        return sorted(str(item) for item in obj)
    return str(obj)


class EventLog:
    """Append-only jsonl of one session's inbound events and outbound calls.

    Every line carries ``seq`` (monotonic, stamped in arrival order), ``ts``
    (when STS recorded it), ``dir`` (``in`` / ``out`` / ``self``), ``kind`` and
    ``event``. Inbound lines add ``sent_ts`` off the envelope, so the wire
    latency is the subtraction rather than something to go and correlate.
    """

    def __init__(
        self,
        session_id: str,
        *,
        directory: Path | None,
        queue_size: int = DEFAULT_QUEUE,
        max_bytes: int = DEFAULT_MAX_BYTES,
        backups: int = DEFAULT_BACKUPS,
    ) -> None:
        self.session_id = session_id
        self._path = (
            None
            if directory is None
            else Path(directory) / f"{_SAFE_NAME.sub('_', session_id)}.jsonl"
        )
        self._max_bytes = max(1, max_bytes)
        self._backups = max(0, backups)
        self._queue: asyncio.Queue[dict[str, Any]] | None = (
            None if self._path is None else asyncio.Queue(maxsize=queue_size)
        )
        self._task: asyncio.Task[None] | None = None
        self._file: Any = None
        self._seq = 0
        self._dropped = 0
        self._closed = False

    @classmethod
    def from_env(cls, session_id: str) -> EventLog:
        """Build one from ``STS_EVENTLOG_*``; disabled when the dir is unset."""
        raw = os.getenv(DIR_ENV, "").strip()
        return cls(
            session_id,
            directory=Path(raw) if raw else None,
            queue_size=_int_env(QUEUE_ENV, DEFAULT_QUEUE),
            max_bytes=_int_env(MAX_BYTES_ENV, DEFAULT_MAX_BYTES),
            backups=_int_env(BACKUPS_ENV, DEFAULT_BACKUPS),
        )

    @property
    def enabled(self) -> bool:
        return self._path is not None

    @property
    def path(self) -> Path | None:
        return self._path

    @property
    def dropped(self) -> int:
        """Records the queue had no room for. Nonzero means the file has holes."""
        return self._dropped

    async def start(self) -> None:
        """Open the file and start the writer. Never fatal to the session.

        A session that cannot write its audit trail still trades. The operator
        finds out from the process log, which is where a missing directory or a
        read-only mount belongs — not in a strategy's failure path.
        """
        if self._path is None or self._task is not None or self._closed:
            return
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._file = self._path.open("a", encoding="utf-8")
        except OSError:
            logger.exception(
                "STS event log unavailable session=%s path=%s — running "
                "without one",
                self.session_id,
                self._path,
            )
            self._path = None
            self._queue = None
            return
        self._task = asyncio.create_task(
            self._drain(), name=f"sts-{self.session_id}-eventlog"
        )

    def record(
        self, kind: str, event: str, *, dir: str = "in", **fields: Any
    ) -> None:
        """Queue one record. Returns immediately; never raises.

        ``fields`` with a ``None`` value are dropped, so a record carries the
        keys that mean something for its kind rather than a fixed set padded
        with nulls.
        """
        queue = self._queue
        if queue is None or self._closed:
            return
        try:
            queue.put_nowait(self._build(kind, event, dir=dir, **fields))
        except asyncio.QueueFull:
            self._dropped += 1
        except Exception:
            self._dropped += 1
            logger.exception(
                "STS event log record failed session=%s kind=%s event=%s",
                self.session_id,
                kind,
                event,
            )

    def _build(
        self, kind: str, event: str, *, dir: str = "in", **fields: Any
    ) -> dict[str, Any]:
        """Stamp one record. ``seq`` is taken here, before the queue is asked.

        Which is what makes a drop visible twice over: the number is spent
        whether or not there was room for the record that owns it, so the file
        carries a hole in its sequence as well as the count that explains it.
        """
        self._seq += 1
        record: dict[str, Any] = {
            "seq": self._seq,
            "ts": round(time.time(), 6),
            "session": self.session_id,
            "dir": dir,
            "kind": kind,
            "event": event,
        }
        for key, value in fields.items():
            if value is not None:
                record[key] = value
        return record

    async def close(self) -> None:
        """Flush what is queued and close the file."""
        if self._closed:
            return
        if self._queue is None or self._task is None:
            self._closed = True
            return
        # Waited into the queue rather than offered to it. This one line is
        # what separates a session that ended from a process that was killed,
        # and dropping it under a momentary backlog would blur exactly that.
        closed = self._build("eventlog", "closed", dir="self")
        self._closed = True
        try:
            await asyncio.wait_for(
                self._queue.put(closed), timeout=CLOSE_TIMEOUT_S
            )
            await asyncio.wait_for(
                self._queue.put(_SENTINEL), timeout=CLOSE_TIMEOUT_S
            )
            await asyncio.wait_for(self._task, timeout=CLOSE_TIMEOUT_S)
        except (TimeoutError, asyncio.CancelledError):
            logger.warning(
                "STS event log did not drain session=%s — %d record(s) lost",
                self.session_id,
                self._queue.qsize(),
            )
            self._task.cancel()
        except Exception:
            logger.exception(
                "STS event log close failed session=%s", self.session_id
            )
        self._task = None
        # A drop landing between the writer's last batch and the flag that
        # stopped new records has nobody left to report it. One line, written
        # here, rather than a file that ends without mentioning its last hole.
        if self._dropped and self._file is not None:
            dropped, self._dropped = self._dropped, 0
            try:
                await asyncio.to_thread(
                    self._write,
                    [
                        {
                            "ts": round(time.time(), 6),
                            "session": self.session_id,
                            "dir": "self",
                            "kind": "eventlog",
                            "event": "dropped",
                            "count": dropped,
                            "at_seq": self._seq,
                        }
                    ],
                )
            except Exception:
                logger.exception(
                    "STS event log final drop marker failed session=%s",
                    self.session_id,
                )
        if self._file is not None:
            try:
                await asyncio.to_thread(self._file.close)
            except Exception:
                logger.exception(
                    "STS event log file close failed session=%s",
                    self.session_id,
                )
            self._file = None

    async def _drain(self) -> None:
        """Batch the queue onto the writer thread until the sentinel arrives."""
        queue = self._queue
        assert queue is not None
        while True:
            first = await queue.get()
            batch: list[dict[str, Any]] = []
            stopping = False
            if first is _SENTINEL:
                stopping = True
            else:
                batch.append(first)
            while not stopping and len(batch) < BATCH_MAX:
                try:
                    item = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if item is _SENTINEL:
                    stopping = True
                    break
                batch.append(item)
            # After the batch, not before it. Everything in hand was queued
            # before the queue filled, so it is older than the records that
            # were turned away; a marker filed ahead of it would date the gap
            # earlier than it happened. ``at_seq`` is the last number stamped,
            # so the hole in the numbering and the count that explains it can
            # be read against each other.
            if self._dropped:
                dropped, self._dropped = self._dropped, 0
                batch.append(
                    {
                        "ts": round(time.time(), 6),
                        "session": self.session_id,
                        "dir": "self",
                        "kind": "eventlog",
                        "event": "dropped",
                        "count": dropped,
                        "at_seq": self._seq,
                    }
                )
            if batch:
                try:
                    await asyncio.to_thread(self._write, batch)
                except Exception:
                    logger.exception(
                        "STS event log write failed session=%s",
                        self.session_id,
                    )
            if stopping:
                return

    def _write(self, batch: list[dict[str, Any]]) -> None:
        """Serialize and append one batch. Runs on a worker thread."""
        if self._file is None:
            return
        lines = []
        for record in batch:
            try:
                lines.append(json.dumps(record, default=_json_default))
            except Exception:
                # One unserializable payload must not cost the batch it rode
                # in with. Keep the metadata and say what happened to the rest.
                lines.append(
                    json.dumps(
                        {
                            "seq": record.get("seq"),
                            "ts": record.get("ts"),
                            "session": self.session_id,
                            "dir": record.get("dir"),
                            "kind": record.get("kind"),
                            "event": record.get("event"),
                            "payload_error": "unserializable",
                        }
                    )
                )
        self._file.write("\n".join(lines) + "\n")
        self._file.flush()
        if self._file.tell() >= self._max_bytes:
            self._rotate()

    def _rotate(self) -> None:
        """Roll the file over, keeping ``backups`` of them. Worker thread."""
        assert self._path is not None
        try:
            self._file.close()
            if self._backups:
                oldest = Path(f"{self._path}.{self._backups}")
                oldest.unlink(missing_ok=True)
                for index in range(self._backups - 1, 0, -1):
                    src = Path(f"{self._path}.{index}")
                    if src.exists():
                        src.rename(f"{self._path}.{index + 1}")
                self._path.rename(f"{self._path}.1")
            else:
                self._path.unlink(missing_ok=True)
        except OSError:
            logger.exception(
                "STS event log rotate failed session=%s", self.session_id
            )
        self._file = self._path.open("a", encoding="utf-8")


#: Null object for anything reachable before a session is bound.
DISABLED = EventLog("-", directory=None)


def eventlog_dir() -> Path | None:
    """Where this process keeps event logs, or None if it keeps none."""
    raw = os.getenv(DIR_ENV, "").strip()
    return Path(raw) if raw else None


def log_parts(session_id: str, *, directory: Path | None = None) -> list[Path]:
    """One session's log files, oldest first.

    Oldest first because that is the order they are read back in: rotation
    names the *previous* file ``.1``, so the on-disk numbering runs backwards
    from the order anything wants them concatenated in.

    The session_id is sanitized the same way the writer sanitizes it, and the
    results are drawn from a directory listing rather than composed from the
    caller's string — so a name that tries to climb out of the directory
    matches nothing instead of resolving somewhere.
    """
    base = eventlog_dir() if directory is None else directory
    if base is None:
        return []
    stem = f"{_SAFE_NAME.sub('_', session_id)}.jsonl"
    current = base / stem
    rotated: list[tuple[int, Path]] = []
    try:
        for path in base.glob(f"{stem}.*"):
            try:
                index = int(path.suffix.lstrip("."))
            except ValueError:
                continue
            rotated.append((index, path))
    except OSError:
        logger.exception("STS event log listing failed dir=%s", base)
        return []
    parts = [path for _index, path in sorted(rotated, reverse=True)]
    if current.exists():
        parts.append(current)
    return parts


def part_path(session_id: str, name: str) -> Path | None:
    """Resolve one part name from :func:`log_parts`, or None if it is not one.

    The name is matched against the listing rather than joined onto the
    directory. A caller cannot name a file this way that the listing would not
    have offered it, whatever the string contains.
    """
    for path in log_parts(session_id):
        if path.name == name:
            return path
    return None


def session_log(strategy: Any) -> EventLog:
    """The event log behind ``strategy``, or a disabled stand-in.

    So the strategy-side accessors — oms, ledger, symbols, tape — can record
    unconditionally. Every one of them is reachable before its session exists
    and from test doubles that have no session at all, and a null object keeps
    that from becoming a guard at each of a dozen call sites.
    """
    session = getattr(strategy, "session", None)
    log = getattr(session, "event_log", None)
    return log if isinstance(log, EventLog) else DISABLED
