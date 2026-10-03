"""How the controller hands a session to its worker process.

B4-03 reads this. The controller does not import
:mod:`mftik_sts.session_worker`. It writes the request and puts the path
on ``argv``.

**Contract.** The file is ``{work_dir}/sessions/{session_id}.json``,
UTF-8 JSON of :class:`~mftik.protocol.messages.StsCreateSessionRequest`
(``model_dump_json``). The write is atomic (a temp file, then
``os.replace``). The default argv is ``python -m mftik_sts.session_worker``
plus that path. Until B4-03 the module exits non-zero and names the
ticket. Tests inject a stand-in through ``argv_for``.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from mftik.protocol import StsCreateSessionRequest


def write_session_request(work_dir: Path, request: StsCreateSessionRequest) -> Path:
    """Write ``request`` under ``work_dir`` and return the path the worker reads."""
    directory = Path(work_dir) / "sessions"
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{request.session_id}.json"
    temporary = directory / f".{request.session_id}.json.tmp"
    temporary.write_text(request.model_dump_json(), encoding="utf-8")
    os.replace(temporary, target)
    return target


def session_worker_argv(path: Path) -> tuple[str, ...]:
    """``python -m mftik_sts.session_worker <request.json>``."""
    return (sys.executable, "-m", "mftik_sts.session_worker", str(path))
