"""Ask a one-shot subprocess whether a tree imports (F39, §5.7).

The controller process does not call :func:`mftik.registry.load.load_class`
or :func:`mftik.registry.protocol.handshake_info`. Those run in
``probe_child.py``, which this module starts as a script so importing it
does not load the tree into the controller.
"""

from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from mftik_sts.hostdisk.replica import TreeReplica, require_digest

#: Long enough for one interpreter to import the SDK, short of the
#: integration call cap (§9.1).
#: Default; adjust from measurement (Appendix D).
PROBE_TIMEOUT_S = 8

#: Same sentence the running sync uses when the directory is not there.
REASON_ABSENT = "not present on this registry disk"

_CHILD = Path(__file__).with_name("probe_child.py")


@dataclass(frozen=True, slots=True)
class ProbeResult:
    """What the probe subprocess reported.

    ``status`` is ``loaded`` or ``skipped``. ``reason`` is set only for
    ``skipped``, and it says why the import did not succeed.
    """

    status: Literal["loaded", "skipped"]
    reason: str | None = None

    def __post_init__(self) -> None:
        if self.status not in {"loaded", "skipped"}:
            raise ValueError("probe status must be loaded or skipped")
        if self.status == "loaded" and self.reason is not None:
            raise ValueError("a loaded probe has no reason")
        if self.status == "skipped" and not self.reason:
            raise ValueError("a skipped probe names a reason")


def probe(
    digest: str,
    env_generation: int | None,
    *,
    replica: TreeReplica,
) -> ProbeResult:
    """Import ``digest`` in a child interpreter and return its report.

    ``env_generation`` is put on the child's ``sys.path`` when that
    generation's ``site-packages`` exists, so the import sees the pinned
    extras. A digest this disk does not have is ``skipped`` without
    starting a process. This process does not import the tree.
    """
    digest = require_digest(digest)
    if env_generation is not None and type(env_generation) is not int:
        raise ValueError("env_generation must be an int or None")
    if env_generation is not None and env_generation < 0:
        raise ValueError("env_generation must be >= 0")
    if replica.path_of(digest) is None:
        return ProbeResult(status="skipped", reason=REASON_ABSENT)
    tree = replica.tree_path(digest)
    generation = "-" if env_generation is None else str(env_generation)
    try:
        completed = subprocess.run(
            [
                sys.executable,
                str(_CHILD),
                str(tree),
                str(replica.data_dir),
                digest,
                generation,
            ],
            capture_output=True,
            text=True,
            timeout=PROBE_TIMEOUT_S,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return ProbeResult(status="skipped", reason="import error: probe timed out")
    return _report(completed.stdout, completed.stderr)


def _report(stdout: str, stderr: str) -> ProbeResult:
    for line in reversed(stdout.splitlines()):
        text = line.strip()
        if not text.startswith("{"):
            continue
        try:
            body = json.loads(text)
        except json.JSONDecodeError:
            continue
        if not isinstance(body, dict):
            continue
        status = body.get("status")
        reason = body.get("reason")
        if status == "loaded" and reason is None:
            return ProbeResult(status="loaded")
        if status == "skipped" and isinstance(reason, str) and reason != "":
            return ProbeResult(status="skipped", reason=reason)
    detail = (stderr or stdout or "probe produced no report").strip()
    if not detail:
        detail = "probe produced no report"
    last = detail.splitlines()[-1]
    return ProbeResult(status="skipped", reason=last[:500])
