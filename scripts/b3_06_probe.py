"""B3-06 probe: a controller that only supervises, for the S-1 to S-3 checks.

Deployed as its own Strategon assignment (``scripts/b3_06_probe.json``)
with ``ociHostPid`` on, never as a plane. It needs no NATS, no database
and no venue. Each release of the image is one controller version:

    python /app/scripts/b3_06_probe.py controller

The controller starts a :class:`Supervisor` under ``$WORK_DIR/procman``,
keeps every worker it finds still running (a roll is ``close(detach)``),
and spawns one worker for its own release when it does not hold one.
Rolling the assignment through several releases therefore leaves one
worker per release, each on the rootfs of the controller that spawned it.

Every ``PROBE_PERIOD_S`` the controller writes ``$WORK_DIR/b306-status.json``
and the worker prints one line to its shim log
(``procman/run/<id>.stdout.log``): a lazy import of a module it has not
loaded yet and a read of a file on its own rootfs. A release that GC
deleted under a live worker shows up there as ``rootfs=FAIL``.
"""

from __future__ import annotations

import asyncio
import importlib
import json
import os
import re
import signal
import sys
import time
from pathlib import Path

from mftik.clock import SystemClock
from mftik.procman import (
    OOM_SCORE_ADJ,
    CloseMode,
    ObservedWorker,
    ProcmanError,
    Supervisor,
    WorkerSpec,
    current_release,
)
from mftik.procman.heartbeat import heartbeat_loop, status_fd

PERIOD_S = float(os.environ.get("PROBE_PERIOD_S", "10"))

#: Modules a worker has not imported at start. One per tick, so each
#: check really opens files on the worker's rootfs.
LAZY_MODULES = (
    "colorsys", "wave", "xml.dom.minidom", "tomllib", "zipapp",
    "difflib", "fractions", "statistics", "plistlib", "netrc", "mailbox",
    "imaplib", "poplib", "ftplib", "smtplib", "graphlib", "shelve", "dbm",
    "calendar", "pydoc", "tabnanny", "pickletools", "filecmp", "fileinput",
)  # fmt: skip


def _work_dir() -> Path:
    return Path(os.environ.get("WORK_DIR") or os.getcwd())


def _worker_id(release: str) -> str:
    return "probe/w-" + re.sub(r"[^A-Za-z0-9._-]", "_", release)


def _proc(pid: int | None, name: str) -> str | None:
    if pid is None:
        return None
    try:
        return Path(f"/proc/{pid}/{name}").read_text().strip()
    except OSError:
        return None


def _root_inode(pid: int | None) -> list[int] | None:
    if pid is None:
        return None
    try:
        st = os.stat(f"/proc/{pid}/root")
    except OSError:
        return None
    return [st.st_dev, st.st_ino]


async def controller() -> None:
    release = current_release()
    instance = os.environ.get("MFTIK_INSTANCE", "probe")
    work_dir = _work_dir() / "procman"
    sup = Supervisor(work_dir, plane="sts", instance=instance)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    found = await sup.start()
    for obs in found:
        ref = obs.spec.code_ref if obs.spec else None
        print(f"[b306] start: {obs.id} observed={obs.observed} code_ref={ref}", flush=True)
        if obs.observed is not ObservedWorker.RUNNING and obs.spec is not None:
            try:
                await sup.release_slot(obs.id)
            except ProcmanError as exc:
                print(f"[b306] release {obs.id}: {exc}", flush=True)

    own = _worker_id(release)
    if own not in {o.id for o in found if o.observed is ObservedWorker.RUNNING}:
        await sup.spawn(
            WorkerSpec(
                id=own,
                plane="sts",
                kind="session",
                incarnation=1,
                argv=(sys.executable, os.path.abspath(__file__), "worker"),
                env={"PATH": os.environ.get("PATH", ""), "PROBE_PERIOD_S": str(PERIOD_S)},
                code_ref=release,
                restart="never",
                start_timeout_s=30.0,
                hb_timeout_s=None,
                oom_score_adj=OOM_SCORE_ADJ[("sts", "session")],
                rlimit_data_bytes=None,
                stop_grace_s=5.0,
                labels={"probe": "b3-06"},
            )
        )
        print(f"[b306] spawned {own}", flush=True)

    ids = sorted({o.id for o in found} | {own})
    while not stop.is_set():
        workers = []
        for worker_id in ids:
            st = await sup.status(worker_id)
            if st is None:
                continue
            workers.append(
                {
                    "id": worker_id,
                    "code_ref": st.spec.code_ref,
                    "phase": str(st.phase),
                    "pid": st.pid,
                    "oom_score_adj": _proc(st.pid, "oom_score_adj"),
                    "root": _root_inode(st.pid),
                }
            )
        snapshot = {
            "ts": time.time(),
            "release": release,
            "controller_pid": os.getpid(),
            "controller_oom_score_adj": _proc(os.getpid(), "oom_score_adj"),
            "controller_root": _root_inode(os.getpid()),
            "host_pid_1": _proc(1, "comm"),
            "workers": workers,
        }
        tmp = _work_dir() / "b306-status.json.tmp"
        tmp.write_text(json.dumps(snapshot, indent=2))
        os.replace(tmp, _work_dir() / "b306-status.json")
        print(f"[b306] {json.dumps(snapshot)}", flush=True)
        try:
            await asyncio.wait_for(stop.wait(), PERIOD_S)
        except TimeoutError:
            pass

    print("[b306] SIGTERM: close(detach)", flush=True)
    await sup.close(CloseMode.DETACH)


async def worker() -> None:
    stop = asyncio.Event()
    beats = asyncio.create_task(
        heartbeat_loop(SystemClock(), ready=lambda: True, period_s=1.0, stop=stop, fd=status_fd())
    )
    release = current_release()
    tick = 0
    while not stop.is_set():
        name = LAZY_MODULES[tick % len(LAZY_MODULES)]
        try:
            importlib.import_module(name)
            Path(__file__).read_bytes()
            rootfs = "ok"
        except Exception as exc:  # noqa: BLE001 - the point is to report it
            rootfs = f"FAIL {type(exc).__name__}: {exc}"
        print(
            f"[b306] worker pid={os.getpid()} release={release} "
            f"import={name} rootfs={rootfs}",
            flush=True,
        )
        tick += 1
        try:
            await asyncio.wait_for(stop.wait(), PERIOD_S)
        except TimeoutError:
            pass
    await beats


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    if mode == "controller":
        asyncio.run(controller())
    elif mode == "worker":
        asyncio.run(worker())
    else:
        raise SystemExit("usage: b3_06_probe.py controller|worker")


if __name__ == "__main__":
    main()
