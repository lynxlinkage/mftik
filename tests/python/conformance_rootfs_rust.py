#!/usr/bin/env python3
"""Pack the Rust strategies with ``mftiko-pack -bin`` and run their rootfs (#479, #480).

``MFTIK_CONFORM_BIN`` (default ``target/release/conform``) and
``MFTIK_MD_WATCH_BIN`` (default ``target/release/md_watch``) are packed by the
pinned ``mftiko-pack -bin``, which takes the manifest's ``contracts``,
``runtime``, and ``sdk`` out of the binary (E08-D001, E08-D005, #489). The
binary carries no Go build info (#490). Each image is pushed to
a registry on 127.0.0.1, then pulled and unpacked with mftiko
``internal/platform/artifact``. Both tools come from the E04-D076 commit, as in
``data_input_e2e.py``. Conformance runs in conform's rootfs as in
``conformance_rootfs.py``: the rootfs is the working directory, the manifest
entrypoint is argv, and ``PATH`` is only the directory that contains
``MFTIK_CONFORMANCE``. md_watch's rootfs runs ``examples/rust/md_watch/test_e2e.py``,
the Python md_watch harness and checks (E08-D009). That needs ``cargo`` and
``nats-server`` (``MFTIK_NATS_SERVER``, or on ``PATH``).
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path

from conformance_rootfs import (
    CONTRACTS,
    REPO,
    require_entrypoint,
    run,
    run_conformance,
    start_registry,
    stop_registry,
)
from data_input_e2e import checkout_mftiko, go

ENTRYPOINT = ["bin/strategy"]
# What Go's debug/buildinfo looks for: the section, and the record's magic.
BUILD_INFO_MARKS = (b".go.buildinfo", b"\xff Go buildinf:")
MD_WATCH_TEST = REPO / "examples" / "rust" / "md_watch" / "test_e2e.py"


def main() -> None:
    tool = Path(os.environ.get("MFTIK_CONFORMANCE", ""))
    if not tool.is_file() or not os.access(tool, os.X_OK):
        raise SystemExit("MFTIK_CONFORMANCE must be the mftiko-conformance binary")
    binaries = {
        "conform": binary_path("MFTIK_CONFORM_BIN", "conform"),
        "md_watch": binary_path("MFTIK_MD_WATCH_BIN", "md_watch"),
    }
    for name, binary in binaries.items():
        require_no_build_info(name, binary.read_bytes())
    report_path = Path(os.environ.get("MFTIK_REPORT", "report-rootfs-rust.json")).resolve()
    work = Path(tempfile.mkdtemp(prefix="mftik-rust-rootfs-"))
    registry = None
    try:
        checkout = checkout_mftiko(work)
        port, registry = start_registry(work)
        home = work / "home"
        (home / ".docker").mkdir(parents=True)
        packer = build_packer(checkout, home)
        digests = {
            name: pack(packer, binary, name, port, home) for name, binary in binaries.items()
        }
        pulled = {
            name: check_image(checkout, work / f"artifact-{name}", port, name, digest)
            for name, digest in digests.items()
        }
        stop_registry(registry)
        registry = None
        version = sdk_version()
        rootfs = {
            name: require_image(
                pulled[name], name, digests[name], binaries[name].read_bytes(), version
            )
            for name in binaries
        }
        entry = require_entrypoint(rootfs["conform"], pulled["conform"].get("entrypoint"))
        run_conformance(tool, rootfs["conform"], entry, report_path, work)
        require_entrypoint(rootfs["md_watch"], pulled["md_watch"].get("entrypoint"))
        run_md_watch(rootfs["md_watch"])
    except Exception:
        print(f"mftik rust rootfs conformance kept {work}", file=sys.stderr)
        raise
    else:
        shutil.rmtree(work, ignore_errors=True)
    finally:
        if registry is not None:
            stop_registry(registry)


def binary_path(variable: str, name: str) -> Path:
    binary = Path(os.environ.get(variable, REPO / "target" / "release" / name))
    if not binary.is_file():
        raise SystemExit(f"{variable} is not a file: {binary}")
    return binary


def require_no_build_info(name: str, program: bytes) -> None:
    """No ``.go.buildinfo`` section and no build info record (#490).

    ``mftiko-pack`` reads the ``mftiko.sdk=`` record; ``go version -m`` finds
    nothing to read.
    """
    for mark in BUILD_INFO_MARKS:
        if mark in program:
            raise SystemExit(f"{name} carries Go build info: {mark!r}")


def sdk_version() -> str:
    """The workspace version, as mftik-rt's ``mftiko.sdk=`` record writes it (no ``v``)."""
    with (REPO / "Cargo.toml").open("rb") as handle:
        return tomllib.load(handle)["workspace"]["package"]["version"]


def build_packer(checkout: Path, home: Path) -> Path:
    """The pinned mftiko-pack, built next to ``home``.

    It is built before any pack runs with ``home`` as ``HOME``, so ``go`` keeps
    its own module cache instead of filling ``home`` with read-only modules.
    """
    tool = home.parent / "mftiko-pack"
    run(["go", "build", "-o", str(tool), "./cmd/mftiko-pack"], cwd=checkout)
    return tool


def pack(tool: Path, binary: Path, name: str, port: int, home: Path) -> str:
    """Push ``binary`` as ``mftik/<name>:<name>`` and return the manifest digest."""
    cmd = [
        str(tool),
        "-bin",
        str(binary.resolve()),
        "-name",
        name,
        "-repo",
        f"127.0.0.1:{port}/mftik/{name}",
        "-tag",
        name,
        "-plain-http",
    ]
    env = os.environ.copy()
    env["HOME"] = str(home)
    env["DOCKER_CONFIG"] = str(home / ".docker")
    print("+", " ".join(cmd), file=sys.stderr)
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)
    if proc.returncode != 0:
        raise SystemExit(f"mftiko-pack -bin failed\n{proc.stderr}")
    lines = [line for line in proc.stdout.splitlines() if line]
    if len(lines) != 1 or not lines[0].startswith("sha256:"):
        raise SystemExit(f"mftiko-pack -bin stdout was {proc.stdout!r}")
    print(lines[0], file=sys.stderr)
    return lines[0]


def check_image(checkout: Path, data: Path, port: int, name: str, digest: str) -> dict[str, object]:
    data.mkdir()
    stdout = go(
        checkout,
        [
            "./cmd/mftik-pack-check",
            "-registry",
            f"127.0.0.1:{port}",
            "-repository",
            f"mftik/{name}",
            "-digest",
            digest,
            "-data",
            str(data),
            "-layers",
            "1",
        ],
    )
    parsed = json.loads(stdout)
    if not isinstance(parsed, dict):
        raise SystemExit(f"check report {stdout!r}")
    return parsed


def require_image(
    report: dict[str, object], name: str, digest: str, program: bytes, version: str
) -> Path:
    """The manifest mftiko-pack writes, and a rootfs that holds only the binary.

    mftiko-pack takes ``runtime`` and ``sdk`` from the binary's ``mftiko.sdk=``
    record (mftiko E09-D099, #489).
    """
    want: dict[str, object] = {
        "digest": digest,
        "kind": "strategy",
        "name": name,
        "os": "linux",
        "arch": "amd64",
        "contracts": CONTRACTS,
        "entrypoint": ENTRYPOINT,
        "env": {},
        "runtime": "rust",
        "sdk_lang": "rust",
        "sdk_version": version,
    }
    for key, value in want.items():
        if report.get(key) != value:
            raise SystemExit(f"{key} is {report.get(key)!r}, want {value!r}")
    rootfs = Path(str(report.get("rootfs", "")))
    if not rootfs.is_dir():
        raise SystemExit(f"rootfs {rootfs}")
    files = sorted(
        path.relative_to(rootfs).as_posix() for path in rootfs.rglob("*") if not path.is_dir()
    )
    if files != ["bin/strategy"]:
        raise SystemExit(f"rootfs files {files}")
    if (rootfs / "bin" / "strategy").read_bytes() != program:
        raise SystemExit("bin/strategy is not the packed binary")
    return rootfs


def run_md_watch(rootfs: Path) -> None:
    """md_watch's e2e test, with the worker started from ``rootfs`` (E08-D009)."""
    env = os.environ.copy()
    env["MFTIK_MD_WATCH_ROOTFS"] = str(rootfs)
    cmd = [sys.executable, str(MD_WATCH_TEST), "-v"]
    print("+", f"MFTIK_MD_WATCH_ROOTFS={rootfs}", " ".join(cmd), file=sys.stderr)
    proc = subprocess.run(cmd, cwd=REPO, env=env, check=False)
    if proc.returncode != 0:
        raise SystemExit(f"md_watch e2e in its rootfs exited {proc.returncode}")


if __name__ == "__main__":
    main()
