#!/usr/bin/env python3
"""Pack the Rust conform strategy with ``mftiko-pack -bin`` and run its rootfs (#479).

``MFTIK_CONFORM_BIN`` (default ``target/release/conform``) is packed by the
pinned ``mftiko-pack -bin``, which takes the manifest's ``contracts`` and
``sdk.version`` out of the binary (E08-D001, E08-D005). The image is pushed to
a registry on 127.0.0.1, then pulled and unpacked with mftiko
``internal/platform/artifact``. Both tools come from the E04-D076 commit, as in
``data_input_e2e.py``. Conformance runs as in ``conformance_rootfs.py``: the
rootfs is the working directory, the manifest entrypoint is argv, and ``PATH``
is only the directory that contains ``MFTIK_CONFORMANCE``.
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


def main() -> None:
    tool = Path(os.environ.get("MFTIK_CONFORMANCE", ""))
    if not tool.is_file() or not os.access(tool, os.X_OK):
        raise SystemExit("MFTIK_CONFORMANCE must be the mftiko-conformance binary")
    binary = Path(os.environ.get("MFTIK_CONFORM_BIN", REPO / "target" / "release" / "conform"))
    if not binary.is_file():
        raise SystemExit(f"MFTIK_CONFORM_BIN is not a file: {binary}")
    report_path = Path(os.environ.get("MFTIK_REPORT", "report-rootfs-rust.json")).resolve()
    work = Path(tempfile.mkdtemp(prefix="mftik-rust-rootfs-"))
    registry = None
    try:
        checkout = checkout_mftiko(work)
        port, registry = start_registry(work)
        home = work / "home"
        (home / ".docker").mkdir(parents=True)
        digest = pack(checkout, binary, port, home)
        pulled = check_image(checkout, work, port, digest)
        stop_registry(registry)
        registry = None
        rootfs = require_image(pulled, digest, binary.read_bytes(), sdk_version())
        entry = require_entrypoint(rootfs, pulled.get("entrypoint"))
        run_conformance(tool, rootfs, entry, report_path, work)
    except Exception:
        print(f"mftik rust rootfs conformance kept {work}", file=sys.stderr)
        raise
    else:
        shutil.rmtree(work, ignore_errors=True)
    finally:
        if registry is not None:
            stop_registry(registry)


def sdk_version() -> str:
    """``v`` and the workspace version, the replacement mftik-rt's build info names."""
    with (REPO / "Cargo.toml").open("rb") as handle:
        return "v" + tomllib.load(handle)["workspace"]["package"]["version"]


def pack(checkout: Path, binary: Path, port: int, home: Path) -> str:
    """Build the pinned mftiko-pack, then run it with ``home`` as ``HOME``.

    It is built first so ``go`` keeps its own module cache instead of filling
    ``home`` with read-only modules.
    """
    tool = home.parent / "mftiko-pack"
    run(["go", "build", "-o", str(tool), "./cmd/mftiko-pack"], cwd=checkout)
    cmd = [
        str(tool),
        "-bin",
        str(binary.resolve()),
        "-name",
        "conform",
        "-repo",
        f"127.0.0.1:{port}/mftik/conform",
        "-tag",
        "conform",
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


def check_image(checkout: Path, work: Path, port: int, digest: str) -> dict[str, object]:
    data = work / "artifact-data"
    data.mkdir()
    stdout = go(
        checkout,
        [
            "./cmd/mftik-pack-check",
            "-registry",
            f"127.0.0.1:{port}",
            "-repository",
            "mftik/conform",
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


def require_image(report: dict[str, object], digest: str, program: bytes, version: str) -> Path:
    """The manifest mftiko-pack writes, and a rootfs that holds only the binary.

    mftiko-pack writes ``runtime`` and ``sdk.lang`` as ``go`` for every binary
    (E08-D005); PROTOCOL §8.2 uses both for display only.
    """
    want: dict[str, object] = {
        "digest": digest,
        "kind": "strategy",
        "name": "conform",
        "os": "linux",
        "arch": "amd64",
        "contracts": CONTRACTS,
        "entrypoint": ENTRYPOINT,
        "env": {},
        "runtime": "go",
        "sdk_lang": "go",
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


if __name__ == "__main__":
    main()
