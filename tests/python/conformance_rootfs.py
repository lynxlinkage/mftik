#!/usr/bin/env python3
"""Run mftiko-conformance on a packed strategy rootfs (E05-02, E05-D021–D025).

Packs ``examples/python`` (``conform:Conform``) with the wheel in
``MFTIK_PACK_WHEEL`` or the single ``mftik-*.whl`` under ``MFTIK_WHEEL_DIR``
(default ``dist``). The image is pushed to a ``registry:2`` on 127.0.0.1, then
pulled and unpacked with mftiko ``internal/platform/artifact`` at the E04-D076
commit. Conformance uses the rootfs as its working directory and the manifest
entrypoint as argv. ``PATH`` for that process is only the directory that
contains ``MFTIK_CONFORMANCE``.

The pinned harness replaces the worker environment and does not apply the
manifest ``PYTHONPATH`` (same limit as E05-01). After unpack, a
``mftik-conform.pth`` in the rootfs interpreter's site-packages adds the
manifest's ``PYTHONPATH`` entries. That file is not part of the image.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
# E04-D076. tests/python/pack_e2e.py uses this same commit.
MFTIKO_COMMIT = "88a03d9de1c0750a2e9be66a5fc83a2d6992ab84"
MFTIKO_URL = "https://github.com/BullionBear/mftiko.git"
CONTRACTS = ["worker.v1", "stsctl.v1", "md.v1", "td.v1"]
ENTRYPOINT = ["python/bin/python3", "-m", "mftik.run", "conform:Conform"]
CHECK_IDS = (
    "flow",
    "beat",
    "deadline",
    "shutdown",
    "buffer_priority",
    "prehook",
    "td_overflow",
    "cloid",
    "subject",
    "recon",
    "ctx",
)
HOST_PYTHONS = ("python", "python3", "python3.11", "python3.12", "python3.13")


def main() -> None:
    python = os.environ.get("MFTIK_PYTHON", "")
    if python not in ("3.11", "3.12", "3.13"):
        raise SystemExit("MFTIK_PYTHON must be 3.11, 3.12, or 3.13")
    tool = Path(os.environ.get("MFTIK_CONFORMANCE", ""))
    if not tool.is_file() or not os.access(tool, os.X_OK):
        raise SystemExit("MFTIK_CONFORMANCE must be the mftiko-conformance binary")
    report_path = Path(os.environ.get("MFTIK_REPORT", "report-rootfs.json")).resolve()
    wheel = wheel_path()
    work = Path(tempfile.mkdtemp(prefix="mftik-rootfs-conform-"))
    registry = None
    try:
        py = install_wheel(work, wheel, python)
        port, registry = start_registry(work)
        home = work / "home"
        home.mkdir()
        (home / ".docker").mkdir()
        digest = pack(py, python, wheel, port, home)
        pulled = check_image(work, port, digest)
        stop_registry(registry)
        registry = None
        rootfs = require_rootfs(pulled, digest)
        entry = require_manifest_entrypoint(rootfs, pulled.get("entrypoint"))
        add_strategy_path(rootfs, python, manifest_pythonpath(pulled))
        run_conformance(tool, rootfs, entry, report_path, work)
    except Exception:
        print(f"mftik rootfs conformance kept {work}", file=sys.stderr)
        raise
    else:
        shutil.rmtree(work, ignore_errors=True)
    finally:
        if registry is not None:
            stop_registry(registry)


def wheel_path() -> Path:
    given = os.environ.get("MFTIK_PACK_WHEEL", "")
    if given:
        path = Path(given)
        if not path.is_file():
            raise SystemExit(f"wheel is not a file: {path}")
        return path
    directory = Path(os.environ.get("MFTIK_WHEEL_DIR", "dist"))
    wheels = sorted(directory.glob("mftik-*.whl"))
    if len(wheels) != 1:
        raise SystemExit(f"expected one wheel in {directory}, got {wheels}")
    return wheels[0]


def install_wheel(work: Path, wheel: Path, python: str) -> Path:
    run(["uv", "python", "install", python], cwd=REPO)
    venv = work / "venv"
    run(["uv", "venv", "--python", python, str(venv)])
    py = venv / "bin" / "python"
    run(["uv", "pip", "install", "--python", str(py), str(wheel)])
    return py


def start_registry(work: Path) -> tuple[int, subprocess.Popen[bytes]]:
    port = free_port()
    binary = os.environ.get("MFTIK_REGISTRY_BIN", "")
    if binary:
        data = work / "registry-data"
        data.mkdir()
        config = work / "registry.yml"
        config.write_text(
            "version: 0.1\n"
            "log:\n"
            "  level: error\n"
            "storage:\n"
            "  filesystem:\n"
            f"    rootdirectory: {data}\n"
            "http:\n"
            f"  addr: 127.0.0.1:{port}\n",
            encoding="utf-8",
        )
        proc = subprocess.Popen([binary, "serve", str(config)])
    elif shutil.which("docker"):
        name = f"mftik-rootfs-registry-{port}"
        proc = subprocess.Popen(
            [
                "docker",
                "run",
                "--rm",
                "--name",
                name,
                "-p",
                f"127.0.0.1:{port}:5000",
                "registry:2",
            ]
        )
        proc.docker_name = name  # type: ignore[attr-defined]
    else:
        raise SystemExit("docker or MFTIK_REGISTRY_BIN is required")
    wait_registry(port, proc)
    return port, proc


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_registry(port: int, proc: subprocess.Popen[bytes]) -> None:
    url = f"http://127.0.0.1:{port}/v2/"
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise SystemExit(f"registry exited {proc.returncode}")
        try:
            with opener.open(url, timeout=1) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, TimeoutError, OSError):
            time.sleep(0.2)
    raise SystemExit("registry did not listen")


def stop_registry(proc: subprocess.Popen[bytes]) -> None:
    name = getattr(proc, "docker_name", "")
    if name:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)
        return
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


def pack(py: Path, python: str, wheel: Path, port: int, home: Path) -> str:
    cmd = [
        str(py),
        "-m",
        "mftik.pack",
        "-dir",
        str(REPO / "examples" / "python"),
        "-target",
        "conform:Conform",
        "-name",
        "conform",
        "-repo",
        f"127.0.0.1:{port}/mftik/conform",
        "-tag",
        "conform",
        "-plain-http",
        "-python",
        python,
        "-wheel",
        str(wheel),
    ]
    env = os.environ.copy()
    env["HOME"] = str(home)
    env["DOCKER_CONFIG"] = str(home / ".docker")
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)
    if proc.returncode != 0:
        raise SystemExit(f"mftik.pack failed\n{proc.stderr}")
    lines = [line for line in proc.stdout.splitlines() if line]
    if len(lines) != 1 or not lines[0].startswith("sha256:"):
        raise SystemExit(f"stdout was {proc.stdout!r}")
    print(lines[0], file=sys.stderr)
    return lines[0]


def check_image(work: Path, port: int, digest: str) -> dict[str, object]:
    checkout = work / "mftiko"
    run(["git", "init", str(checkout)])
    run(["git", "-C", str(checkout), "remote", "add", "origin", MFTIKO_URL])
    run(["git", "-C", str(checkout), "fetch", "--depth", "1", "origin", MFTIKO_COMMIT])
    run(["git", "-C", str(checkout), "checkout", "--detach", "FETCH_HEAD"])
    dest = checkout / "cmd" / "mftik-pack-check"
    dest.mkdir()
    shutil.copy(REPO / "tests" / "pack" / "check" / "main.go", dest / "main.go")
    data = work / "artifact-data"
    data.mkdir()
    proc = subprocess.run(
        [
            "go",
            "run",
            "./cmd/mftik-pack-check",
            "-registry",
            f"127.0.0.1:{port}",
            "-repository",
            "mftik/conform",
            "-digest",
            digest,
            "-data",
            str(data),
        ],
        cwd=checkout,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise SystemExit(f"mftik-pack-check failed\n{proc.stderr}")
    parsed = json.loads(proc.stdout)
    if not isinstance(parsed, dict):
        raise SystemExit(f"check report {proc.stdout!r}")
    return parsed


def require_rootfs(report: dict[str, object], digest: str) -> Path:
    if report.get("digest") != digest:
        raise SystemExit(f"digest {report.get('digest')!r}")
    if report.get("contracts") != CONTRACTS:
        raise SystemExit(f"contracts {report.get('contracts')!r}")
    if report.get("runtime") != "python" or report.get("kind") != "strategy":
        raise SystemExit(f"runtime {report.get('runtime')!r} kind {report.get('kind')!r}")
    rootfs = Path(str(report.get("rootfs", "")))
    if not rootfs.is_dir():
        raise SystemExit(f"rootfs {rootfs}")
    strategy = rootfs / "app" / "conform" / "__init__.py"
    if not strategy.is_file():
        raise SystemExit(f"packed strategy missing {strategy}")
    return rootfs


def manifest_pythonpath(report: dict[str, object]) -> str:
    env = report.get("env")
    if not isinstance(env, dict):
        raise SystemExit(f"env {env!r}")
    pythonpath = env.get("PYTHONPATH")
    if pythonpath != "app:site-packages":
        raise SystemExit(f"PYTHONPATH {pythonpath!r}")
    return str(pythonpath)


def require_entrypoint(rootfs: Path, entrypoint: object) -> list[str]:
    """Argv whose program is an executable file inside ``rootfs``.

    The first element must be relative, with no empty, ``.``, or ``..``
    segment. The lexical path and its symlink target both stay under the
    rootfs (PROTOCOL §4.6 checks the path literally; a symlink to the host
    interpreter is still a host Python).
    """
    if not isinstance(entrypoint, list) or not entrypoint:
        raise SystemExit(f"entrypoint {entrypoint!r}")
    if any(not isinstance(part, str) or not part for part in entrypoint):
        raise SystemExit(f"entrypoint {entrypoint!r}")
    exe = entrypoint[0]
    parts = exe.split("/")
    if exe.startswith("/") or any(part in ("", ".", "..") for part in parts):
        raise SystemExit("entrypoint is not a path inside the rootfs")
    lexical = rootfs.joinpath(*parts)
    try:
        lexical.relative_to(rootfs)
    except ValueError:
        raise SystemExit("entrypoint is not a path inside the rootfs") from None
    if not lexical.is_file() or not os.access(lexical, os.X_OK):
        raise SystemExit("entrypoint is not a path inside the rootfs")
    root = rootfs.resolve()
    real = lexical.resolve()
    if root != real and root not in real.parents:
        raise SystemExit("entrypoint is not a path inside the rootfs")
    return [str(part) for part in entrypoint]


def require_manifest_entrypoint(rootfs: Path, entrypoint: object) -> list[str]:
    """``require_entrypoint`` plus the packed conform argv."""
    got = require_entrypoint(rootfs, entrypoint)
    if got != ENTRYPOINT:
        raise SystemExit(f"entrypoint {got!r}")
    print(f"entrypoint {got[0]} is inside {rootfs}", file=sys.stderr)
    return got


def add_strategy_path(rootfs: Path, python: str, pythonpath: str) -> None:
    """Point the unpacked interpreter at the manifest ``PYTHONPATH``.

    ``mftiko-conformance`` does not forward ``PYTHONPATH``. The ``.pth`` is
    written after unpack and is not in the image. STS applies the manifest
    environment itself.
    """
    parts = pythonpath.split(":")
    if any(not part or part.startswith("/") or ".." in part.split("/") for part in parts):
        raise SystemExit(f"PYTHONPATH {pythonpath!r}")
    site = rootfs / "python" / "lib" / f"python{python}" / "site-packages"
    if not site.is_dir():
        raise SystemExit(f"interpreter site-packages missing: {site}")
    lines: list[str] = []
    root = rootfs.resolve()
    for part in parts:
        target = rootfs.joinpath(*part.split("/"))
        if not target.is_dir():
            raise SystemExit(f"PYTHONPATH entry missing: {target}")
        rel = Path(os.path.relpath(target, start=site)).as_posix()
        got = Path(os.path.abspath(os.path.join(os.fspath(site), rel))).resolve()
        if root != got and root not in got.parents:
            raise SystemExit(f"{rel} left the rootfs")
        if got != target.resolve():
            raise SystemExit(f"{rel} resolved to {got}, want {target}")
        lines.append(rel)
    text = "".join(f"{line}\n" for line in lines)
    (site / "mftik-conform.pth").write_text(text, encoding="utf-8")


def tool_dir_has_no_python(tool_dir: Path) -> None:
    for name in HOST_PYTHONS:
        found = shutil.which(name, path=str(tool_dir))
        if found:
            raise SystemExit(f"{name} is on the conformance PATH: {found}")
    if shutil.which("mftiko-conformance", path=str(tool_dir)) is None:
        raise SystemExit(f"mftiko-conformance is not on PATH {tool_dir}")


def run_conformance(
    tool: Path,
    rootfs: Path,
    entry: list[str],
    report_path: Path,
    work: Path,
) -> None:
    tool_dir = tool.resolve().parent
    tool_dir_has_no_python(tool_dir)
    home = work / "conform-home"
    home.mkdir()
    env = {
        "PATH": str(tool_dir),
        "HOME": str(home),
        "TMPDIR": os.environ.get("TMPDIR", "/tmp"),
    }
    cmd = [str(tool.resolve()), "-json", str(report_path), "--", *entry]
    print("+", "PATH=" + env["PATH"], "cwd=" + str(rootfs), " ".join(cmd), file=sys.stderr)
    proc = subprocess.run(cmd, cwd=rootfs, env=env, check=False)
    if not report_path.is_file():
        raise SystemExit(f"mftiko-conformance exited {proc.returncode} without a report")
    assert_conformance_report(json.loads(report_path.read_text(encoding="utf-8")))
    if proc.returncode != 0:
        raise SystemExit(f"mftiko-conformance exited {proc.returncode}")


def assert_conformance_report(report: dict[str, object]) -> None:
    checks = report.get("checks")
    if not isinstance(checks, list) or len(checks) != len(CHECK_IDS):
        raise SystemExit(f"report checks {checks!r}")
    for row, want in zip(checks, CHECK_IDS, strict=True):
        if not isinstance(row, dict) or row.get("id") != want or row.get("status") != "pass":
            raise SystemExit(f"check {row!r}")


def run(cmd: list[str], *, cwd: Path | None = None) -> None:
    print("+", " ".join(cmd), file=sys.stderr)
    proc = subprocess.run(cmd, cwd=cwd, check=False)
    if proc.returncode != 0:
        raise SystemExit(f"command failed ({proc.returncode}): {' '.join(cmd)}")


if __name__ == "__main__":
    main()
