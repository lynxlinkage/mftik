#!/usr/bin/env python3
"""Pack one strategy and run the unpacked rootfs (E04-D076).

Builds the mftik wheel, pushes the same image twice to a registry on
127.0.0.1, checks it with mftiko ``internal/platform/artifact`` at
``dd73a2d421ca``, then runs Init, Start, and Stop through the fake shim.
Nothing is pushed to a host other than 127.0.0.1.
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
import zipfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
MFTIKO_COMMIT = "a9414bcc74a1dc3df6042d267cc094486834c849"
MFTIKO_URL = "https://github.com/BullionBear/mftiko.git"
CONTRACTS = ["worker.v1", "stsctl.v1", "md.v1", "td.v1"]


def main() -> None:
    if not os.environ.get("MFTIK_NATS_SERVER"):
        raise SystemExit("MFTIK_NATS_SERVER is required")
    work = Path(tempfile.mkdtemp(prefix="mftik-pack-e2e-"))
    registry = None
    try:
        if os.environ.get("MFTIK_PACK_WHEEL"):
            wheel = Path(os.environ["MFTIK_PACK_WHEEL"])
        else:
            wheel = build_wheel(work)
        py = install_wheel(work, wheel)
        strategy, requirements = write_strategy(work)
        port, registry = start_registry(work)
        home = work / "home"
        home.mkdir()
        (home / ".docker").mkdir()
        digest = pack(py, strategy, requirements, wheel, port, home)
        again = pack(py, strategy, requirements, wheel, port, home)
        if digest != again:
            raise SystemExit(f"digest changed\n{digest}\n{again}")
        report = check_image(work, port, digest)
        stop_registry(registry)
        registry = None
        assert_report(report, digest)
        launch = write_launch(work, report)
        run_worker(launch)
    except Exception:
        print(f"mftik-pack e2e kept {work}", file=sys.stderr)
        raise
    else:
        shutil.rmtree(work, ignore_errors=True)
    finally:
        if registry is not None:
            stop_registry(registry)


def build_wheel(work: Path) -> Path:
    out = work / "dist"
    out.mkdir()
    run(["uv", "python", "install", "3.11"], cwd=REPO)
    run(
        [
            "uv",
            "sync",
            "--frozen",
            "--only-dev",
            "--no-install-project",
            "--managed-python",
            "--python",
            "3.11",
        ],
        cwd=REPO,
    )
    run(
        ["uv", "run", "--frozen", "--no-sync", "maturin", "build", "--release", "--out", str(out)],
        cwd=REPO,
    )
    wheels = sorted(out.glob("mftik-*.whl"))
    if len(wheels) != 1:
        raise SystemExit(f"expected one wheel, got {wheels}")
    return wheels[0]


def install_wheel(work: Path, wheel: Path) -> Path:
    venv = work / "venv"
    run(["uv", "venv", "--python", "3.11", str(venv)])
    py = venv / "bin" / "python"
    run(["uv", "pip", "install", "--python", str(py), str(wheel), "pip"])
    return py


def write_strategy(work: Path) -> tuple[Path, Path]:
    strategy = work / "strategy"
    strategy.mkdir()
    (strategy / "hello.py").write_text(
        "import packsample\n"
        "import shutil\n"
        "import sys\n"
        "\n"
        "from mftik import Strategy\n"
        "\n"
        "\n"
        "class Hello(Strategy):\n"
        "    def on_init(self, ctx):\n"
        "        if packsample.marker() != 'pack-dep':\n"
        "            raise AssertionError(packsample.marker())\n"
        "        print('PACK-init', flush=True)\n"
        "        print('EXE', sys.executable, flush=True)\n"
        "        print('PREFIX', sys.prefix, flush=True)\n"
        "        print('WHICH', shutil.which('python3'), flush=True)\n"
        "\n"
        "    def on_start(self, ctx):\n"
        "        print('PACK-start', flush=True)\n"
        "\n"
        "    def on_stop(self, ctx):\n"
        "        print('PACK-stop', flush=True)\n",
        encoding="utf-8",
    )
    wheel_path = work / "packsample-1.0.0-py3-none-any.whl"
    payload = {
        "packsample/__init__.py": b"def marker():\n    return 'pack-dep'\n",
        "packsample-1.0.0.dist-info/METADATA": (
            b"Metadata-Version: 2.1\nName: packsample\nVersion: 1.0.0\n\n"
        ),
        "packsample-1.0.0.dist-info/WHEEL": (
            b"Wheel-Version: 1.0\nGenerator: mftik-pack-e2e\n"
            b"Root-Is-Purelib: true\nTag: py3-none-any\n"
        ),
    }
    with zipfile.ZipFile(wheel_path, "w") as zf:
        for name, data in payload.items():
            zf.writestr(name, data)
    requirements = work / "requirements.txt"
    requirements.write_text(f"{wheel_path}\n", encoding="utf-8")
    return strategy, requirements


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
        name = f"mftik-pack-registry-{port}"
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


def pack(py: Path, strategy: Path, requirements: Path, wheel: Path, port: int, home: Path) -> str:
    cmd = [
        str(py),
        "-m",
        "mftik.pack",
        "-dir",
        str(strategy),
        "-requirements",
        str(requirements),
        "-target",
        "hello:Hello",
        "-name",
        "pack-hello",
        "-repo",
        f"127.0.0.1:{port}/mftik/pack-hello",
        "-tag",
        "pack",
        "-env",
        "PACK_MARK=pack-mark",
        "-plain-http",
        "-python",
        "3.12",
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
            "mftik/pack-hello",
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
    return json.loads(proc.stdout)


def assert_report(report: dict[str, object], digest: str) -> None:
    expect = {
        "media_type": "application/vnd.mftiko.worker.config.v1+json",
        "schema": "mftiko.worker.v1",
        "kind": "strategy",
        "name": "pack-hello",
        "runtime": "python",
        "os": "linux",
        "arch": "amd64",
        "sdk_lang": "python",
        "sdk_version": "1.0.0",
        "digest": digest,
    }
    for key, value in expect.items():
        if report.get(key) != value:
            raise SystemExit(f"{key} is {report.get(key)!r}")
    if report.get("contracts") != CONTRACTS:
        raise SystemExit(f"contracts {report.get('contracts')!r}")
    entry = report.get("entrypoint")
    if entry != ["python/bin/python3", "-m", "mftik.run", "hello:Hello"]:
        raise SystemExit(f"entrypoint {entry!r}")
    env = report.get("env")
    if not isinstance(env, dict):
        raise SystemExit(f"env {env!r}")
    if env != {
        "MFTIK_RELEASE": "1.0.0",
        "PACK_MARK": "pack-mark",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONPATH": "app:site-packages",
    }:
        raise SystemExit(f"env {env!r}")
    layer_bytes = report.get("layer_bytes")
    if not isinstance(layer_bytes, int) or layer_bytes < 5_000_000:
        raise SystemExit(f"layer_bytes {layer_bytes!r}")
    rootfs = Path(str(report["rootfs"]))
    pyc = next((rootfs / "app" / "__pycache__").glob("hello.cpython-312.pyc"))
    flags = int.from_bytes(pyc.read_bytes()[4:8], "little")
    if flags & 0b11 != 0b1:
        raise SystemExit(f"pyc flags {flags:#x}")
    if not (rootfs / "site-packages" / "packsample" / "__init__.py").is_file():
        raise SystemExit("dependency was not installed")
    sdk = rootfs / "python" / "lib" / "python3.12" / "site-packages" / "mftik"
    if not sdk.is_dir() or not any(sdk.glob("*.so")):
        raise SystemExit("mftik wheel was not installed")
    for dirpath, dirnames, filenames in os.walk(rootfs, followlinks=False):
        for name in [*dirnames, *filenames]:
            path = Path(dirpath) / name
            if not path.is_symlink():
                continue
            target = os.readlink(path)
            if ".." in target.split("/"):
                raise SystemExit(f"unsafe symlink {path} -> {target}")


def write_launch(work: Path, report: dict[str, object]) -> Path:
    env = report["env"]
    entry = report["entrypoint"]
    if not isinstance(env, dict) or not isinstance(entry, list):
        raise SystemExit("report is incomplete")
    lines = [
        f"rootfs\t{report['rootfs']}",
        f"program\t{entry[0]}",
        f"release\t{env['MFTIK_RELEASE']}",
    ]
    lines.extend(f"arg\t{arg}" for arg in entry[1:])
    lines.extend(f"env\t{key}\t{value}" for key, value in env.items())
    path = work / "launch.txt"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def run_worker(launch: Path) -> None:
    env = os.environ.copy()
    env["MFTIK_PACK_LAUNCH"] = str(launch)
    run(
        [
            "cargo",
            "test",
            "-p",
            "mftik-py",
            "--test",
            "pack",
            "--",
            "--ignored",
            "--test-threads=1",
            "unpacked_rootfs_runs_without_host_python",
        ],
        env=env,
        cwd=REPO,
    )


def run(cmd: list[str], *, env: dict[str, str] | None = None, cwd: Path | None = None) -> None:
    print("+", " ".join(cmd), file=sys.stderr)
    proc = subprocess.run(cmd, env=env, cwd=cwd, check=False)
    if proc.returncode != 0:
        raise SystemExit(f"command failed ({proc.returncode}): {' '.join(cmd)}")


if __name__ == "__main__":
    main()
