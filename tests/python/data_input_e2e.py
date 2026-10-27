#!/usr/bin/env python3
"""Pack a strategy that reads a data input, and run it from the rootfs (#455).

Pushes a model directory with the pinned ``mftiko-pack data``, and packs
``examples/python/pretrained`` with ``mftik.pack``, both to a registry on
127.0.0.1. The pinned mftiko ``internal/platform/artifact`` pulls both: the
image is unpacked as in ``pack_e2e.py``, and the data artifact read-only
under ``sessions/<id>/inputs/model``, as the STS agent stages it. The worker
then runs Init, Start, and Stop through the fake shim with
``MFTIKO_INPUT_DIR`` set. The pin and the helpers are ``pack_e2e.py``'s.
Nothing is pushed to a host other than 127.0.0.1.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
from pathlib import Path

from pack_e2e import (
    MFTIKO_COMMIT,
    MFTIKO_URL,
    REPO,
    build_wheel,
    install_wheel,
    run,
    start_registry,
    stop_registry,
    write_launch,
)

STRATEGY = REPO / "examples" / "python" / "pretrained"
WEIGHTS = b'{"bias":0.5,"scale":[1,2,3]}\n'


def main() -> None:
    if not os.environ.get("MFTIK_NATS_SERVER"):
        raise SystemExit("MFTIK_NATS_SERVER is required")
    work = Path(tempfile.mkdtemp(prefix="mftik-data-input-"))
    registry = None
    try:
        if os.environ.get("MFTIK_PACK_WHEEL"):
            wheel = Path(os.environ["MFTIK_PACK_WHEEL"])
        else:
            wheel = build_wheel(work)
        py = install_wheel(work, wheel)
        model = write_model(work)
        checkout = checkout_mftiko(work)
        port, registry = start_registry(work)
        home = work / "home"
        (home / ".docker").mkdir(parents=True)
        data_digest = push_data(checkout, model, port, home)
        again = push_data(checkout, model, port, home)
        if data_digest != again:
            raise SystemExit(f"data digest changed\n{data_digest}\n{again}")
        image = pack(py, wheel, port, home)
        report = check_image(checkout, work, port, image)
        inputs = work / "sts" / "sessions" / "s1" / "inputs"
        check_data(checkout, work, port, data_digest, inputs / "model")
        stop_registry(registry)
        registry = None
        assert_input_tree(model, inputs / "model")
        launch = write_launch(work, report)
        with launch.open("a", encoding="utf-8") as handle:
            handle.write(f"input_dir\t{inputs}\n")
            handle.write(f"sha256\t{hashlib.sha256(WEIGHTS).hexdigest()}\n")
        run_worker(launch)
    except Exception:
        print(f"mftik data-input e2e kept {work}", file=sys.stderr)
        raise
    else:
        remove_tree(work)
    finally:
        if registry is not None:
            stop_registry(registry)


def write_model(work: Path) -> Path:
    model = work / "model"
    (model / "tokenizer").mkdir(parents=True)
    (model / "weights.json").write_bytes(WEIGHTS)
    (model / "tokenizer" / "vocab.txt").write_text("buy\nsell\n", encoding="utf-8")
    return model


def checkout_mftiko(work: Path) -> Path:
    checkout = work / "mftiko"
    run(["git", "init", str(checkout)])
    run(["git", "-C", str(checkout), "remote", "add", "origin", MFTIKO_URL])
    run(["git", "-C", str(checkout), "fetch", "--depth", "1", "origin", MFTIKO_COMMIT])
    run(["git", "-C", str(checkout), "checkout", "--detach", "FETCH_HEAD"])
    for name, source in (("mftik-pack-check", "check"), ("mftik-data-check", "data")):
        dest = checkout / "cmd" / name
        dest.mkdir()
        shutil.copy(REPO / "tests" / "pack" / source / "main.go", dest / "main.go")
    return checkout


def go(checkout: Path, args: list[str], home: Path | None = None) -> str:
    env = os.environ.copy()
    if home is not None:
        env["HOME"] = str(home)
        env["DOCKER_CONFIG"] = str(home / ".docker")
    print("+ go run", " ".join(args), file=sys.stderr)
    proc = subprocess.run(
        ["go", "run", *args],
        cwd=checkout,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise SystemExit(f"go run {args[0]} failed\n{proc.stderr}")
    return proc.stdout


def push_data(checkout: Path, model: Path, port: int, home: Path) -> str:
    stdout = go(
        checkout,
        [
            "./cmd/mftiko-pack",
            "data",
            "-dir",
            str(model),
            "-repo",
            f"127.0.0.1:{port}/mftik/model",
            "-tag",
            "v1",
            "-plain-http",
        ],
        home,
    )
    lines = [line for line in stdout.splitlines() if line]
    if len(lines) != 1 or not lines[0].startswith("sha256:"):
        raise SystemExit(f"mftiko-pack data stdout was {stdout!r}")
    print(lines[0], file=sys.stderr)
    return lines[0]


def pack(py: Path, wheel: Path, port: int, home: Path) -> str:
    cmd = [
        str(py),
        "-m",
        "mftik.pack",
        "-dir",
        str(STRATEGY),
        "-target",
        "pretrained:Pretrained",
        "-name",
        "pretrained",
        "-repo",
        f"127.0.0.1:{port}/mftik/pretrained",
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
    return lines[0]


def check_image(checkout: Path, work: Path, port: int, digest: str) -> dict[str, object]:
    data = work / "artifact-data"
    data.mkdir(exist_ok=True)
    stdout = go(
        checkout,
        [
            "./cmd/mftik-pack-check",
            "-registry",
            f"127.0.0.1:{port}",
            "-repository",
            "mftik/pretrained",
            "-digest",
            digest,
            "-data",
            str(data),
        ],
    )
    report = json.loads(stdout)
    entry = report.get("entrypoint")
    if entry != ["python/bin/python3", "-m", "mftik.run", "pretrained:Pretrained"]:
        raise SystemExit(f"entrypoint {entry!r}")
    return report


def check_data(checkout: Path, work: Path, port: int, digest: str, dest: Path) -> None:
    data = work / "artifact-data"
    data.mkdir(exist_ok=True)
    stdout = go(
        checkout,
        [
            "./cmd/mftik-data-check",
            "-registry",
            f"127.0.0.1:{port}",
            "-repository",
            "mftik/model",
            "-digest",
            digest,
            "-data",
            str(data),
            "-dest",
            str(dest),
        ],
    )
    report = json.loads(stdout)
    expect = {"digest": digest, "schema": "mftiko.data.v1", "has_source": False}
    for key, value in expect.items():
        if report.get(key) != value:
            raise SystemExit(f"data {key} is {report.get(key)!r}")


def assert_input_tree(source: Path, unpacked: Path) -> None:
    """Same entries and bytes as the uploaded directory; files 0444, directories 0555."""
    want = sorted(p.relative_to(source) for p in source.rglob("*"))
    got = sorted(p.relative_to(unpacked) for p in unpacked.rglob("*"))
    if want != got:
        raise SystemExit(f"input entries {got} != {want}")
    for rel in want:
        src, dst = source / rel, unpacked / rel
        mode = stat.S_IMODE(dst.lstat().st_mode)
        if src.is_dir():
            if mode != 0o555:
                raise SystemExit(f"{rel} mode {mode:o}")
        else:
            if mode != 0o444:
                raise SystemExit(f"{rel} mode {mode:o}")
            if dst.read_bytes() != src.read_bytes():
                raise SystemExit(f"{rel} differs")
    if stat.S_IMODE(unpacked.lstat().st_mode) != 0o555:
        raise SystemExit("input root is writable")


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
            "--exact",
            "--test-threads=1",
            "unpacked_rootfs_reads_a_data_input",
        ],
        env=env,
        cwd=REPO,
    )


def remove_tree(path: Path) -> None:
    """The input is read-only, so its directories are made writable before removal."""
    for dirpath, _dirnames, _filenames in os.walk(path):
        os.chmod(dirpath, 0o755)
    shutil.rmtree(path, ignore_errors=True)


if __name__ == "__main__":
    main()
