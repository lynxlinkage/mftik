"""Upload a docker-save tar to Strategon and apply the plane assignments.

Rehearsed by hand for v0.7.3 / v0.7.4: one OCI image, registered under each
plane name, then ApplyAssignment on cp (JP) and yite (TW). ApplyAssignment
replaces env entirely, so DATABASE_URL* must be in the process environment —
never in the spec file.

NATS cluster rollout is out of scope here.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

DEFAULT_URL = "https://s7n.lynkora.com/strategyplatform.v1.ControlPlaneService"
OCI = "ARTIFACT_TYPE_OCI_IMAGE"
MEMORY_BYTES = "805306368"


def rpc(base: str, token: str, method: str, body: dict[str, Any]) -> dict[str, Any]:
    req = urllib.request.Request(
        f"{base.rstrip('/')}/{method}",
        data=json.dumps(body).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Connect-Protocol-Version": "1",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        detail = e.read().decode()
        raise SystemExit(f"{method} HTTP {e.code}: {detail}") from e


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return "sha256:" + h.hexdigest()


def put_tar(put_url: str, tar: Path) -> None:
    size = tar.stat().st_size
    with tar.open("rb") as f:
        req = urllib.request.Request(
            put_url,
            data=f,
            method="PUT",
            headers={
                "Content-Type": "application/octet-stream",
                "Content-Length": str(size),
            },
        )
        try:
            with urllib.request.urlopen(req) as resp:
                code = resp.status
        except urllib.error.HTTPError as e:
            raise SystemExit(f"PUT artifact HTTP {e.code}: {e.read().decode()}") from e
    if code not in (200, 201):
        raise SystemExit(f"PUT artifact HTTP {code}")


def plane_env(spec: dict[str, Any], plane: dict[str, Any]) -> dict[str, str]:
    env = {str(k): str(v) for k, v in (spec.get("commonEnv") or {}).items()}
    env["NATS_URL"] = plane["nats"]
    for key in spec.get("secretKeysFromProdEnv") or ():
        value = os.environ.get(key, "")
        if not value:
            raise SystemExit(f"missing environment variable {key}")
        env[key] = value
    if plane.get("instance"):
        env["MFTIK_INSTANCE"] = plane["instance"]
    env.update({str(k): str(v) for k, v in (plane.get("env") or {}).items()})
    return env


def upload_and_register(
    base: str, token: str, tar: Path, version: str, names: list[str]
) -> str:
    digest = sha256_file(tar)
    print(f"tar {tar.name} {digest} ({tar.stat().st_size} bytes)", flush=True)
    created = rpc(
        base,
        token,
        "CreateArtifactUpload",
        {
            "name": names[0],
            "version": version,
            "digest": digest,
            "type": OCI,
        },
    )
    put_tar(created["putUrl"], tar)
    uri = created["s3Uri"]
    print(f"uploaded {uri}", flush=True)
    for name in names:
        rpc(
            base,
            token,
            "RegisterArtifact",
            {
                "artifact": {
                    "type": OCI,
                    "name": name,
                    "version": version,
                    "digest": digest,
                    "uri": uri,
                }
            },
        )
        print(f"registered {name} {version}", flush=True)
    return digest


def apply_planes(
    base: str, token: str, spec: dict[str, Any], version: str
) -> None:
    policy = spec["deployPolicy"]
    for plane in spec["planes"]:
        body = {
            "machineId": plane["machineId"],
            "strategy": plane["strategy"],
            "artifactVersion": version,
            "stopped": False,
            "args": plane["args"],
            "env": plane_env(spec, plane),
            "deployPolicy": policy,
            "limits": {"memoryBytes": MEMORY_BYTES},
        }
        out = rpc(base, token, "ApplyAssignment", body)
        print(
            f"applied {plane['strategy']} on {plane['machineId']} "
            f"gen={out.get('generation')}",
            flush=True,
        )


def _live(strategy: dict[str, Any]) -> bool:
    return any(
        c.get("type") == "Live" and c.get("status") == "CONDITION_STATUS_TRUE"
        for c in strategy.get("conditions") or []
    )


def wait_healthy(
    base: str, token: str, spec: dict[str, Any], version: str, timeout: int
) -> None:
    want = {(p["machineId"], p["strategy"]) for p in spec["planes"]}
    deadline = time.time() + timeout
    while True:
        leftover = set(want)
        for machine_id in {m for m, _ in want}:
            raw = rpc(base, token, "GetMachine", {"machineId": machine_id})
            machine = raw.get("machine") or raw
            for row in machine.get("strategies") or []:
                key = (machine_id, row.get("strategy"))
                if key not in leftover:
                    continue
                running = (row.get("runningArtifact") or {}).get("version")
                if running == version and _live(row):
                    leftover.discard(key)
                else:
                    print(
                        f"wait {key[1]} on {key[0]} "
                        f"phase={row.get('phase')} art={running} "
                        f"err={row.get('lastError')}",
                        flush=True,
                    )
        if not leftover:
            print("all planes live", flush=True)
            return
        if time.time() >= deadline:
            missing = ", ".join(f"{m}/{s}" for m, s in sorted(leftover))
            raise SystemExit(f"timed out waiting for {missing}")
        time.sleep(10)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tar", type=Path, required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument(
        "--spec",
        type=Path,
        default=Path("deployment/planes/assignments.json"),
    )
    parser.add_argument("--url", default=os.environ.get("STRATEGON_URL", DEFAULT_URL))
    parser.add_argument("--wait-seconds", type=int, default=180)
    args = parser.parse_args()

    token = os.environ.get("STRATEGON_API_KEY") or os.environ.get("S7N_TOKEN")
    if not token:
        raise SystemExit("STRATEGON_API_KEY is not set")
    if not args.tar.is_file():
        raise SystemExit(f"tar not found: {args.tar}")

    spec = json.loads(args.spec.read_text())
    names = [p["strategy"] for p in spec["planes"]]
    upload_and_register(args.url, token, args.tar, args.version, names)
    apply_planes(args.url, token, spec, args.version)
    if args.wait_seconds > 0:
        wait_healthy(args.url, token, spec, args.version, args.wait_seconds)


if __name__ == "__main__":
    sys.exit(main())
