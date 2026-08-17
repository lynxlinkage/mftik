"""Delete old container versions from GHCR, keeping the newest N tagged ones.

A multi-architecture image is not one thing in the registry. It is a tagged
*index* pointing at one untagged manifest per architecture, and those children
are what a pull actually fetches. So "delete everything untagged" — the obvious
rule, and the one most cleanup snippets use — hollows out every index you kept:
the tag still resolves, and pulling it fails on a missing manifest.

This resolves each kept index and keeps its children with it. Anything not
reachable from a kept tag goes.

Run with --dry-run first. Deleting a version cannot be undone.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from typing import Any


def gh_api(path: str, *, method: str = "GET", paginate: bool = False) -> Any:
    cmd = ["gh", "api", "-X", method, path]
    if paginate:
        cmd.append("--paginate")
        # Without this each page is its own JSON document and the result is
        # not parseable as one.
        cmd += ["--slurp"]
    out = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if out.returncode != 0:
        raise RuntimeError(f"gh api {method} {path} failed: {out.stderr.strip()}")
    if not out.stdout.strip():
        return None
    body = json.loads(out.stdout)
    if paginate and isinstance(body, list):
        # --slurp gives a list of pages; flatten it.
        return [item for page in body for item in page]
    return body


def children_of(image: str, digest: str) -> set[str]:
    """The per-architecture manifests a multi-arch index points at.

    A single-architecture image is its own manifest and has none, which is not
    an error — it is what a registry that was pushed for one platform looks
    like.
    """
    out = subprocess.run(
        ["docker", "buildx", "imagetools", "inspect", "--raw", f"{image}@{digest}"],
        capture_output=True,
        text=True,
        check=False,
    )
    if out.returncode != 0:
        # Reachability is what decides deletion, so a manifest that cannot be
        # read has to count as reachable. Guessing "no children" here would
        # delete the architectures of an image we were asked to keep.
        raise RuntimeError(
            f"cannot read manifest {image}@{digest}: {out.stderr.strip()}"
        )
    try:
        body = json.loads(out.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{image}@{digest} is not JSON: {exc}") from exc
    return {m["digest"] for m in body.get("manifests", []) if "digest" in m}


def prune(owner: str, package: str, keep: int, dry_run: bool) -> int:
    image = f"ghcr.io/{owner}/{package}"
    versions = gh_api(
        f"/orgs/{owner}/packages/container/{package}/versions", paginate=True
    )
    if not versions:
        print(f"{package}: no versions")
        return 0

    def tags(v: dict) -> list[str]:
        return (v.get("metadata") or {}).get("container", {}).get("tags") or []

    tagged = sorted(
        (v for v in versions if tags(v)),
        key=lambda v: v["created_at"],
        reverse=True,
    )
    keeping = tagged[:keep]
    print(f"{package}: {len(versions)} versions, {len(tagged)} tagged")

    reachable: set[str] = set()
    for v in keeping:
        reachable.add(v["name"])
        reachable |= children_of(image, v["name"])

    doomed = [v for v in versions if v["name"] not in reachable]
    for v in doomed:
        label = ",".join(tags(v)) or "<untagged>"
        print(f"  {'would delete' if dry_run else 'deleting'} {label} {v['name'][:19]}")
        if not dry_run:
            gh_api(
                f"/orgs/{owner}/packages/container/{package}/versions/{v['id']}",
                method="DELETE",
            )
    kept_tags = ",".join(t for v in keeping for t in tags(v))
    print(f"  kept {len(reachable)} versions across {len(keeping)} tags: {kept_tags}")
    return len(doomed)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--owner", required=True)
    parser.add_argument("--package", action="append", required=True)
    parser.add_argument("--keep", type=int, default=10)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.keep < 1:
        print("--keep must be at least 1", file=sys.stderr)
        return 2

    total = 0
    for package in args.package:
        total += prune(args.owner, package, args.keep, args.dry_run)
    verb = "would delete" if args.dry_run else "deleted"
    print(f"\n{verb} {total} version(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
