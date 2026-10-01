"""Turn one CI run into appendix C — B0-02 (#155), one-off.

Reads the `--junitxml` of an uninstrumented `just test` run (the authoritative
durations) and, optionally, the JSON that `pytest_cost_probe` writes from a
second instrumented run (what the time was spent on). Prints Markdown: totals,
every test module's duration, the fifty slowest tests with their breakdown, and
the suite-wide NATS share that F31 turns on.

The per-test "cause" column is a *proposal* from the measured breakdown. The
ticket requires the tag in appendix C to come from reading the test, so the
numbers are printed beside it and the tag is written by hand.

    python scripts/baseline_durations.py --junit junit.xml --probe probe.json \
        --wall 231 > report.md
"""

from __future__ import annotations

import argparse
import json
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path
from typing import Any

#: A bucket has to account for this much of a test to be named as its cause.
#: Below it the breakdown is not saying anything a reader should trust.
SHARE = 0.5

#: What `pytest_cost_probe` charges time to. The three NATS ones are reported
#: apart because connecting, asking and publishing are three different claims
#: about why the suite is slow, and F31 only addresses the first.
BUCKETS = (
    "nats_connect",
    "nats_request",
    "nats_other",
    "sleep",
    "sleep_bg",
    "subprocess",
    "pg_connect",
    "timeout",
    "timeout_bg",
)
NATS_BUCKETS = ("nats_connect", "nats_request", "nats_other")

#: The broker's own re-ask loop. A request to a subject nobody serves sleeps
#: `_NO_RESPONDERS_GRACE_S` and asks again until its share of the caller's
#: timeout is spent, and that sleep is *outside* `Client.request` — so it lands
#: in `sleep`, not in any `nats_*` bucket, and a NATS total without it is
#: wrong. Matched on the tail of the call site, which the probe reports
#: relative to the repository root.
REASK_SITE = "broker/transport/nats.py:268"


def is_reask(site: str) -> bool:
    return site.endswith(REASK_SITE)


def parse_junit(path: Path) -> list[dict[str, Any]]:
    root = ET.parse(path).getroot()
    tests = []
    for case in root.iter("testcase"):
        name = case.get("name", "")
        # `file` is an xunit1 attribute; `junit_family=xunit2`, the default,
        # drops it and leaves only the dotted module in `classname`.
        file = case.get("file") or (
            case.get("classname", "").replace(".", "/") + ".py"
        )
        tests.append(
            {
                "nodeid": f"{file}::{name}",
                "file": file,
                "name": name,
                "time": float(case.get("time", 0.0)),
                "skipped": case.find("skipped") is not None,
            }
        )
    return tests


def junit_totals(path: Path) -> dict[str, Any]:
    root = ET.parse(path).getroot()
    suite = root if root.tag == "testsuite" else root.find("testsuite")
    assert suite is not None
    return {
        "tests": int(suite.get("tests", 0)),
        "failures": int(suite.get("failures", 0)),
        "errors": int(suite.get("errors", 0)),
        "skipped": int(suite.get("skipped", 0)),
        "time": float(suite.get("time", 0.0)),
    }


def probe_index(path: Path) -> dict[str, dict[str, Any]]:
    """Probe records keyed the way :func:`parse_junit` keys its own.

    A pytest nodeid is already ``path/to/test_x.py::test_y[param]``, and the
    junit side spells those two halves separately — so the keys meet as long as
    both runs had the same rootdir, which in CI they do.
    """
    data = json.loads(path.read_text())
    return {record["nodeid"]: record for record in data["tests"]}


def reask_seconds(record: dict[str, Any]) -> float:
    """How long this test spent in the broker's no-responders re-ask loop."""
    return sum(
        sec for _, site, sec in record.get("wait_sites", []) if is_reask(site)
    )


def dominant(record: dict[str, Any], total: float) -> str:
    """The bucket that accounts for most of a test, or ``other``."""
    seconds = record.get("seconds", {})
    nats = sum(seconds.get(k, 0.0) for k in NATS_BUCKETS) + reask_seconds(record)
    own_reask = sum(
        sec
        for bucket, site, sec in record.get("wait_sites", [])
        if is_reask(site) and bucket == "sleep"
    )
    candidates = {
        # Minus the re-ask loop, which is a sleep but is NATS's bill.
        "sleep": seconds.get("sleep", 0.0) - own_reask,
        "timeout": seconds.get("timeout", 0.0),
        "nats": nats,
        "subprocess": seconds.get("subprocess", 0.0),
        "postgres": seconds.get("pg_connect", 0.0),
    }
    bucket, value = max(candidates.items(), key=lambda pair: pair[1])
    if total <= 0 or value / total < SHARE:
        return "other"
    return bucket


def table(rows: list[list[str]], header: list[str]) -> str:
    lines = ["| " + " | ".join(header) + " |"]
    lines.append("|" + "|".join("---" for _ in header) + "|")
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--junit", type=Path, required=True)
    ap.add_argument("--probe", type=Path)
    ap.add_argument("--probe-junit", type=Path)
    ap.add_argument("--wall", type=float, help="wall seconds of the test step")
    ap.add_argument("--probe-wall", type=float)
    ap.add_argument("--top", type=int, default=50)
    args = ap.parse_args()

    tests = parse_junit(args.junit)
    totals = junit_totals(args.junit)
    probe = probe_index(args.probe) if args.probe else {}

    print("## Totals\n")
    rows = [
        ["tests (junit `tests`)", str(totals["tests"])],
        ["skipped", str(totals["skipped"])],
        ["failures / errors", f"{totals['failures']} / {totals['errors']}"],
        ["sum of per-test time", f"{totals['time']:.1f} s"],
    ]
    if args.wall:
        rows.append(["wall time of the step", f"{args.wall:.0f} s"])
    if args.probe_wall:
        rows.append(["wall time, instrumented run", f"{args.probe_wall:.0f} s"])
    if args.probe_junit and args.probe_junit.exists():
        rows.append(
            [
                "sum of per-test time, instrumented",
                f"{junit_totals(args.probe_junit)['time']:.1f} s",
            ]
        )
    print(table(rows, ["", "value"]))

    by_module: dict[str, dict[str, float]] = defaultdict(
        lambda: {"time": 0.0, "count": 0.0}
    )
    for test in tests:
        entry = by_module[test["file"]]
        entry["time"] += test["time"]
        entry["count"] += 1
    ordered = sorted(by_module.items(), key=lambda kv: -kv[1]["time"])

    print(f"\n## Per module ({len(ordered)} modules)\n")
    print(
        table(
            [
                [
                    module,
                    str(int(entry["count"])),
                    f"{entry['time']:.1f}",
                    f"{100 * entry['time'] / totals['time']:.1f}%",
                ]
                for module, entry in ordered
            ],
            ["module", "tests", "seconds", "share"],
        )
    )

    print(f"\n## {args.top} slowest tests\n")
    slowest = sorted(tests, key=lambda t: -t["time"])[: args.top]
    rows = []
    for test in slowest:
        record = probe.get(test["nodeid"], {})
        seconds = record.get("seconds", {})
        nats = sum(seconds.get(k, 0.0) for k in NATS_BUCKETS)
        sites = record.get("wait_sites", [])
        rows.append(
            [
                test["nodeid"],
                f"{test['time']:.2f}",
                f"{seconds.get('sleep', 0.0):.2f}",
                f"{seconds.get('timeout', 0.0):.2f}",
                f"{nats:.2f}",
                f"{reask_seconds(record):.2f}",
                f"{seconds.get('subprocess', 0.0):.2f}",
                f"{seconds.get('pg_connect', 0.0):.2f}",
                dominant(record, record.get("total") or test["time"]),
                "; ".join(
                    f"{bucket} {site} {sec:.2f}s"
                    for bucket, site, sec in sites[:6]
                )
                or "—",
            ]
        )
    print(
        table(
            rows,
            [
                "test",
                "s",
                "sleep",
                "timeout",
                "nats",
                "re-ask",
                "subproc",
                "pg conn",
                "proposed",
                "top waits, own task and background",
            ],
        )
    )

    if not probe:
        return

    print("\n## Suite-wide, from the instrumented run\n")
    bucket_totals: dict[str, float] = defaultdict(float)
    bucket_calls: dict[str, int] = defaultdict(int)
    probe_total = 0.0
    for record in probe.values():
        probe_total += record.get("total", 0.0)
        for bucket, value in record.get("seconds", {}).items():
            bucket_totals[bucket] += value
        for bucket, value in record.get("calls", {}).items():
            bucket_calls[bucket] += value
    rows = [
        [
            bucket,
            f"{bucket_totals.get(bucket, 0.0):.1f}",
            f"{100 * bucket_totals.get(bucket, 0.0) / probe_total:.1f}%",
            str(bucket_calls.get(bucket, 0)),
        ]
        for bucket in BUCKETS
    ]
    reask = sum(reask_seconds(record) for record in probe.values())
    reask_bounded = sum(
        min(reask_seconds(record), record.get("total", 0.0))
        for record in probe.values()
    )
    nats_total = sum(bucket_totals.get(k, 0.0) for k in NATS_BUCKETS)
    rows.append(
        [
            f"re-ask loop (`{REASK_SITE}`)",
            f"{reask:.1f}",
            f"{100 * reask / probe_total:.1f}%",
            "",
        ]
    )
    rows.append(
        [
            "re-ask loop, capped at each test's own time",
            f"{reask_bounded:.1f}",
            f"{100 * reask_bounded / probe_total:.1f}%",
            "",
        ]
    )
    rows.append(
        [
            "**nats, all three plus the re-ask loop**",
            f"{nats_total + reask:.1f}",
            f"{100 * (nats_total + reask) / probe_total:.1f}%",
            "",
        ]
    )
    rows.append(["**sum of per-test time**", f"{probe_total:.1f}", "100%", ""])
    print(table(rows, ["bucket", "seconds", "share", "calls"]))

    connects = bucket_calls.get("nats_connect", 0)
    if connects:
        per = bucket_totals.get("nats_connect", 0.0) / connects
        print(
            f"\n`Client.connect` was called {connects} times, "
            f"{bucket_totals['nats_connect']:.1f} s in total, "
            f"{1000 * per:.1f} ms each."
        )
    pg = bucket_calls.get("pg_connect", 0)
    if pg:
        per = bucket_totals.get("pg_connect", 0.0) / pg
        print(
            f"`asyncpg.connect` was called {pg} times, "
            f"{bucket_totals['pg_connect']:.1f} s in total, "
            f"{1000 * per:.1f} ms each."
        )

    print("\n## Worst wait call sites, suite-wide\n")
    sites: dict[tuple[str, str], float] = defaultdict(float)
    for record in probe.values():
        for bucket, site, value in record.get("wait_sites", []):
            sites[(bucket, site)] += value
    print(
        table(
            [
                [bucket, site, f"{value:.1f}"]
                for (bucket, site), value in sorted(
                    sites.items(), key=lambda kv: -kv[1]
                )[:30]
            ],
            [
                "bucket",
                "call site (top four per test only)",
                "seconds",
            ],
        )
    )


if __name__ == "__main__":
    main()
