"""``mftik alert`` — the Source → Matcher → Alert graph, from a terminal.

The same endpoints the ``/alerts`` page calls, so nothing here is a second
source of truth about what an Alert is. What a terminal changes is how the
webhook arrives: never as an argument. A URL on the command line is written
into shell history, into ``ps`` output while the process runs, and into the
scrollback of whoever was pair-typing — three places the Alert document's
invariant 6 spends its time keeping this string out of. It is read from a
prompt, or from stdin with ``--webhook-url-stdin``, and from nowhere else.

Verbs rather than flags on one command, for the reason ``env`` has them:
``rm`` and ``add`` write, ``list`` reads, and one parser would put the first
two a typo away from the third. ``source`` and ``matcher`` nest under this
command rather than claiming two more top-level names, because they are not
useful on their own — a Source with nothing wired to it is a row that does
nothing.
"""

from __future__ import annotations

import argparse
import getpass
import sys
from datetime import UTC, datetime
from typing import Any

from mftik.cli.client import CliError, connected
from mftik.cli.output import table

#: Long enough to see which webhook it is, short enough not to be the secret.
_MASK_WIDTH = 44


def _rows(payload: Any, key: str) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    listed = payload.get(key)
    return list(listed) if isinstance(listed, list) else []


def _when(ts: float | None) -> str:
    if not ts:
        return ""
    return datetime.fromtimestamp(float(ts), tz=UTC).strftime("%Y-%m-%d %H:%M:%SZ")


def _clip(text: str, width: int = _MASK_WIDTH) -> str:
    return text if len(text) <= width else f"{text[: width - 1]}…"


# --- alerts -----------------------------------------------------------------


def list_alerts(args: argparse.Namespace) -> int:
    """Every Alert on the node, with the mask the API returns."""
    _, client = connected(args.profile)
    with client:
        payload = client.get("/alerts")
    alerts = _rows(payload, "alerts")
    if not alerts:
        print("no alerts — create one with: mftik alert add --name <name>")
        return 0
    rows = [
        (
            row.get("id", ""),
            row.get("name", ""),
            "yes" if row.get("enabled") else "no",
            f"{row.get('flush_interval_s', '')}s",
            len(row.get("matcher_ids") or ()),
            _clip(str(row.get("webhook_masked", ""))),
        )
        for row in alerts
    ]
    print(table(("ID", "NAME", "ENABLED", "WINDOW", "MATCHERS", "WEBHOOK"), rows))
    return 0


def _read_webhook(args: argparse.Namespace) -> str:
    """The webhook URL, from a prompt or from stdin. Never from a flag.

    ``--webhook-url-stdin`` is explicit on purpose rather than switching on
    ``isatty()`` alone. A cron job or a CI runner has no TTY *and* often has
    nothing on stdin either; guessing would read an empty string and send a
    422 the operator then has to reverse-engineer. Asked for it plainly, an
    empty pipe can say so.
    """
    if args.webhook_url_stdin:
        url = sys.stdin.readline().strip()
        if not url:
            raise CliError(
                "--webhook-url-stdin was given but stdin was empty; "
                "pipe the URL in, e.g. pass show discord/ops | mftik alert add ..."
            )
        return url
    if not sys.stdin.isatty():
        raise CliError(
            "nothing to read a webhook URL from — pipe it in with: "
            "mftik alert add --name <name> --webhook-url-stdin < url.txt"
        )
    url = getpass.getpass("webhook url: ").strip()
    if not url:
        raise CliError("no webhook URL given")
    return url


def add_alert(args: argparse.Namespace) -> int:
    """Create an Alert. The URL is prompted for, or piped in."""
    url = _read_webhook(args)
    body: dict[str, Any] = {
        "name": args.name,
        "webhook_url": url,
        "enabled": not args.disabled,
        "dedupe": not args.no_dedupe,
    }
    if args.window is not None:
        body["flush_interval_s"] = args.window
    if args.max_events is not None:
        body["max_events_in_payload"] = args.max_events
    if args.max_buffer is not None:
        body["max_buffer_events"] = args.max_buffer
    _, client = connected(args.profile)
    with client:
        created = client.post("/alerts", json_body=body)
    print(
        f"alert {created.get('id')} {created.get('name')} "
        f"→ {created.get('webhook_masked')}"
    )
    print("nothing fires until a Matcher is wired to it:")
    print(f"    mftik alert wire --matcher <id> --alert {created.get('id')}")
    return 0


def rm_alert(args: argparse.Namespace) -> int:
    """Delete an Alert, its wires, and its deliveries."""
    _, client = connected(args.profile)
    with client:
        client.delete(f"/alerts/{args.alert_id}")
    print(f"deleted alert {args.alert_id} (its deliveries went with it)")
    return 0


def test_alert(args: argparse.Namespace) -> int:
    """Fire a fixed embed at the webhook, and report what Discord said."""
    _, client = connected(args.profile)
    with client:
        payload = client.post(f"/alerts/{args.alert_id}/test")
    delivery = (payload or {}).get("delivery") or {}
    status = delivery.get("http_status")
    error = delivery.get("error")
    if error:
        print(f"test fire failed: {error}" + (f" (HTTP {status})" if status else ""))
        return 1
    print(f"test fire delivered (HTTP {status})")
    return 0


def deliveries(args: argparse.Namespace) -> int:
    """The fire log for one Alert — why #ops did or did not speak."""
    _, client = connected(args.profile)
    with client:
        payload = client.get(f"/alerts/{args.alert_id}/deliveries")
    rows = _rows(payload, "deliveries")
    if not rows:
        print(f"alert {args.alert_id} has never fired")
        return 0
    print(
        table(
            ("WHEN", "EVENTS", "DROPPED", "STATUS", "ERROR"),
            [
                (
                    _when(row.get("ts")),
                    row.get("event_count", 0),
                    row.get("dropped_count", 0),
                    row.get("http_status") if row.get("http_status") else "—",
                    row.get("error") or "",
                )
                for row in rows
            ],
        )
    )
    return 0


# --- sources ----------------------------------------------------------------


def list_sources(args: argparse.Namespace) -> int:
    _, client = connected(args.profile)
    with client:
        payload = client.get("/alerts/sources")
    sources = _rows(payload, "sources")
    if not sources:
        print("no sources — add one with: mftik alert source add --domain sts ...")
        return 0
    print(
        table(
            ("ID", "DOMAIN", "SELECTOR", "MATCHERS"),
            [
                (
                    row.get("id", ""),
                    row.get("domain", ""),
                    row.get("selector", ""),
                    ",".join(str(m) for m in row.get("matcher_ids") or ()) or "—",
                )
                for row in sources
            ],
        )
    )
    return 0


def add_source(args: argparse.Namespace) -> int:
    """Subscribe to a stream. For STS the selector is the kind, not a session.

    ``sts_sessions.session_id`` is minted per deploy, so a Source keyed on one
    dies with the strategy — the case the Alert epic exists to stop. A hex id
    is a legal selector and the API stores it; it simply never matches. The
    warning is here because the API cannot tell the difference and a person
    can.
    """
    selector = args.selector
    if args.domain == "sts" and len(selector) == 32 and _is_hex(selector):
        print(
            "warning: that looks like a session_id, not a strategy type. "
            "It will be stored and will never match — see: mftik alert types",
            file=sys.stderr,
        )
    _, client = connected(args.profile)
    with client:
        created = client.post(
            "/alerts/sources",
            json_body={"domain": args.domain, "selector": selector},
        )
    print(
        f"source {created.get('id')} "
        f"{created.get('domain')}:{created.get('selector')}"
    )
    return 0


def _is_hex(value: str) -> bool:
    return all(c in "0123456789abcdefABCDEF" for c in value)


def rm_source(args: argparse.Namespace) -> int:
    _, client = connected(args.profile)
    with client:
        client.delete(f"/alerts/sources/{args.source_id}")
    print(f"deleted source {args.source_id}")
    return 0


def list_types(args: argparse.Namespace) -> int:
    """What an ``sts`` selector may be: the deployable kinds this node knows."""
    _, client = connected(args.profile)
    with client:
        payload = client.get("/sts/types")
    types = (payload or {}).get("types") or []
    if not types:
        print("no deployable strategy types")
        return 0
    for name in types:
        print(name)
    return 0


# --- matchers ---------------------------------------------------------------


def list_matchers(args: argparse.Namespace) -> int:
    _, client = connected(args.profile)
    with client:
        payload = client.get("/alerts/matchers")
    matchers = _rows(payload, "matchers")
    if not matchers:
        print("no matchers — add one with: mftik alert matcher add --name ...")
        return 0
    print(
        table(
            ("ID", "NAME", "KIND", "SPEC", "SOURCES", "ALERTS"),
            [
                (
                    row.get("id", ""),
                    row.get("name", ""),
                    row.get("kind", ""),
                    _spec_summary(row),
                    ",".join(str(s) for s in row.get("source_ids") or ()) or "—",
                    ",".join(str(a) for a in row.get("alert_ids") or ()) or "—",
                )
                for row in matchers
            ],
        )
    )
    disabled = [row for row in matchers if row.get("disabled_reason")]
    for row in disabled:
        print(
            f"\nmatcher {row.get('id')} is not judging: {row.get('disabled_reason')}",
            file=sys.stderr,
        )
    return 0


def _spec_summary(row: dict[str, Any]) -> str:
    """One column's worth of a spec. The full JSON is not a table cell."""
    spec = row.get("spec") or {}
    kind = row.get("kind")
    if kind == "level":
        return ",".join(str(x) for x in spec.get("levels") or ())
    if kind == "regex":
        return _clip(str(spec.get("pattern", "")), 32)
    if kind == "extract":
        return _clip(
            f"{spec.get('pattern', '')} {spec.get('op', '')} {spec.get('value', '')}",
            32,
        )
    return ""


def _matcher_spec(args: argparse.Namespace) -> dict[str, Any]:
    """Build the spec for one ``kind``, refusing what the API would refuse.

    The coercion of ``--value`` happens here rather than being left to JSON,
    because ``as: float`` with a string value is stored happily and then fails
    at match time, inside a worker, on a line nobody is watching.
    """
    if args.kind == "level":
        levels = [level.lower() for level in (args.level or [])]
        if not levels:
            raise CliError("a level matcher needs at least one --level")
        return {"levels": levels}
    if args.kind == "regex":
        if not args.pattern:
            raise CliError("a regex matcher needs --pattern")
        return {"pattern": args.pattern}
    if not args.pattern:
        raise CliError("an extract matcher needs --pattern")
    if args.value is None:
        raise CliError("an extract matcher needs --value")
    value: Any = args.value
    if args.as_ in {"float", "int"}:
        try:
            value = float(args.value) if args.as_ == "float" else int(args.value)
        except ValueError as exc:
            raise CliError(
                f"--value {args.value!r} is not a {args.as_}; "
                f"use --as str to compare it as text"
            ) from exc
    return {
        "pattern": args.pattern,
        "group": args.group,
        "as": args.as_,
        "op": args.op,
        "value": value,
    }


def add_matcher(args: argparse.Namespace) -> int:
    """Create a judgement. It is idle until a Source is wired to it."""
    spec = _matcher_spec(args)
    _, client = connected(args.profile)
    with client:
        created = client.post(
            "/alerts/matchers",
            json_body={"name": args.name, "kind": args.kind, "spec": spec},
        )
    print(f"matcher {created.get('id')} {created.get('name')} ({created.get('kind')})")
    return 0


def rm_matcher(args: argparse.Namespace) -> int:
    _, client = connected(args.profile)
    with client:
        client.delete(f"/alerts/matchers/{args.matcher_id}")
    print(f"deleted matcher {args.matcher_id}")
    return 0


# --- wiring -----------------------------------------------------------------


def _wire_path(args: argparse.Namespace) -> str:
    """The one legal edge the flags name, or an error that says why not.

    Two join tables, two shapes. There is deliberately no source → alert row
    to create: a Source is a subscription and an Alert is a webhook, and what
    sits between them — the judgement — is the whole point of the layer.
    """
    source, matcher, alert = args.source, args.matcher, args.alert
    if source is not None and matcher is not None and alert is None:
        return f"/alerts/sources/{source}/matchers/{matcher}"
    if matcher is not None and alert is not None and source is None:
        return f"/alerts/matchers/{matcher}/alerts/{alert}"
    if source is not None and alert is not None:
        raise CliError(
            "a Source does not wire to an Alert — the graph is "
            "Source → Matcher → Alert. Wire each half:\n"
            f"    mftik alert wire --source {source} --matcher <id>\n"
            f"    mftik alert wire --matcher <id> --alert {alert}"
        )
    raise CliError(
        "name one edge: --source <id> --matcher <id>, "
        "or --matcher <id> --alert <id>"
    )


def wire(args: argparse.Namespace) -> int:
    """Draw one wire. Idempotent — the same wire twice is still one row."""
    path = _wire_path(args)
    _, client = connected(args.profile)
    with client:
        client.put(path)
    print(f"wired {_edge_label(args)}")
    return 0


def unwire(args: argparse.Namespace) -> int:
    """Remove one wire. The nodes at both ends stay."""
    path = _wire_path(args)
    _, client = connected(args.profile)
    with client:
        client.delete(path)
    print(f"unwired {_edge_label(args)}")
    return 0


def _edge_label(args: argparse.Namespace) -> str:
    if args.source is not None and args.matcher is not None:
        return f"source {args.source} → matcher {args.matcher}"
    return f"matcher {args.matcher} → alert {args.alert}"


def show_graph(args: argparse.Namespace) -> int:
    """The whole graph in one read, which is how a wiring mistake shows up."""
    _, client = connected(args.profile)
    with client:
        sources = _rows(client.get("/alerts/sources"), "sources")
        matchers = _rows(client.get("/alerts/matchers"), "matchers")
        alerts = _rows(client.get("/alerts"), "alerts")
    if not (sources or matchers or alerts):
        print("the graph is empty")
        return 0
    by_matcher = {row.get("id"): row for row in matchers}
    by_alert = {row.get("id"): row for row in alerts}
    for source in sources:
        print(
            f"{source.get('domain')}:{source.get('selector')}"
            f"  (source {source.get('id')})"
        )
        wired = source.get("matcher_ids") or ()
        if not wired:
            print("    └── (nothing wired — this Source judges nothing)")
            continue
        for matcher_id in wired:
            matcher = by_matcher.get(matcher_id, {})
            print(
                f"    └── {matcher.get('name', matcher_id)} "
                f"[{matcher.get('kind', '?')}]"
            )
            targets = matcher.get("alert_ids") or ()
            if not targets:
                print("            └── (no Alert — this Matcher fires nothing)")
            for alert_id in targets:
                alert = by_alert.get(alert_id, {})
                state = "" if alert.get("enabled", True) else "  (disabled)"
                print(f"            └── {alert.get('name', alert_id)}{state}")
    orphans = [row for row in matchers if not (row.get("source_ids") or ())]
    for row in orphans:
        print(f"\nmatcher {row.get('id')} {row.get('name')} has no Source — idle")
    return 0
