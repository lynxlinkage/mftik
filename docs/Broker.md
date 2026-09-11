# The broker — what a plane may say, and what NATS does to answer it

NATS is the bus. Fan-out, request-reply, and the fenced session link are
core subjects. Tape lives on standalone Redis per region. The ledger and
OMS live in the TD that holds the account. Session logs live in Postgres.
There is no JetStream: `connect()` opens a connection and does not ensure
streams or KV.

Six processes and none of them import each other. What they share is
`mftik.broker.Broker`. `Broker` is a façade that knows about envelopes
and nothing else; `BrokerTransport` is what the bus owes it; and there
is one of those.

```
Broker            envelopes, IncomingRequest, LeasedSessionLink
  └── BrokerTransport          serialized strings in, serialized strings out
        └── NatsTransport      core NATS
```

The design that got here is [`JetStreamRemoval.md`](JetStreamRemoval.md).

## The seam is serialized envelopes, not store primitives

The families below are named for what a *caller* wants. A transport
handles `str` in and `str` out. It has never seen a pydantic model.

Everything that is not about the bus stays above the line and has
exactly one implementation: envelope encoding, `IncomingRequest`,
`LeasedSessionLink`.

## The rule, and where it is checked

Anything under an `src` tree may use the broker's vocabulary and nothing below
it:

- no `redis` or `nats` import, so no second client and no store's exception
  types spelled out in a domain's error handling;
- no `.redis`, `.js` or `.nc` — the escape hatches;
- no `.key_prefix`, because a name a caller builds is a name the broker cannot
  change.

`packages/common/src/mftik/broker/` is exempt: the transport *is* the
store-specific code. The MD tape module (`apps/md/.../tape_store.py`) is
the other exemption: regional Redis is that plane's disk, not a second
broker, and STS still must not import `redis`. `scripts/` and the test
suites are outside the rule.

[`packages/common/tests/test_broker_is_the_only_transport.py`](../packages/common/tests/test_broker_is_the_only_transport.py)
walks the trees and fails with file and line.

## What a plane may say

| Family | Methods | What it is for |
|---|---|---|
| Fan-out | `publish`, `subscribe`, `psubscribe` | Market data, heartbeats, per-session events, session logs, `status.sts`. Best effort: a message published while nobody is subscribed is gone. Late `/ws/{domain}/{id}` reads `session_logs`; late `/ws/status/sts` reads the session list. |
| Fan-out alias | `publish_log` | Same as `publish`. `maxlen` / `ttl_seconds` are ignored. |
| Request-reply | `request`, `probe`, `serve`, `serve_handler` | The control plane. Attach, deploy, stop, health, backfill, tape RPC, ledger/OMS views. Nobody serving is an immediate error. |
| Session link | `leased_link` / `LeasedSessionLink` | The fenced STS↔MD / STS↔TD heartbeat: token echo, three missed intervals both ways, arm on the first ack. |

Tape is not a broker family. MD records on regional Redis and answers
`md.tape.tail` on `Topics.md(instance)`. STS `StrategyTape.read` requests
the MD this session attached; an unattached feed raises.

**A request nobody is serving fails at once.** There is no work-queue
stream. `serve` is a core NATS queue-group subscription. Work that has
to happen eventually is asked again by the cron, or noticed when three
heartbeats are missed.

## How NATS answers

| The broker's | What NATS does |
|---|---|
| `publish` / `subscribe` | Core NATS. `subscribe` drains the write buffer so this process's server has the interest before the iterator starts. |
| `psubscribe` | The same, with a wildcard subject. Patterns use one `*` per segment; see `Topics.log_pattern`. |
| `publish_log` | `publish`. |
| `request` / `probe` | Core request-reply. No responders is an immediate error. Re-asked first, for half of what the caller brought and never more than a second. `probe` spends only the boot-race grace. |
| `serve` | A core NATS queue-group subscription. The stop event is delivered *through* the inbound queue rather than raced against it. |
| Reply inbox | `reply_inbox` returns `None` and `serve` produces the address on the way in. |
| `key_prefix` | A subject root (`{prefix}.ps.` / `{prefix}.rpc.`). |
| Connection policy | `max_reconnect_attempts=-1` and an 8 MB pending buffer. Reconnect forever: the alternative is a plane that gave up on the bus and stays up not doing anything. |

A subject token may not contain whitespace, `*` or `>`. A bad topic
raises with the offending value named.

## Deployment

`NATS_URL` is read by `BrokerConfig.from_env` and nowhere else.

The dev stack in `docker-compose.yml` and the published node template
(`mftik node init`) run NATS **without** `-js` / `-sd`. Port 8222 is
the monitoring endpoint the healthcheck asks.

Tape Redis is standalone, one process per region, `appendonly yes` plus
a volume. Production Redis is not in this repo; a roll adds the same
service beside the regional MD.

The client floor is still nats-py as declared in
`packages/common/pyproject.toml`. The server no longer needs JetStream
features.

## Testing

This is not a fake. `broker_harness.a_broker` hands each test a
`key_prefix` nobody else has. Core NATS stores nothing, so teardown is
`close`.

Two files, and the split is the point:

- `test_broker*.py` describe what a *caller* is promised.
- `test_nats_transport.py` describes the bus internals — no-responders
  timing, subscribe flush, a cancelled loop leaving nothing pending.

## What a second transport would owe

`BrokerTransport` in
[`packages/common/src/mftik/broker/transport/base.py`](../packages/common/src/mftik/broker/transport/base.py),
and the tests above it. `build()` in `transport/__init__.py` is the seam.
