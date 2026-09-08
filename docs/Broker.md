# The broker — what a plane may say, and what NATS does to answer it

Six processes and none of them import each other. What they share is
`mftik.broker.Broker`. It used to be doing two jobs at once: it was the
vocabulary a plane speaks, and it was the store implementation of that
vocabulary. Those are now separate. `Broker` is a façade that knows about
envelopes, continuity arithmetic and nothing else; `BrokerTransport` is what
the store owes it; and there is one of those.

```
Broker            envelopes, tape continuity, IncomingRequest,
                  LeasedSessionLink, StateProjection
  └── BrokerTransport          serialized strings in, serialized strings out
        └── NatsTransport      core NATS, JetStream, KV
```

This document is three lists. What a plane may say, which is the whole of the
interface. How NATS answers it, and why each mechanism was chosen. And what a
second transport would owe if one were ever justified again.

## The seam is serialized envelopes, not store primitives

The obvious seam would have been push, pop, hash-set, expire — store
primitives named for one idiom, with NATS emulating each behind them, badly.
So the families below are named for what a *caller* wants, and the transport
answers in its own idiom: a lease is "may I be the one who runs this", not a
compare-and-set primitive leaked upward.

Everything that is not about the store stays above the line and has exactly one
implementation: envelope encoding, the tape's continuity marks and gap
arithmetic, `IncomingRequest`, `LeasedSessionLink`, `StateProjection`. A
transport handles `str` in and `str` out. It has never seen a pydantic model.

## The rule, and where it is checked

Anything under an `src` tree may use the broker's vocabulary and nothing below
it:

- no `redis` or `nats` import, so no second client and no store's exception
  types spelled out in a domain's error handling;
- no `.redis`, `.js` or `.nc` — the escape hatches;
- no `.key_prefix`, because a name a caller builds is a name the broker cannot
  change. The prefix is a subject root, a stream name and a KV bucket at once,
  and a caller's flat string is not any of them.

`packages/common/src/mftik/broker/` is exempt: the transport *is* the
store-specific code. `scripts/` and the test suites are outside the rule —
`redacted_url` is a credential's problem and names it on purpose, and
`broker_harness` reaches through deliberately so the tests above it do not have
to.

[`packages/common/tests/test_broker_is_the_only_transport.py`](../packages/common/tests/test_broker_is_the_only_transport.py)
walks the trees and fails with file and line. It also asserts the walk found
the tree, so a guard that has stopped looking at anything fails rather than
passing. Closing the three reach-throughs that existed — liveness keys, TD's
backfill lock, STS's cid slot — is what made the port a change to one package
instead of a hunt through every file. `redis` stays in the forbidden-import
list after the dependency is gone: one string, and it stops the client coming
back in through a side door.

## What a plane may say

| Family | Methods | What it is for |
|---|---|---|
| Fan-out | `publish`, `subscribe`, `psubscribe` | Market data, heartbeats, per-session events. Best effort: a message published while nobody is subscribed is gone. `subscribe` / `psubscribe` on a `log.` / `status.` topic also see the log stream, so a live log subscriber does not need a second call. |
| Fan-out with a tail | `publish_log`, `fetch_log_buffer` | Logs, where a UI socket that opens after the deploy still wants the last hundred lines. Own stream (`{prefix}.log.>`), own per-subject ring. |
| Request-reply | `request`, `probe`, `serve`, `serve_handler` | The control plane. Attach, deploy, stop, health, backfill, market-data queries. Nobody serving is an immediate error. |
| Session link | `leased_link` / `LeasedSessionLink` | The fenced STS↔MD / STS↔TD heartbeat: token echo, grace watchdog, expiry on a sibling task. |
| Shared state | `state_put`, `state_put_many`, `state_replace`, `state_get`, `state_all`, `state_drop`, `state_clear`, `state_watch`, `state_projection` | TD's order book and ledger. Writers `put` / `replace`; readers that care about the cost open a `StateProjection`. |
| Leases | `lease_put`, `lease_take`, `lease_owner`, `lease_held`, `lease_hold`, `lease_release`, `lease_drop` | "Is anybody still running this", "may I be the one who runs it". Session liveness, account ownership, the backfill lock. |
| Counters | `counter_next` | STS's cid slot, allocated across processes that all serve one subject. |
| Recorded tape | `tape_append`, `tape_tail`, `tape_trim_before`, `tape_mark_recording`, `tape_mark_stopped`, `tape_coverage` | MD's recording, and the warm-up a strategy reads out of it. |

Four of those carry a promise a caller used to make for itself, and each is a
promise the transport has to keep rather than reimplement:

**A lease is not a key with a TTL.** `lease_hold` and `lease_release` are
conditional — extend or drop *if it is still ours*. NATS KV closes the race,
because `update` and `delete` take the revision they were read at. Two callers
used to document a read-then-write workaround separately; now neither knows
there was one.

**A request nobody is serving fails at once.** There is no work-queue stream.
The four callers that used to `post` each named a backstop outside the broker
— the settlement cursor, or the liveness lease — and `serve` is a core NATS
queue-group subscription. Work that has to happen eventually is asked again
by the cron, or noticed when the lease expires.

**A tape record's stamp is the broker's clock, not the venue's.** `tape_tail`
returns `(recorded_ms, fields)`. It used to return a store id and let the
strategy SDK pull `<ms>-<seq>` apart, which was the same leak as the others
wearing different clothes.

**`maxlen` means `maxlen`, and a transport that cannot must say so.** Trimming
is exact. Approximate forms stop at macro-node boundaries, so the fuse did not
hold until a feed was a hundred records past it and a sweep reported nothing
dropped. `publish_log` has the other half of the rule: a ring longer than the
log stream's per-subject cap (`LOG_MAX_MSGS_PER_SUBJECT`, 256) raises, because
a caller quietly handed half of what it asked for reads the same as a topic
that has been quiet. A smaller `maxlen` is the replay cap `fetch_log_buffer`
honours; the stream holds its own ring.

## How NATS answers

| The broker's | What NATS does |
|---|---|
| `publish` / `subscribe` | JetStream, one ephemeral consumer per subscriber, `DeliverPolicy.NEW` and no acknowledgement. The stream is `{prefix}.ps.>` with a per-subject cap. A `log.` / `status.` topic also opens a consumer on `{prefix}.log.>`. |
| `psubscribe` | The same, with a wildcard subject. Patterns were already one `*` per segment; see `Topics.log_pattern`. A `log.` / `status.` pattern listens on both streams. |
| `publish_log` / `fetch_log_buffer` | A dedicated stream (`{prefix}.log.>`), `max_msgs_per_subject = LOG_MAX_MSGS_PER_SUBJECT` (256). The server holds the ring; there is no purge after each line. `maxlen` above that cap raises. `fetch_log_buffer` trims the replay to `BROKER_LOG_BUFFER_MAXLEN` (or the passed `maxlen`). `ttl_seconds` is a per-message TTL, so a line expires on its own clock rather than the buffer expiring as a whole. |
| `request` / `probe` | Core request-reply. No responders is an immediate error, so the control plane learns a plane is down without spending its whole timeout on it. Re-asked first, for half of what the caller brought and never more than a second: a serve loop registering as its process boots is not a plane being down, and neither is an account subject three hundred milliseconds into a handover — order entry brings two seconds. `probe` opts out and spends only the boot-race grace, because "down" is the answer a probe is *for*. |
| `serve` | A core NATS queue-group subscription. The stop event is delivered *through* the inbound queue rather than raced against it, so a serve loop stops on the message after the one it is reading, and everything queued ahead of the stop is still handed over. |
| Reply inbox | The protocol's own reply subject. `reply_inbox` returns `None` and `serve` produces the address on the way in, so nothing is stamped on the envelope. |
| `state_*` | A KV bucket, one key per field, `:` mapped to `.`. `state_all` is one consumer over the bucket's subject tree delivering last-per-subject, not a `keys()` and a get each. One field lands whole; a multi-field write is several keys, issued together but not a snapshot — see below. `state_all` is complete or it raises `StateReadIncompleteError`: a consumer can come up short, and a book missing rows reads exactly like a book that small. |
| `state_replace` | Write the new fields, then drop what the last-written set had extra — KV has no cross-key transaction. A name has one writer, so the transport caches that set and does not scan first. That order is the one where a reader in the middle sees a stale field rather than an empty state. The writes are serialised per name in-process, so "last issued wins" still holds inside one process. |
| `state_drop` / `state_clear` | A KV delete per live field, so a watcher sees each one go, then a stream purge so the delete marker does not stay. A key that is already gone is not deleted again. `state_clear` deletes every live field of the name. |
| `state_watch` / `StateProjection` | KV `watch` on the name's prefix, one-shot. The projection reseeds from `state_all` and opens another watch when that one ends. A hard failure clears `live` so STS views fall back to `state_all` / `state_get` rather than serving a frozen map. STS starts a projection per attached `td.oms.{id}` / `td.ledger.{id}`. `oms.order` and ledger `available` / `free` / `prelock` always `state_get` the field they asked for. |
| `lease_*` | KV with per-message TTL. `lease_take` is `create`; `lease_hold` and `lease_release` compare-and-set against the revision they read. |
| `counter_next` | Read, add one, `update` at the revision that was read, retried on a loss. KV has no atomic increment, and the server-side counter that would give one is 2.12 while the floor here is 2.11. Cheap because the only caller allocates a slot once per session, not once per order. |
| `tape_append` / `tape_tail` / `tape_trim_before` | A JetStream stream per feed. Per-feed, because retention is per feed in the interface and a stream's limits are the stream's — and because "the newest N records" is then subtraction on a sequence rather than a scan past every other feed's prints. |
| `tape_coverage` / `tape_coverage_put` | One KV entry holding the whole record, written durably — no per-message TTL, no half-life renewal. The tape stream already expires prints via `max_age`; coverage is a fact about those prints and stays until the next mark overwrites it. |
| `key_prefix` | A subject root, and the name of every stream and KV bucket this node owns. |
| `consumer_idle_seconds` | `inactive_threshold` on every consumer. Subjects are per-session and per-account, so the consumer count follows the fleet; this is what stops a node that has churned a thousand sessions from carrying a thousand consumers. For a *read*'s consumer it is only the backstop: `unsubscribe` on a pull subscription tears down the client's inboxes and leaves the server's consumer alone, so every read deletes its own and the threshold covers the process that died before it could. |
| Connection policy | `max_reconnect_attempts=-1` and an 8 MB pending buffer. Reconnect forever: the alternative is a plane that gave up on the bus and stays up not doing anything. |

### Properties a caller has to know

**Leases are whole seconds, with a one second floor.** NATS' per-message TTL
is whole seconds (ADR-43); a `Nats-TTL` below it — `0` included — is rejected
and the message discarded. So `_ttl_seconds` rounds *up*: a lease that expired
early is two processes holding one account, and a lease that expired a
fraction late is a redeploy waiting slightly longer. Production leases are
thirty seconds, so nothing real is affected. Tests that watch a claim lapse
ask `broker_harness.MIN_LEASE_TTL` (1.0) for the shortest the store can
express and wait that out instead.

**A multi-field write is not a snapshot.** A field is a KV key of its own, so
`state_put_many` is several writes and a reader can catch a two-asset ledger
update with one asset moved and the other not. Every value it sees is a value
the writer wrote — the field is the unit written whole — but it is not the
same instant. The writes go out together rather than one round trip at a
time, which is as close as key-per-field gets; closing it outright would mean
one key per *name* holding the whole mapping, which buys the guarantee by
turning every single-field write on the order path into a read-modify-write.
`BrokerTransport.state_put_many` states the promise so a caller that needs two
numbers to move together knows to put them in one field, and `state_replace`
is explicit that seeing old and new at once is allowed.

**Names are a restricted character set.** A KV key is `[-/_=.a-zA-Z0-9]`, and
a subject token may not contain whitespace, `*` or `>`. Lease and counter
names were spelled with colons — `sts:alive:{session}`,
`backfill:lock:{api_id}` — so those become dots, which is what the segments
always were. Anything still outside the set raises with the offending value
named, because the values that could get that far come from outside the node:
a venue's symbol, a credential's api_key.

## The tape is not the fan-out yet

A JetStream subject already has a history, so in principle MD's recorded tape
and MD's live fan-out are one stream read two ways, and the tape stops being a
second write of the same prints. That is the interesting version of this port
and it is not what is here.

What stands in the way is not the broker. The tape carries flattened string
fields and the fan-out carries whole envelopes; the subject spaces differ; and
retention is per feed while a fan-out subject is capped by count. Unifying them
is a change to MD's dispatcher and to `StrategyTape`, and it would make the
recorded window a property of which stream a feed's subject lands in rather than
of a `tape_append` call. Worth doing on its own, with its own tests — tracked
as a follow-up rather than carried here.

## Deployment

`NATS_URL` is read by `BrokerConfig.from_env` and nowhere else.

The dev stack in `docker-compose.yml` runs NATS. The published node template
(`mftik node init`) does too. Two things there are not optional:

- `-js`. Only bare request-reply is core NATS; state, leases, fan-out, logs
  and the tape are all JetStream or KV, so a server without it accepts the
  connection and then refuses everything a plane does.
- `-sd` on a volume. That state is the node's open orders, its ledger and its
  recorded tape. A node restarted with an empty store comes back reading as an
  account that closed everything.

Port 8222 is the monitoring endpoint. It is what the healthcheck asks, and it
is worth pointing `nats stream ls` at when a subject is not behaving.

**Two version floors, and they are not the same number.** The *server* floor is
2.11, where per-message TTL lands (ADR-43) and a lease becomes expressible at
all. The *client* floor is nats-py 2.14, and it is higher than the server's
because the client grew the pieces one release at a time: `Header.MSG_TTL` in
2.12, `KeyValue.create(msg_ttl=...)` in 2.13, and a `ConsumerConfig` that will
take `opt_start_time` as a datetime rather than an int in 2.14 — which the
tape's retention sweep needs. Below any of those the transport imports and
connects and then raises on the first account claim or the first trim, so the
floor is declared in `packages/common/pyproject.toml` rather than left to the
lock file: CI resolves to the newest available and only the published wheel ever
sees the bottom of the range.

## Testing

This is not a fake. There was one — fakeredis — and it is gone, for the reason
the database suite gave up on sqlite-only: an in-process imitation agrees with
the real server right up to the behaviour you are trying to test. It hid exact
trimming, it hid a subscription that is not live the instant it is created, and
each of those was a real bug behind a green suite. JetStream has no in-process
imitation at all, and writing one would have meant asserting against our own
guess at what a consumer does.

`broker_harness.a_broker` hands each test a `key_prefix` nobody else has and
drops everything under it afterwards — that prefix names the streams and
buckets, so a teardown is a handful of `delete_stream` calls.

Two files, and the split is the point:

- `test_broker*.py` describe what a *caller* is promised. They are where a new
  promise belongs.
- `test_nats_transport.py` describes the store's internals — a consumer being
  ephemeral, a stream name collision being refused.

## What a second transport would owe

`BrokerTransport` in
[`packages/common/src/mftik/broker/transport/base.py`](../packages/common/src/mftik/broker/transport/base.py),
and the tests above it. `build()` in `transport/__init__.py` is the seam — a
plain factory, not a registry — so a second implementation is still an entry
there rather than a rewrite of the client. If a family genuinely does not fit,
add it to the interface and implement it on the existing side rather than
reaching for the new store's own command at a call site: a caller that does is
a caller that only works on the transport it was written against, and this is
exactly the state the guard above exists to keep the tree out of.
