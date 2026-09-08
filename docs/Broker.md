# The broker — what a plane may say, and what a transport has to answer

Six processes and none of them import each other. What they share is
`mftik.broker.Broker`, and it used to be doing two jobs at once: it was the
vocabulary a plane speaks, and it was the Redis implementation of that
vocabulary. Those are now separate. `Broker` is a façade that knows about
envelopes, continuity arithmetic and nothing else; `BrokerTransport` is what a
store owes it; and there are two of those.

```
Broker            envelopes, tape continuity, IncomingRequest, BidirectionalStream
  └── BrokerTransport          serialized strings in, serialized strings out
        ├── RedisTransport     lists, hashes, pub/sub, streams
        └── NatsTransport      core NATS, JetStream, KV
```

NATS is the default. Redis is complete, tested on every CI run, and is what a
rollback selects — `BROKER_TRANSPORT=redis`, no code change.

This document is three lists. What a plane may say, which is the whole of the
interface. How each transport answers it, which is where the two stores stop
being interchangeable. And what a third one would owe.

## The seam is serialized envelopes, not store primitives

The obvious seam would have been push, pop, hash-set, expire — and it would
have been Redis' vocabulary with a NATS emulation of each behind it, badly. So
the families below are named for what a *caller* wants, and each transport
answers in its own idiom: `post` is a list on one and a work-queue stream on
the other, and neither has to pretend to be the other.

Everything that is not about the store stays above the line and has exactly one
implementation: envelope encoding, the tape's continuity marks and gap
arithmetic, `IncomingRequest`, `BidirectionalStream`. A transport handles
`str` in and `str` out. It has never seen a pydantic model.

## The rule, and where it is checked

Anything under an `src` tree may use the broker's vocabulary and nothing below
it:

- no `redis` or `nats` import, so no second client and no store's exception
  types spelled out in a domain's error handling;
- no `.redis`, `.js` or `.nc` — the escape hatches, one per transport;
- no `.key_prefix`, because a name a caller builds is a name the broker cannot
  change. Under Redis it was a key it could not reshape. Under NATS the prefix
  is a subject root, a stream name and a KV bucket at once, and a caller's flat
  string is not any of them.

`packages/common/src/mftik/broker/` is exempt: the transports *are* the
store-specific code. `scripts/` and the test suites are outside the rule —
`redacted_url` is a Redis credential's problem and names it on purpose, and
`broker_harness` reaches through deliberately so the tests above it do not have
to.

[`packages/common/tests/test_broker_is_the_only_transport.py`](../packages/common/tests/test_broker_is_the_only_transport.py)
walks the trees and fails with file and line. It also asserts the walk found
the tree, so a guard that has stopped looking at anything fails rather than
passing. Closing the three reach-throughs that existed — liveness keys, TD's
backfill lock, STS's cid slot — is what made this port a change to one package
instead of a hunt through every file.

## What a plane may say

| Family | Methods | What it is for |
|---|---|---|
| Fan-out | `publish`, `subscribe`, `psubscribe` | Market data, heartbeats, per-session events. Best effort: a message published while nobody is subscribed is gone. |
| Fan-out with a tail | `publish_log`, `fetch_log_buffer` | Logs, where a UI socket that opens after the deploy still wants the last hundred lines. |
| Request-reply | `request`, `post`, `probe`, `serve`, `serve_handler` | The control plane. Attach, deploy, stop, health, backfill, market-data queries. |
| Duplex | `bistream`, `bistream_pair` | A topic pair as one object. |
| Shared state | `state_put`, `state_put_many`, `state_replace`, `state_get`, `state_all`, `state_drop`, `state_clear` | TD's order book and ledger, read by whoever needs the current answer rather than folded from a fan-out. |
| Leases | `lease_put`, `lease_take`, `lease_owner`, `lease_held`, `lease_hold`, `lease_release`, `lease_drop` | "Is anybody still running this", "may I be the one who runs it". Session liveness, account ownership, the backfill lock. |
| Counters | `counter_next` | STS's cid slot, allocated across processes that all serve one subject. |
| Recorded tape | `tape_append`, `tape_tail`, `tape_trim_before`, `tape_mark_recording`, `tape_mark_stopped`, `tape_coverage` | MD's recording, and the warm-up a strategy reads out of it. |

Four of those carry a promise a caller used to make for itself, and each is a
promise a transport has to keep rather than reimplement:

**A lease is not a key with a TTL.** `lease_hold` and `lease_release` are
conditional — extend or drop *if it is still ours*. Redis cannot do either
atomically without scripting, so the Redis transport documents the race it
leaves open and which way it loses it: a refresh arriving after a rival took
the lease extends the rival's rather than stealing it back. NATS KV closes it,
because `update` and `delete` take the revision they were read at. Two callers
used to document that workaround separately; now neither knows there was one.

**A request nobody is serving waits — if it was `post`ed.** A parked backfill is
recovery, not litter, and `Broker.post`'s docstring turns on it. `request` and
`probe` are different: both have a caller with a deadline, and work executed
after that deadline passed is a side effect nobody is expecting. That is why
`Topics.td_order`'s docstring now hands the question of what a request in a
cutover gap does to the transport rather than answering it itself.

**A tape record's stamp is the broker's clock, not the venue's.** `tape_tail`
returns `(recorded_ms, fields)`. It used to return the Redis stream id and let
the strategy SDK pull `<ms>-<seq>` apart, which was the same leak as the others
wearing different clothes.

**`maxlen` means `maxlen`, and a transport that cannot must say so.** Both trim
exactly. Redis' approximate forms — `XADD MAXLEN ~`, `XTRIM MINID ~` — stop at
macro node boundaries, so the fuse did not hold until a feed was a hundred
records past it and a sweep reported nothing dropped. fakeredis trimmed exactly,
which is why nothing said so until the suite met a real server. `publish_log` has
the other half of the rule: a ring longer than the NATS fan-out stream's
per-subject cap raises, because a caller quietly handed half of what it asked for
reads the same as a topic that has been quiet.

## How each transport answers

| The broker's | Redis | NATS |
|---|---|---|
| `publish` / `subscribe` | `PUBLISH` / `SUBSCRIBE` | JetStream, one ephemeral consumer per subscriber, `DeliverPolicy.NEW` and no acknowledgement. The stream is `{prefix}.ps.>` with a per-subject cap. |
| `psubscribe` | `PSUBSCRIBE`, glob | The same, with a wildcard subject. Patterns were already one `*` per segment; see `Topics.log_pattern`. |
| `publish_log` / `fetch_log_buffer` | `RPUSH` + `LTRIM` + `EXPIRE` + `PUBLISH`, one pipelined round trip | The same fan-out stream, plus a `purge … keep=maxlen` behind the publish — two round trips, because a purge cannot ride along the way an `LTRIM` can. `maxlen` above the stream's per-subject cap raises: the stream has already discarded by then and a caller handed half the ring it asked for cannot tell that from a quiet hour. `ttl_seconds` is a per-message TTL, so a line expires on its own clock rather than the buffer expiring as a whole and being refreshed by each write. |
| `request` / `probe` | List + `BLPOP`, reply list with a TTL | Core request-reply. No responders is an immediate error, so the control plane learns a plane is down without spending its whole timeout on it. Re-asked first, for half of what the caller brought and never more than a second: a serve loop registering as its process boots is not a plane being down, and neither is an account subject three hundred milliseconds into a handover — order entry brings two seconds and Redis would have parked through it. `probe` opts out and spends only the boot-race grace, because "down" is the answer a probe is *for*. |
| `post` | The same list | A work-queue stream, on a *different subject space* (`{prefix}.post.>`). It has to be different: a stream is a subscriber like any other, so one whose filter covered the RPC subjects would answer every core request with a publish acknowledgement, on the requester's own reply subject, and the caller would parse `{"stream": …, "seq": 1}` as its answer. |
| `serve` | `BLPOP` on one list, competing consumers | Two sources merged: a core queue subscription (the queue group is what makes several processes a pool) and a pull consumer on the work queue, sharing a durable by name. |
| Reply inbox | A reply list with a TTL, addressed in the envelope | The protocol's own reply subject. `reply_inbox` returns `None` and `serve` produces the address on the way in, so nothing is stamped on the envelope. |
| `state_*` | One hash per name, JSON per field | A KV bucket, one key per field, `:` mapped to `.`. `state_all` is one consumer over the bucket's subject tree delivering last-per-subject, not a `keys()` and a get each. One field lands whole either way; a multi-field write is one `HSET` on Redis and several keys here, issued together but not a snapshot — see below. |
| `state_replace` | `MULTI`: delete then write | Write the new fields, then drop what the old set had extra — KV has no cross-key transaction. That order is the one where a reader in the middle sees a stale field rather than an empty state. And the writes are serialised per name in-process, because `MULTI` being one round trip is what used to make "last issued wins" true. |
| `lease_*` | `SET px`, `SET NX px`, `GET`, `PEXPIRE` | KV with per-message TTL. `lease_take` is `create`; `lease_hold` and `lease_release` compare-and-set against the revision they read. |
| `counter_next` | `INCR` | Read, add one, `update` at the revision that was read, retried on a loss. KV has no atomic increment, and the server-side counter that would give one is 2.12 while the floor here is 2.11. Cheap because the only caller allocates a slot once per session, not once per order. |
| `tape_append` / `tape_tail` / `tape_trim_before` | One stream per feed: `XADD maxlen` + `XREVRANGE` + `XTRIM MINID` | A JetStream stream per feed. Per-feed, because retention is per feed in the interface and a stream's limits are the stream's — and because "the newest N records" is then subtraction on a sequence rather than a scan past every other feed's prints. |
| `key_prefix` | Every key's first segment | A subject root, and the name of every stream and KV bucket this node owns. |
| `serve_poll_seconds` | `BLPOP` granularity, and why shutdown waits out a poll | Unused. A subscription is cancellable, so a serve loop stops when it is told. |
| `consumer_idle_seconds` | Unused | `inactive_threshold` on every consumer. Subjects are per-session and per-account, so the consumer count follows the fleet; this is what stops a node that has churned a thousand sessions from carrying a thousand consumers. |
| Connection policy | `build_redis`: pooled, health checks, retry on `ConnectionError` | `max_reconnect_attempts=-1` and an 8 MB pending buffer. Reconnect forever for the same reason Redis retries: the alternative is a plane that gave up on the bus and stays up not doing anything. |

### Three things NATS cannot do that Redis could

**Sub-second leases.** NATS' per-message TTL is whole seconds with a one second
floor (ADR-43); a `Nats-TTL` below it — `0` included — is rejected and the
message discarded. So `_ttl_seconds` rounds *up*: a lease that expired early is
two processes holding one account, and a lease that expired a fraction late is
a redeploy waiting slightly longer. Production leases are thirty seconds, so
nothing real is affected. Tests that watched a claim lapse in 20ms ask
`broker_harness.MIN_LEASE_TTL` for the shortest the selected transport can
express and wait that out instead.

**A multi-field write as a snapshot.** `HSET` takes a mapping in one command,
so a reader either sees all of it or none of it. A field is a KV key of its own
here, so `state_put_many` is several writes and a reader can catch a two-asset
ledger update with one asset moved and the other not. Every value it sees is a
value the writer wrote — the field is the unit both transports write whole — but
it is not the same instant. The writes go out together rather than one round
trip at a time, which is as close as key-per-field gets; closing it outright
would mean one key per *name* holding the whole mapping, which buys the
guarantee by turning every single-field write on the order path into a
read-modify-write. `BrokerTransport.state_put_many` states the promise so a
caller that needs two numbers to move together knows to put them in one field,
and `state_replace` is explicit that seeing old and new at once is allowed.

**Any name at all.** A KV key is `[-/_=.a-zA-Z0-9]`, and a subject token may
not contain whitespace, `*` or `>`. The lease and counter names were Redis key
tails spelled with colons — `sts:alive:{session}`, `backfill:lock:{api_id}` —
so those become dots, which is what the segments always were. Anything still
outside the set raises with the offending value named, because the values that
could get that far come from outside the node: a venue's symbol, a credential's
api_key.

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

`BROKER_TRANSPORT` picks the store. `NATS_URL` and `REDIS_URL` are read by
`BrokerConfig.from_env` and nowhere else.

The dev stack in `docker-compose.yml` runs both, because the suite is run
against each. The published node template (`mftik node init`) runs only NATS —
a node on it has no use for a Redis. Two things there are not optional:

- `-js`. Only bare request-reply is core NATS; state, leases, fan-out, posted
  work and the tape are all JetStream or KV, so a server without it accepts the
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

**Switching a live node is not a migration.** Nothing copies state between the
two, so a node that has been running on one does not find its sessions,
its ledger or its tape on the other. Switch with nothing attached.

## Testing

`MFTIK_TEST_BROKER` chooses the transport, exactly as `MFTIK_TEST_LOOP` chooses
the loop and `TEST_POSTGRES_URL` chooses the database. It defaults to `nats`;
CI runs a full pass on each.

Neither is a fake. There was one — fakeredis — and it is gone, for the reason
the database suite gave up on sqlite-only: an in-process imitation agrees with
the real server right up to the behaviour you are trying to test. It hid exact
trimming, it hid a subscription that is not live the instant it is created, and
each of those was a real bug behind a green suite. JetStream has no in-process
imitation at all, and writing one would have meant asserting against our own
guess at what a consumer does.

`broker_harness.a_broker` hands each test a `key_prefix` nobody else has and
drops everything under it afterwards — under NATS that prefix names the streams
and buckets, so a teardown is a handful of `delete_stream` calls.

Three files, and the split is the point:

- `test_broker*.py` describe what a *caller* is promised and carry no marker.
  They run on both transports and are where a new promise belongs.
- `test_redis_transport.py` and `test_nats_transport.py` describe one store's
  internals — a list being capped, a consumer being ephemeral, a stream name
  collision being refused. `only_on(...)` skips them on the other pass.

## What a third transport would owe

`BrokerTransport` in
[`packages/common/src/mftik/broker/transport/base.py`](../packages/common/src/mftik/broker/transport/base.py),
and the tests above it. Register it in `transport/__init__.py` the way venues
are registered, and the suite it has to pass is the unmarked one — not a new
file of its own. If a family genuinely does not fit, add it to the interface
and implement it on both existing sides rather than reaching for the new
store's own command at a call site: a caller that does is a caller that only
works on the transport it was written against, and this is exactly the state
the guard above exists to keep the tree out of.
