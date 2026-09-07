# The broker — what a plane may say, and what a transport has to answer

Six processes and none of them import each other. What they share is
`mftik.broker.Broker`, and until this change "share" was doing two jobs at
once: everything went *through* the broker, and nothing stopped a caller
going *around* it. `Broker.redis` is a live redis-py client, and three
callers used it.

That distinction did not cost anything while the transport was Redis and was
going to stay Redis. It stops being free the moment the transport is meant to
become NATS JetStream, because every reach-through is a call site a port has
to find by reading files, and the cost of missing one is a plane still
talking to a Redis nobody else is using any more.

So this document is two lists. What a plane is allowed to say, which is now
the whole of the interface. And what each of those things needs from whatever
is underneath, which is the work the swap actually is.

## The rule, and where it is checked

Anything under an `src` tree may use the broker's vocabulary and nothing
below it:

- no `redis` import, so no second client and no Redis exception types spelled
  out in a domain's error handling;
- no `.redis`, which is the escape hatch itself;
- no `.key_prefix`, because a key shape a caller builds is a key shape the
  broker cannot change — and under JetStream cannot honour at all, where
  names are streams and buckets rather than one flat keyspace.

`packages/common/src/mftik/broker/` is exempt: it *is* the Redis
implementation. `scripts/` and the test suites are outside the rule — `just
loop-bench` times Redis on purpose, and a test that breaks `blpop` to prove a
serve loop survives is describing this broker rather than reaching around it.

[`packages/common/tests/test_broker_is_the_only_transport.py`](../packages/common/tests/test_broker_is_the_only_transport.py)
walks the trees and fails with file and line. It also asserts the walk found
the tree, so a guard that has stopped looking at anything fails rather than
passing.

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

Three of those rows carry a promise that is easy to lose in a port, and each
is a promise a caller used to make for itself.

**A lease is not a key with a TTL.** `lease_hold` and `lease_release` are
conditional — extend or drop *if it is still ours* — and Redis cannot do
either atomically without scripting, which the test suite's Redis has none
of. So both
document the race they leave open and which way they lose it: a refresh that
arrives after a rival took the lease extends the rival's rather than stealing
it back. Two callers used to document that workaround separately. A transport
with compare-and-set closes it once, inside these two methods, and no caller
changes.

**A request that nobody is serving waits.** `request` and `post` push onto a
list, and a plane that is down has its work taken by the next process to come
up. This is deliberate and load-bearing: a parked attach or backfill is
recovery, not litter, and `Topics.td_order`'s docstring turns on it.
`probe` is the one exception, and says so — a liveness probe nobody answered
has no value once the caller stopped waiting, so its queue is capped and
expiring.

**A tape record's stamp is the broker's clock, not the venue's.** `tape_tail`
returns `(recorded_ms, fields)`. It used to return the Redis stream id and
let the strategy SDK pull `<ms>-<seq>` apart, which is the same leak as the
three above wearing different clothes.

## What JetStream would have to answer

Roughly, and in the order of how much thinking each needs rather than how
much code.

| The broker's | Redis today | JetStream |
|---|---|---|
| `publish` / `subscribe` | `PUBLISH` / `SUBSCRIBE` | Core NATS, directly. Subjects already look like this — `md.{session_id}`, `td.{api_id}.global`. |
| `psubscribe` | `PSUBSCRIBE`, glob | Core NATS wildcards. Patterns are already written one `*` per segment; see `Topics.log_pattern`. |
| `state_*` | One hash per name, JSON per field | A KV bucket, one key per field. `state_replace` is the awkward one: it is a `MULTI` of delete-then-write today so no reader sees the empty gap, and KV has no multi-key transaction. |
| `lease_*` | `SET px`, `SET NX px`, `GET`, `PEXPIRE` | KV with per-key TTL, which needs server 2.11+ and limit markers enabled on the bucket (`LimitMarkerTTL`, minimum 1s — the leases here are 30s). `lease_take` is `create`, which is compare-and-swap against revision 0; `lease_hold` and `lease_release` become `update` against the revision they read, which is the race they currently lose. |
| `counter_next` | `INCR` | KV has no atomic increment: it is a read, a compute and an `update` at the expected revision, retried on conflict. A stream with `allow_msg_counter` does it server-side and returns the total in the ack, but that is 2.12. Or the cid slot stops being a counter. |
| `publish_log` / `fetch_log_buffer` | `RPUSH` + `LTRIM` + `EXPIRE` + `PUBLISH` | A stream per log subject with `MaxMsgs` and `MaxAge`, read back for the replay. |
| `tape_append` / `tape_trim_before` | `XADD maxlen` + `XTRIM MINID` | A stream with `MaxMsgs` and `MaxAge`; the age limit replaces the trim call entirely. |
| `tape_tail` | `XREVRANGE`, newest N | The one genuine mismatch. JetStream reads from a sequence or a time, not "the last N", so this becomes arithmetic on `num_pending` or a direct get by sequence. |
| `request` / `post` / `serve` | List + `BLPOP`, competing consumers | Core request-reply will not do: it fails immediately when nobody is listening, and the durable queue is the point. A work-queue stream with a pull consumer per subject. |
| `probe` | Capped, expiring list | Core request-reply *is* the right shape here — no responders is an immediate error, which is exactly the answer a probe wants. |
| Reply inbox | Reply list with a TTL | A NATS inbox subject. |
| `serve_poll_seconds` | `BLPOP` granularity | Gone, along with "a serving loop cannot be cancelled out of a blocking pop", which is why shutdown waits out a poll today. |
| `key_prefix` | Every key's first segment | There is no flat keyspace to prefix. It becomes stream and bucket names, or a subject prefix, or a NATS account. All of the key building is in one file now. |
| Connection policy | `build_redis`: pooled, health checks, retry on `ConnectionError` only | Different failure model entirely, and the reasoning in `build_redis`'s comments is about redis-py's pool rather than about MFTIK. Read it before porting it — much of it does not transfer. |

Two things worth deciding before any of it, because they are not mechanical:

**The RPC durability model.** Every plane's control loop is `serve` on a
subject, and the promise that an unserved request waits is what makes attach,
deploy and backfill recoverable. A work-queue stream gives that, at the cost
of a per-subject consumer where today there is a key that springs into
existence on first `RPUSH` — and the subjects are per-session
(`sts.control.{session_id}`) and per-account (`td.order.{api_id}`), so there
are as many of them as there are live sessions and attached accounts.

**`redis_url`.** `BrokerConfig` still names it, `from_env` still reads
`REDIS_URL`, and both are read only inside the broker. Renaming them is a
deployment-visible change — `.env.example`, the compose files, the
`mftik node-init` templates, `justfile` — so it belongs with the swap rather
than ahead of it.

## What has not changed

Key strings. A lease name is the whole tail under the prefix, so
`{prefix}:sts:alive:{session}` is byte-for-byte what MD and STS were renewing
before there was a `lease_put` to renew it through. A rolling upgrade in which
half the fleet asked a different key whether a session was running would have
been two answers to the one question that must not have two.
