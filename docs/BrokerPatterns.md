# What the system actually asks a broker for

This is the enumeration of what the interface is asked to do — counted from
call sites rather than recalled from the design — and what landed when those
seven patterns were made native.

A, B, F and G are the families [`docs/JetStreamRemoval.md`](JetStreamRemoval.md)
retires: the log ring, the per-feed tape, KV leases/counters, and KV state.
C and E stay (E counts three missed intervals both ways; the timeout
stays, because `subscribe` does not wake on silence). Until that
work lands, the table below is what the tree calls.

The first pass counted ten patterns. Three of those were one pattern under
different names, so the real answer is seven — and which three collapsed says
more than the number does.

A, B, C and F were already NATS primitives. The rest is done:

- **A** has its own log stream. `publish_log` no longer purges.
- **B / F** cache the KV bucket status. Tape coverage is a durable put.
- **D** is gone. `post` and the work-queue stream are deleted; `serve` is
  core NATS. The four callers use `request`, or they do not ask.
- **E** is `LeasedSessionLink`. MD and TD no longer write the loop twice.
- **G** is KV watch plus a local `StateProjection`. The three design
  questions are answered below.

## How this was counted

Two interfaces, and telling them apart is what makes the rest of this document
short:

- **`BrokerTransport`** (`transport/base.py`, 32 abstract methods) — one
  implementation and one caller, `client.py`. Since #84 removed the second
  implementation, this is an internal seam rather than an interface. Changing
  it is invisible to every domain.
- **`Broker`** (`client.py`) — what `sts`, `md`, `td`, `sym`, `api` and the
  strategy runtime see.

`test_broker_is_the_only_transport.py` enforces the separation: nothing under
an `src` tree may import `redis` or `nats`, touch `.js`/`.nc`, or read
`.key_prefix`. So the surface below is the whole surface.

Counting broker-qualified calls across `apps/*/src` and `packages/*/src`:

| | |
|---|---|
| Files touching the broker at all | 37 — common 12, api 9, td 6, md 5, sts 3, sym 1, paper 1 |
| Files touching `state_*` / `lease_*` / `counter_*` / `tape_*` | **7** |

The seven: `apps/md/tape.py`, `apps/sts/session/manager.py`,
`apps/td/backfill/executor.py`, `apps/td/session/session.py`,
`packages/common/liveness.py`, `packages/common/strategy/tape.py`,
`packages/common/strategy/ledger.py`.

## Seven patterns

The first pass of this document counted ten. Three of them were the same
pattern wearing different names, and saying so is worth more than the count:

| | Pattern | Broker surface | Where NATS stands |
|---|---|---|---|
| **A** | Subject log — fan-out with a bounded replay tail | `publish` / `subscribe` / `psubscribe` / `publish_log` / `fetch_log_buffer` | Native. Two streams: `{prefix}.ps.>` and `{prefix}.log.>`. |
| **B** | Keyed log — one stream per feed | `tape_append` / `tape_tail` / `tape_trim_before` | Native. "Newest N" is sequence arithmetic because a feed owns its stream. Coverage is a durable KV put. |
| **C** | Request / response | `request` / `probe` / `serve` | Native. Core request-reply; no-responders answers a request to nobody at once rather than at the timeout. |
| **D** | *(removed)* | — | The work-queue stream is gone. Nobody asked for at-least-once delivery. |
| **E** | Fenced session link | `LeasedSessionLink` / `broker.leased_link` | Native transport, one abstraction. Today: MD grace 3s, TD grace 5s, sibling watchdog. Destination ([`JetStreamRemoval.md`](JetStreamRemoval.md)): three missed intervals both ways; timeout kept (`wait_for` or the sibling timer); interval on the heartbeat or as a constant. |
| **F** | Atomic register | `lease_*`, `counter_next`, all of `liveness.py` | KV with per-message TTL and compare-and-set on revision. Signatures stay apart from G. Bucket status is cached. |
| **G** | Shared mutable state | `state_*`, `state_watch`, `StateProjection` | Key-per-field, push via KV watch, last-written cache on the writer. |

A and C are the traffic — roughly half of all broker calls in the tree are
`publish`, `subscribe`, `request`, `probe` and `serve`. There was nothing in
them to make native. They already were.

### What the merges were

**Fan-out and "fan-out with a replay tail" are one pattern (A).** They used
to share `{prefix}.ps.>` and a purge after every log line. They now share
the *pattern* and not the stream: `publish` stays on `.ps.>`; `publish_log`
writes `{prefix}.log.>` with `max_msgs_per_subject = LOG_MAX_MSGS_PER_SUBJECT`.
`subscribe` / `psubscribe` listen on the log stream only for `log.` /
`status.` topics, so a live log subscriber does not need a second call
and a lease or a feed does not pay a second consumer. The difference is
still a retention argument, not a second pattern. `fetch_log_buffer`
trims the replay to `BROKER_LOG_BUFFER_MAXLEN` (or the passed `maxlen`).

**Liveness was never a pattern (folded into F).** `packages/common/liveness.py`
makes eight broker calls and **all eight are `lease_*`**: `mark_alive` is
`lease_put`, `claim_alive` is `lease_take`, `is_alive` is `lease_held`,
`claim_owner` / `hold_owner` / `release_owner` are `lease_take` / `lease_hold` /
`lease_release`. It is not built on leases; it is leases under another set of
names. Listing it separately was an error in the first pass.

**The counter is a register too (folded into F).** A lease is a compare-and-set
owner with a TTL; a counter is a compare-and-set integer. One primitive, two
value types. `counter_next` also has exactly one caller —
`apps/sts/session/manager.py:296`, allocating a cid slot.

### What was left unmerged, and why

**A and B are the same NATS mechanism.** A stream with limits, read through a
consumer with a start policy. Merging them honestly gives six patterns rather
than seven.

They are kept apart on lifecycle, not on mechanism. A tape stream is created
per feed at runtime, carries retention the caller hands it, and is read by
sequence arithmetic for the newest N; the fan-out stream is one stream, ensured
once, read as "everything this subject still holds". One interface over both
grows a parameter for each of those differences, which is what merging too far
looks like. Worth deciding deliberately rather than by default.

**F and G share a substrate and must not share a signature.** Both are KV, and
that is the whole temptation. But F wants compare-and-set on one key with a
TTL, and G wants a map of keys with a watch. An interface serving both is a
lowest common denominator, which is the exact mistake this document exists to
undo — it is how `state_*` became a Redis hash in the first place. Share the
implementation; keep the surfaces apart.

### One thing that is not a merge

E's fencing token does not come from F. `apps/sts/session/session.py:646` is
`self._token += 1` — an in-process counter, not `counter_next`. So the token
detects a stale message from the *same* session and nothing more.

Today, what stops a second process claiming the same session is the lease
(F, `claim_alive`). [`JetStreamRemoval.md`](JetStreamRemoval.md) withdraws
that: one instance name is one process, every STS row is stamped with the
accepting instance, and E's miss count is the only remaining liveness on
an already-attached link. A same-name second process is refused at boot
if `probe` gets a responder; two that pass `probe` together still both
look green.

### D is gone

`post` was at-least-once delivery: the ask outlived a plane that was not up
yet. Every caller of it said, in its own comment, that it did not need that.

| Caller | What it said the backstop is | What it does now |
|---|---|---|
| `apps/api/backfill_cron.py` | the cron itself — the next tick asks again | `request` with a 5s timeout; no-responders is a log line |
| `apps/td/backfill/trigger.py` (session detach) | "none of them is the reason the record eventually settles" | short-timeout `request` while still serving; otherwise the cron |
| `apps/td/app.py` (TD shutdown) | the same | deleted — TD has already stopped serving; the successor looks at the cursor |
| `apps/sts/session/session.py` | "**The lease covers this.**" | short-timeout `request`; the lease still tears the attach down |
| `apps/api/orchestrate.py` | "**The lease covers this.**" | short-timeout `request` on rollback |

Two backstops between them, and neither is the broker: the backfill cursor —
a Postgres row (`BackfillCursorRow`), advanced *during* a walk rather than at
the end of one — and the liveness lease, which is F.

`BackfillSession` acks as soon as it accepts the walk (`reason="accepted"`)
and runs it in the background, so the cron learns whether that instance is
there and whether the account is already running. Losing a reply is not
losing work: the cursor already moved as far as the walk got.

What left with D: the work-queue stream, `_pump_posted`, and the two-source
merge inside `serve`. `serve` is a core NATS queue-group subscription. The
stop-event handover in `_iter_until_stopped` stays — it is still how a
cancelled loop does not swallow a message it has already taken.

Failure behaviour is "known failed immediately, backstop handles it".

### E is `LeasedSessionLink`

The session fencing lease between STS and both MD and TD is what decides that
a strategy session has gone and its feeds and its account link should be torn
down. Both domains used to build it by hand.

`packages/common/src/mftik/broker/link.py` is that loop once: subscribe `rx`,
ack on `tx`, echo the fencing token, expire when the grace window lapses,
resubscribe after a transport failure, and hand every other envelope to
`on_message`. Expiry and an unexpected exit run on a sibling task so they
cannot cancel the loop from inside itself. [`JetStreamRemoval.md`](JetStreamRemoval.md)
keeps a timeout — the receive loop does not wake when STS dies —
and counts three missed intervals both ways. STS applies the same
rule to TD that it already applies to MD. The interval is on
`LeaseHeartbeat` or is a protocol constant.

| | MD | TD |
|---|---|---|
| inbound | `Topics.sts_md_session(sid)` | `Topics.sts_td_session(sid)` |
| outbound | `Topics.md_session(sid)` | `Topics.td_session(api_id, sid)` |
| grace | 3s | 5s — not unified; the two planes do not fail the same way |
| unexpected exit | `detach(reason="lease_loop_died")` | the same |

`bistream` / `BidirectionalStream` / `stream.py` are deleted. STS still
heartbeats with `publish`; it was never a consumer of the link.

### A has its own log stream

`publish_log` used to publish onto the fan-out stream and then purge the
subject to `keep=maxlen`. Two bounds, one stream, so the smaller one was
enforced by hand on every line.

Logs now own `{prefix}.log.>` with `max_msgs_per_subject =
LOG_MAX_MSGS_PER_SUBJECT` (256). That covers both the default log buffer and
the status ring STS and the API ask for (`_STATUS_BUFFER` = 200). The server
holds the ring; the purge is gone.

## G is push, still key-per-field

`state_*` was a Redis hash with the serial numbers filed off. One KV key per
field, `state_all` reassembling a hash, `state_replace` emulating a `MULTI`.
The write side is unchanged: TD still `state_replace`s the book. The read
side is not.

Three questions, now answered:

1. **The unit stays key-per-field.** TD already has single-field
   `write_order` / `write_ledger`. A key per *name* would turn those into a
   read-modify-write on the order path, which is the cost the projection was
   meant to remove from the *reader*, not add to the writer.
2. **A broken watch is reopened.** The watch itself is one-shot. The
   projection reseeds from `state_all` on an interval and when the watch
   ends, so a purged delete marker is not mistaken for a live field. A
   hard failure clears `live` so readers fall back to a pull rather than
   a frozen map. Gaps are not reconstructed from history; they are
   overwritten by that snapshot.
3. **A name has one writer.** `_state_lock`'s docstring already asserted
   this. The transport caches the last-written field set, so `state_replace`
   no longer reads before it writes.

`state_drop` / `state_clear` delete each live field so a watcher sees it
go, then purge the delete marker so `_kv_scan` does not transfer one extra
subject per historical order. A key that is already gone is not deleted
again.

STS starts a `StateProjection` per attached `td.oms.{id}` and
`td.ledger.{id}` on session start, and closes them on stop.
`view` / `orders` / `balances` read the local map when it is live, and
fall back to `state_all` when it is not. A single-field read after a
write (`oms.order`, ledger `available` / `free` / `prelock`) always
`state_get`s that field so it does not race the watch.

Bucket status is cached (`_kv_status`). That is the raft read `state_all`
used to pay on every call.

## What this change touches

Most of the thirty-seven files still use only A, B, C and F, and those
signatures did not move. First-classing the patterns is *not* renaming
`publish` to `broker.a.publish`.

The integration surface:

- **D** — four callers, plus TD's `BackfillSession` ack, plus the tests that
  used to assert work-queue backlog.
- **E** — `apps/md/session/manager.py` and `apps/td/session/manager.py`.
  Small, and the path a fencing bug breaks quietly.
- **G** — `strategy/oms.py`, `strategy/ledger.py`, STS session start/stop.

`test_broker_is_the_only_transport.py` still keeps the seam: nothing under
an `src` tree may import `nats` or reach through `.js` / `.nc`.
