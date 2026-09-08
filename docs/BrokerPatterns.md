# What the system actually asks a broker for

Before redefining the broker's interface around NATS, this is the enumeration
of what the interface is currently asked to do — counted from call sites rather
than recalled from the design.

The conclusion is narrower than expected, and in one place the count is not the
interesting part.

The first pass counted ten patterns. Three of those were one pattern under
different names, so the real answer is seven — and which three collapsed says
more than the number does.

Five of the seven are already NATS' own primitives, or close enough that a
rewrite would move them sideways. Three are worth acting on, for three
different reasons:

- **G** is still shaped like a Redis hash and sits on every strategy's read
  path.
- **E** is native at the transport and has no shared abstraction at all, so two
  domains wrote the same one twice.
- **D** should not exist. All four of its callers document a backstop outside
  the broker, so at-least-once delivery is buying something none of them asked
  for — and it is the most expensive pattern here to keep.

## How this was counted

Two interfaces, and telling them apart is what makes the rest of this document
short:

- **`BrokerTransport`** (`transport/base.py`, 32 abstract methods) — one
  implementation and one caller, `client.py`. Since #84 removes the second
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
| **A** | Subject log — fan-out with a bounded replay tail | `publish` / `subscribe` / `psubscribe` / `publish_log` / `fetch_log_buffer` | Native. One stream, per-subject bounds. |
| **B** | Keyed log — one stream per feed | `tape_append` / `tape_tail` / `tape_trim_before` | Native. "Newest N" is sequence arithmetic because a feed owns its stream. |
| **C** | Request / response | `request` / `probe` | Native. Core request-reply; no-responders answers a request to nobody at once rather than at the timeout. |
| **D** | Durable work queue | `post` / `serve` | Native — and **removable**. No caller needs it. See below. |
| **E** | Fenced session link | hand-rolled from `subscribe` + `publish` | Native transport, **no shared abstraction**. Built twice. |
| **F** | Atomic register | `lease_*`, `counter_next`, all of `liveness.py` | KV with per-message TTL and compare-and-set on revision. Better than the Redis original, which documented losing a race this one wins. |
| **G** | **Shared mutable state** | `state_*` | **The one still shaped like a Redis hash.** |

A, C and D are the traffic — roughly half of all broker calls in the tree are
`publish`, `subscribe`, `request`, `probe`, `post` and `serve`. There is nothing
in them to make native. They already are.

### What the merges were

**Fan-out and "fan-out with a replay tail" are one pattern (A).** `publish`
(`nats.py:481`) and `publish_log` (`nats.py:569`) resolve the same
`_fanout_subject` onto the same `_fanout_stream`. Every plain publish already
has a replay tail — `max_msgs_per_subject` is 256 — and nobody reads it. The
difference is a TTL header and a tighter ring, which is a retention argument,
not a second pattern.

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
detects a stale message from the *same* session and nothing more; what stops a
second process claiming the same session is the lease, which is F. The two are
related, but not in the way a shared token would make them.

### D has four callers and none of them wants it

`post` is at-least-once delivery: the ask outlives a plane that is not up yet.
Every caller of it says, in its own comment, that it does not need that.

| Caller | What it says the backstop is |
|---|---|
| `apps/api/backfill_cron.py:80` | the cron itself — "a failed sweep is logged and the loop goes on: the next tick asks again" |
| `apps/td/backfill/trigger.py:65` (session detach, and TD shutdown) | "Best-effort by design… **none of them is the reason the record eventually settles**" |
| `apps/sts/session/session.py:610` | "**The lease covers this.** Worth a line because a broker that cannot take a write is a problem in its own right, not because the attach is now stuck" |
| `apps/api/orchestrate.py:409` | "**The lease covers this:** MD tears the attach down when this session's heartbeat stops, which failing it is about to do" |

Two backstops between them, and neither is the broker: the backfill cursor —
a Postgres row (`BackfillCursorRow`), advanced *during* a walk rather than at
the end of one — and the liveness lease, which is F.

The transport agrees about how little is on offer. `_pump_posted` acknowledges
each message as it hands it over, not after the handler is done, and says why:
"the durability this buys is 'nobody was serving the subject yet' … and not
'the process died half way through the work', which neither transport has ever
offered." A guarantee that narrow is worth one cron interval of latency, and
the cron interval is fifteen minutes.

**The backfill cron is the clean first move, and TD needs no change at all.**
`BackfillSession` already builds a full `TdBackfillResult` and calls
`req.reply` with it (`backfill/session.py:192`); on the posted path that is a
silent no-op, because `_pump_posted` hands work over with no reply address on
purpose. Point a `request` at the same subject and the reply that is being
built and discarded today arrives. The cron gains what it does not have now —
whether TD is there at all, and whether that account's walk is still running —
and loses nothing, because losing a reply is not losing work: the cursor
already moved as far as the walk got.

**The shutdown caller is the one that cannot be an RPC**, and it does not need
to be one. TD asks *after* it has stopped serving the subject, so by
construction nothing is listening; an RPC there always fails. The queue exists
to carry that ask to whichever TD comes up next. But the successor could look
instead of being told — which accounts have cursors behind the settlement line
is a database question — and looking is strictly better, because it does not
depend on the predecessor having managed to post before it died. The cron's own
docstring already lists that failure: "a process may die before it asks".

**What removing D takes with it.** The work-queue stream, `_pump_posted`
entirely, and the two-source merge inside `serve` — which is the whole stage on
which #82 played out: the handover ordering that `_iter_until_stopped` has to
get right exists because posted work is acknowledged on delivery and can land
behind the stop sentinel. With one source that complexity has nothing to
describe.

The durable consumer per served subject is **not** on that list, and an earlier
draft of this document was wrong to put it there. `_pump_posted` gives it
`inactive_threshold=consumer_idle_seconds`, and since NATS 2.9 that reaps
durables as well as ephemerals — this node's floor is 2.11 and CI runs
`nats:2.11-alpine`. A durable for a subject nobody serves any more is gone five
minutes later. `docs/Broker.md` says exactly this in its `consumer_idle_seconds`
row, and says why: it "stops a node that has churned a thousand sessions from
carrying a thousand consumers."

What does survive the correction is about messages rather than consumers. The
work-queue stream is created with no `max_msgs`, `max_age` or `max_bytes`, and
work-queue retention removes a message only when it is acknowledged. So a
subject that is posted to and never served accumulates with nothing to stop it,
and reaping the idle durable does not help — it removes the reader, not the
backlog. That is a defect in the stream's configuration and is worth fixing on
its own, whether or not D survives: a `max_age` gives unserved work an end
without contradicting the intent `_ensure_post_stream` documents, which is that
work waits rather than expires. It just makes "waits" finite.

**What it costs, stated honestly.** Failure behaviour changes from "delivered
eventually" to "known failed immediately, backstop handles it". Four comments
assert the backstops hold. That is intent, not observation, and which of them
actually fires is the kind of thing only production answers — see the
sequencing note at the end.

### E is the pattern the broker named and then under-specified

The session fencing lease between STS and both MD and TD *is* a bidirectional
stream, and it is as load-bearing as anything here: it is what decides that a
strategy session has gone and its feeds and its account link should be torn
down.

It does not go through `broker.bistream()`. Both domains build it by hand, and
they build the same thing:

| | MD (`apps/md/session/manager.py:625`) | TD (`apps/td/session/manager.py:784`) |
|---|---|---|
| inbound | `subscribe(Topics.sts_md_session(sid))` | `subscribe(Topics.sts_td_session(sid))` |
| outbound | `publish(Topics.md_session(sid))` | `publish(Topics.td_session(api_id, sid))` |
| fencing token | `link.last_token = hb.token`, echoed in `MdLeaseAck` | the same, in `TD_LEASE_ACK` |
| grace | `_watch_timeout` at 0.5s against `LEASE_GRACE_S` (5.0) | the same |
| expiry action | `detach(reason="lease_expired")` | `detach(reason="lease_expired")` |

That is exactly the shape `bistream_pair` offers — a named pair of up and down
topics, one side subscribing and the other publishing — written twice by hand,
with a `StsLink` dataclass, a watchdog and a token each time.

So the reading is not "delete the unused API". It is that **the broker
identified this pattern and then offered too little of it to be worth using.**
`BidirectionalStream` carries envelopes in two directions and stops there. What
the two call sites needed on top of it, and therefore wrote themselves, is:

- a **fencing token** echoed back on the ack, so a stale writer is detectable;
- a **liveness grace**, separate from the transport's own connection state,
  because a peer that stopped heartbeating has gone even though the subject is
  still there;
- an **expiry action**, since noticing is not the point — detaching is.

That is a leased session link, not a byte pipe. A native redesign should either
build that primitive once — F (`lease_*`) already has the token and TTL
half of it, on a KV key rather than over a link — or delete `bistream` and say
in the interface that this pattern belongs to the domains. What it should not
do is carry `BidirectionalStream` across unchanged: the two call sites that
needed this pattern both looked at it and wrote their own instead.

The duplication is worth pricing on its own: `StsLink`, `_lease_loop`,
`_watch_timeout` and `LEASE_GRACE_S` exist twice, in two apps, and a fix to the
fencing logic has to be made in both.

### A costs one round trip more than it needs to

`publish_log` publishes and then purges the subject to `keep=maxlen`, because
the fan-out stream's `max_msgs_per_subject` is `FANOUT_MAX_MSGS_PER_SUBJECT`
(256) while a log ring is `log_buffer_maxlen` (100). Two bounds, one stream, so
the smaller one is enforced by hand on every line.

Giving logs their own stream with `max_msgs_per_subject = log_buffer_maxlen`
lets the server hold the ring and the purge goes. In practice there is one
value: `client.py:489` passes `config.log_buffer_maxlen` unless a caller
overrides, and none does.

## G, which is the actual subject

`state_*` is a Redis hash with the serial numbers filed off. One KV key per
field, `state_all` reassembling a hash, `state_replace` emulating a `MULTI`.
Everything that made it awkward follows from that choice: no cross-key
transaction so `state_put_many` is not a snapshot; a lock in-process to restore
"last issued wins"; a scan to find out which fields to drop.

**What it costs today.** `state_all` is five round trips and a JetStream
consumer created and destroyed: `bucket.status()`, `stream_info` for the
subject counts, `pull_subscribe`, `fetch`, `delete_consumer`. In a clustered
NATS, creating a consumer is a raft operation.

And it is on the read path of every strategy:

- `strategy/ledger.py:93` — `LedgerView.view()` calls `state_all` per access.
  A strategy asking for a balance pays the whole thing.
- `strategy/oms.py:87` — `OmsView.view()`, the same, for the order book.

**What the write side looks like.** `Session.publish_oms`
(`apps/td/session/session.py:508`) is named "publish" and publishes nothing: it
is a `state_replace`. TD writes the book; STS re-reads it when it next wants to
know. Write-then-poll, because a Redis hash has no other mode.

**What native looks like.** NATS KV has `watch`, `watchall` and `history` —
verified present in the pinned nats-py. A state model designed for it is push,
not pull: TD writes, and STS is told. The strategy's `view()` reads a locally
maintained projection instead of paying five round trips to rebuild a hash the
writer already had in memory.

That is not a tidier spelling of the same thing. It is a different cost
structure for the domain that reads most, and it is unreachable from a hash.

Three questions the design has to answer, none of which this document settles:

- **Ordering and gaps.** A watch that misses an update because a client
  reconnected has to resynchronise. KV history gives a revision per key; what
  the projection does with a gap is the real design.
- **The unit.** Key-per-field is what makes a single-field write cheap and a
  multi-field write non-atomic. A key per *name* holding the whole record
  inverts both. With a watch feeding a projection, the read cost that motivated
  key-per-field partly goes away, so this is worth re-deciding rather than
  inheriting.
- **Who may write.** `_state_lock`'s docstring asserts a state name has exactly
  one writer, the session that owns that account. If that holds, the transport
  can cache the field set it last wrote and `state_replace` stops reading
  before it writes. If it does not hold, the lock is already insufficient and
  that is a bug independent of any redesign.

## What a redesign does not touch

Most of the thirty-seven files use only A, B, C and D. Those are already
NATS primitives, and neither redesign reaches them.

The integration surface for **G** is the seven files listed above, and
the two strategy-facing views inside them — `strategy/ledger.py` and
`strategy/oms.py` — are where the behaviour actually changes.

The integration surface for **E** is two files, both called
`session/manager.py`, one in `apps/md` and one in `apps/td`. It is small, but
unlike G it is a change to how a session is torn down, which is the
path a bad change breaks quietly: a fencing bug does not fail a request, it
lets two writers believe they own the same feed.

This is the part worth stating plainly, because "redefine the interface and
integrate it into sts/md/td/sym/api" sounds like thirty-seven files and is
nine.

## Sequencing, and why not now

The expensive half of this work is already done. #77 and #80 pushed the store's
vocabulary out of the domains and put a test in front of it. What is left is
not a big-bang; the seam has been bought and paid for.

What is missing is the thing that should shape the design: **no part of this
system has run in production.** Four defects surfaced in review on 2026-09-08
(#82, #83), every one of them invisible until a node had been up a while. An
interface designed today is designed from round-trip counts — including every
number in this document. An interface designed after two weeks of real traffic
is designed from where the time actually goes.

Deferring costs almost nothing here, which is unusual enough to be worth using:
`test_broker_is_the_only_transport.py` keeps the seam from rotting while it
waits.

So:

1. **#84** — remove the Redis transport. Frees `base.py` from lowest-common-
   denominator semantics; prerequisite either way.
2. **Round-trip work behind `client.py`** — cache the bucket status that is
   re-fetched per call, keep the last-written field set so `state_replace`
   stops reading first, move tape coverage off its TTL renewal, give logs their
   own stream. No domain file changes.
3. **Production, and two weeks of it.**
4. **Then** redesign G against measurements, with those seven files as
   the integration surface.

**D splits across that line rather than sitting on one side of it.** Pointing
the backfill cron at `request` instead of `post` belongs in step 2: TD needs no
change, the reply is already built, and the cron gains an answer it does not
have today. Deleting the pattern outright belongs after step 3, because what
changes is failure behaviour and the four backstops are asserted rather than
observed. Running the cron on RPC through a production window is what turns
them into observations — no-responders and lease expiry either fire as the
comments claim or they do not, and either way the answer arrives before
anything irreversible is deleted.

Step 4 is what produces the clean version. Doing it before step 3 produces a
clean version of a guess.

**E does not have to wait for step 3** either, and that is the other place this
sequencing bends. It needs no measurement — the argument for it is that the
same fencing logic exists twice and a fix has to be made in both — and the
duplication is a live correctness risk rather than a cost. It is also the one
piece that production would make *harder* to change rather than easier, since
teardown is what a deploy exercises. If anything here should happen before the
first deploy, it is this and not G.
